"""从 rosbag2 导出通用格式的数据资产。"""

from __future__ import annotations

import argparse
import importlib
import datetime as _dt
import io
import logging
import math
import shutil
import struct
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import yaml
from PIL import Image
from sensor_msgs.msg import CompressedImage, Image as RosImage, PointCloud2, PointField

from ros2_bag_utils.common import ConfigError, ensure_logging_configured, load_yaml_config, resolve_output_uri

logger = logging.getLogger(__name__)


_LZF_MODULE = None

_INTENSITY_FIELD_ALIASES: Set[str] = {'intensity', 'i'}
_TIME_FIELD_ALIASES: Set[str] = {'time', 't', 'timestamp', 'time_stamp', 'relative_time'}


KNOWN_LIDAR_TYPES: Set[str] = {'sensor_msgs/msg/PointCloud2'}
KNOWN_CAMERA_TYPES: Set[str] = {
	'sensor_msgs/msg/Image',
	'sensor_msgs/msg/CompressedImage',
}
TF_STATIC_TOPIC = '/tf_static'
TF_STATIC_TYPE = 'tf2_msgs/msg/TFMessage'


@dataclass(frozen=True)
class ExportConfig:
	"""最终的导出配置。"""

	input_bag: Path
	output_root: Path
	base_frame: str
	lidar_topics: List[str]
	camera_topics: List[str]
	lidar_subdir: str
	camera_subdir: str
	calib_subdir: str
	include_tf_static: bool
	overwrite: bool
	filename_style: str
	filename_time_source: str
	filename_time_format: str
	pointcloud_format: str
	pcd_format: str


def _normalize_topic_list(raw: object) -> List[str]:
	if raw is None:
		return []
	if isinstance(raw, dict):
		raise ConfigError('topics 配置必须是数组格式')
	if isinstance(raw, str):
		raise ConfigError('topics 配置不应是字符串')
	if not isinstance(raw, Iterable):
		raise ConfigError('topics 配置必须是可迭代对象')
	topics: List[str] = []
	for item in raw:
		topic = str(item).strip()
		if not topic:
			raise ConfigError('topics 列表中不允许出现空字符串')
		if topic not in topics:
			topics.append(topic)
	return topics


def _expand_topics_from_cli(cli_topics: Iterable[str]) -> List[str]:
	return _normalize_topic_list(list(cli_topics))


def _sanitize_frame_id(frame_id: str) -> str:
	frame = frame_id.strip()
	if frame.startswith('/'):
		frame = frame[1:]
	return frame


def _safe_path_component(name: str) -> str:
	sanitized = name.strip()
	if sanitized.startswith('/'):
		sanitized = sanitized[1:]
	replacements = {"/": "_", "\\": "_", " ": "_"}
	for src, dst in replacements.items():
		sanitized = sanitized.replace(src, dst)
	sanitized = ''.join(ch if ch.isalnum() or ch in {'_', '-', '.'} else '_' for ch in sanitized)
	return sanitized or 'unknown'


def _resolve_subdir(raw: Optional[object], default: str) -> str:
	if raw is None:
		return default
	value = str(raw).strip()
	if not value:
		raise ConfigError('子目录名称不能为空')
	return value


def _resolve_bool(raw: Optional[object], default: bool) -> bool:
	if raw is None:
		return default
	if isinstance(raw, bool):
		return raw
	if isinstance(raw, str):
		lower = raw.strip().lower()
		if lower in {'true', '1', 'yes', 'on'}:
			return True
		if lower in {'false', '0', 'no', 'off'}:
			return False
	raise ConfigError(f'无法解析布尔值: {raw!r}')


def _resolve_time_format(raw: Optional[object]) -> str:
	if raw is None:
		return '%Y%m%dT%H%M%S_%f'
	value = str(raw).strip()
	if not value:
		raise ConfigError('filename_time_format 不能为空')
	return value


def _resolve_filename_style(raw: Optional[object]) -> str:
	if raw is None:
		return 'ns'
	style = str(raw).strip().lower()
	if style not in {'ns', 'datetime'}:
		raise ConfigError("filename_style 仅支持 'ns' 或 'datetime'")
	return style


def _resolve_time_source(raw: Optional[object]) -> str:
	if raw is None:
		return 'header'
	source = str(raw).strip().lower()
	if source not in {'header', 'bag'}:
		raise ConfigError("filename_time_source 仅支持 'header' 或 'bag'")
	return source


def _resolve_pointcloud_format(raw: Optional[object]) -> str:
	if raw is None:
		return 'auto'
	value = str(raw).strip().lower()
	if value not in {'auto', 'xyz', 'xyzi', 'xyzit'}:
		raise ConfigError("pointcloud_format 仅支持 'auto','xyz','xyzi' 或 'xyzit'")
	return value


def _resolve_pcd_format(raw: Optional[object]) -> str:
	if raw is None:
		return 'uncompressed'
	value = str(raw).strip().lower()
	if value not in {'uncompressed', 'ascii', 'compressed'}:
		raise ConfigError("pcd_format 仅支持 'uncompressed','ascii' 或 'compressed'")
	return value


def _format_filename_token(timestamp_ns: int, *, style: str, time_format: str) -> str:
	if style == 'ns':
		return f'{timestamp_ns:019d}'
	if style == 'datetime':
		formatted = _format_timestamp(timestamp_ns, time_format)
		return _safe_path_component(formatted)
	raise RuntimeError(f'不支持的 filename_style: {style}')


def _extract_header_timestamp_ns(message: object) -> Optional[int]:
	header = getattr(message, 'header', None)
	if header is None:
		return None
	stamp = getattr(header, 'stamp', None)
	if stamp is None:
		return None
	sec = getattr(stamp, 'sec', None)
	nanosec = getattr(stamp, 'nanosec', None)
	if sec is None or nanosec is None:
		return None
	try:
		sec_int = int(sec)
		nano_int = int(nanosec)
	except (TypeError, ValueError):
		return None
	total = sec_int * 1_000_000_000 + nano_int
	return total if total > 0 else None


def _select_timestamp_ns(
	*,
	config: ExportConfig,
	header_ns: Optional[int],
	bag_ns: int,
	topic_name: str,
	warning_cache: Set[str],
) -> int:
	if config.filename_time_source == 'header':
		if header_ns is not None:
			return header_ns
		if topic_name not in warning_cache:
			logger.warning("topic '%s' 的 header.stamp 为空，将使用 bag 时间戳", topic_name)
			warning_cache.add(topic_name)
		return bag_ns
	if config.filename_time_source == 'bag':
		return bag_ns
	raise RuntimeError(f"未知的 filename_time_source: {config.filename_time_source}")


def _build_unique_filename(
	*,
	directory: Path,
	base_name: str,
	extension: str,
	counters: Dict[Tuple[str, str], int],
) -> str:
	key = (str(directory), base_name)
	count = counters[key]
	if count:
		filename = f"{base_name}_{count}{extension}"
	else:
		filename = f"{base_name}{extension}"
	counters[key] = count + 1
	return filename


class _NoopProgress:
	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc, exc_tb):  # noqa: D401 - context manager
		return False

	def add_task(self, *args, **kwargs):  # noqa: D401 - noop
		return None

	def advance(self, task_id, advance=1):  # noqa: D401 - noop
		return None

	def update(self, task_id, *args, **kwargs):  # noqa: D401 - noop
		return None

	def stop(self):  # noqa: D401 - noop
		return None


def _get_lzf_module():
	global _LZF_MODULE
	if _LZF_MODULE is None:
		try:
			_LZF_MODULE = importlib.import_module('lzf')
		except ImportError as exc:  # pragma: no cover - 依赖外部库
			raise RuntimeError(
				'写出 PCD 需要 python-lzf 包，请先安装 (pip install python-lzf 或 sudo apt install python3-lzf)',
			) from exc
	return _LZF_MODULE


def _compress_lzf(data: bytes) -> bytes:
	if not data:
		return b''
	module = _get_lzf_module()
	max_length = len(data) + max(32, len(data) // 8)
	compressed = module.compress(data, max_length)
	if compressed is None:
		raise RuntimeError('LZF 压缩失败，返回空结果')
	return compressed


def _build_structured_dtype(message: PointCloud2) -> np.dtype:
	if not message.fields:
		raise RuntimeError('PointCloud2 消息缺少字段描述，无法导出')
	names: List[str] = []
	formats: List[object] = []
	offsets: List[int] = []
	for field in sorted(message.fields, key=lambda item: item.offset):
		_, element_size, base_dtype = _pointfield_type_info(field.datatype)
		# 对于 count>1 的字段，NumPy 需要形状描述
		if field.count and field.count > 1:
			formats.append((base_dtype, field.count))
		else:
			formats.append(base_dtype)
		names.append(field.name)
		offsets.append(field.offset)
	return np.dtype({'names': names, 'formats': formats, 'offsets': offsets, 'itemsize': message.point_step})


def _find_field_name(dtype: np.dtype, aliases: Set[str]) -> Optional[str]:
	if dtype.names is None:
		return None
	alias_lower = {alias.lower() for alias in aliases}
	for name in dtype.names:
		if name.lower() in alias_lower:
			return name
	return None


def _resolve_required_field(dtype: np.dtype, canonical: str) -> str:
	if dtype.names is None:
		raise RuntimeError('PointCloud2 dtype 缺少任何字段')
	for name in dtype.names:
		if name.lower() == canonical:
			return name
	raise RuntimeError(f"PointCloud2 消息缺少 '{canonical}' 字段，无法写出 PCD")


def _extract_scalar_column(data: np.ndarray, field_name: str) -> np.ndarray:
	values = data[field_name]
	array = np.asarray(values)
	if array.ndim == 1:
		return array.astype(np.float32, copy=False)
	if array.ndim == 2:
		if array.shape[1] != 1:
			raise RuntimeError(f"字段 '{field_name}' 的 count={array.shape[1]} 暂不支持导出")
		return array[:, 0].astype(np.float32, copy=False)
	raise RuntimeError(f"字段 '{field_name}' 的数据维度 {array.ndim} 暂不支持导出")


def _create_progress():
	try:
		progress_module = importlib.import_module('rich.progress')
		console_module = importlib.import_module('rich.console')
	except ImportError:  # pragma: no cover - fallback when rich is absent
		logger.info('rich 未安装，进度条输出将被禁用')
		return _NoopProgress()

	BarColumn = getattr(progress_module, 'BarColumn')
	ProgressCls = getattr(progress_module, 'Progress')
	TaskProgressColumn = getattr(progress_module, 'TaskProgressColumn')
	TimeElapsedColumn = getattr(progress_module, 'TimeElapsedColumn')
	TimeRemainingColumn = getattr(progress_module, 'TimeRemainingColumn')
	ConsoleCls = getattr(console_module, 'Console')
	console = ConsoleCls(stderr=True, force_terminal=True)

	return ProgressCls(
		"[progress.description]{task.description}",
		BarColumn(),
		TaskProgressColumn(),
		TimeElapsedColumn(),
		TimeRemainingColumn(),
		console=console,
		transient=False,
		disable=False,
		redirect_stdout=False,
		redirect_stderr=False,
	)


def build_effective_config(
	yaml_config: Dict[str, object],
	cli_input_bag: Optional[str],
	cli_output_root: Optional[str],
	cli_base_frame: Optional[str],
	cli_lidar_topics: Iterable[str],
	cli_camera_topics: Iterable[str],
	*,
	cli_disable_tf_static: bool,
	cli_overwrite: bool,
	cli_lidar_subdir: Optional[str],
	cli_camera_subdir: Optional[str],
	cli_calib_subdir: Optional[str],
	cli_filename_style: Optional[str],
	cli_time_source: Optional[str],
	cli_time_format: Optional[str],
	cli_pointcloud_format: Optional[str],
	cli_pcd_format: Optional[str],
) -> ExportConfig:
	input_bag_str = cli_input_bag or yaml_config.get('input_bag')
	if not input_bag_str:
		raise ConfigError('必须指定 input_bag')

	output_root_str = cli_output_root or yaml_config.get('output_root')

	base_frame_raw = cli_base_frame or yaml_config.get('base_frame') or 'base_link'
	base_frame = _sanitize_frame_id(str(base_frame_raw))
	if not base_frame:
		raise ConfigError('base_frame 不能为空')

	lidar_section_raw = yaml_config.get('lidar')
	lidar_section = lidar_section_raw if isinstance(lidar_section_raw, dict) else None
	lidar_topics_yaml = _normalize_topic_list((lidar_section.get('topics') if lidar_section else yaml_config.get('lidar_topics')))
	lidar_topics_cli = _expand_topics_from_cli(cli_lidar_topics)

	lidar_topics: List[str] = []
	for topic in lidar_topics_yaml + lidar_topics_cli:
		if topic not in lidar_topics:
			lidar_topics.append(topic)

	camera_section_raw = yaml_config.get('camera')
	camera_section = camera_section_raw if isinstance(camera_section_raw, dict) else None
	camera_topics_yaml = _normalize_topic_list((camera_section.get('topics') if camera_section else yaml_config.get('camera_topics')))
	camera_topics_cli = _expand_topics_from_cli(cli_camera_topics)

	camera_topics: List[str] = []
	for topic in camera_topics_yaml + camera_topics_cli:
		if topic not in camera_topics:
			camera_topics.append(topic)

	tf_section_raw = yaml_config.get('tf_static')
	tf_section = tf_section_raw if isinstance(tf_section_raw, dict) else None
	include_tf_static_yaml = _resolve_bool(
		tf_section.get('enabled') if tf_section else yaml_config.get('tf_static_enabled'),
		True,
	)
	include_tf_static = include_tf_static_yaml and not cli_disable_tf_static

	lidar_subdir = _resolve_subdir(
		cli_lidar_subdir
		or (yaml_config.get('lidar_subdir') if yaml_config.get('lidar_subdir') else (lidar_section.get('output_subdir') if lidar_section else None)),
		'lidar',
	)
	camera_subdir = _resolve_subdir(
		cli_camera_subdir
		or (yaml_config.get('camera_subdir') if yaml_config.get('camera_subdir') else (camera_section.get('output_subdir') if camera_section else None)),
		'camera',
	)
	calib_subdir = _resolve_subdir(
		cli_calib_subdir
		or (yaml_config.get('calib_subdir') if yaml_config.get('calib_subdir') else (tf_section.get('output_subdir') if tf_section else None)),
		'calib',
	)

	filename_style = _resolve_filename_style(cli_filename_style or yaml_config.get('filename_style'))
	filename_time_source = _resolve_time_source(cli_time_source or yaml_config.get('filename_time_source'))
	time_format_raw = cli_time_format or yaml_config.get('filename_time_format')
	if filename_style == 'datetime':
		filename_time_format = _resolve_time_format(time_format_raw)
	else:
		filename_time_format = _resolve_time_format(time_format_raw) if time_format_raw else '%Y%m%dT%H%M%S_%f'

	input_bag = Path(str(input_bag_str)).expanduser().resolve()
	if not input_bag.exists():
		raise ConfigError(f'输入 bag {input_bag} 不存在')

	output_root = (
		Path(str(output_root_str)).expanduser().resolve()
		if output_root_str
		else resolve_output_uri(input_bag, None, suffix='_exported')
	)

	if input_bag == output_root:
		raise ConfigError('输出目录不能与输入目录相同')

	overwrite_yaml = _resolve_bool(yaml_config.get('overwrite'), False)
	overwrite = overwrite_yaml or cli_overwrite

	return ExportConfig(
		input_bag=input_bag,
		output_root=output_root,
		base_frame=base_frame,
		lidar_topics=lidar_topics,
		camera_topics=camera_topics,
		lidar_subdir=lidar_subdir,
		camera_subdir=camera_subdir,
		calib_subdir=calib_subdir,
		include_tf_static=include_tf_static,
		overwrite=overwrite,
		filename_style=filename_style,
		filename_time_source=filename_time_source,
		filename_time_format=filename_time_format,
		pointcloud_format=_resolve_pointcloud_format(cli_pointcloud_format or (lidar_section.get('output_format') if lidar_section else yaml_config.get('pointcloud_format'))),
		pcd_format=_resolve_pcd_format(cli_pcd_format or (lidar_section.get('output_pcd_format') if lidar_section else yaml_config.get('pcd_format'))),
	)


def _import_rosbag2():
	try:
		import rosbag2_py
	except ImportError as exc:  # pragma: no cover - 依赖 ROS 环境
		raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
	return rosbag2_py


def _format_timestamp(timestamp_ns: int, format_str: str) -> str:
	seconds = timestamp_ns / 1e9
	dt = _dt.datetime.utcfromtimestamp(seconds)
	microsec = int(round(timestamp_ns % 1_000_000_000 / 1000.0))
	dt = dt.replace(microsecond=microsec)
	return dt.strftime(format_str)


def _pointfield_type_info(datatype: int) -> Tuple[str, int, np.dtype]:
	if datatype == PointField.INT8:
		return 'I', 1, np.dtype('<i1')
	if datatype == PointField.UINT8:
		return 'U', 1, np.dtype('<u1')
	if datatype == PointField.INT16:
		return 'I', 2, np.dtype('<i2')
	if datatype == PointField.UINT16:
		return 'U', 2, np.dtype('<u2')
	if datatype == PointField.INT32:
		return 'I', 4, np.dtype('<i4')
	if datatype == PointField.UINT32:
		return 'U', 4, np.dtype('<u4')
	if datatype == PointField.FLOAT32:
		return 'F', 4, np.dtype('<f4')
	if datatype == PointField.FLOAT64:
		return 'F', 8, np.dtype('<f8')
	raise RuntimeError(f'不支持的 PointField datatype: {datatype}')


def _write_pointcloud_binary_compressed(
	message: PointCloud2,
	output_path: Path,
	*,
	format_pref: Optional[str] = None,
	pcd_format: Optional[str] = None,
) -> None:
	if message.is_bigendian:
		raise RuntimeError('暂不支持 big-endian PointCloud2 数据')

	num_points = int(message.width) * int(message.height)
	point_step = int(message.point_step)

	if num_points == 0:
		raw_buffer = b''
	else:
		expected_size = point_step * num_points
		raw_buffer = bytes(message.data)
		if len(raw_buffer) < expected_size:
			raise RuntimeError('PointCloud2 数据长度小于预期，无法写出 PCD')
		raw_buffer = raw_buffer[:expected_size]

	dtype = _build_structured_dtype(message)
	structured = np.frombuffer(raw_buffer, dtype=dtype, count=num_points)

	x_field = _resolve_required_field(dtype, 'x')
	y_field = _resolve_required_field(dtype, 'y')
	z_field = _resolve_required_field(dtype, 'z')
	intensity_field = _find_field_name(dtype, _INTENSITY_FIELD_ALIASES)
	time_field = _find_field_name(dtype, _TIME_FIELD_ALIASES)

	x_values = _extract_scalar_column(structured, x_field)
	y_values = _extract_scalar_column(structured, y_field)
	z_values = _extract_scalar_column(structured, z_field)

	if intensity_field is not None:
		intensity_values = _extract_scalar_column(structured, intensity_field)
	else:
		intensity_values = np.zeros(num_points, dtype=np.float32)
		logger.debug('PointCloud2 消息缺少 intensity 字段，输出 PCD 将以 0 填充')

	if time_field is not None:
		time_values = _extract_scalar_column(structured, time_field)
	else:
		time_values = None

	# Determine desired output layout based on format preference
	# format_pref: 'auto'|'xyz'|'xyzi'|'xyzit' or None -> auto
	pref = (format_pref or 'auto').lower()
	if pref == 'auto':
		include_intensity = True
		include_time = time_values is not None
	elif pref == 'xyz':
		include_intensity = False
		include_time = False
	elif pref == 'xyzi':
		include_intensity = True
		include_time = False
	elif pref == 'xyzit':
		include_intensity = True
		include_time = True
	else:
		raise RuntimeError(f'未知的 pointcloud format: {format_pref}')

	output_fields = [('x', '<f4'), ('y', '<f4'), ('z', '<f4')]
	if include_intensity:
		output_fields.append(('intensity', '<f4'))
	if include_time:
		output_fields.append(('time', '<f4'))

	output_dtype = np.dtype(output_fields)
	output_array = np.empty(num_points, dtype=output_dtype)
	output_array['x'] = x_values.astype(np.float32, copy=False)
	output_array['y'] = y_values.astype(np.float32, copy=False)
	output_array['z'] = z_values.astype(np.float32, copy=False)
	if include_intensity:
		output_array['intensity'] = intensity_values.astype(np.float32, copy=False)
	if include_time:
		# If time_values is None but format requested time, fill with 0
		if time_values is None:
			output_array['time'] = np.zeros(num_points, dtype=np.float32)
		else:
			output_array['time'] = time_values.astype(np.float32, copy=False)

	raw_data = output_array.tobytes()

	# Determine desired PCD container format
	pcd_fmt = (pcd_format or 'uncompressed').lower()

	# Build header fields from actual output_fields to ensure header matches payload
	field_names = [name for name, _ in output_fields]
	field_count = len(field_names)
	offsets_line = ' '.join(str(index * 4) for index in range(field_count))

	headers = [
		'# .PCD v0.7 - Point Cloud Data file format',
		'VERSION 0.7',
		'FIELDS ' + ' '.join(field_names),
		'SIZE ' + ' '.join('4' for _ in field_names),
		'TYPE ' + ' '.join('F' for _ in field_names),
		'COUNT ' + ' '.join('1' for _ in field_names),
		'OFFSET ' + offsets_line,
		f'WIDTH {message.width}',
		f'HEIGHT {message.height}',
		'VIEWPOINT 0 0 0 1 0 0 0',
		f'POINTS {num_points}',
	]

	output_path.parent.mkdir(parents=True, exist_ok=True)

	if pcd_fmt == 'ascii':
		headers.append('DATA ascii')
		headers_blob = ('\n'.join(headers) + '\n').encode('ascii')
		with output_path.open('wb') as fh:
			fh.write(headers_blob)
			for point in output_array:
				line = ' '.join(str(float(point[name])) for name in field_names) + '\n'
				fh.write(line.encode('ascii'))
		return

	if pcd_fmt == 'uncompressed':
		headers.append('DATA binary')
		headers_blob = ('\n'.join(headers) + '\n').encode('ascii')
		body = raw_data
		output_path.write_bytes(headers_blob + body)
		return

	if pcd_fmt == 'compressed':
		headers.append('DATA binary_compressed')
		headers_blob = ('\n'.join(headers) + '\n').encode('ascii')
		uncompressed_size = len(raw_data)
		compressed_data = _compress_lzf(raw_data)
		body = struct.pack('<II', len(compressed_data), uncompressed_size) + compressed_data
		output_path.write_bytes(headers_blob + body)
		return

	raise RuntimeError(f'未知的 pcd_format: {pcd_format}')


def _image_message_to_numpy(msg: RosImage) -> Tuple[np.ndarray, str]:
	encoding = msg.encoding.strip().lower()
	data = msg.data
	if not data:
		raise RuntimeError('图像消息数据为空')

	if encoding in {'mono8', '8uc1'}:
		dtype = np.uint8
		channels = 1
		mode = 'L'
	elif encoding in {'mono16', '16uc1', '16sc1'}:
		dtype = np.uint16
		channels = 1
		mode = 'I;16'
	elif encoding in {'bgr8', 'rgb8'}:
		dtype = np.uint8
		channels = 3
		mode = 'RGB'
	elif encoding in {'bgra8', 'rgba8'}:
		dtype = np.uint8
		channels = 4
		mode = 'RGBA'
	else:
		raise RuntimeError(f'暂不支持的图像编码: {msg.encoding}')

	row_stride = msg.step // np.dtype(dtype).itemsize
	array = np.frombuffer(data, dtype=dtype)
	if array.size != msg.height * row_stride:
		raise RuntimeError('图像数据尺寸与 step/height 不匹配')
	array = array.reshape((msg.height, row_stride))

	if channels > 1:
		array = array.reshape((msg.height, row_stride // channels, channels))
		array = array[:, : msg.width, :]
	else:
		array = array[:, : msg.width]

	if encoding.startswith('bgr'):
		array = array[:, :, ::-1]
	elif encoding.startswith('bgra'):
		array = array[:, :, [2, 1, 0, 3]]

	return array.copy(), mode


def _ros_image_to_pil(msg: RosImage) -> Image.Image:
	array, mode = _image_message_to_numpy(msg)
	return Image.fromarray(array, mode=mode)


def _compressed_image_to_pil(msg: CompressedImage) -> Image.Image:
	if not msg.data:
		raise RuntimeError('压缩图像数据为空')
	buffer = io.BytesIO(msg.data)
	with Image.open(buffer) as im:
		converted = im.convert('RGB') if im.mode not in {'RGB', 'RGBA', 'L', 'I;16'} else im.copy()
	return converted


@dataclass
class _StaticTransform:
	parent: str
	child: str
	translation: np.ndarray
	quaternion: np.ndarray  # xyzw


def _normalize_tf_frame(frame: str) -> str:
	return _sanitize_frame_id(frame)


def _quaternion_to_matrix(q: np.ndarray) -> np.ndarray:
	qx, qy, qz, qw = q
	norm = math.sqrt(qx * qx + qy * qy + qz * qz + qw * qw)
	if norm == 0:
		raise RuntimeError('四元数范数为 0')
	qx /= norm
	qy /= norm
	qz /= norm
	qw /= norm

	xx = qx * qx
	yy = qy * qy
	zz = qz * qz
	xy = qx * qy
	xz = qx * qz
	yz = qy * qz
	xw = qx * qw
	yw = qy * qw
	zw = qz * qw

	return np.array(
		[
			[1 - 2 * (yy + zz), 2 * (xy - zw), 2 * (xz + yw), 0],
			[2 * (xy + zw), 1 - 2 * (xx + zz), 2 * (yz - xw), 0],
			[2 * (xz - yw), 2 * (yz + xw), 1 - 2 * (xx + yy), 0],
			[0, 0, 0, 1],
		],
		dtype=np.float64,
	)


def _matrix_to_quaternion(matrix: np.ndarray) -> np.ndarray:
	trace = matrix[0, 0] + matrix[1, 1] + matrix[2, 2]
	if trace > 0:
		s = math.sqrt(trace + 1.0) * 2
		qw = 0.25 * s
		qx = (matrix[2, 1] - matrix[1, 2]) / s
		qy = (matrix[0, 2] - matrix[2, 0]) / s
		qz = (matrix[1, 0] - matrix[0, 1]) / s
	elif matrix[0, 0] > matrix[1, 1] and matrix[0, 0] > matrix[2, 2]:
		s = math.sqrt(1.0 + matrix[0, 0] - matrix[1, 1] - matrix[2, 2]) * 2
		qw = (matrix[2, 1] - matrix[1, 2]) / s
		qx = 0.25 * s
		qy = (matrix[0, 1] + matrix[1, 0]) / s
		qz = (matrix[0, 2] + matrix[2, 0]) / s
	elif matrix[1, 1] > matrix[2, 2]:
		s = math.sqrt(1.0 + matrix[1, 1] - matrix[0, 0] - matrix[2, 2]) * 2
		qw = (matrix[0, 2] - matrix[2, 0]) / s
		qx = (matrix[0, 1] + matrix[1, 0]) / s
		qy = 0.25 * s
		qz = (matrix[1, 2] + matrix[2, 1]) / s
	else:
		s = math.sqrt(1.0 + matrix[2, 2] - matrix[0, 0] - matrix[1, 1]) * 2
		qw = (matrix[1, 0] - matrix[0, 1]) / s
		qx = (matrix[0, 2] + matrix[2, 0]) / s
		qy = (matrix[1, 2] + matrix[2, 1]) / s
		qz = 0.25 * s

	quat = np.array([qx, qy, qz, qw], dtype=np.float64)
	quat /= np.linalg.norm(quat)
	return quat


def _compose_static_transforms(registry: Dict[str, _StaticTransform], base_frame: str) -> Dict[str, np.ndarray]:
	parents = {transform.parent for transform in registry.values()}
	children = set(registry.keys())
	if base_frame not in parents and base_frame not in children:
		raise RuntimeError(f"tf_static 中找不到基座坐标系 '{base_frame}'，无法生成外参")

	graph: Dict[str, List[str]] = {}
	for child, transform in registry.items():
		graph.setdefault(transform.parent, []).append(child)

	result: Dict[str, np.ndarray] = {base_frame: np.identity(4)}
	queue: deque[str] = deque([base_frame])

	while queue:
		parent = queue.popleft()
		for child in graph.get(parent, []):
			if child in result:
				continue
			transform = registry[child]
			translation = transform.translation
			matrix = _quaternion_to_matrix(transform.quaternion)
			matrix[:3, 3] = translation
			result[child] = result[parent] @ matrix
			queue.append(child)

	return result


def _save_extrinsics(
	config: ExportConfig,
	registry: Dict[str, _StaticTransform],
	frames_to_export: Dict[str, str],
) -> int:
	if not config.include_tf_static:
		return 0

	if not registry:
		logger.warning('未从 /tf_static 中获取到任何静态变换，跳过外参写入')
		return 0

	composed = _compose_static_transforms(registry, config.base_frame)
	calib_dir = config.output_root / config.calib_subdir
	calib_dir.mkdir(parents=True, exist_ok=True)

	written = 0
	for frame_id, original in frames_to_export.items():
		if frame_id == config.base_frame:
			continue
		if frame_id not in composed:
			logger.warning("frame '%s' 无法通过 tf_static 转换到 '%s'，跳过外参导出", frame_id, config.base_frame)
			continue
		matrix = composed[frame_id]
		translation = matrix[:3, 3]
		rotation = _matrix_to_quaternion(matrix[:3, :3])

		file_name = f"{_safe_path_component(original)}_extrinsics.yaml"
		file_path = calib_dir / file_name
		payload = {
			'parent_frame': config.base_frame,
			'child_frame': original,
			'transform': {
				'translation': {
					'x': float(translation[0]),
					'y': float(translation[1]),
					'z': float(translation[2]),
				},
				'rotation_quaternion': {
					'x': float(rotation[0]),
					'y': float(rotation[1]),
					'z': float(rotation[2]),
					'w': float(rotation[3]),
				},
			},
			'meta': {
				'source_topic': TF_STATIC_TOPIC,
				'exported_at': _dt.datetime.utcnow().isoformat() + 'Z',
			},
		}
		file_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))
		written += 1
	return written


def export_bag_assets(config: ExportConfig) -> None:
	rosbag2_py = _import_rosbag2()
	from rclpy.serialization import deserialize_message
	from rosidl_runtime_py.utilities import get_message

	if not config.input_bag.exists():
		raise ConfigError(f'输入 bag 路径 {config.input_bag} 不存在')

	if config.output_root.exists():
		if config.overwrite:
			shutil.rmtree(config.output_root)
		else:
			raise ConfigError(f'输出目录 {config.output_root} 已存在，如需覆盖请使用 --overwrite')

	config.output_root.mkdir(parents=True, exist_ok=True)

	info = rosbag2_py.Info()
	metadata = info.read_metadata(str(config.input_bag), '')
	storage_id = metadata.storage_identifier

	topics_info = metadata.topics_with_message_count
	topics_by_name = {topic.topic_metadata.name: topic.topic_metadata for topic in topics_info}

	available_topics = set(topics_by_name.keys())

	def _validate_topics(requested: Sequence[str], allowed_types: Set[str], kind: str) -> Dict[str, str]:
		selection: Dict[str, str] = {}
		missing = sorted(set(requested) - available_topics)
		if missing:
			raise ConfigError(f'{kind} topic 不存在于 bag: {", ".join(missing)}')
		for name in requested:
			topic_type = topics_by_name[name].type
			if topic_type not in allowed_types:
				raise ConfigError(f"topic '{name}' 类型为 {topic_type}，不是预期的 {kind} 类型")
			selection[name] = topic_type
		return selection

	if config.lidar_topics:
		lidar_topics = _validate_topics(config.lidar_topics, KNOWN_LIDAR_TYPES, '激光雷达')
	else:
		lidar_topics = {
			meta.name: meta.type
			for meta in topics_by_name.values()
			if meta.type in KNOWN_LIDAR_TYPES
		}
		if not lidar_topics:
			logger.warning('未在 bag 中检测到 PointCloud2 topic，跳过点云导出')

	if config.camera_topics:
		camera_topics = _validate_topics(config.camera_topics, KNOWN_CAMERA_TYPES, '相机')
	else:
		camera_topics = {
			meta.name: meta.type
			for meta in topics_by_name.values()
			if meta.type in KNOWN_CAMERA_TYPES
		}
		if not camera_topics:
			logger.warning('未在 bag 中检测到图像 topic，跳过图像导出')

	tf_static_present = TF_STATIC_TOPIC in topics_by_name and topics_by_name[TF_STATIC_TOPIC].type == TF_STATIC_TYPE
	if config.include_tf_static and not tf_static_present:
		logger.warning('bag 中没有 /tf_static，无法导出外参')

	message_counts = {topic.topic_metadata.name: topic.message_count for topic in topics_info}
	lidar_total = sum(message_counts.get(name, 0) for name in lidar_topics)
	camera_total = sum(message_counts.get(name, 0) for name in camera_topics)

	progress = _create_progress()
	filename_counters: Dict[Tuple[str, str], int] = defaultdict(int)
	missing_header_topics: Set[str] = set()

	type_cache: Dict[str, object] = {}
	frame_lookup: Dict[str, str] = {}
	lidar_count = 0
	camera_count = 0
	static_transforms: Dict[str, _StaticTransform] = {}

	converter_options = rosbag2_py.ConverterOptions('', '')
	storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)
	reader = rosbag2_py.SequentialReader()
	reader.open(storage_options, converter_options)

	with progress:
		lidar_task = progress.add_task('点云', total=lidar_total or None) if getattr(progress, 'add_task', None) else None
		camera_task = progress.add_task('图像', total=camera_total or None) if getattr(progress, 'add_task', None) else None

		while reader.has_next():
			topic_name, serialized, timestamp = reader.read_next()
			if topic_name in lidar_topics:
				topic_type = lidar_topics[topic_name]
				if topic_type not in type_cache:
					type_cache[topic_type] = get_message(topic_type)
				msg_type = type_cache[topic_type]
				message = deserialize_message(serialized, msg_type)
				if not isinstance(message, PointCloud2):
					raise RuntimeError(f"topic '{topic_name}' 的消息类型不是 PointCloud2")
				if lidar_task is not None:
					progress.advance(lidar_task, 1)
				frame_id = _sanitize_frame_id(message.header.frame_id)
				if not frame_id:
					logger.warning("topic '%s' 的 PointCloud2 消息缺少 frame_id，已跳过", topic_name)
					continue
				header_ns = _extract_header_timestamp_ns(message)
				effective_ns = _select_timestamp_ns(
					config=config,
					header_ns=header_ns,
					bag_ns=timestamp,
					topic_name=topic_name,
					warning_cache=missing_header_topics,
				)
				token = _format_filename_token(
					effective_ns,
					style=config.filename_style,
					time_format=config.filename_time_format,
				)
				frame_lookup.setdefault(frame_id, frame_id)
				file_dir = config.output_root / config.lidar_subdir / _safe_path_component(frame_id)
				file_name = _build_unique_filename(
					directory=file_dir,
					base_name=token,
					extension='.pcd',
					counters=filename_counters,
				)
				_write_pointcloud_binary_compressed(
					message,
					file_dir / file_name,
					format_pref=config.pointcloud_format,
					pcd_format=config.pcd_format,
				)
				lidar_count += 1
			elif topic_name in camera_topics:
				topic_type = camera_topics[topic_name]
				if topic_type not in type_cache:
					type_cache[topic_type] = get_message(topic_type)
				msg_type = type_cache[topic_type]
				message = deserialize_message(serialized, msg_type)
				if camera_task is not None:
					progress.advance(camera_task, 1)
				if topic_type == 'sensor_msgs/msg/Image':
					if not isinstance(message, RosImage):
						raise RuntimeError(f"topic '{topic_name}' 的消息类型不是 Image")
					image = _ros_image_to_pil(message)
					frame_id = _sanitize_frame_id(message.header.frame_id)
				elif topic_type == 'sensor_msgs/msg/CompressedImage':
					if not isinstance(message, CompressedImage):
						raise RuntimeError(f"topic '{topic_name}' 的消息类型不是 CompressedImage")
					image = _compressed_image_to_pil(message)
					frame_id = _sanitize_frame_id(message.header.frame_id)
				else:
					raise RuntimeError(f'未知的相机消息类型: {topic_type}')
				if not frame_id:
					logger.warning("topic '%s' 的图像消息缺少 frame_id，已跳过", topic_name)
					continue
				header_ns = _extract_header_timestamp_ns(message)
				effective_ns = _select_timestamp_ns(
					config=config,
					header_ns=header_ns,
					bag_ns=timestamp,
					topic_name=topic_name,
					warning_cache=missing_header_topics,
				)
				token = _format_filename_token(
					effective_ns,
					style=config.filename_style,
					time_format=config.filename_time_format,
				)
				frame_lookup.setdefault(frame_id, frame_id)
				file_dir = config.output_root / config.camera_subdir / _safe_path_component(frame_id)
				file_name = _build_unique_filename(
					directory=file_dir,
					base_name=token,
					extension='.png',
					counters=filename_counters,
				)
				file_dir.mkdir(parents=True, exist_ok=True)
				image.save(file_dir / file_name, format='PNG')
				camera_count += 1
			elif config.include_tf_static and topic_name == TF_STATIC_TOPIC:
				if TF_STATIC_TYPE not in type_cache:
					type_cache[TF_STATIC_TYPE] = get_message(TF_STATIC_TYPE)
				message = deserialize_message(serialized, type_cache[TF_STATIC_TYPE])
				for transform in message.transforms:
					parent = _normalize_tf_frame(transform.header.frame_id)
					child = _normalize_tf_frame(transform.child_frame_id)
					translation = np.array(
						[
							transform.transform.translation.x,
							transform.transform.translation.y,
							transform.transform.translation.z,
						],
						dtype=np.float64,
					)
					quaternion = np.array(
						[
							transform.transform.rotation.x,
							transform.transform.rotation.y,
							transform.transform.rotation.z,
							transform.transform.rotation.w,
						],
						dtype=np.float64,
					)
					if not child:
						continue
					static_transforms[child] = _StaticTransform(parent=parent, child=child, translation=translation, quaternion=quaternion)

	logger.info('导出完成: 点云 %d 帧, 图像 %d 帧', lidar_count, camera_count)

	if config.include_tf_static:
		written = _save_extrinsics(config, static_transforms, frame_lookup)
		if written:
			logger.info('写出 %d 个外参文件', written)


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description='将 rosbag2 中的点云、图像与外参导出为通用格式。')
	parser.add_argument('--config', type=str, help='YAML 配置文件路径')
	parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
	parser.add_argument('--output-root', type=str, help='导出的根目录')
	parser.add_argument('--base-frame', type=str, help='外参统一到的基准坐标系，默认 base_link')
	parser.add_argument('--lidar-topic', action='append', default=[], help='指定需要导出的激光雷达 topic，可重复')
	parser.add_argument('--camera-topic', action='append', default=[], help='指定需要导出的相机 topic，可重复')
	parser.add_argument('--lidar-subdir', type=str, help='点云输出子目录，默认 lidar')
	parser.add_argument('--camera-subdir', type=str, help='图像输出子目录，默认 camera')
	parser.add_argument('--calib-subdir', type=str, help='外参输出子目录，默认 calib')
	parser.add_argument('--filename-style', type=str, choices=['ns', 'datetime'], help='文件名时间格式，ns 为纳秒整数，datetime 使用 strftime 格式')
	parser.add_argument('--filename-time-source', type=str, choices=['header', 'bag'], help='文件名时间来源，header 或 bag')
	parser.add_argument('--filename-time-format', type=str, help='当文件名样式为 datetime 时使用的 strftime 格式，默认 %Y%m%dT%H%M%S_%f')
	parser.add_argument('--pointcloud-format', type=str, choices=['auto', 'xyz', 'xyzi', 'xyzit'], help='点云导出格式，auto/xyz/xyzi/xyzit，默认为 auto')
	parser.add_argument('--pcd-format', type=str, choices=['uncompressed', 'ascii', 'compressed'], help='生成的 PCD 存储格式：uncompressed/ascii/compressed，默认 uncompressed')
	parser.add_argument('--disable-tf-static', action='store_true', help='禁用 tf_static 外参导出')
	parser.add_argument('--overwrite', action='store_true', help='如果输出目录已存在则删除重建')
	return parser


def main(argv: Optional[List[str]] = None) -> int:
	ensure_logging_configured()
	parser = build_arg_parser()
	args = parser.parse_args(argv)

	try:
		yaml_config = load_yaml_config(Path(args.config).expanduser().resolve()) if args.config else {}
		config = build_effective_config(
			yaml_config=yaml_config,
			cli_input_bag=args.input_bag,
			cli_output_root=args.output_root,
			cli_base_frame=args.base_frame,
			cli_lidar_topics=args.lidar_topic,
			cli_camera_topics=args.camera_topic,
			cli_disable_tf_static=args.disable_tf_static,
			cli_overwrite=args.overwrite,
			cli_lidar_subdir=args.lidar_subdir,
			cli_camera_subdir=args.camera_subdir,
			cli_calib_subdir=args.calib_subdir,
			cli_filename_style=args.filename_style,
			cli_time_source=args.filename_time_source,
			cli_time_format=args.filename_time_format,
			cli_pointcloud_format=args.pointcloud_format,
			cli_pcd_format=args.pcd_format,
		)
		export_bag_assets(config)
	except ConfigError as exc:
		logger.error('%s', exc)
		return 2
	except Exception as exc:  # noqa: BLE001
		logger.exception('执行失败: %s', exc)
		return 1

	return 0


if __name__ == '__main__':  # pragma: no cover
	sys.exit(main())