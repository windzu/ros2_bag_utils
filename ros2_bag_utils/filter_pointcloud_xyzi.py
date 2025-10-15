"""Tools for rewriting PointCloud2 messages to contain only XYZI fields."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
from sensor_msgs.msg import PointCloud2, PointField
from sensor_msgs_py import point_cloud2 as pc2

from ros2_bag_utils.common import (
	ConfigError,
	ensure_logging_configured,
	load_yaml_config,
	resolve_output_uri,
)

logger = logging.getLogger(__name__)


REQUIRED_FIELDS: List[str] = ['x', 'y', 'z', 'intensity']
XYZI_FIELDS: List[PointField] = [
	PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
	PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
	PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
	PointField(name='intensity', offset=12, datatype=PointField.FLOAT32, count=1),
]
XYZI_POINT_STEP = 4 * 4  # 4 fields * sizeof(float32)


@dataclass(frozen=True)
class FilterXYZIConfig:
	"""Resolved configuration for XYZI filtering."""

	input_bag: Path
	output_bag: Path
	topics: List[str]


def _normalize_topic_list(raw: Optional[Iterable[object]]) -> List[str]:
	if raw is None:
		return []
	if isinstance(raw, dict):
		raise ConfigError('topics 配置必须是列表而非字典')
	normalized: List[str] = []
	for item in raw:
		topic = str(item).strip()
		if not topic:
			raise ConfigError('topics 列表中不允许出现空字符串')
		if topic not in normalized:
			normalized.append(topic)
	return normalized


def build_effective_config(
	yaml_config: Dict[str, object],
	cli_input_bag: Optional[str],
	cli_output_bag: Optional[str],
	cli_topics: Iterable[str],
) -> FilterXYZIConfig:
	input_bag_str = cli_input_bag or yaml_config.get('input_bag')
	if not input_bag_str:
		raise ConfigError('必须指定 input_bag')

	output_bag_str = cli_output_bag or yaml_config.get('output_bag')

	yaml_topics_raw = yaml_config.get('topics')
	if yaml_topics_raw is not None and not isinstance(yaml_topics_raw, list):
		raise ConfigError('配置中的 topics 必须是列表')
	yaml_topics = _normalize_topic_list(yaml_topics_raw)

	cli_topics_normalized = _normalize_topic_list(cli_topics)

	topics: List[str] = []
	for topic in yaml_topics + cli_topics_normalized:
		if topic not in topics:
			topics.append(topic)

	if not topics:
		raise ConfigError('至少需要一个待处理的 topic')

	input_bag = Path(str(input_bag_str)).expanduser().resolve()
	output_bag = (
		Path(str(output_bag_str)).expanduser().resolve()
		if output_bag_str
		else resolve_output_uri(input_bag, None, suffix='_xyzi_filtered')
	)

	if input_bag == output_bag:
		raise ConfigError('输出路径不能与输入路径相同')

	return FilterXYZIConfig(input_bag=input_bag, output_bag=output_bag, topics=topics)


def _import_rosbag2():
	try:
		import rosbag2_py
	except ImportError as exc:  # pragma: no cover - depends on ROS environment
		raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
	return rosbag2_py


def _validate_topic_metadata(topic: str, metadata) -> None:
	if metadata.type != 'sensor_msgs/msg/PointCloud2':
		raise ConfigError(f"topic '{topic}' 类型为 {metadata.type}，不是 PointCloud2")


def _ensure_required_fields(message: PointCloud2, topic: str) -> Dict[str, PointField]:
	field_map: Dict[str, PointField] = {field.name: field for field in message.fields}

	missing = [name for name in REQUIRED_FIELDS if name not in field_map]
	if missing:
		raise RuntimeError(
			f"topic '{topic}' 的 PointCloud2 缺少字段: {', '.join(missing)}"
		)

	for name in REQUIRED_FIELDS:
		field = field_map[name]
		if field.datatype != PointField.FLOAT32 or field.count != 1:
			raise RuntimeError(
				f"topic '{topic}' 的字段 '{name}' 必须是单个 float32，但实际为 datatype={field.datatype}, count={field.count}"
			)

	return field_map


def _filter_pointcloud_to_xyzi(message: PointCloud2, topic: str) -> PointCloud2:
	_ensure_required_fields(message, topic)

	has_mixed_datatypes = any(field.datatype != PointField.FLOAT32 for field in message.fields)

	points_array: Optional[np.ndarray] = None
	if not has_mixed_datatypes:
		try:
			points_array = pc2.read_points_numpy(
				message,
				field_names=REQUIRED_FIELDS,
				skip_nans=False,
			)
		except (AttributeError, AssertionError, ValueError):
			points_array = None

	if points_array is None:
		# sensor_msgs_py 新旧版本在以下场景可能抛出异常：
		# 1. 旧版本缺少 read_points_numpy 方法
		# 2. 消息存在非 float32 字段，read_points_numpy 会抛出 AssertionError
		# 3. 其他数值解析错误
		rows = list(pc2.read_points(message, field_names=REQUIRED_FIELDS, skip_nans=False))
		if rows:
			normalized_rows = [tuple(float(value) for value in row) for row in rows]
			points_array = np.asarray(normalized_rows, dtype=np.float32)
		else:
			points_array = np.empty((0, len(REQUIRED_FIELDS)), dtype=np.float32)

	if points_array.ndim == 3:
		points_array = points_array.reshape(-1, points_array.shape[-1])

	if points_array.dtype != np.float32:
		try:
			points_array = points_array.astype(np.float32, copy=False)
		except TypeError as exc:  # pragma: no cover - defensive
			raise RuntimeError(f"topic '{topic}' 的字段无法转换为 float32") from exc

	expected_points = message.width * message.height
	if expected_points != points_array.shape[0]:
		# 在 skip_nans=False 情况下应相等，否则视为不可恢复的格式问题
		raise RuntimeError(
			f"topic '{topic}' 的点数量与 PointCloud2 元信息不一致: header={expected_points}, 实际={points_array.shape[0]}"
		)

	filtered = PointCloud2()
	filtered.header = message.header
	filtered.height = message.height
	filtered.width = message.width
	filtered.fields = XYZI_FIELDS
	filtered.is_bigendian = message.is_bigendian
	filtered.point_step = XYZI_POINT_STEP
	filtered.row_step = XYZI_POINT_STEP * message.width
	filtered.is_dense = message.is_dense

	if expected_points == 0:
		filtered.data = b''
	else:
		filtered.data = points_array.reshape(-1).astype(np.float32, copy=False).tobytes()

	return filtered


def filter_pointclouds(config: FilterXYZIConfig) -> None:
	rosbag2_py = _import_rosbag2()
	from rclpy.serialization import deserialize_message, serialize_message
	from rosidl_runtime_py.utilities import get_message

	if not config.input_bag.exists():
		raise ConfigError(f"输入 bag 路径 {config.input_bag} 不存在")
	if config.output_bag.exists():
		raise ConfigError(f"输出路径 {config.output_bag} 已存在，请先删除或指定其他目录")

	info = rosbag2_py.Info()
	metadata = info.read_metadata(str(config.input_bag), '')
	storage_id = metadata.storage_identifier

	topics_info = metadata.topics_with_message_count
	topics_by_name: Dict[str, Any] = {}
	message_counts: Dict[str, int] = {}
	for topic_with_count in topics_info:
		topic_meta = topic_with_count.topic_metadata
		topics_by_name[topic_meta.name] = topic_meta
		message_counts[topic_meta.name] = topic_with_count.message_count

	missing_topics = sorted(set(config.topics) - set(topics_by_name))
	if missing_topics:
		raise ConfigError(f"配置中的 topic 不存在于 bag: {', '.join(missing_topics)}")

	empty_topics = [name for name in config.topics if message_counts.get(name, 0) == 0]
	if empty_topics:
		raise ConfigError(f"以下 topic 在 bag 中没有消息: {', '.join(empty_topics)}")

	for topic in config.topics:
		_validate_topic_metadata(topic, topics_by_name[topic])

	converter_options = rosbag2_py.ConverterOptions('', '')
	reader_storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)
	reader = rosbag2_py.SequentialReader()
	reader.open(reader_storage_options, converter_options)

	config.output_bag.parent.mkdir(parents=True, exist_ok=True)
	writer = rosbag2_py.SequentialWriter()
	writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
	writer_converter_options = rosbag2_py.ConverterOptions('', '')
	writer.open(writer_storage_options, writer_converter_options)

	for topic_meta in topics_by_name.values():
		writer.create_topic(topic_meta)

	type_cache: Dict[str, object] = {}
	processed_messages = {topic: 0 for topic in config.topics}
	target_topics = set(config.topics)

	while reader.has_next():
		topic_name, data, timestamp = reader.read_next()
		if topic_name in target_topics:
			if topic_name not in type_cache:
				try:
					type_cache[topic_name] = get_message(topics_by_name[topic_name].type)
				except (AttributeError, ModuleNotFoundError, ValueError) as exc:
					raise RuntimeError(
						f"无法加载消息类型 '{topics_by_name[topic_name].type}' 用于 topic '{topic_name}'"
					) from exc
			msg_type = type_cache[topic_name]
			message = deserialize_message(data, msg_type)
			if not isinstance(message, PointCloud2):
				raise RuntimeError(f"topic '{topic_name}' 的消息类型不是 PointCloud2")
			filtered_message = _filter_pointcloud_to_xyzi(message, topic_name)
			data = serialize_message(filtered_message)
			processed_messages[topic_name] += 1
		writer.write(topic_name, data, timestamp)

	incomplete = [topic for topic, count in processed_messages.items() if count == 0]
	if incomplete:
		raise RuntimeError(f"未能处理以下 topic 的任何消息: {', '.join(incomplete)}")

	logger.info('成功写出新的 bag: %s', config.output_bag)


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description='Filter PointCloud2 topics to XYZI fields.')
	parser.add_argument('--config', type=str, help='YAML 配置文件路径')
	parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
	parser.add_argument('--output-bag', type=str, help='输出 bag 路径 (可选)')
	parser.add_argument(
		'--topic',
		action='append',
		default=[],
		help='待处理的 PointCloud2 topic，可重复指定，多次指定会去重',
	)
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
			cli_output_bag=args.output_bag,
			cli_topics=args.topic,
		)
		filter_pointclouds(config)
	except ConfigError as exc:
		logger.error('%s', exc)
		return 2
	except Exception as exc:  # noqa: BLE001
		logger.exception('执行失败: %s', exc)
		return 1

	return 0


if __name__ == '__main__':
	sys.exit(main())
