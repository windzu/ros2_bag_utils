from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pytest
from sensor_msgs.msg import PointCloud2, PointField
from sensor_msgs_py import point_cloud2 as pc2
from std_msgs.msg import Header

from ros2_bag_utils.common import ConfigError
from ros2_bag_utils.filter_pointcloud_xyzi import (
	REQUIRED_FIELDS,
	build_effective_config,
	_filter_pointcloud_to_xyzi,
)


POINTFIELD_BYTE_SIZES: Dict[int, int] = {
	PointField.INT8: 1,
	PointField.UINT8: 1,
	PointField.INT16: 2,
	PointField.UINT16: 2,
	PointField.INT32: 4,
	PointField.UINT32: 4,
	PointField.FLOAT32: 4,
	PointField.FLOAT64: 8,
}

POINTFIELD_NP_DTYPE: Dict[int, np.dtype] = {
	PointField.INT8: np.dtype(np.int8),
	PointField.UINT8: np.dtype(np.uint8),
	PointField.INT16: np.dtype(np.int16),
	PointField.UINT16: np.dtype(np.uint16),
	PointField.INT32: np.dtype(np.int32),
	PointField.UINT32: np.dtype(np.uint32),
	PointField.FLOAT32: np.dtype(np.float32),
	PointField.FLOAT64: np.dtype(np.float64),
}


def _make_pointcloud(
	points_xyzi: np.ndarray,
	*,
	width: int | None = None,
	height: int = 1,
	intensity_datatype: int = PointField.FLOAT32,
	extra_fields: List[Tuple[str, int, int]] | None = None,
	is_dense: bool = True,
) -> PointCloud2:
	if width is None:
		width = points_xyzi.shape[0] if height == 1 else points_xyzi.shape[0] // height
	assert height * width == points_xyzi.shape[0]

	field_defs: List[Tuple[str, int, int]] = [
		('x', PointField.FLOAT32, 1),
		('y', PointField.FLOAT32, 1),
		('z', PointField.FLOAT32, 1),
		('intensity', intensity_datatype, 1),
	]
	if extra_fields:
		field_defs.extend(extra_fields)

	offset = 0
	fields: List[PointField] = []
	for name, datatype, count in field_defs:
		if datatype not in POINTFIELD_BYTE_SIZES:
			raise ValueError(f'unsupported datatype {datatype}')
		fields.append(PointField(name=name, offset=offset, datatype=datatype, count=count))
		offset += POINTFIELD_BYTE_SIZES[datatype] * count

	point_step = offset
	num_points = width * height
	buf = np.zeros((num_points, point_step), dtype=np.uint8)

	field_values: Dict[str, np.ndarray] = {
		'x': points_xyzi[:, 0],
		'y': points_xyzi[:, 1],
		'z': points_xyzi[:, 2],
		'intensity': points_xyzi[:, 3],
	}

	if extra_fields:
		for name, dtype, count in extra_fields:
			values = np.arange(num_points * count, dtype=POINTFIELD_NP_DTYPE[dtype])
			values = values.reshape(num_points, count)
			if count == 1:
				values = values.reshape(num_points)
			field_values[name] = values

	for field in fields:
		dtype = POINTFIELD_NP_DTYPE[field.datatype]
		values = field_values[field.name]
		arr = np.asarray(values, dtype=dtype)
		arr = arr.reshape(num_points, field.count)
		arr = np.ascontiguousarray(arr)
		raw = arr.view(np.uint8).reshape(
			num_points,
			POINTFIELD_BYTE_SIZES[field.datatype] * field.count,
		)
		buf[:, field.offset : field.offset + raw.shape[1]] = raw

	msg = PointCloud2()
	msg.header = Header(frame_id='base_link')
	msg.height = height
	msg.width = width
	msg.fields = fields
	msg.is_bigendian = False
	msg.point_step = point_step
	msg.row_step = point_step * width
	msg.is_dense = is_dense
	msg.data = buf.tobytes()

	return msg


def test_build_effective_config_merge(tmp_path: Path):
	yaml_cfg: Dict[str, object] = {
		'input_bag': str(tmp_path / 'input_bag'),
		'output_bag': str(tmp_path / 'output_yaml'),
		'topics': ['/topic_a', '/topic_b'],
	}
	config = build_effective_config(
		yaml_config=yaml_cfg,
		cli_input_bag=None,
		cli_output_bag=str(tmp_path / 'output_cli'),
		cli_topics=['/topic_b', '   /topic_c   '],
	)

	assert config.input_bag == (tmp_path / 'input_bag').resolve()
	assert config.output_bag == (tmp_path / 'output_cli').resolve()
	assert config.topics == ['/topic_a', '/topic_b', '/topic_c']


def test_build_effective_config_requires_topics(tmp_path: Path):
	yaml_cfg: Dict[str, object] = {'input_bag': str(tmp_path / 'input_bag')}
	with pytest.raises(ConfigError):
		build_effective_config(yaml_cfg, None, None, [])


def test_build_effective_config_rejects_non_list(tmp_path: Path):
	yaml_cfg: Dict[str, object] = {
		'input_bag': str(tmp_path / 'input_bag'),
		'topics': {'/foo': True},
	}
	with pytest.raises(ConfigError):
		build_effective_config(yaml_cfg, None, None, [])


def test_filter_pointcloud_to_xyzi_basic():
	points = np.array([
		[1.0, 2.0, 3.0, 0.1],
		[4.0, 5.0, 6.0, 0.2],
		[7.0, 8.0, 9.0, 0.3],
	], dtype=np.float32)

	msg = _make_pointcloud(points, extra_fields=[('ring', PointField.UINT16, 1)])
	filtered = _filter_pointcloud_to_xyzi(msg, '/lidar')

	assert filtered.height == msg.height
	assert filtered.width == msg.width
	assert filtered.point_step == 16
	assert filtered.row_step == 16 * filtered.width
	assert [field.name for field in filtered.fields] == REQUIRED_FIELDS

	filtered_points = pc2.read_points_numpy(filtered, field_names=REQUIRED_FIELDS, skip_nans=False)
	np.testing.assert_array_almost_equal(filtered_points, points)


def test_filter_pointcloud_to_xyzi_numpy_fallback(monkeypatch):
	points = np.array([
		[1.5, 2.5, 3.5, 0.7],
		[4.5, 5.5, 6.5, 0.8],
	], dtype=np.float32)

	msg = _make_pointcloud(points, extra_fields=[('ring', PointField.UINT16, 1)])

	def _raise(*_args, **_kwargs):
		raise AssertionError('All fields need to have the same datatype. Use `read_points()` otherwise.')

	monkeypatch.setattr(pc2, 'read_points_numpy', _raise)

	filtered = _filter_pointcloud_to_xyzi(msg, '/lidar')
	filtered_rows = [
		tuple(float(value) for value in row)
		for row in pc2.read_points(filtered, field_names=REQUIRED_FIELDS, skip_nans=False)
	]
	np.testing.assert_array_almost_equal(np.asarray(filtered_rows, dtype=np.float32), points)


def test_filter_pointcloud_to_xyzi_height_width_preserved():
	points = np.array([
		[0.0, 0.0, 0.0, 0.0],
		[1.0, 1.0, 1.0, 1.0],
		[2.0, 2.0, 2.0, 2.0],
		[3.0, 3.0, 3.0, 3.0],
	], dtype=np.float32)

	msg = _make_pointcloud(points, width=2, height=2, extra_fields=[('timestamp', PointField.UINT32, 1)])
	filtered = _filter_pointcloud_to_xyzi(msg, '/lidar')

	assert filtered.height == 2
	assert filtered.width == 2
	assert filtered.row_step == 16 * 2
	assert len(filtered.data) == 16 * 4

	filtered_points = pc2.read_points_numpy(filtered, field_names=REQUIRED_FIELDS, skip_nans=False)
	filtered_points = filtered_points.reshape(filtered.height, filtered.width, len(REQUIRED_FIELDS))
	assert filtered_points.shape == (2, 2, 4)


def test_filter_pointcloud_to_xyzi_missing_field():
	points = np.array([[1.0, 2.0, 3.0, 0.4]], dtype=np.float32)
	# 构造缺少 intensity 的消息
	msg = _make_pointcloud(points, extra_fields=None)
	msg.fields = [field for field in msg.fields if field.name != 'intensity']
	msg.point_step -= 4
	msg.row_step = msg.point_step * msg.width
	msg.data = msg.data[: msg.point_step * msg.width]

	with pytest.raises(RuntimeError):
		_filter_pointcloud_to_xyzi(msg, '/lidar')


def test_filter_pointcloud_to_xyzi_invalid_intensity_type():
	points = np.array([[1.0, 2.0, 3.0, 5.0]], dtype=np.float32)
	msg = _make_pointcloud(points, intensity_datatype=PointField.UINT16)

	with pytest.raises(RuntimeError):
		_filter_pointcloud_to_xyzi(msg, '/lidar')
