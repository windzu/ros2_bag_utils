from pathlib import Path

import numpy as np
from numpy.testing import assert_allclose

import lzf
from sensor_msgs.msg import PointCloud2, PointField

import importlib.util
from importlib.machinery import SourceFileLoader

# Prefer loading the local source file to avoid an installed package shadowing it
local_mod = SourceFileLoader('local_export_assets', '/workspace/src/ros2_bag_utils/ros2_bag_utils/export_assets.py').load_module()
_write_pointcloud_binary_compressed = local_mod._write_pointcloud_binary_compressed


def _make_pointcloud(
	points,
	*,
	intensities=None,
	times=None,
	intensity_field_name='intensity',
	time_field_name='time',
):
	points_array = np.asarray(points, dtype=np.float32)
	msg = PointCloud2()
	msg.height = 1
	msg.width = points_array.shape[0]
	msg.is_bigendian = False
	msg.is_dense = True

	dtype_fields = [('x', '<f4'), ('y', '<f4'), ('z', '<f4')]
	if intensities is not None:
		dtype_fields.append((intensity_field_name, '<f4'))
	if times is not None:
		dtype_fields.append((time_field_name, '<f4'))

	structured = np.zeros(msg.width, dtype=np.dtype(dtype_fields))
	if msg.width:
		structured['x'] = points_array[:, 0]
		structured['y'] = points_array[:, 1]
		structured['z'] = points_array[:, 2]
		if intensities is not None:
			structured[intensity_field_name] = np.asarray(intensities, dtype=np.float32)
		if times is not None:
			structured[time_field_name] = np.asarray(times, dtype=np.float32)

	msg.point_step = structured.dtype.itemsize
	msg.row_step = msg.point_step * msg.width

	offset = 0
	fields = []
	for name, _ in dtype_fields:
		fields.append(PointField(name=name, offset=offset, datatype=PointField.FLOAT32, count=1))
		offset += 4
	msg.fields = fields
	msg.data = structured.tobytes()
	return msg


def _read_pcd_payload(path: Path):
	blob = path.read_bytes()
	header_marker = b'DATA binary_compressed\n'
	header_end = blob.index(header_marker) + len(header_marker)
	headers = blob[:header_end].decode('ascii')
	body = blob[header_end:]
	compressed_size = int.from_bytes(body[0:4], byteorder='little', signed=False)
	uncompressed_size = int.from_bytes(body[4:8], byteorder='little', signed=False)
	payload = body[8:8 + compressed_size]
	return headers, compressed_size, uncompressed_size, payload


def test_write_pointcloud_adds_default_intensity(tmp_path):
	cloud = _make_pointcloud(
		[(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)],
	)
	output = tmp_path / 'cloud_xyzi.pcd'

	_write_pointcloud_binary_compressed(cloud, output, pcd_format='compressed')

	headers, compressed_size, uncompressed_size, payload = _read_pcd_payload(output)

	assert 'FIELDS x y z intensity' in headers
	assert 'OFFSET 0 4 8 12' in headers
	assert f'POINTS {cloud.width * cloud.height}' in headers

	expected_dtype = np.dtype([
		('x', '<f4'),
		('y', '<f4'),
		('z', '<f4'),
		('intensity', '<f4'),
	])
	expected = np.zeros(cloud.width, dtype=expected_dtype)
	expected['x'] = [1.0, 4.0]
	expected['y'] = [2.0, 5.0]
	expected['z'] = [3.0, 6.0]

	assert compressed_size == len(payload)
	assert uncompressed_size == expected.nbytes
	decompressed = lzf.decompress(payload, uncompressed_size)
	result = np.frombuffer(decompressed, dtype=expected_dtype)

	assert_allclose(result['x'], expected['x'])
	assert_allclose(result['y'], expected['y'])
	assert_allclose(result['z'], expected['z'])
	assert_allclose(result['intensity'], expected['intensity'])


def test_write_pointcloud_preserves_intensity_and_time(tmp_path):
	cloud = _make_pointcloud(
		[(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)],
		intensities=[0.5, 1.5],
		times=[0.1, 0.2],
		intensity_field_name='i',
		time_field_name='timestamp',
	)
	output = tmp_path / 'cloud_xyzt.pcd'

	_write_pointcloud_binary_compressed(cloud, output, pcd_format='compressed')

	headers, compressed_size, uncompressed_size, payload = _read_pcd_payload(output)

	assert 'FIELDS x y z intensity time' in headers
	assert 'OFFSET 0 4 8 12 16' in headers

	expected_dtype = np.dtype([
		('x', '<f4'),
		('y', '<f4'),
		('z', '<f4'),
		('intensity', '<f4'),
		('time', '<f4'),
	])
	expected = np.zeros(cloud.width, dtype=expected_dtype)
	expected['x'] = [1.0, 4.0]
	expected['y'] = [2.0, 5.0]
	expected['z'] = [3.0, 6.0]
	expected['intensity'] = [0.5, 1.5]
	expected['time'] = [0.1, 0.2]

	assert compressed_size == len(payload)
	assert uncompressed_size == expected.nbytes
	decompressed = lzf.decompress(payload, uncompressed_size)
	result = np.frombuffer(decompressed, dtype=expected_dtype)

	assert_allclose(result['x'], expected['x'])
	assert_allclose(result['y'], expected['y'])
	assert_allclose(result['z'], expected['z'])
	assert_allclose(result['intensity'], expected['intensity'])
	assert_allclose(result['time'], expected['time'])
