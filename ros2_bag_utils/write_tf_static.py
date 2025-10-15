"""Tools for writing tf_static transforms into rosbag2 recordings."""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Optional, Set, Tuple

from ros2_bag_utils.common import (
    ConfigError,
    ensure_logging_configured,
    load_yaml_config,
    resolve_output_uri,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformConfig:
    """A single transform configuration from extrinsics file."""
    
    parent_frame: str
    child_frame: str
    translation: Tuple[float, float, float]  # x, y, z
    rotation_quaternion: Tuple[float, float, float, float]  # x, y, z, w


@dataclass(frozen=True)
class TfStaticConfig:
    """Resolved configuration for tf_static writing operation."""

    input_bag: Path
    output_bag: Path
    transforms: List[TransformConfig]


def _validate_frame_name(frame_name: str) -> bool:
    """Validate ROS frame name follows naming conventions."""
    if not frame_name:
        return False
    # ROS frame names: letters, numbers, underscores, forward slashes
    # Should not start with slash, no consecutive slashes, no trailing slash
    pattern = r'^[a-zA-Z0-9_][a-zA-Z0-9_/]*[a-zA-Z0-9_]$|^[a-zA-Z0-9_]$'
    if not re.match(pattern, frame_name):
        return False
    # No consecutive slashes
    if '//' in frame_name:
        return False
    return True


def _load_extrinsics_file(extrinsics_path: Path) -> Optional[TransformConfig]:
    """Load and parse a single extrinsics YAML file."""
    try:
        if not extrinsics_path.exists():
            logger.warning("外参文件不存在: %s", extrinsics_path)
            return None
        
        extrinsics_data = load_yaml_config(extrinsics_path)
        
        # Required fields
        parent_frame = extrinsics_data.get('parent_frame')
        child_frame = extrinsics_data.get('child_frame')
        transform_data = extrinsics_data.get('transform')
        
        if not all([parent_frame, child_frame, transform_data]):
            logger.warning("外参文件缺少必需字段 (parent_frame, child_frame, transform): %s", extrinsics_path)
            return None
        
        parent_frame = str(parent_frame).strip()
        child_frame = str(child_frame).strip()
        
        # Validate frame names
        if not _validate_frame_name(parent_frame):
            logger.warning("parent_frame 命名不规范: '%s' in %s", parent_frame, extrinsics_path)
            return None
        if not _validate_frame_name(child_frame):
            logger.warning("child_frame 命名不规范: '%s' in %s", child_frame, extrinsics_path)
            return None
        
        # Parse transform
        if not isinstance(transform_data, dict):
            logger.warning("transform 字段必须是字典: %s", extrinsics_path)
            return None
        
        translation_data = transform_data.get('translation', {})
        rotation_data = transform_data.get('rotation_quaternion', {})
        
        if not isinstance(translation_data, dict) or not isinstance(rotation_data, dict):
            logger.warning("translation 和 rotation_quaternion 必须是字典: %s", extrinsics_path)
            return None
        
        try:
            translation = (
                float(translation_data.get('x', 0.0)),
                float(translation_data.get('y', 0.0)),
                float(translation_data.get('z', 0.0)),
            )
            rotation_quaternion = (
                float(rotation_data.get('x', 0.0)),
                float(rotation_data.get('y', 0.0)),
                float(rotation_data.get('z', 0.0)),
                float(rotation_data.get('w', 1.0)),
            )
        except (ValueError, TypeError) as exc:
            logger.warning("变换数值解析失败: %s, 错误: %s", extrinsics_path, exc)
            return None
        
        return TransformConfig(
            parent_frame=parent_frame,
            child_frame=child_frame,
            translation=translation,
            rotation_quaternion=rotation_quaternion,
        )
    
    except Exception as exc:
        logger.warning("加载外参文件失败: %s, 错误: %s", extrinsics_path, exc)
        return None


def _extract_transforms_from_sensors(sensors_config: Dict[str, object], sensor_type: str) -> List[TransformConfig]:
    """Extract transforms from lidars or cameras configuration."""
    transforms = []
    
    if not isinstance(sensors_config, dict):
        logger.warning("%s 配置必须是字典格式", sensor_type)
        return transforms
    
    for sensor_id, sensor_data in sensors_config.items():
        if not isinstance(sensor_data, dict):
            logger.warning("%s '%s' 配置必须是字典格式", sensor_type, sensor_id)
            continue
        
        extrinsics_path_str = sensor_data.get('extrinsics')
        if not extrinsics_path_str:
            logger.warning("%s '%s' 缺少 extrinsics 字段", sensor_type, sensor_id)
            continue
        
        extrinsics_path = Path(str(extrinsics_path_str)).expanduser().resolve()
        transform_config = _load_extrinsics_file(extrinsics_path)
        
        if transform_config:
            transforms.append(transform_config)
        else:
            logger.warning("跳过 %s '%s' 的外参配置", sensor_type, sensor_id)
    
    return transforms


def build_effective_config(
    yaml_config: Dict[str, object],
    cli_input_bag: Optional[str],
    cli_output_bag: Optional[str],
) -> TfStaticConfig:
    """Build effective configuration from YAML and CLI arguments."""
    input_bag_str = cli_input_bag or yaml_config.get('input_bag')
    if not input_bag_str:
        raise ConfigError('必须指定 input_bag')

    output_bag_str = cli_output_bag or yaml_config.get('output_bag')

    # Extract transforms from lidars and cameras
    transforms = []
    
    lidars_config = yaml_config.get('lidars')
    if lidars_config and isinstance(lidars_config, dict):
        lidar_transforms = _extract_transforms_from_sensors(lidars_config, 'lidar')
        transforms.extend(lidar_transforms)
    
    cameras_config = yaml_config.get('cameras')
    if cameras_config and isinstance(cameras_config, dict):
        camera_transforms = _extract_transforms_from_sensors(cameras_config, 'camera')
        transforms.extend(camera_transforms)
    
    if not transforms:
        logger.warning('未找到有效的外参变换，将仅复制原始 bag')

    # Check for duplicate child frames
    child_frames: Set[str] = set()
    unique_transforms = []
    for transform in transforms:
        if transform.child_frame in child_frames:
            logger.warning("重复的 child_frame '%s'，跳过后续定义", transform.child_frame)
            continue
        child_frames.add(transform.child_frame)
        unique_transforms.append(transform)

    input_bag = Path(str(input_bag_str)).expanduser().resolve()
    output_bag = (
        Path(str(output_bag_str)).expanduser().resolve()
        if output_bag_str
        else resolve_output_uri(input_bag, None, suffix='_tf_static_added')
    )

    if input_bag == output_bag:
        raise ConfigError('输出路径不能与输入路径相同')

    return TfStaticConfig(input_bag=input_bag, output_bag=output_bag, transforms=unique_transforms)


def _import_rosbag2() -> ModuleType:
    try:
        import rosbag2_py
    except ImportError as exc:
        raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
    return rosbag2_py


def _create_tf_message(transforms: List[TransformConfig], timestamp_ns: int) -> bytes:
    """Create serialized tf2_msgs/TFMessage from transform configs."""
    try:
        from geometry_msgs.msg import TransformStamped
        from rclpy.serialization import serialize_message
        from rclpy.time import Time
        from std_msgs.msg import Header
        from tf2_msgs.msg import TFMessage
    except ImportError as exc:
        raise RuntimeError('无法导入 ROS2 消息类型，请确认环境配置') from exc
    
    transform_stamped_list = []
    ros_time = Time(nanoseconds=timestamp_ns)
    
    for transform_config in transforms:
        transform_stamped = TransformStamped()
        
        # Header
        transform_stamped.header = Header()
        transform_stamped.header.stamp = ros_time.to_msg()
        transform_stamped.header.frame_id = transform_config.parent_frame
        
        # Child frame
        transform_stamped.child_frame_id = transform_config.child_frame
        
        # Transform
        transform_stamped.transform.translation.x = transform_config.translation[0]
        transform_stamped.transform.translation.y = transform_config.translation[1]
        transform_stamped.transform.translation.z = transform_config.translation[2]
        
        transform_stamped.transform.rotation.x = transform_config.rotation_quaternion[0]
        transform_stamped.transform.rotation.y = transform_config.rotation_quaternion[1]
        transform_stamped.transform.rotation.z = transform_config.rotation_quaternion[2]
        transform_stamped.transform.rotation.w = transform_config.rotation_quaternion[3]
        
        transform_stamped_list.append(transform_stamped)
    
    tf_message = TFMessage()
    tf_message.transforms = transform_stamped_list
    
    return serialize_message(tf_message)


def write_tf_static(config: TfStaticConfig) -> None:
    """Write tf_static transforms into a rosbag2 recording."""
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

    # Get first message timestamp for tf_static
    converter_options = rosbag2_py.ConverterOptions('', '')
    reader_storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)
    reader = rosbag2_py.SequentialReader()
    reader.open(reader_storage_options, converter_options)
    
    first_timestamp = None
    if reader.has_next():
        _, _, timestamp = reader.read_next()
        first_timestamp = timestamp
    # Note: SequentialReader will auto-close when object is destroyed
    
    if first_timestamp is None:
        logger.warning("输入 bag 为空，使用当前时间作为 tf_static 时间戳")
        import time
        first_timestamp = int(time.time() * 1e9)

    # Check for existing /tf_static
    topics_info = metadata.topics_with_message_count
    existing_tf_static = None
    topics_by_name = {}
    
    for topic_with_count in topics_info:
        topic_meta = topic_with_count.topic_metadata
        topics_by_name[topic_meta.name] = topic_meta
        if topic_meta.name == '/tf_static':
            existing_tf_static = topic_meta
            logger.warning("检测到现有 /tf_static topic，将进行合并")

    # Setup writer
    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    # Create all topics including /tf_static
    for topic_meta in topics_by_name.values():
        writer.create_topic(topic_meta)
    
    if '/tf_static' not in topics_by_name:
        # Create new /tf_static topic
        tf_static_meta = rosbag2_py.TopicMetadata(
            name='/tf_static',
            type='tf2_msgs/msg/TFMessage',
            serialization_format='cdr',
            offered_qos_profiles='',
        )
        writer.create_topic(tf_static_meta)

    # Write new tf_static message if we have transforms
    if config.transforms:
        try:
            tf_static_data = _create_tf_message(config.transforms, first_timestamp)
            writer.write('/tf_static', tf_static_data, first_timestamp)
            logger.info("写入 %d 个 tf_static 变换", len(config.transforms))
        except Exception as exc:
            logger.error("创建 tf_static 消息失败: %s", exc)
            raise

    # Copy all messages from input bag
    reader = rosbag2_py.SequentialReader()
    reader.open(reader_storage_options, converter_options)
    
    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()
        writer.write(topic_name, data, timestamp)

    logger.info('成功写出新的 bag: %s', config.output_bag)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Write tf_static transforms into a rosbag2 recording.')
    parser.add_argument('--config', type=str, help='YAML 配置文件路径')
    parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
    parser.add_argument('--output-bag', type=str, help='输出 bag 路径 (可选)')
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
        )
        write_tf_static(config)
    except ConfigError as exc:
        logger.error('%s', exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.exception('执行失败: %s', exc)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())