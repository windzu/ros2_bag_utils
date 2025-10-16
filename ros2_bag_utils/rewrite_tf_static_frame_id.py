"""Tools for rewriting frame names inside /tf_static of rosbag2 recordings."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, Iterable, List, Optional, Set, Tuple

from ros2_bag_utils.common import (
    ConfigError,
    ensure_logging_configured,
    load_yaml_config,
    parse_topic_map_arguments,
    resolve_output_uri,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RewriteTfStaticConfig:
    """Configuration for rewriting tf_static frames."""

    input_bag: Path
    output_bag: Path
    frame_mappings: Dict[str, str]


class TfStaticRewriteStats:
    """Collect statistics about tf_static frame rewrites."""

    def __init__(self) -> None:
        self.replacements: int = 0
        self.messages_updated: int = 0
        self.touched_edges_before: Set[Tuple[str, str]] = set()
        self.touched_edges_after: Set[Tuple[str, str]] = set()
        self.touched_frames: Set[str] = set()

    def record(
        self,
        *,
        parent_before: str,
        child_before: str,
        parent_after: Optional[str],
        child_after: Optional[str],
    ) -> None:
        if parent_after is None or child_after is None:
            raise RuntimeError('tf_static 改写统计记录时出现空 frame 名称')
        self.touched_edges_before.add((parent_before, child_before))
        self.touched_edges_after.add((parent_after, child_after))
        if parent_before != parent_after:
            self.touched_frames.add(parent_before)
            self.touched_frames.add(parent_after)
            self.replacements += 1
        if child_before != child_after:
            self.touched_frames.add(child_before)
            self.touched_frames.add(child_after)
            self.replacements += 1
        if parent_before != parent_after or child_before != child_after:
            self.messages_updated += 1


def _normalize_frame_map(raw: Optional[Dict[str, object]]) -> Dict[str, str]:
    if raw is None:
        return {}
    normalized: Dict[str, str] = {}
    for key, value in raw.items():
        original = str(key).strip()
        if not original:
            raise ConfigError('配置中的 frame 名称不能为空')
        new_frame = str(value).strip()
        if not new_frame:
            raise ConfigError(f"frame '{original}' 对应的新 frame 名称不能为空")
        if original in normalized and normalized[original] != new_frame:
            raise ConfigError(f"frame '{original}' 在配置中重复出现且目标不同")
        normalized[original] = new_frame
    return normalized


def _build_frame_mapping(
    frame_mappings: Dict[str, str],
    cli_pairs: Iterable[str],
) -> Dict[str, str]:
    cli_map = parse_topic_map_arguments(cli_pairs)
    # parse_topic_map_arguments 允许 topic 名称开头 '/'，这里不强制限制
    merged = frame_mappings.copy()
    for original, renamed in cli_map.items():
        if original in merged and merged[original] != renamed:
            raise ConfigError(
                f"frame '{original}' 同时被映射到 '{merged[original]}' 和 '{renamed}'，请检查配置或参数"
            )
        merged[original] = renamed
    return merged


def build_effective_config(
    yaml_config: Dict[str, object],
    cli_input_bag: Optional[str],
    cli_output_bag: Optional[str],
    cli_frame_map: List[str],
) -> RewriteTfStaticConfig:
    input_bag_str = cli_input_bag or yaml_config.get('input_bag')
    if not input_bag_str:
        raise ConfigError('必须指定 input_bag')

    output_bag_str = cli_output_bag or yaml_config.get('output_bag')
    raw_mappings = yaml_config.get('frames')
    if raw_mappings is not None and not isinstance(raw_mappings, dict):
        raise ConfigError('配置中的 frames 必须是字典结构')

    frame_mappings = _normalize_frame_map(raw_mappings)
    frame_mappings = _build_frame_mapping(frame_mappings, cli_frame_map)

    if not frame_mappings:
        raise ConfigError('至少需要一个 frame 的映射配置')

    input_bag = Path(str(input_bag_str)).expanduser().resolve()
    output_bag = (
        Path(str(output_bag_str)).expanduser().resolve()
        if output_bag_str
        else resolve_output_uri(input_bag, None, suffix='_tfstatic_fix')
    )

    if input_bag == output_bag:
        raise ConfigError('输出路径不能与输入路径相同')

    return RewriteTfStaticConfig(input_bag=input_bag, output_bag=output_bag, frame_mappings=frame_mappings)


def _import_rosbag2() -> ModuleType:
    try:
        import rosbag2_py
    except ImportError as exc:
        raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
    return rosbag2_py


def _rewrite_tf_static_message(message: object, frame_map: Dict[str, str], stats: TfStaticRewriteStats) -> None:
    transforms = getattr(message, 'transforms', None)
    if transforms is None:
        raise RuntimeError('/tf_static 消息缺少 transforms 字段，无法进行 frame_id 替换')

    for transform in transforms:
        header = getattr(transform, 'header', None)
        if header is None or not hasattr(header, 'frame_id'):
            raise RuntimeError('/tf_static 消息中的 TransformStamped 缺少 header.frame_id')

        child_frame = getattr(transform, 'child_frame_id', None)
        if child_frame is None:
            raise RuntimeError('/tf_static 消息中的 TransformStamped 缺少 child_frame_id')

        original_parent = header.frame_id
        original_child = child_frame
        new_parent = frame_map.get(original_parent)
        new_child = frame_map.get(original_child)

        if new_parent is not None:
            header.frame_id = new_parent
        else:
            new_parent = original_parent

        if new_child is not None:
            transform.child_frame_id = new_child
        else:
            new_child = original_child

        if new_parent != original_parent or new_child != original_child:
            stats.record(
                parent_before=original_parent,
                child_before=original_child,
                parent_after=new_parent,
                child_after=new_child,
            )


def rewrite_tf_static_frames(config: RewriteTfStaticConfig) -> None:
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
    topics_by_name = {topic.topic_metadata.name: topic.topic_metadata for topic in topics_info}

    if '/tf_static' not in topics_by_name:
        logger.warning("输入 bag 中未找到 /tf_static，直接复制原始 bag")
        _copy_bag_without_modification(config, rosbag2_py, storage_id)
        return

    converter_options = rosbag2_py.ConverterOptions('', '')
    storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)

    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    for topic_meta in topics_by_name.values():
        writer.create_topic(topic_meta)

    tf_static_type = None
    stats = TfStaticRewriteStats()

    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()
        if topic_name == '/tf_static':
            if tf_static_type is None:
                try:
                    tf_static_type = get_message('tf2_msgs/msg/TFMessage')
                except (AttributeError, ModuleNotFoundError, ValueError) as exc:
                    raise RuntimeError("无法加载消息类型 'tf2_msgs/msg/TFMessage' 用于 topic '/tf_static'") from exc

            message = deserialize_message(data, tf_static_type)
            _rewrite_tf_static_message(message, config.frame_mappings, stats)
            data = serialize_message(message)
        writer.write(topic_name, data, timestamp)

    unconvered_frames = sorted(frame for frame in config.frame_mappings if frame not in stats.touched_frames)
    if unconvered_frames:
        logger.warning('以下 frame 未在 /tf_static 中找到，未做改写: %s', ', '.join(unconvered_frames))

    if stats.messages_updated:
        before_lines = '\n'.join(f"  {parent} -> {child}" for parent, child in sorted(stats.touched_edges_before))
        after_lines = '\n'.join(f"  {parent} -> {child}" for parent, child in sorted(stats.touched_edges_after))
        mapping_lines = '\n'.join(f"  {old} -> {new}" for old, new in sorted(config.frame_mappings.items()))

        logger.info('frame 映射列表:\n%s', mapping_lines)
        logger.info(
            "成功改写 /tf_static 中 %d 条 TransformStamped，共替换 %d 处 frame_id",
            stats.messages_updated,
            stats.replacements,
        )
        if before_lines:
            logger.info('tf_static 变更前的相关连线:\n%s', before_lines)
        if after_lines:
            logger.info('tf_static 变更后的相关连线:\n%s', after_lines)
    else:
        logger.warning('未在 /tf_static 中检测到任何符合条件的 frame，未做改写')

    logger.info('成功写出新的 bag: %s', config.output_bag)


def _copy_bag_without_modification(config: RewriteTfStaticConfig, rosbag2_py: ModuleType, storage_id: str) -> None:
    converter_options = rosbag2_py.ConverterOptions('', '')
    reader_storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)

    reader = rosbag2_py.SequentialReader()
    reader.open(reader_storage_options, converter_options)

    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    info = rosbag2_py.Info()
    metadata = info.read_metadata(str(config.input_bag), '')
    topics_meta = metadata.topics_with_message_count

    for topic_with_count in topics_meta:
        writer.create_topic(topic_with_count.topic_metadata)

    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()
        writer.write(topic_name, data, timestamp)

    logger.info('成功复制原始 bag 到: %s', config.output_bag)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Rewrite frame names in /tf_static of a rosbag2 recording.')
    parser.add_argument('--config', type=str, help='YAML 配置文件路径')
    parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
    parser.add_argument('--output-bag', type=str, help='输出 bag 路径 (可选)')
    parser.add_argument(
        '--frame-map',
        action='append',
        default=[],
        help="old_frame:=new_frame 格式，可重复指定，多条时后者覆盖前者",
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
            cli_frame_map=args.frame_map,
        )
        rewrite_tf_static_frames(config)
    except ConfigError as exc:
        logger.error('%s', exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.exception('执行失败: %s', exc)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
