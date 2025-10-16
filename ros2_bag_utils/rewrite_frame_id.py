"""Tools for rewriting frame_id fields inside rosbag2 recordings."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Optional, Set, Tuple

from ros2_bag_utils.common import (
    ConfigError,
    ensure_logging_configured,
    load_yaml_config,
    parse_topic_map_arguments,
    resolve_output_uri,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RewriteConfig:
    """Resolved configuration for a rewrite operation."""

    input_bag: Path
    output_bag: Path
    topic_mappings: Dict[str, str]


def _register_frame_mapping(
    frame_mapping: Dict[str, str],
    topic_original_frames: Dict[str, str],
    *,
    topic_name: str,
    original_frame_id: str,
    target_frame_id: str,
) -> None:
    """记录 topic 的原始 frame_id 并维护旧→新 frame 映射。"""

    if original_frame_id is None or original_frame_id == '':
        raise RuntimeError(f"topic '{topic_name}' 的消息缺少可修改的 header.frame_id")

    recorded_frame = topic_original_frames.get(topic_name)
    if recorded_frame is None:
        topic_original_frames[topic_name] = original_frame_id
    elif recorded_frame != original_frame_id:
        raise RuntimeError(
            f"topic '{topic_name}' 的消息 header.frame_id 不一致: 之前为 '{recorded_frame}', 当前为 '{original_frame_id}'"
        )

    mapped_target = frame_mapping.get(original_frame_id)
    if mapped_target is None:
        frame_mapping[original_frame_id] = target_frame_id
    elif mapped_target != target_frame_id:
        raise RuntimeError(
            f"frame '{original_frame_id}' 同时被映射到 '{mapped_target}' 和 '{target_frame_id}', 请检查配置"
        )


def _rewrite_tf_static_message(
    message: object, frame_mapping: Dict[str, str]
) -> Tuple[int, Set[Tuple[str, str]], Set[Tuple[str, str]]]:
    """将 TFMessage 中出现的 frame 名称根据映射进行替换，返回 (替换次数, 变更前连线集合, 变更后连线集合)。"""

    transforms = getattr(message, 'transforms', None)
    if transforms is None:
        raise RuntimeError('/tf_static 消息缺少 transforms 字段，无法进行 frame_id 替换')

    replacements = 0
    edges_before: Set[Tuple[str, str]] = set()
    edges_after: Set[Tuple[str, str]] = set()
    for transform in transforms:
        header = getattr(transform, 'header', None)
        if header is None or not hasattr(header, 'frame_id'):
            raise RuntimeError('/tf_static 消息中的 TransformStamped 缺少 header.frame_id')

        child_frame = getattr(transform, 'child_frame_id', None)
        if child_frame is None:
            raise RuntimeError('/tf_static 消息中的 TransformStamped 缺少 child_frame_id')
        original_parent = header.frame_id
        original_child = child_frame
        new_parent = original_parent
        new_child = original_child

        if original_parent in frame_mapping:
            new_parent = frame_mapping[original_parent]
            header.frame_id = new_parent
            replacements += 1

        if original_child in frame_mapping:
            new_child = frame_mapping[original_child]
            transform.child_frame_id = new_child
            replacements += 1

        if original_parent != new_parent or original_child != new_child:
            edges_before.add((original_parent, original_child))
            edges_after.add((new_parent, new_child))

    return replacements, edges_before, edges_after


def _normalize_topic_map(raw: Optional[Dict[str, object]]) -> Dict[str, str]:
    if raw is None:
        return {}
    normalized: Dict[str, str] = {}
    for key, value in raw.items():
        topic = str(key).strip()
        if not topic:
            raise ConfigError('配置中的 topic 名称不能为空')
        frame = str(value).strip()
        if not frame:
            raise ConfigError(f"topic '{topic}' 对应的 frame_id 不能为空")
        normalized[topic] = frame
    return normalized


def build_effective_config(
    yaml_config: Dict[str, object],
    cli_input_bag: Optional[str],
    cli_output_bag: Optional[str],
    cli_topic_map: Dict[str, str],
) -> RewriteConfig:
    input_bag_str = cli_input_bag or yaml_config.get('input_bag')
    if not input_bag_str:
        raise ConfigError('必须指定 input_bag')

    output_bag_str = cli_output_bag or yaml_config.get('output_bag')

    raw_mappings = yaml_config.get('mappings')
    if raw_mappings is not None and not isinstance(raw_mappings, dict):
        raise ConfigError('配置中的 mappings 必须是字典结构')
    yaml_topics = _normalize_topic_map(raw_mappings)
    topic_mappings = yaml_topics.copy()
    topic_mappings.update(cli_topic_map)

    if not topic_mappings:
        raise ConfigError('至少需要一个 topic 的 frame_id 映射')

    input_bag = Path(str(input_bag_str)).expanduser().resolve()
    output_bag = (
        Path(str(output_bag_str)).expanduser().resolve()
        if output_bag_str
        else resolve_output_uri(input_bag, None, suffix='_frameid_fix')
    )

    if input_bag == output_bag:
        raise ConfigError('输出路径不能与输入路径相同')

    return RewriteConfig(input_bag=input_bag, output_bag=output_bag, topic_mappings=topic_mappings)


def _import_rosbag2() -> ModuleType:
    try:
        import rosbag2_py
    except ImportError as exc:
        raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
    return rosbag2_py


def rewrite_frame_ids(config: RewriteConfig) -> None:
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
    topics_by_name = {}
    message_counts = {}
    for topic_with_count in topics_info:
        topic_meta = topic_with_count.topic_metadata
        topics_by_name[topic_meta.name] = topic_meta
        message_counts[topic_meta.name] = topic_with_count.message_count

    missing_topics = sorted(set(config.topic_mappings) - set(topics_by_name))
    if missing_topics:
        raise ConfigError(f"配置中的 topic 不存在于 bag: {', '.join(missing_topics)}")

    empty_topics = [
        name for name in config.topic_mappings if message_counts.get(name, 0) == 0
    ]
    if empty_topics:
        raise ConfigError(f"以下 topic 在 bag 中没有消息: {', '.join(empty_topics)}")

    converter_options = rosbag2_py.ConverterOptions('', '')
    storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)

    type_cache: Dict[str, object] = {}

    def _ensure_message_type(topic: str) -> object:
        if topic not in type_cache:
            try:
                type_cache[topic] = get_message(topics_by_name[topic].type)
            except (AttributeError, ModuleNotFoundError, ValueError) as exc:
                raise RuntimeError(
                    f"无法加载消息类型 '{topics_by_name[topic].type}' 用于 topic '{topic}'"
                ) from exc
        return type_cache[topic]

    # 第一次遍历：收集每个 topic 的原始 frame_id，生成旧→新 frame 映射
    analysis_reader = rosbag2_py.SequentialReader()
    analysis_reader.open(storage_options, converter_options)

    topic_original_frames: Dict[str, str] = {}
    frame_mapping: Dict[str, str] = {}
    remaining_topics: Set[str] = set(config.topic_mappings)

    while analysis_reader.has_next() and remaining_topics:
        topic_name, data, _ = analysis_reader.read_next()
        if topic_name not in remaining_topics:
            continue

        msg_type = _ensure_message_type(topic_name)
        message = deserialize_message(data, msg_type)
        header = getattr(message, 'header', None)
        if header is None or not hasattr(header, 'frame_id'):
            raise RuntimeError(f"topic '{topic_name}' 的消息缺少可修改的 header.frame_id")

        original_frame = header.frame_id
        target_frame = config.topic_mappings[topic_name]
        _register_frame_mapping(
            frame_mapping,
            topic_original_frames,
            topic_name=topic_name,
            original_frame_id=original_frame,
            target_frame_id=target_frame,
        )
        remaining_topics.discard(topic_name)

    if remaining_topics:
        raise RuntimeError(f"无法收集以下 topic 的原始 frame_id: {', '.join(sorted(remaining_topics))}")

    # 重新开启 reader 写出新的 bag
    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    for topic_meta in topics_by_name.values():
        writer.create_topic(topic_meta)

    processed_messages = {topic: 0 for topic in config.topic_mappings}
    tf_static_type: Optional[object] = None
    tf_static_replacements = 0
    tf_static_messages_updated = 0
    tf_static_edges_before: Set[Tuple[str, str]] = set()
    tf_static_edges_after: Set[Tuple[str, str]] = set()

    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()

        if topic_name in config.topic_mappings:
            msg_type = _ensure_message_type(topic_name)
            message = deserialize_message(data, msg_type)
            header = getattr(message, 'header', None)
            if header is None or not hasattr(header, 'frame_id'):
                raise RuntimeError(f"topic '{topic_name}' 的消息缺少可修改的 header.frame_id")

            expected_frame = topic_original_frames[topic_name]
            current_frame = header.frame_id
            if current_frame != expected_frame:
                raise RuntimeError(
                    f"topic '{topic_name}' 的消息 header.frame_id 不一致: 之前为 '{expected_frame}', 当前为 '{current_frame}'"
                )

            header.frame_id = config.topic_mappings[topic_name]
            data = serialize_message(message)
            processed_messages[topic_name] += 1

        elif topic_name == '/tf_static' and frame_mapping:
            if tf_static_type is None:
                try:
                    tf_static_type = get_message('tf2_msgs/msg/TFMessage')
                except (AttributeError, ModuleNotFoundError, ValueError) as exc:
                    raise RuntimeError("无法加载消息类型 'tf2_msgs/msg/TFMessage' 用于 topic '/tf_static'") from exc

            message = deserialize_message(data, tf_static_type)
            replacements, edges_before, edges_after = _rewrite_tf_static_message(message, frame_mapping)
            if replacements > 0:
                data = serialize_message(message)
                tf_static_replacements += replacements
                tf_static_messages_updated += 1
                tf_static_edges_before.update(edges_before)
                tf_static_edges_after.update(edges_after)

        writer.write(topic_name, data, timestamp)

    incomplete = [topic for topic, count in processed_messages.items() if count == 0]
    if incomplete:
        raise RuntimeError(f"未能重写以下 topic 的任何消息: {', '.join(incomplete)}")

    sorted_topics = sorted(config.topic_mappings)
    original_lines = [f"  {topic}: {topic_original_frames[topic]}" for topic in sorted_topics]
    new_lines = [f"  {topic}: {config.topic_mappings[topic]}" for topic in sorted_topics]
    logger.info('目标 topic 原始 frame_id:\n%s', '\n'.join(original_lines))
    logger.info('目标 topic 新 frame_id:\n%s', '\n'.join(new_lines))

    if tf_static_messages_updated:
        before_lines = [f"  {parent} -> {child}" for parent, child in sorted(tf_static_edges_before)]
        after_lines = [f"  {parent} -> {child}" for parent, child in sorted(tf_static_edges_after)]
        logger.info(
            "同步更新 /tf_static 中 %d 条消息，共替换 %d 处 frame_id",
            tf_static_messages_updated,
            tf_static_replacements,
        )
        if before_lines:
            logger.info('tf_static 变更前的相关连线:\n%s', '\n'.join(before_lines))
        if after_lines:
            logger.info('tf_static 变更后的相关连线:\n%s', '\n'.join(after_lines))
    elif '/tf_static' in topics_by_name:
        logger.info('/tf_static 存在但未检测到需要替换的 frame_id')

    logger.info('成功写出新的 bag: %s', config.output_bag)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Rewrite frame_id values in a rosbag2 recording.')
    parser.add_argument('--config', type=str, help='YAML 配置文件路径')
    parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
    parser.add_argument('--output-bag', type=str, help='输出 bag 路径 (可选)')
    parser.add_argument(
        '--topic-map',
        action='append',
        default=[],
        help="topic:=frame_id 格式，可重复指定，多条时后者覆盖前者",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    ensure_logging_configured()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        yaml_config = load_yaml_config(Path(args.config).expanduser().resolve()) if args.config else {}
        cli_topic_map = parse_topic_map_arguments(args.topic_map)
        config = build_effective_config(
            yaml_config=yaml_config,
            cli_input_bag=args.input_bag,
            cli_output_bag=args.output_bag,
            cli_topic_map=cli_topic_map,
        )
        rewrite_frame_ids(config)
    except ConfigError as exc:
        logger.error('%s', exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.exception('执行失败: %s', exc)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
