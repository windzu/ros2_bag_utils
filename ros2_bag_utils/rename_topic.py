"""Tools for renaming topics inside rosbag2 recordings."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, Iterable, List, Optional

from ros2_bag_utils.common import (
    ConfigError,
    ensure_logging_configured,
    load_yaml_config,
    parse_topic_map_arguments,
    resolve_output_uri,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RenameConfig:
    """Resolved configuration for a topic rename operation."""

    input_bag: Path
    output_bag: Path
    topic_mappings: Dict[str, str]


def _normalize_topic_map(raw: Optional[Dict[str, object]]) -> Dict[str, str]:
    if raw is None:
        return {}
    normalized: Dict[str, str] = {}
    for key, value in raw.items():
        old_topic = str(key).strip()
        if not old_topic:
            raise ConfigError('配置中的旧 topic 名称不能为空')
        new_topic = str(value).strip()
        if not new_topic:
            raise ConfigError(f"旧 topic '{old_topic}' 对应的新 topic 名称不能为空")
        if old_topic == new_topic:
            raise ConfigError(f"旧 topic '{old_topic}' 的新名称与原名称相同")
        normalized[old_topic] = new_topic
    return normalized


def _ensure_unique_targets(mappings: Dict[str, str]) -> None:
    seen = set()
    duplicates = set()
    for new_topic in mappings.values():
        if new_topic in seen:
            duplicates.add(new_topic)
        else:
            seen.add(new_topic)
    if duplicates:
        dup_list = ', '.join(sorted(duplicates))
        raise ConfigError(f'目标 topic 不允许重复: {dup_list}')


def build_effective_config(
    yaml_config: Dict[str, object],
    cli_input_bag: Optional[str],
    cli_output_bag: Optional[str],
    cli_topic_map: Dict[str, str],
) -> RenameConfig:
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
        raise ConfigError('至少需要一个 topic 的重命名规则')

    for old_topic, new_topic in topic_mappings.items():
        if old_topic == new_topic:
            raise ConfigError(f"旧 topic '{old_topic}' 的新名称与原名称相同")

    _ensure_unique_targets(topic_mappings)

    input_bag = Path(str(input_bag_str)).expanduser().resolve()
    output_bag = (
        Path(str(output_bag_str)).expanduser().resolve()
        if output_bag_str
        else resolve_output_uri(input_bag, None, suffix='_topic_renamed')
    )

    if input_bag == output_bag:
        raise ConfigError('输出路径不能与输入路径相同')

    return RenameConfig(input_bag=input_bag, output_bag=output_bag, topic_mappings=topic_mappings)


def validate_mappings_against_topics(
    mappings: Dict[str, str],
    existing_topics: Iterable[str],
    message_counts: Dict[str, int],
) -> None:
    existing_set = set(existing_topics)

    missing_topics = sorted(set(mappings) - existing_set)
    if missing_topics:
        raise ConfigError(f"配置中的旧 topic 不存在于 bag: {', '.join(missing_topics)}")

    empty_topics = [topic for topic in mappings if message_counts.get(topic, 0) == 0]
    if empty_topics:
        raise ConfigError(f"以下旧 topic 在 bag 中没有消息: {', '.join(empty_topics)}")

    conflicting_targets = sorted({new for new in mappings.values() if new in existing_set})
    if conflicting_targets:
        raise ConfigError(f"目标 topic 已存在于 bag 中: {', '.join(conflicting_targets)}")


def _import_rosbag2() -> ModuleType:
    try:
        import rosbag2_py
    except ImportError as exc:
        raise RuntimeError('无法导入 rosbag2_py，请确认已在 ROS2 环境下运行') from exc
    return rosbag2_py


def rename_topics(config: RenameConfig) -> None:
    rosbag2_py = _import_rosbag2()

    if not config.input_bag.exists():
        raise ConfigError(f"输入 bag 路径 {config.input_bag} 不存在")
    if config.output_bag.exists():
        raise ConfigError(f"输出路径 {config.output_bag} 已存在，请先删除或指定其他目录")

    info = rosbag2_py.Info()
    metadata = info.read_metadata(str(config.input_bag), '')
    storage_id = metadata.storage_identifier

    topics_info = metadata.topics_with_message_count
    topics_by_name: Dict[str, object] = {}
    message_counts: Dict[str, int] = {}
    for topic_with_count in topics_info:
        topic_meta = topic_with_count.topic_metadata
        topics_by_name[topic_meta.name] = topic_meta
        message_counts[topic_meta.name] = topic_with_count.message_count

    validate_mappings_against_topics(config.topic_mappings, topics_by_name.keys(), message_counts)

    converter_options = rosbag2_py.ConverterOptions('', '')
    reader_storage_options = rosbag2_py.StorageOptions(uri=str(config.input_bag), storage_id=storage_id)
    reader = rosbag2_py.SequentialReader()
    reader.open(reader_storage_options, converter_options)

    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    topic_name_map: Dict[str, str] = {}
    for topic_with_count in topics_info:
        old_meta = topic_with_count.topic_metadata
        old_name = old_meta.name
        new_name = config.topic_mappings.get(old_name)
        if new_name is None:
            new_name = old_name
        new_meta = rosbag2_py.TopicMetadata(
            name=new_name,
            type=old_meta.type,
            serialization_format=old_meta.serialization_format,
            offered_qos_profiles=old_meta.offered_qos_profiles,
        )
        writer.create_topic(new_meta)
        topic_name_map[old_name] = new_name

    processed_messages = {topic: 0 for topic in config.topic_mappings}

    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()
        writer.write(topic_name_map[topic_name], data, timestamp)
        if topic_name in processed_messages:
            processed_messages[topic_name] += 1

    incomplete = [topic for topic, count in processed_messages.items() if count == 0]
    if incomplete:
        raise RuntimeError(f"未能重命名以下 topic 的任何消息: {', '.join(incomplete)}")

    logger.info('成功写出新的 bag: %s', config.output_bag)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Rename topics inside a rosbag2 recording.')
    parser.add_argument('--config', type=str, help='YAML 配置文件路径')
    parser.add_argument('--input-bag', type=str, help='输入 bag 路径')
    parser.add_argument('--output-bag', type=str, help='输出 bag 路径 (可选)')
    parser.add_argument(
        '--topic-map',
        action='append',
        default=[],
        help="old_topic:=new_topic 格式，可重复指定，多条时后者覆盖前者",
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
        rename_topics(config)
    except ConfigError as exc:
        logger.error('%s', exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.exception('执行失败: %s', exc)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
