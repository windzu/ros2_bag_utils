"""Tools for rewriting frame_id fields inside rosbag2 recordings."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, Iterable, List, Optional

import yaml

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when user-facing configuration is invalid."""


@dataclass(frozen=True)
class RewriteConfig:
    """Resolved configuration for a rewrite operation."""

    input_bag: Path
    output_bag: Path
    topic_mappings: Dict[str, str]


def _setup_logging() -> None:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def parse_topic_map_arguments(pairs: Iterable[str]) -> Dict[str, str]:
    """Parse CLI style topic:=frame_id pairs."""
    mapping: Dict[str, str] = {}
    for raw in pairs:
        if ':=' not in raw:
            raise ConfigError(f"Topic mapping '{raw}' 缺少 ':=' 分隔符")
        topic, frame = raw.split(':=', 1)
        topic = topic.strip()
        frame = frame.strip()
        if not topic or not frame:
            raise ConfigError(f"Topic mapping '{raw}' 含空的 topic 或 frame_id")
        mapping[topic] = frame
    return mapping


def load_yaml_config(path: Path) -> Dict[str, object]:
    if not path:
        return {}
    if not path.exists():
        raise ConfigError(f"配置文件 {path} 不存在")
    try:
        data = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"解析 YAML 失败: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError('配置文件的根节点必须是字典')
    return data


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


def resolve_output_uri(input_uri: Path, desired_output: Optional[Path]) -> Path:
    if desired_output:
        return desired_output

    base_path = input_uri
    if base_path.suffix:
        base_path = base_path.with_suffix('')

    suffix = '_frameid_fix'
    parent = base_path.parent
    base_name = base_path.name
    candidate = parent / f"{base_name}{suffix}"
    counter = 1
    while candidate.exists():
        candidate = parent / f"{base_name}{suffix}_{counter}"
        counter += 1
    return candidate


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
        else resolve_output_uri(input_bag, None)
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

    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    config.output_bag.parent.mkdir(parents=True, exist_ok=True)
    writer = rosbag2_py.SequentialWriter()
    writer_storage_options = rosbag2_py.StorageOptions(uri=str(config.output_bag), storage_id=storage_id)
    writer_converter_options = rosbag2_py.ConverterOptions('', '')
    writer.open(writer_storage_options, writer_converter_options)

    for topic_meta in topics_by_name.values():
        writer.create_topic(topic_meta)

    type_cache: Dict[str, object] = {}
    processed_messages = {topic: 0 for topic in config.topic_mappings}

    while reader.has_next():
        topic_name, data, timestamp = reader.read_next()
        if topic_name in config.topic_mappings:
            if topic_name not in type_cache:
                try:
                    type_cache[topic_name] = get_message(topics_by_name[topic_name].type)
                except (AttributeError, ModuleNotFoundError, ValueError) as exc:
                    raise RuntimeError(
                        f"无法加载消息类型 '{topics_by_name[topic_name].type}' 用于 topic '{topic_name}'"
                    ) from exc
            msg_type = type_cache[topic_name]
            message = deserialize_message(data, msg_type)
            header = getattr(message, 'header', None)
            if header is None or not hasattr(header, 'frame_id'):
                raise RuntimeError(f"topic '{topic_name}' 的消息缺少可修改的 header.frame_id")
            header.frame_id = config.topic_mappings[topic_name]
            data = serialize_message(message)
            processed_messages[topic_name] += 1
        writer.write(topic_name, data, timestamp)

    incomplete = [topic for topic, count in processed_messages.items() if count == 0]
    if incomplete:
        raise RuntimeError(f"未能重写以下 topic 的任何消息: {', '.join(incomplete)}")

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
    _setup_logging()
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
