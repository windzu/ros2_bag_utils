"""Shared utilities for ros2_bag_utils tools."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import yaml


class ConfigError(Exception):
    """Raised when user-facing configuration is invalid."""


def ensure_logging_configured() -> None:
    """Configure root logging if no handlers are present."""
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def parse_topic_map_arguments(pairs: Iterable[str], separator: str = ':=') -> Dict[str, str]:
    """Parse CLI style topic mappings such as ``/topic:=new_value``."""
    mapping: Dict[str, str] = {}
    for raw in pairs:
        if separator not in raw:
            raise ConfigError(f"Topic mapping '{raw}' 缺少 '{separator}' 分隔符")
        key, value = raw.split(separator, 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ConfigError(f"Topic mapping '{raw}' 含空的 topic 或目标值")
        mapping[key] = value
    return mapping


def load_yaml_config(path: Optional[Path]) -> Dict[str, object]:
    """Load YAML config from *path* and return a dictionary."""
    if path is None:
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


def resolve_output_uri(
    input_uri: Path,
    desired_output: Optional[Path],
    *,
    suffix: str,
) -> Path:
    """Return the output URI, auto-generating one with *suffix* if needed."""
    if desired_output:
        return desired_output

    base_path = input_uri
    if base_path.suffix:
        base_path = base_path.with_suffix('')

    parent = base_path.parent
    base_name = base_path.name
    candidate = parent / f"{base_name}{suffix}"
    counter = 1
    while candidate.exists():
        candidate = parent / f"{base_name}{suffix}_{counter}"
        counter += 1
    return candidate
