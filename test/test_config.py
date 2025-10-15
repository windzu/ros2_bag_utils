from pathlib import Path
from typing import Dict

import pytest

from ros2_bag_utils.rewrite_frame_id import (
    ConfigError,
    build_effective_config,
    parse_topic_map_arguments,
    resolve_output_uri,
)


def test_parse_topic_map_arguments_success():
    mapping = parse_topic_map_arguments(['/laser:=base_laser', '/camera:=camera_link'])
    assert mapping == {'/laser': 'base_laser', '/camera': 'camera_link'}


def test_parse_topic_map_arguments_invalid_format():
    with pytest.raises(ConfigError):
        parse_topic_map_arguments(['invalid_format'])


def test_resolve_output_uri_generates_unique_path(tmp_path: Path):
    input_uri = tmp_path / 'bag1'
    candidate1 = resolve_output_uri(input_uri, None)
    assert candidate1 == tmp_path / 'bag1_frameid_fix'

    candidate1.mkdir()
    candidate2 = resolve_output_uri(input_uri, None)
    assert candidate2 == tmp_path / 'bag1_frameid_fix_1'


def test_resolve_output_uri_strips_known_suffix(tmp_path: Path):
    input_dir = tmp_path / 'bags'
    input_dir.mkdir()
    input_uri = input_dir / 'session_0.db3'

    candidate = resolve_output_uri(input_uri, None)
    assert candidate == input_dir / 'session_0_frameid_fix'


def test_build_effective_config_merge(tmp_path: Path):
    input_bag = tmp_path / 'input_bag'
    yaml_cfg = {
        'input_bag': str(input_bag),
        'output_bag': str(tmp_path / 'output_from_yaml'),
        'mappings': {'/topic1': 'frame_a'},
    }
    cli_topic_map = {'/topic2': 'frame_b', '/topic1': 'frame_override'}

    config = build_effective_config(
        yaml_config=yaml_cfg,
        cli_input_bag=None,
        cli_output_bag=str(tmp_path / 'output_cli'),
        cli_topic_map=cli_topic_map,
    )

    assert config.input_bag == input_bag.resolve()
    assert config.output_bag == (tmp_path / 'output_cli').resolve()
    assert config.topic_mappings['/topic1'] == 'frame_override'
    assert config.topic_mappings['/topic2'] == 'frame_b'


def test_build_effective_config_requires_mappings(tmp_path: Path):
    yaml_cfg: Dict[str, object] = {'input_bag': str(tmp_path / 'input_bag')}
    with pytest.raises(ConfigError):
        build_effective_config(yaml_cfg, None, None, {})


def test_build_effective_config_rejects_non_dict_mappings(tmp_path: Path):
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(tmp_path / 'input_bag'),
        'mappings': ['invalid'],
    }
    with pytest.raises(ConfigError):
        build_effective_config(yaml_cfg, None, None, {})
