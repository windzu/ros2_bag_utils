from pathlib import Path
from typing import Dict

import pytest

from ros2_bag_utils.common import ConfigError
from ros2_bag_utils.rename_topic import (
    build_effective_config,
    validate_mappings_against_topics,
)


def test_build_effective_config_merge_and_override(tmp_path: Path):
    input_bag = tmp_path / 'input_bag'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'output_bag': str(tmp_path / 'yaml_output'),
        'mappings': {'/old1': '/new1'},
    }
    cli_topic_map = {'/old2': '/new2', '/old1': '/new1_override'}

    config = build_effective_config(
        yaml_config=yaml_cfg,
        cli_input_bag=None,
        cli_output_bag=str(tmp_path / 'cli_output'),
        cli_topic_map=cli_topic_map,
    )

    assert config.input_bag == input_bag.resolve()
    assert config.output_bag == (tmp_path / 'cli_output').resolve()
    assert config.topic_mappings == {'/old1': '/new1_override', '/old2': '/new2'}


def test_build_effective_config_auto_output_suffix(tmp_path: Path):
    input_db = tmp_path / 'session_0.db3'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_db),
        'mappings': {'/old': '/new'},
    }

    config = build_effective_config(yaml_cfg, None, None, {})

    assert config.output_bag == (tmp_path / 'session_0_topic_renamed')


@pytest.mark.parametrize(
    'yaml_mappings',
    [
        {'/dup1': '/dup_target', '/dup2': '/dup_target'},
        {'/same': '/same'},
    ],
)
def test_build_effective_config_rejects_invalid_mappings(tmp_path: Path, yaml_mappings: Dict[str, str]):
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(tmp_path / 'input_bag'),
        'mappings': yaml_mappings,
    }
    with pytest.raises(ConfigError):
        build_effective_config(yaml_cfg, None, None, {})


def test_validate_mappings_against_topics_success():
    mappings = {'/scan': '/scan_filtered'}
    existing = ['/scan', '/camera']
    message_counts = {'/scan': 5, '/camera': 10}

    validate_mappings_against_topics(mappings, existing, message_counts)


def test_validate_mappings_missing_topic():
    mappings = {'/missing': '/new_topic'}
    existing = ['/scan']
    message_counts = {'/scan': 3}

    with pytest.raises(ConfigError):
        validate_mappings_against_topics(mappings, existing, message_counts)


def test_validate_mappings_no_messages():
    mappings = {'/scan': '/scan_filtered'}
    existing = ['/scan']
    message_counts = {'/scan': 0}

    with pytest.raises(ConfigError):
        validate_mappings_against_topics(mappings, existing, message_counts)


def test_validate_mappings_conflicting_target():
    mappings = {'/scan': '/camera'}
    existing = ['/scan', '/camera']
    message_counts = {'/scan': 5, '/camera': 1}

    with pytest.raises(ConfigError):
        validate_mappings_against_topics(mappings, existing, message_counts)
