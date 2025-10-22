from pathlib import Path

import pytest

from ros2_bag_utils.common import ConfigError
from ros2_bag_utils.export_assets import build_effective_config


def _call_build(tmp_path: Path, yaml_cfg: dict, **overrides):
    """Helper to call build_effective_config with default CLI overrides."""
    input_dir = tmp_path / 'bag'
    input_dir.mkdir(exist_ok=True)
    yaml_cfg.setdefault('input_bag', str(input_dir))

    return build_effective_config(
        yaml_config=yaml_cfg,
        cli_input_bag=overrides.get('cli_input_bag'),
        cli_output_root=overrides.get('cli_output_root'),
        cli_base_frame=overrides.get('cli_base_frame'),
        cli_lidar_topics=overrides.get('cli_lidar_topics', []),
        cli_camera_topics=overrides.get('cli_camera_topics', []),
        cli_disable_tf_static=overrides.get('cli_disable_tf_static', False),
        cli_overwrite=overrides.get('cli_overwrite', False),
        cli_lidar_subdir=overrides.get('cli_lidar_subdir'),
        cli_camera_subdir=overrides.get('cli_camera_subdir'),
        cli_calib_subdir=overrides.get('cli_calib_subdir'),
        cli_filename_style=overrides.get('cli_filename_style'),
        cli_time_source=overrides.get('cli_time_source'),
        cli_time_format=overrides.get('cli_time_format'),
    )


def test_defaults_use_header_ns(tmp_path: Path):
    config = _call_build(tmp_path, {})

    assert config.filename_style == 'ns'
    assert config.filename_time_source == 'header'
    # 默认为纳秒命名时仍保留默认格式字符串
    assert config.filename_time_format == '%Y%m%dT%H%M%S_%f'


def test_datetime_style_requires_format(tmp_path: Path):
    yaml_cfg = {
        'filename_style': 'datetime',
        'filename_time_format': '%Y-%m-%d_%H:%M:%S',
        'filename_time_source': 'bag',
    }

    config = _call_build(tmp_path, yaml_cfg)

    assert config.filename_style == 'datetime'
    assert config.filename_time_source == 'bag'
    assert config.filename_time_format == '%Y-%m-%d_%H:%M:%S'


def test_invalid_filename_style(tmp_path: Path):
    with pytest.raises(ConfigError):
        _call_build(tmp_path, {'filename_style': 'invalid-style'})
