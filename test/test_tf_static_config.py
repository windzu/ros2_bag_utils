from pathlib import Path
from typing import Dict, List

import pytest

from ros2_bag_utils.common import ConfigError
from ros2_bag_utils.write_tf_static import (
    TransformConfig,
    _load_extrinsics_file,
    _validate_frame_name,
    build_effective_config,
)


def test_validate_frame_name_valid():
    assert _validate_frame_name('base_link')
    assert _validate_frame_name('lidar_lidar0')
    assert _validate_frame_name('camera/left')
    assert _validate_frame_name('ns/frame_id')


def test_validate_frame_name_invalid():
    assert not _validate_frame_name('')
    assert not _validate_frame_name('/leading_slash')
    assert not _validate_frame_name('trailing_slash/')
    assert not _validate_frame_name('double//slash')
    assert not _validate_frame_name('space frame')
    assert not _validate_frame_name('frame-dash')


def test_load_extrinsics_file_success(tmp_path: Path):
    extrinsics_content = """
parent_frame: base_link
child_frame: lidar_lidar0
transform:
  translation:
    x: 1.25
    y: 0.0
    z: 1.6
  rotation_quaternion:
    x: 0.0
    y: 0.0
    z: 0.0
    w: 1.0
meta:
  calibrated_at: "2025-09-25T00:00:00Z"
"""
    extrinsics_file = tmp_path / 'test_extrinsics.yaml'
    extrinsics_file.write_text(extrinsics_content)
    
    result = _load_extrinsics_file(extrinsics_file)
    
    assert result is not None
    assert result.parent_frame == 'base_link'
    assert result.child_frame == 'lidar_lidar0'
    assert result.translation == (1.25, 0.0, 1.6)
    assert result.rotation_quaternion == (0.0, 0.0, 0.0, 1.0)


def test_load_extrinsics_file_missing():
    result = _load_extrinsics_file(Path('/nonexistent/path.yaml'))
    assert result is None


def test_load_extrinsics_file_invalid_frame_names(tmp_path: Path):
    extrinsics_content = """
parent_frame: "bad frame name"
child_frame: lidar_lidar0
transform:
  translation: {x: 0, y: 0, z: 0}
  rotation_quaternion: {x: 0, y: 0, z: 0, w: 1}
"""
    extrinsics_file = tmp_path / 'bad_frame.yaml'
    extrinsics_file.write_text(extrinsics_content)
    
    result = _load_extrinsics_file(extrinsics_file)
    assert result is None


def test_build_effective_config_no_sensors(tmp_path: Path):
    input_bag = tmp_path / 'input_bag'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
    }
    
    config = build_effective_config(yaml_cfg, None, None)
    
    assert config.input_bag == input_bag.resolve()
    assert config.transforms == []
    assert config.output_bag == (tmp_path / 'input_bag_tf_static_added')


def test_build_effective_config_with_lidars(tmp_path: Path):
    # Create mock extrinsics file
    extrinsics_file = tmp_path / 'lidar0.yaml'
    extrinsics_content = """
parent_frame: base_link
child_frame: lidar_lidar0
transform:
  translation: {x: 1.0, y: 0.0, z: 2.0}
  rotation_quaternion: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}
"""
    extrinsics_file.write_text(extrinsics_content)
    
    input_bag = tmp_path / 'input_bag'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'lidars': {
            'lidar0': {
                'topic': '/lidar0/points',
                'extrinsics': str(extrinsics_file),
            }
        }
    }
    
    config = build_effective_config(yaml_cfg, None, None)
    
    assert len(config.transforms) == 1
    transform = config.transforms[0]
    assert transform.parent_frame == 'base_link'
    assert transform.child_frame == 'lidar_lidar0'
    assert transform.translation == (1.0, 0.0, 2.0)


def test_build_effective_config_duplicate_child_frames(tmp_path: Path):
    # Create two extrinsics files with same child_frame
    for i in range(2):
        extrinsics_file = tmp_path / f'lidar{i}.yaml'
        extrinsics_content = f"""
parent_frame: base_link
child_frame: duplicate_frame  # Same child frame
transform:
  translation: {{x: {i}.0, y: 0.0, z: 0.0}}
  rotation_quaternion: {{x: 0.0, y: 0.0, z: 0.0, w: 1.0}}
"""
        extrinsics_file.write_text(extrinsics_content)
    
    input_bag = tmp_path / 'input_bag'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'lidars': {
            'lidar0': {'extrinsics': str(tmp_path / 'lidar0.yaml')},
            'lidar1': {'extrinsics': str(tmp_path / 'lidar1.yaml')},
        }
    }
    
    config = build_effective_config(yaml_cfg, None, None)
    
    # Only first transform should be kept due to duplicate child_frame
    assert len(config.transforms) == 1
    assert config.transforms[0].child_frame == 'duplicate_frame'
    assert config.transforms[0].translation == (0.0, 0.0, 0.0)


def test_build_effective_config_cameras_and_lidars(tmp_path: Path):
    # Create extrinsics files for both lidar and camera
    lidar_file = tmp_path / 'lidar.yaml'
    lidar_file.write_text("""
parent_frame: base_link
child_frame: lidar_frame
transform:
  translation: {x: 1.0, y: 0.0, z: 2.0}
  rotation_quaternion: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}
""")
    
    camera_file = tmp_path / 'camera.yaml'
    camera_file.write_text("""
parent_frame: base_link
child_frame: camera_frame
transform:
  translation: {x: 0.5, y: 0.1, z: 1.5}
  rotation_quaternion: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}
""")
    
    input_bag = tmp_path / 'input_bag'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'lidars': {
            'lidar0': {'extrinsics': str(lidar_file)},
        },
        'cameras': {
            'cam0': {'extrinsics': str(camera_file)},
        }
    }
    
    config = build_effective_config(yaml_cfg, None, None)
    
    assert len(config.transforms) == 2
    child_frames = {t.child_frame for t in config.transforms}
    assert child_frames == {'lidar_frame', 'camera_frame'}