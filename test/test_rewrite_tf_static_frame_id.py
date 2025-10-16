from typing import Dict

import pytest

import ros2_bag_utils.rewrite_tf_static_frame_id as module


class _DummyHeader:
    def __init__(self, frame_id: str) -> None:
        self.frame_id = frame_id


class _DummyTransform:
    def __init__(self, parent: str, child: str, *, with_header: bool = True, with_child: bool = True) -> None:
        self.child_frame_id = child if with_child else None
        if with_header:
            self.header = _DummyHeader(parent)
        else:
            self.header = None


class _DummyTfMessage:
    def __init__(self, transforms):
        self.transforms = transforms


def test_build_effective_config_merges_cli(tmp_path):
    input_bag = tmp_path / 'input'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'output_bag': str(tmp_path / 'out_yaml'),
        'frames': {'frame_a': 'frame_b'},
    }

    config = module.build_effective_config(
        yaml_config=yaml_cfg,
        cli_input_bag=None,
        cli_output_bag=str(tmp_path / 'out_cli'),
        cli_frame_map=['frame_b:=frame_c'],
    )

    assert config.input_bag == input_bag.resolve()
    assert config.output_bag == (tmp_path / 'out_cli').resolve()
    assert config.frame_mappings == {'frame_a': 'frame_b', 'frame_b': 'frame_c'}


def test_build_effective_config_requires_frames(tmp_path):
    yaml_cfg: Dict[str, object] = {'input_bag': str(tmp_path / 'input')}
    with pytest.raises(module.ConfigError):
        module.build_effective_config(yaml_cfg, None, None, [])


def test_build_effective_config_conflicting_frames(tmp_path):
    input_bag = tmp_path / 'input'
    yaml_cfg: Dict[str, object] = {
        'input_bag': str(input_bag),
        'frames': {'frame_a': 'frame_b'},
    }

    with pytest.raises(module.ConfigError):
        module.build_effective_config(yaml_cfg, None, None, ['frame_a:=frame_c'])


def test_rewrite_tf_static_message_changes_parent_and_child():
    stats = module.TfStaticRewriteStats()
    message = _DummyTfMessage([
        _DummyTransform('frame_a', 'child_a'),
        _DummyTransform('frame_b', 'child_b'),
    ])

    module._rewrite_tf_static_message(  # type: ignore[attr-defined]
        message,
        {'frame_a': 'frame_a_new', 'child_b': 'child_b_new'},
        stats,
    )

    assert message.transforms[0].header is not None
    assert message.transforms[0].header.frame_id == 'frame_a_new'
    assert message.transforms[0].child_frame_id == 'child_a'
    assert message.transforms[1].header is not None
    assert message.transforms[1].header.frame_id == 'frame_b'
    assert message.transforms[1].child_frame_id == 'child_b_new'
    assert stats.messages_updated == 2
    assert stats.replacements == 2
    assert ('frame_a', 'child_a') in stats.touched_edges_before
    assert ('frame_a_new', 'child_a') in stats.touched_edges_after


def test_rewrite_tf_static_message_missing_transforms():
    stats = module.TfStaticRewriteStats()

    class _InvalidMessage:
        pass

    with pytest.raises(RuntimeError):
        module._rewrite_tf_static_message(_InvalidMessage(), {'a': 'b'}, stats)  # type: ignore[attr-defined]


def test_rewrite_tf_static_message_missing_header():
    stats = module.TfStaticRewriteStats()
    message = _DummyTfMessage([_DummyTransform('frame', 'child', with_header=False)])

    with pytest.raises(RuntimeError):
        module._rewrite_tf_static_message(message, {'frame': 'new'}, stats)  # type: ignore[attr-defined]


def test_rewrite_tf_static_message_missing_child():
    stats = module.TfStaticRewriteStats()
    message = _DummyTfMessage([_DummyTransform('frame', 'child', with_child=False)])

    with pytest.raises(RuntimeError):
        module._rewrite_tf_static_message(message, {'frame': 'new'}, stats)  # type: ignore[attr-defined]
