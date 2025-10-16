import pytest

from ros2_bag_utils import rewrite_frame_id as rewrite_frame_id_module

_register_frame_mapping = rewrite_frame_id_module._register_frame_mapping  # type: ignore[attr-defined]
_rewrite_tf_static_message = rewrite_frame_id_module._rewrite_tf_static_message  # type: ignore[attr-defined]


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
    def __init__(self, transforms) -> None:
        self.transforms = transforms


def test_register_frame_mapping_records_and_validates():
    frame_mapping = {}
    topic_frames = {}

    _register_frame_mapping(
        frame_mapping,
        topic_frames,
        topic_name='/lidar',
        original_frame_id='old_lidar',
        target_frame_id='new_lidar',
    )

    assert topic_frames['/lidar'] == 'old_lidar'
    assert frame_mapping['old_lidar'] == 'new_lidar'

    # 重复注册同一 topic 的相同 frame 应当允许
    _register_frame_mapping(
        frame_mapping,
        topic_frames,
        topic_name='/lidar',
        original_frame_id='old_lidar',
        target_frame_id='new_lidar',
    )


def test_register_frame_mapping_topic_frame_conflict():
    frame_mapping = {}
    topic_frames = {}

    _register_frame_mapping(
        frame_mapping,
        topic_frames,
        topic_name='/lidar',
        original_frame_id='frame_a',
        target_frame_id='new_lidar',
    )

    with pytest.raises(RuntimeError):
        _register_frame_mapping(
            frame_mapping,
            topic_frames,
            topic_name='/lidar',
            original_frame_id='frame_b',
            target_frame_id='new_lidar',
        )


def test_register_frame_mapping_old_frame_conflict():
    frame_mapping = {}
    topic_frames = {}

    _register_frame_mapping(
        frame_mapping,
        topic_frames,
        topic_name='/lidar_a',
        original_frame_id='frame_shared',
        target_frame_id='lidar_a',
    )

    with pytest.raises(RuntimeError):
        _register_frame_mapping(
            frame_mapping,
            topic_frames,
            topic_name='/lidar_b',
            original_frame_id='frame_shared',
            target_frame_id='lidar_b',
        )


def test_rewrite_tf_static_message_rewrites_parent_and_child():
    message = _DummyTfMessage(
        [
            _DummyTransform('frame_a', 'child_a'),
            _DummyTransform('frame_b', 'child_b'),
        ]
    )

    replacements, edges_before, edges_after = _rewrite_tf_static_message(
        message,
        {
            'frame_a': 'frame_a_new',
            'child_b': 'child_b_new',
        },
    )

    assert replacements == 2
    assert edges_before == {('frame_a', 'child_a'), ('frame_b', 'child_b')}
    assert edges_after == {('frame_a_new', 'child_a'), ('frame_b', 'child_b_new')}
    assert message.transforms[0].header is not None
    assert message.transforms[0].header.frame_id == 'frame_a_new'
    assert message.transforms[0].child_frame_id == 'child_a'
    assert message.transforms[1].header is not None
    assert message.transforms[1].header.frame_id == 'frame_b'
    assert message.transforms[1].child_frame_id == 'child_b_new'


def test_rewrite_tf_static_message_missing_transforms():
    class _InvalidMessage:
        pass

    with pytest.raises(RuntimeError):
        _rewrite_tf_static_message(_InvalidMessage(), {'frame_a': 'frame_a_new'})


def test_rewrite_tf_static_message_missing_child():
    message = _DummyTfMessage([_DummyTransform('frame', 'child', with_child=False)])

    with pytest.raises(RuntimeError):
        _rewrite_tf_static_message(message, {'frame': 'frame_new'})


def test_rewrite_tf_static_message_missing_header():
    message = _DummyTfMessage([_DummyTransform('frame', 'child', with_header=False)])

    with pytest.raises(RuntimeError):
        _rewrite_tf_static_message(message, {'frame': 'frame_new'})
