from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch.actions import ExecuteProcess


def _generate_process(context):
    config_file = LaunchConfiguration('config_file').perform(context)
    input_bag = LaunchConfiguration('input_bag').perform(context)
    output_bag = LaunchConfiguration('output_bag').perform(context)
    topic_maps = LaunchConfiguration('topic_map').perform(context)

    cmd = ['ros2', 'run', 'ros2_bag_utils', 'rewrite_frame_id']

    if config_file:
        cmd.extend(['--config', config_file])
    if input_bag:
        cmd.extend(['--input-bag', input_bag])
    if output_bag:
        cmd.extend(['--output-bag', output_bag])

    if topic_maps:
        for mapping in topic_maps.split(';'):
            mapping = mapping.strip()
            if mapping:
                cmd.extend(['--topic-map', mapping])

    return [ExecuteProcess(cmd=cmd, output='screen')]


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            'config_file',
            default_value='',
            description='Path to YAML configuration file.'
        ),
        DeclareLaunchArgument(
            'input_bag',
            default_value='',
            description='Input rosbag URI (overrides YAML).'
        ),
        DeclareLaunchArgument(
            'output_bag',
            default_value='',
            description='Output rosbag URI (overrides YAML).'
        ),
        DeclareLaunchArgument(
            'topic_map',
            default_value='',
            description='Semicolon-separated list of topic:=frame_id pairs overriding YAML mappings.'
        ),
        OpaqueFunction(function=_generate_process)
    ])
