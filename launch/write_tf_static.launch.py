from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, ExecuteProcess, OpaqueFunction
from launch.substitutions import LaunchConfiguration


def _generate_process(context):
    config_file = LaunchConfiguration('config_file').perform(context)
    input_bag = LaunchConfiguration('input_bag').perform(context)
    output_bag = LaunchConfiguration('output_bag').perform(context)

    cmd = ['ros2', 'run', 'ros2_bag_utils', 'write_tf_static']

    if config_file:
        cmd.extend(['--config', config_file])
    if input_bag:
        cmd.extend(['--input-bag', input_bag])
    if output_bag:
        cmd.extend(['--output-bag', output_bag])

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
        OpaqueFunction(function=_generate_process)
    ])