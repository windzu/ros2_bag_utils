from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, ExecuteProcess, OpaqueFunction
from launch.substitutions import LaunchConfiguration


def _generate_process(context):
    config_file = LaunchConfiguration('config_file').perform(context)
    input_bag = LaunchConfiguration('input_bag').perform(context)
    output_bag = LaunchConfiguration('output_bag').perform(context)
    topics = LaunchConfiguration('topics').perform(context)

    cmd = ['ros2', 'run', 'ros2_bag_utils', 'filter_pointcloud_xyzi']

    if config_file:
        cmd.extend(['--config', config_file])
    if input_bag:
        cmd.extend(['--input-bag', input_bag])
    if output_bag:
        cmd.extend(['--output-bag', output_bag])

    if topics:
        for topic in topics.split(';'):
            topic = topic.strip()
            if topic:
                cmd.extend(['--topic', topic])

    return [ExecuteProcess(cmd=cmd, output='screen')]


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            'config_file',
            default_value='',
            description='YAML 配置文件路径'
        ),
        DeclareLaunchArgument(
            'input_bag',
            default_value='',
            description='输入 rosbag 路径 (覆盖 YAML)'
        ),
        DeclareLaunchArgument(
            'output_bag',
            default_value='',
            description='输出 rosbag 路径 (覆盖 YAML)'
        ),
        DeclareLaunchArgument(
            'topics',
            default_value='',
            description='使用分号分隔的 topic 列表，覆盖 YAML topics'
        ),
        OpaqueFunction(function=_generate_process),
    ])
