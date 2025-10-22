from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, ExecuteProcess, OpaqueFunction
from launch.substitutions import LaunchConfiguration


def _generate_process(context):
    config_file = LaunchConfiguration('config_file').perform(context)
    input_bag = LaunchConfiguration('input_bag').perform(context)
    output_root = LaunchConfiguration('output_root').perform(context)
    base_frame = LaunchConfiguration('base_frame').perform(context)
    lidar_topics = LaunchConfiguration('lidar_topics').perform(context)
    camera_topics = LaunchConfiguration('camera_topics').perform(context)
    disable_tf_static = LaunchConfiguration('disable_tf_static').perform(context)
    overwrite = LaunchConfiguration('overwrite').perform(context)

    cmd = ['ros2', 'run', 'ros2_bag_utils', 'export_assets']

    if config_file:
        cmd.extend(['--config', config_file])
    if input_bag:
        cmd.extend(['--input-bag', input_bag])
    if output_root:
        cmd.extend(['--output-root', output_root])
    if base_frame:
        cmd.extend(['--base-frame', base_frame])

    if lidar_topics:
        for topic in filter(None, (item.strip() for item in lidar_topics.split(';'))):
            cmd.extend(['--lidar-topic', topic])

    if camera_topics:
        for topic in filter(None, (item.strip() for item in camera_topics.split(';'))):
            cmd.extend(['--camera-topic', topic])

    if disable_tf_static.lower() in {'true', '1', 'yes'}:
        cmd.append('--disable-tf-static')

    if overwrite.lower() in {'true', '1', 'yes'}:
        cmd.append('--overwrite')

    return [ExecuteProcess(cmd=cmd, output='screen')]


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('config_file', default_value='', description='YAML 配置文件路径'),
        DeclareLaunchArgument('input_bag', default_value='', description='输入 rosbag2 路径'),
        DeclareLaunchArgument('output_root', default_value='', description='数据导出根目录'),
        DeclareLaunchArgument('base_frame', default_value='', description='外参对齐的基准坐标系'),
        DeclareLaunchArgument('lidar_topics', default_value='', description='以分号分隔的 PointCloud2 topic 列表'),
        DeclareLaunchArgument('camera_topics', default_value='', description='以分号分隔的图像 topic 列表'),
        DeclareLaunchArgument('disable_tf_static', default_value='false', description='设置为 true 时禁用外参导出'),
        DeclareLaunchArgument('overwrite', default_value='false', description='设置为 true 时覆盖已存在的输出目录'),
        OpaqueFunction(function=_generate_process),
    ])
