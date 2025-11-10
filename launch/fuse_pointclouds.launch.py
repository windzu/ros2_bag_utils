from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, ExecuteProcess, OpaqueFunction
from launch.substitutions import LaunchConfiguration


def _generate_process(context):
    config_file = LaunchConfiguration("config_file").perform(context)

    cmd = ["ros2", "run", "ros2_bag_utils", "fuse_pointclouds"]

    if config_file:
        cmd.extend(["--config", config_file])

    return [ExecuteProcess(cmd=cmd, output="screen")]


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "config_file",
                default_value="",
                description="Path to YAML configuration file for point cloud fusion.",
            ),
            OpaqueFunction(function=_generate_process),
        ]
    )
