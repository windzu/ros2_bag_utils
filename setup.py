from setuptools import setup

package_name = "ros2_bag_utils"

setup(
    name=package_name,
    version="0.1.0",
    packages=[package_name],
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml", "README.md"]),
        (
            "share/" + package_name + "/config",
            [
                "config/rewrite_frame_id.example.yaml",
                "config/rewrite_tf_static_frame_id.example.yaml",
                "config/rename_topic.example.yaml",
                "config/write_tf_static.example.yaml",
                "config/filter_pointcloud_xyzi.example.yaml",
                "config/export_assets.example.yaml",
            ],
        ),
        (
            "share/" + package_name + "/launch",
            [
                "launch/rewrite_frame_id.launch.py",
                "launch/rewrite_tf_static_frame_id.launch.py",
                "launch/rename_topic.launch.py",
                "launch/write_tf_static.launch.py",
                "launch/filter_pointcloud_xyzi.launch.py",
                "launch/export_assets.launch.py",
            ],
        ),
    ],
    install_requires=[
        "setuptools",
        "numpy",
        "Pillow",
        "rich",
        "python-lzf",
        "wind-pypcd",
    ],
    zip_safe=True,
    maintainer="Windzu",
    maintainer_email="windzu@example.com",
    description="Utilities for manipulating ROS 2 bag files.",
    license="Apache License 2.0",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "rewrite_frame_id = ros2_bag_utils.rewrite_frame_id:main",
            "rewrite_tf_static_frame_id = ros2_bag_utils.rewrite_tf_static_frame_id:main",
            "rename_topic = ros2_bag_utils.rename_topic:main",
            "write_tf_static = ros2_bag_utils.write_tf_static:main",
            "filter_pointcloud_xyzi = ros2_bag_utils.filter_pointcloud_xyzi:main",
            "export_assets = ros2_bag_utils.export_assets:main",
        ],
    },
)
