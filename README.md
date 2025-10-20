# ros2_bag_utils

提供针对 ROS 2 bag 的实用工具，包含 frame_id 重写、topic 重命名、tf_static 写入等功能。

## 功能简介

* **rewrite_frame_id**：根据配置，将已录制 rosbag2 中指定 topic 的 `header.frame_id` 批量替换，并生成新的 bag。
* **rewrite_tf_static_frame_id**：对 `/tf_static` topic 中的 frame 名称进行批量改写，并生成新的 bag。
* **rename_topic**：根据配置，将已录制 rosbag2 中的 topic 名称进行重命名，并生成新的 bag。
* **write_tf_static**：根据外参配置文件，向 rosbag2 中写入 tf_static 变换信息。
* **filter_pointcloud_xyzi**：将指定 PointCloud2 topic 的点云字段裁剪为常见的 `x/y/z/intensity` 四个字段。
* **export_assets**：解析 rosbag2 中的点云、图像、/tf_static，并导出为 PCD/PNG/YAML 等通用格式。

## 安装与构建

```bash
colcon build --packages-select ros2_bag_utils
source install/setup.bash
```

## rewrite_frame_id

### 配置文件格式（rewrite_frame_id）

示例：`config/rewrite_frame_id.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀
mappings:
  /example/topic: base_link
  /another/topic: camera_link
```

### 命令行使用（rewrite_frame_id）

```bash
ros2 run ros2_bag_utils rewrite_frame_id --config /path/to/config.yaml
```

命令行参数可覆盖配置文件：

* `--input-bag`：输入 bag 目录
* `--output-bag`：输出 bag 目录（可选）
* `--topic-map topic:=frame`：可重复指定，覆盖原配置

### Launch 调用（rewrite_frame_id）

```bash
ros2 launch ros2_bag_utils rewrite_frame_id.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag \
  topic_map:="/topic:=new_frame;/topic2:=map"
```

### 行为规范（rewrite_frame_id）

* 如配置中的 topic 不存在或没有消息，将直接报错退出。
* 如消息缺少 `header.frame_id` 字段，同样报错。
* 如果 bag 中存在 `/tf_static`，且变换中的 parent/child frame 命中被替换的旧 frame，会自动同步改写，保持 tf 树一致。
* 当检测到同一 topic 的消息使用不同的 `frame_id`，或同一旧 frame 被要求映射到多个新 frame 时会报错提示。
* 输出 bag 与输入 bag 采用相同的 storage/serialization 插件，并保存到新的目录中。

## rewrite_tf_static_frame_id

### 配置文件格式（rewrite_tf_static_frame_id）

示例：`config/rewrite_tf_static_frame_id.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀
frames:
  old_frame_a: new_frame_a
  old_frame_b: new_frame_b
```

### 命令行使用（rewrite_tf_static_frame_id）

```bash
ros2 run ros2_bag_utils rewrite_tf_static_frame_id --config /path/to/config.yaml
```

命令行参数可覆盖配置文件：

* `--input-bag`：输入 bag 目录
* `--output-bag`：输出 bag 目录（可选）
* `--frame-map old:=new`：可重复指定，覆盖原配置

### Launch 调用（rewrite_tf_static_frame_id）

```bash
ros2 launch ros2_bag_utils rewrite_tf_static_frame_id.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag \
  frame_map:="old:=new;foo:=bar"
```

### 行为规范（rewrite_tf_static_frame_id）

* 仅对 `/tf_static` topic 进行处理，其余 topic 原样拷贝。
* 同时改写 `TransformStamped` 的 `header.frame_id`（parent）与 `child_frame_id`。
* 当旧 frame 在 bag 中未出现时给出警告但不会中断，仍会输出新 bag。
* 改写成功后输出改写统计，包括映射列表及修改前后的 tf 连线摘要。

## rename_topic

### 配置文件格式（rename_topic）

示例：`config/rename_topic.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀
mappings:
  /old/topic: /new/topic
  /another/old: /another/new
```

### 命令行使用（rename_topic）

```bash
ros2 run ros2_bag_utils rename_topic --config /path/to/config.yaml
```

命令行参数可覆盖配置文件：

* `--input-bag`：输入 bag 目录
* `--output-bag`：输出 bag 目录（可选）
* `--topic-map old:=new`：可重复指定，覆盖原配置

### Launch 调用（rename_topic）

```bash
ros2 launch ros2_bag_utils rename_topic.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag \
  topic_map:="/old:=/new;/foo:=/bar"
```

### 行为规范（rename_topic）

* 若旧 topic 不存在或无消息，将直接报错终止。
* 若新 topic 已存在于原 bag 或多个旧 topic 指向同一新 topic，同样报错。
* 输出 bag 使用与输入相同的 storage/serialization 插件并写入新目录，metadata 中的 topic 名称会同步更新。

## write_tf_static

### 配置文件格式（write_tf_static）

示例：`config/write_tf_static.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀

lidars:  # 可选，存在则处理
  lidar0:
    topic: "/sensing/lidar/lidar0/pointcloud_raw"
    extrinsics: "/absolute/path/to/lidar0_extrinsics.yaml"
  lidar1:
    topic: "/sensing/lidar/lidar1/pointcloud_raw"
    extrinsics: "/absolute/path/to/lidar1_extrinsics.yaml"

cameras:  # 可选，存在则处理
  cam0:
    topic: "/sensing/camera/cam0/image_ptr"
    extrinsics: "/absolute/path/to/cam0_extrinsics.yaml"
```

外参文件格式：

```yaml
parent_frame: base_link
child_frame: lidar_lidar0
transform:
  translation:
    x: 1.250
    y: 0.000
    z: 1.600
  rotation_quaternion:
    x: 0.000
    y: 0.000
    z: 0.000
    w: 1.000
```

### 命令行使用（write_tf_static）

```bash
ros2 run ros2_bag_utils write_tf_static --config /path/to/config.yaml
```

### Launch 调用（write_tf_static）

```bash
ros2 launch ros2_bag_utils write_tf_static.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag
```

### 行为规范（write_tf_static）

* 外参文件不存在或格式错误时发出警告并跳过该设备，不会终止程序。
* 检查 frame 名称是否符合 ROS 规范，不规范时跳过。
* 若原 bag 已存在 `/tf_static` topic，会发出警告并合并变换。
* 使用原 bag 第一条消息的时间戳作为 tf_static 的时间戳。

## filter_pointcloud_xyzi

### 配置文件格式（filter_pointcloud_xyzi）

示例：`config/filter_pointcloud_xyzi.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀
topics:
  - /sensing/lidar/front/pointcloud_raw
  - /sensing/lidar/rear/pointcloud_raw
```

### 命令行使用（filter_pointcloud_xyzi）

```bash
ros2 run ros2_bag_utils filter_pointcloud_xyzi --config /path/to/config.yaml
```

命令行参数可覆盖配置文件：

* `--input-bag`：输入 bag 目录
* `--output-bag`：输出 bag 目录（可选）
* `--topic /foo/points`：可重复指定，覆盖原配置

### Launch 调用（filter_pointcloud_xyzi）

```bash
ros2 launch ros2_bag_utils filter_pointcloud_xyzi.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag \
  topics:="/topic_a;/topic_b"
```

### 行为规范（filter_pointcloud_xyzi）

* 仅当目标 topic 的所有消息同时包含 `x`、`y`、`z`、`intensity` 且类型为 `float32` 时会执行裁剪。
* 如任意目标 topic 的任意消息缺失上述字段或字段类型不符合要求，将立即报错并终止。
* 输出 bag 与输入 bag 使用相同的 storage/serialization 插件，并写入新的目录，目标 topic 的消息仅保留 `xyzi` 字段。

## export_assets

### 配置文件格式（export_assets）

示例：`config/export_assets.example.yaml`

```yaml
input_bag: /path/to/rosbag2_directory
output_root: /path/to/export_root  # 可选，缺省时自动生成 *_exported
base_frame: base_link              # 可选，外参统一到的基准坐标系

lidar:
  topics:
    - /sensing/lidar/front/points_raw
  output_subdir: lidar

camera:
  topics:
    - /sensing/camera/front/image_raw
  output_subdir: camera

tf_static:
  enabled: true
  output_subdir: calib

filename_time_format: "%Y%m%dT%H%M%S_%f"
overwrite: false
```

### 命令行使用（export_assets）

```bash
ros2 run ros2_bag_utils export_assets --config /path/to/config.yaml
```

常用覆盖参数：

* `--input-bag`：输入 bag 目录
* `--output-root`：导出根目录
* `--base-frame`：外参统一到的基准 frame
* `--lidar-topic /topic`：可重复指定点云 topic
* `--camera-topic /topic`：可重复指定图像 topic
* `--disable-tf-static`：跳过外参导出
* `--overwrite`：若输出目录已存在则清空后重建

### Launch 调用（export_assets）

```bash
ros2 launch ros2_bag_utils export_assets.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/path/to/bag \
  output_root:=/path/to/export \
  lidar_topics:="/lidar_a;/lidar_b" \
  camera_topics:="/cam/image" \
  overwrite:=true
```

### 行为规范（export_assets）

* 自动识别 `sensor_msgs/msg/PointCloud2`、`sensor_msgs/msg/Image`、`sensor_msgs/msg/CompressedImage` 以及 `/tf_static`。
* 点云导出为 `.pcd`，采用 PCD v0.7 `binary_compressed` 格式并保留全部字段。
* 图像统一保存为 `.png`，原始图像根据编码解码，压缩图像先解压再保存。
* `tf_static` 将尝试构建到 `base_frame` 的变换链，缺失时给出警告；如 bag 中不存在 `base_frame`，会报错并停止写入外参。
* 点云、图像分别按 `frame_id` 归档到 `lidar/`、`camera/` 子目录，每帧以采样时间命名；外参写入 `calib/` 下 `frame_extrinsics.yaml` 文件。

## 测试

```bash
colcon test --packages-select ros2_bag_utils
```
