# ros2_bag_utils

提供针对 ROS 2 bag 的实用工具，目前包含 `frame_id` 重写功能。

## 功能简介

* **rewrite_frame_id**：根据配置，将已录制 rosbag2 中指定 topic 的 `header.frame_id` 批量替换，并生成新的 bag。

## 安装与构建

```bash
colcon build --packages-select ros2_bag_utils
source install/setup.bash
```

## 配置文件格式

示例：`config/rewrite_frame_id.example.yaml`

```yaml
input_bag: /path/to/original_bag
output_bag: /path/to/output_bag  # 可选，缺省时自动追加后缀
mappings:
  /example/topic: base_link
  /another/topic: camera_link
```

## 命令行使用

```bash
ros2 run ros2_bag_utils rewrite_frame_id --config /path/to/config.yaml
```

命令行参数可覆盖配置文件：

* `--input-bag`：输入 bag 目录
* `--output-bag`：输出 bag 目录（可选）
* `--topic-map topic:=frame`：可重复指定，覆盖原配置

## Launch 调用

```bash
ros2 launch ros2_bag_utils rewrite_frame_id.launch.py \
  config_file:=/path/to/config.yaml \
  input_bag:=/override/input/bag \
  topic_map:="/topic:=new_frame;/topic2:=map"
```

## 行为规范

* 如配置中的 topic 不存在或没有消息，将直接报错退出。
* 如消息缺少 `header.frame_id` 字段，同样报错。
* 输出 bag 与输入 bag 采用相同的 storage/serialization 插件，并保存到新的目录中。

## 测试

```bash
colcon test --packages-select ros2_bag_utils
```
