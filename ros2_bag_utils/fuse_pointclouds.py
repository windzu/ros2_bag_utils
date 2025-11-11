"""融合多个激光雷达点云到统一坐标系。"""

from __future__ import annotations

import argparse
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import yaml  # type: ignore[import]
from wind_pypcd import pypcd
from wind_pypcd.pypcd import PointCloud as PypcdPointCloud
from scipy.spatial.transform import Rotation  # type: ignore[import]

from ros2_bag_utils.common import (
    ConfigError,
    ensure_logging_configured,
    load_yaml_config,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FusionConfig:
    """解析后的点云融合配置。"""

    input_root: Path
    base_frame: str
    reference_lidar: str
    time_tolerance_ns: int
    lidar_subdir: str
    calib_subdir: str
    overwrite: bool


def _sanitize_frame_id(frame_id: str) -> str:
    frame = frame_id.strip()
    if frame.startswith("/"):
        frame = frame[1:]
    return frame


def _resolve_non_empty(raw: Optional[object], *, field: str) -> str:
    if raw is None:
        raise ConfigError(f"必须配置 {field}")
    value = str(raw).strip()
    if not value:
        raise ConfigError(f"{field} 不能为空")
    return value


def _resolve_bool(raw: Optional[object], default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    raise ConfigError(f"无法解析布尔值 {raw!r}")


def _resolve_positive_number(raw: Optional[object], *, field: str) -> float:
    if raw is None:
        raise ConfigError(f"必须配置 {field}")
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if not text:
        raise ConfigError(f"{field} 不能为空")
    try:
        return float(text)
    except ValueError as exc:  # noqa: TRY003
        raise ConfigError(f"{field} 需要为数字，当前值 {raw!r}") from exc


def build_effective_config(
    yaml_config: Dict[str, object],
    *,
    cli_input_root: Optional[str],
    cli_base_frame: Optional[str],
    cli_reference_lidar: Optional[str],
    cli_time_tolerance_ms: Optional[float],
    cli_lidar_subdir: Optional[str],
    cli_calib_subdir: Optional[str],
    cli_overwrite: bool,
) -> FusionConfig:
    input_root_raw = cli_input_root or yaml_config.get("input_root")
    if not input_root_raw:
        raise ConfigError("必须指定 input_root")
    input_root = Path(str(input_root_raw)).expanduser().resolve()
    if not input_root.exists():
        raise ConfigError(f"输入目录 {input_root} 不存在")

    base_frame_raw = cli_base_frame or yaml_config.get("base_frame") or "base_link"
    base_frame = _sanitize_frame_id(str(base_frame_raw))
    if not base_frame:
        raise ConfigError("base_frame 不能为空")

    reference_lidar = _resolve_non_empty(
        cli_reference_lidar or yaml_config.get("reference_lidar"),
        field="reference_lidar",
    )

    time_tolerance_ms = cli_time_tolerance_ms
    if time_tolerance_ms is None:
        time_tolerance_ms = _resolve_positive_number(
            yaml_config.get("time_tolerance_ms"),
            field="time_tolerance_ms",
        )
    if time_tolerance_ms < 0:
        raise ConfigError("time_tolerance_ms 不能为负数")
    time_tolerance_ns = int(round(time_tolerance_ms * 1_000_000.0))

    lidar_subdir_raw = cli_lidar_subdir or yaml_config.get("lidar_subdir") or "lidar"
    lidar_subdir = str(lidar_subdir_raw).strip()
    if not lidar_subdir:
        raise ConfigError("lidar_subdir 不能为空")

    calib_subdir_raw = cli_calib_subdir or yaml_config.get("calib_subdir") or "calib"
    calib_subdir = str(calib_subdir_raw).strip()
    if not calib_subdir:
        raise ConfigError("calib_subdir 不能为空")

    overwrite = cli_overwrite or _resolve_bool(yaml_config.get("overwrite"), False)

    return FusionConfig(
        input_root=input_root,
        base_frame=base_frame,
        reference_lidar=reference_lidar,
        time_tolerance_ns=time_tolerance_ns,
        lidar_subdir=lidar_subdir,
        calib_subdir=calib_subdir,
        overwrite=overwrite,
    )


XYZ_FIELDS = ("x", "y", "z")


def _validate_point_cloud(pc: PypcdPointCloud, path: Path) -> None:
    fields: Sequence[str] = list(getattr(pc, "fields"))
    sizes: Sequence[int] = list(getattr(pc, "size"))
    types: Sequence[str] = list(getattr(pc, "type"))
    counts: Sequence[int] = list(getattr(pc, "count"))

    missing_fields = [field for field in XYZ_FIELDS if field not in fields]
    if missing_fields:
        raise RuntimeError(f"点云 {path} 缺少 {','.join(missing_fields)} 字段")

    for field, size, type_code, count in zip(fields, sizes, types, counts):
        if str(type_code).upper() != "F":
            raise RuntimeError(f"点云 {path} 字段 {field} 类型 {type_code} 非 float32")
        if int(size) != 4 or int(count) != 1:
            raise RuntimeError(f"点云 {path} 字段 {field} 仅支持 4 字节单通道浮点")


def _load_point_cloud(path: Path) -> PypcdPointCloud:
    pc = pypcd.point_cloud_from_path(str(path))
    _validate_point_cloud(pc, path)
    return pc


def _ensure_point_cloud_compatible(
    reference: PypcdPointCloud, other: PypcdPointCloud, path: Path
) -> None:
    reference_fields = list(getattr(reference, "fields"))
    other_fields = list(getattr(other, "fields"))
    if reference_fields != other_fields:
        raise RuntimeError(f"点云字段与参考格式不一致: {path}")

    reference_size = list(getattr(reference, "size"))
    other_size = list(getattr(other, "size"))
    reference_type = list(getattr(reference, "type"))
    other_type = list(getattr(other, "type"))
    if reference_size != other_size or reference_type != other_type:
        raise RuntimeError(f"点云类型描述与参考格式不一致: {path}")

    reference_count = list(getattr(reference, "count"))
    other_count = list(getattr(other, "count"))
    if reference_count != other_count:
        raise RuntimeError(f"点云 count 与参考格式不一致: {path}")


def _transform_point_cloud(
    pc: PypcdPointCloud, rotation: np.ndarray, translation: np.ndarray
) -> PypcdPointCloud:
    transformed = pc.copy()
    data_array = getattr(transformed, "pc_data")
    xyz_data = np.column_stack(
        [data_array[field].astype(np.float64) for field in XYZ_FIELDS]
    )
    rotated = (rotation @ xyz_data.T).T + translation
    for idx, field in enumerate(XYZ_FIELDS):
        data_array[field] = rotated[:, idx].astype(np.float32)
    return transformed


def _save_point_cloud(pc: PypcdPointCloud, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = str(getattr(pc, "data", "ascii")).lower()
    if fmt == "ascii":
        pypcd.save_point_cloud(pc, str(path))
    elif fmt == "binary":
        pypcd.save_point_cloud_bin(pc, str(path))
    elif fmt == "binary_compressed":
        pypcd.save_point_cloud_bin_compressed(pc, str(path))
    else:
        raise RuntimeError(f"未知的点云数据格式 {fmt}，无法写出 {path}")


def _quaternion_to_rotation(quaternion: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(quaternion))
    if norm == 0.0:
        raise RuntimeError("四元数范数为 0")
    normalized = (quaternion / norm).astype(np.float64, copy=False)
    rotation = Rotation.from_quat(normalized)
    return rotation.as_matrix().astype(np.float64, copy=False)


def _load_extrinsics(extrinsics_path: Path) -> Tuple[str, str, np.ndarray, np.ndarray]:
    try:
        data = yaml.safe_load(extrinsics_path.read_text())
    except yaml.YAMLError as exc:  # noqa: TRY003
        raise RuntimeError(f"解析外参文件 {extrinsics_path} 失败: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"外参文件 {extrinsics_path} 格式非法")

    parent = _sanitize_frame_id(str(data.get("parent_frame", "")).strip())
    child = _sanitize_frame_id(str(data.get("child_frame", "")).strip())
    if not parent or not child:
        raise RuntimeError(
            f"外参文件 {extrinsics_path} 缺少 parent_frame 或 child_frame"
        )

    transform = data.get("transform") or {}
    translation_raw = (
        (transform.get("translation") or {}) if isinstance(transform, dict) else {}
    )
    rotation_raw = (
        (transform.get("rotation_quaternion") or {})
        if isinstance(transform, dict)
        else {}
    )

    try:
        translation = np.array(
            [
                float(translation_raw.get("x", 0.0)),
                float(translation_raw.get("y", 0.0)),
                float(translation_raw.get("z", 0.0)),
            ],
            dtype=np.float64,
        )
    except (TypeError, ValueError) as exc:  # noqa: TRY003
        raise RuntimeError(
            f"外参文件 {extrinsics_path} 中 translation 字段非法"
        ) from exc

    try:
        rotation = np.array(
            [
                float(rotation_raw.get("x", 0.0)),
                float(rotation_raw.get("y", 0.0)),
                float(rotation_raw.get("z", 0.0)),
                float(rotation_raw.get("w", 1.0)),
            ],
            dtype=np.float64,
        )
    except (TypeError, ValueError) as exc:  # noqa: TRY003
        raise RuntimeError(
            f"外参文件 {extrinsics_path} 中 rotation_quaternion 字段非法"
        ) from exc

    rotation_matrix = _quaternion_to_rotation(rotation)
    return parent, child, rotation_matrix, translation


def _parse_timestamp_from_name(path: Path) -> int:
    stem = path.stem
    token = stem.split("_", 1)[0]
    if not token.isdigit():
        raise RuntimeError(f"PCD 文件 {path} 的文件名不包含纳秒时间戳")
    return int(token)


def _build_time_index(files: Iterable[Path]) -> Tuple[List[int], List[Path]]:
    timestamps: List[int] = []
    file_list: List[Path] = []
    for file_path in sorted(files):
        if file_path.suffix.lower() != ".pcd":
            continue
        timestamp = _parse_timestamp_from_name(file_path)
        timestamps.append(timestamp)
        file_list.append(file_path)
    return timestamps, file_list


def _find_closest_timestamp(
    timestamps: Sequence[int], target: int
) -> Tuple[Optional[int], Optional[int]]:
    if not timestamps:
        return None, None
    import bisect

    idx = bisect.bisect_left(timestamps, target)
    candidates: List[Tuple[int, int]] = []
    if idx < len(timestamps):
        candidates.append((abs(timestamps[idx] - target), idx))
    if idx > 0:
        candidates.append((abs(timestamps[idx - 1] - target), idx - 1))
    if not candidates:
        return None, None
    candidates.sort()
    _, best_index = candidates[0]
    return timestamps[best_index], best_index


def fuse_pointclouds(config: FusionConfig) -> None:
    lidar_root = config.input_root / config.lidar_subdir
    calib_root = config.input_root / config.calib_subdir

    if not lidar_root.exists() or not lidar_root.is_dir():
        raise ConfigError(f"点云目录 {lidar_root} 不存在或不是目录")
    if not calib_root.exists() or not calib_root.is_dir():
        raise ConfigError(f"外参目录 {calib_root} 不存在或不是目录")

    output_dir = lidar_root / config.base_frame
    if output_dir.exists():
        if config.overwrite:
            shutil.rmtree(output_dir)
        else:
            raise ConfigError(f"输出目录 {output_dir} 已存在，如需覆盖请启用 overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)

    lidar_dirs = {
        item.name: item
        for item in lidar_root.iterdir()
        if item.is_dir() and item.name != config.base_frame
    }

    extrinsics_by_lidar: Dict[str, Tuple[str, np.ndarray, np.ndarray]] = {}
    for extrinsics_file in sorted(calib_root.glob("*.extrinsics.yaml")):
        lidar_name = extrinsics_file.name[: -len(".extrinsics.yaml")]
        if lidar_name not in lidar_dirs:
            continue
        parent_frame, child_frame, rotation_matrix, translation = _load_extrinsics(
            extrinsics_file
        )
        if parent_frame != config.base_frame:
            raise ConfigError(
                f"外参 {extrinsics_file} 的 parent_frame={parent_frame} 与配置 base_frame="
                f"{config.base_frame} 不一致"
            )
        extrinsics_by_lidar[lidar_name] = (child_frame, rotation_matrix, translation)

    if config.reference_lidar not in extrinsics_by_lidar:
        raise ConfigError(f"参考激光雷达 {config.reference_lidar} 缺少匹配的外参或目录")

    matched_lidars = sorted(extrinsics_by_lidar.keys())
    if not matched_lidars:
        raise ConfigError("未找到任何可用于融合的激光雷达数据")

    indexes: Dict[str, Tuple[List[int], List[Path]]] = {}
    for lidar_name in matched_lidars:
        timestamps, files = _build_time_index((lidar_dirs[lidar_name]).glob("*.pcd"))
        if not timestamps:
            logger.warning(
                "激光雷达 %s 未找到任何 PCD 文件，将导致全部帧丢弃", lidar_name
            )
        indexes[lidar_name] = (timestamps, files)

    ref_timestamps, ref_files = indexes[config.reference_lidar]
    if not ref_timestamps:
        raise ConfigError(f"参考激光雷达 {config.reference_lidar} 未找到任何 PCD 文件")

    reference_pc: Optional[PypcdPointCloud] = None

    total_frames = 0
    fused_frames = 0
    skipped_frames = 0

    for ref_timestamp, ref_file in zip(ref_timestamps, ref_files):
        total_frames += 1
        candidate_files: Dict[str, Path] = {config.reference_lidar: ref_file}
        success = True
        for lidar_name in matched_lidars:
            if lidar_name == config.reference_lidar:
                continue
            timestamps, files = indexes[lidar_name]
            match_ts, match_idx = _find_closest_timestamp(timestamps, ref_timestamp)
            if match_ts is None or match_idx is None:
                success = False
                break
            if abs(match_ts - ref_timestamp) > config.time_tolerance_ns:
                success = False
                break
            candidate_files[lidar_name] = files[match_idx]
        if not success:
            skipped_frames += 1
            continue

        point_clouds: List[PypcdPointCloud] = []
        for lidar_name, file_path in candidate_files.items():
            pc = _load_point_cloud(file_path)
            if reference_pc is None:
                reference_pc = pc
            else:
                _ensure_point_cloud_compatible(reference_pc, pc, file_path)

            rotation, translation = extrinsics_by_lidar[lidar_name][1:]
            transformed_pc = _transform_point_cloud(pc, rotation, translation)
            point_clouds.append(transformed_pc)

        if not point_clouds:
            skipped_frames += 1
            continue

        fused_pc = point_clouds[0]
        for extra_pc in point_clouds[1:]:
            fused_pc = pypcd.cat_point_clouds(fused_pc, extra_pc)

        output_path = output_dir / ref_file.name
        _save_point_cloud(fused_pc, output_path)
        fused_frames += 1

    logger.info(
        "融合完成: 总参考帧 %d, 成功 %d, 丢弃 %d",
        total_frames,
        fused_frames,
        skipped_frames,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将多个激光雷达 PCD 点云融合到统一坐标系。"
    )
    parser.add_argument("--config", type=str, help="YAML 配置文件路径")
    parser.add_argument("--input-root", type=str, help="包含点云与外参的根目录")
    parser.add_argument("--base-frame", type=str, help="目标基准坐标系")
    parser.add_argument("--reference-lidar", type=str, help="参考时间轴的激光雷达名称")
    parser.add_argument(
        "--time-tolerance-ms", type=float, help="时间匹配容差，单位毫秒"
    )
    parser.add_argument("--lidar-subdir", type=str, help="点云子目录名称，默认 lidar")
    parser.add_argument("--calib-subdir", type=str, help="外参子目录名称，默认 calib")
    parser.add_argument(
        "--overwrite", action="store_true", help="若输出目录已存在则覆盖"
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    ensure_logging_configured()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        yaml_config = (
            load_yaml_config(Path(args.config).expanduser().resolve())
            if args.config
            else {}
        )
        config = build_effective_config(
            yaml_config=yaml_config,
            cli_input_root=args.input_root,
            cli_base_frame=args.base_frame,
            cli_reference_lidar=args.reference_lidar,
            cli_time_tolerance_ms=args.time_tolerance_ms,
            cli_lidar_subdir=args.lidar_subdir,
            cli_calib_subdir=args.calib_subdir,
            cli_overwrite=args.overwrite,
        )
        fuse_pointclouds(config)
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.exception("执行失败: %s", exc)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
