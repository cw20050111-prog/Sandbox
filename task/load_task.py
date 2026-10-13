#!/usr/bin/env python3
"""L1 数据准备：instance_id -> manifest 行 -> 模板和轨迹。仅使用标准库。"""

from __future__ import annotations

import argparse
import contextlib
import csv
import importlib.util
import json
import math
import sys
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path, PurePosixPath
from typing import Any, Callable


class DataPreparationError(ValueError):
    """可由上层识别的输入错误，code 稳定，message 带具体文件或任务信息。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DataPreparationError("INVALID_FIELD", f"{name} 必须是非空字符串")
    return value.strip()


def _json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"重复 JSON 字段：{key}")
        result[key] = value
    return result


def _json_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"JSON 数值必须有限：{value}")
    return number


def _json_constant(value: str) -> None:
    raise ValueError(f"不支持 JSON 常量：{value}")


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"),
                          object_pairs_hook=_json_object,
                          parse_float=_json_float, parse_constant=_json_constant)
    except (OSError, UnicodeError) as exc:
        raise DataPreparationError("FILE_READ_ERROR", f"读取 {path} 失败：{exc}") from exc
    except ValueError as exc:
        raise DataPreparationError("INVALID_JSON", f"解析 {path} 失败：{exc}") from exc


def _read_csv_rows(path: Path, required: set[str]) -> tuple[list[str], list[dict[str, str]]]:
    """支持 UTF-8 BOM、CSV 引号和字段两端空白，拒绝列数错误和重复表头。"""
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            if reader.fieldnames is None:
                raise DataPreparationError("INVALID_MANIFEST", f"{path} 是空文件")
            headers = [name.strip() for name in reader.fieldnames]
            if any(not name for name in headers) or len(headers) != len(set(headers)):
                raise DataPreparationError("INVALID_CSV_HEADER", f"{path} 表头为空或重复")
            missing = required - set(headers)
            if missing:
                raise DataPreparationError("MISSING_COLUMN", f"{path} 缺少列：{', '.join(sorted(missing))}")
            reader.fieldnames = headers
            rows: list[dict[str, str]] = []
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    raise DataPreparationError("INVALID_CSV_ROW", f"{path} 第 {reader.line_num} 行列数错误")
                clean = {key: value.strip() for key, value in row.items()}
                if any(clean.values()):
                    rows.append(clean)
            return headers, rows
    except (OSError, UnicodeError, csv.Error) as exc:
        raise DataPreparationError("CSV_READ_ERROR", f"读取 {path} 失败：{exc}") from exc


class ManifestIndex:
    """启动时读一次 CSV 建索引，之后按 instance_id 查询为 O(1)。"""

    def __init__(self, manifest_path: str | Path):
        self.path = Path(manifest_path).expanduser().resolve()
        self.headers, rows = _read_csv_rows(
            self.path, {"instance_id", "workdir", "language", "replay_file"})
        if not {"image", "image_arm64", "image_x86"}.intersection(self.headers):
            raise DataPreparationError("MISSING_COLUMN", f"{self.path} 缺少 image 或架构镜像列")
        self._rows: dict[str, dict[str, str]] = {}
        for row in rows:
            instance_id = _text(row["instance_id"], "manifest.instance_id")
            if instance_id in self._rows:
                raise DataPreparationError("DUPLICATE_INSTANCE_ID", f"{self.path} 存在重复 ID：{instance_id}")
            self._rows[instance_id] = row

    def get(self, instance_id: str) -> dict[str, str]:
        key = _text(instance_id, "instance_id")
        if key not in self._rows:
            raise DataPreparationError("INSTANCE_NOT_FOUND", f"{self.path} 找不到 instance_id={key}")
        return dict(self._rows[key])


def find_manifest_row(manifest_path: str | Path, instance_id: str) -> dict[str, str]:
    """单次查询入口；批量回放请复用 ManifestIndex 或 ReplayDataLoader。"""
    return ManifestIndex(manifest_path).get(instance_id)


def _architecture(value: str | None) -> str | None:
    if value is None:
        return None
    aliases = {"arm64": "arm64", "aarch64": "arm64", "x86": "x86",
               "x86_64": "x86", "amd64": "x86"}
    key = _text(value, "architecture").lower()
    if key not in aliases:
        raise DataPreparationError("INVALID_ARCHITECTURE", f"不支持架构：{value}")
    return aliases[key]


def select_image(row: dict[str, str], architecture: str | None) -> tuple[str, str | None]:
    arch = _architecture(architecture)
    if arch is not None and f"image_{arch}" in row:
        return _text(row[f"image_{arch}"], f"manifest.image_{arch}"), arch
    if row.get("image"):
        return row["image"], arch
    available = [(row.get(f"image_{kind}", ""), kind) for kind in ("arm64", "x86")]
    available = [(image, kind) for image, kind in available if image]
    if arch is not None:
        raise DataPreparationError("IMAGE_NOT_FOUND", f"manifest 中没有 {arch} 对应的镜像")
    if len(available) == 1:
        return available[0]
    if len(available) > 1:
        raise DataPreparationError("ARCHITECTURE_REQUIRED", "存在双架构镜像，请显式指定 --arch arm64 或 x86")
    raise DataPreparationError("IMAGE_NOT_FOUND", "manifest 镜像字段均为空")


def load_image_map(path: str | Path) -> dict[str, str]:
    """JSON {完整镜像: 模板}，或 CSV image,template_id（也接受 template 列）。"""
    file_path = Path(path).expanduser().resolve()
    if file_path.suffix.lower() == ".csv":
        headers, rows = _read_csv_rows(file_path, {"image"})
        if not {"template_id", "template"}.intersection(headers):
            raise DataPreparationError("MISSING_COLUMN", f"{file_path} 缺少 template_id 或 template 列")
        mapping: dict[str, str] = {}
        for row in rows:
            image = _text(row["image"], "image-map.image")
            template = _manifest_template(row)
            if not template:
                raise DataPreparationError("INVALID_IMAGE_MAP", f"{image} 的模板为空")
            if image in mapping:
                raise DataPreparationError("DUPLICATE_IMAGE", f"image-map 中镜像重复：{image}")
            mapping[image] = template
        return mapping
    data = _read_json(file_path)
    if not isinstance(data, dict):
        raise DataPreparationError("INVALID_IMAGE_MAP", "image-map JSON 必须为 {完整镜像: 模板ID或别名}")
    return {_text(image, "image-map.image"): _text(template, f"image-map[{image}]")
            for image, template in data.items()}


def _manifest_template(row: dict[str, str]) -> str | None:
    values = [row[name] for name in ("template_id", "template") if row.get(name)]
    if len(set(values)) > 1:
        raise DataPreparationError("TEMPLATE_CONFLICT", "同一 manifest 行的 template_id 与 template 不一致")
    return values[0] if values else None


def load_template_function(specification: str) -> Callable[[str], str]:
    """载入 FILE.py:FUNCTION，例如真实仓库中的 import_2u4g.py:template_name。"""
    file_name, separator, function_name = specification.rpartition(":")
    if not separator or not file_name or not function_name.isidentifier():
        raise DataPreparationError("INVALID_TEMPLATE_FUNCTION", "格式应为 /path/helper.py:template_name")
    path = Path(file_name).expanduser().resolve()
    module_name = "_l1_template_" + uuid.uuid4().hex
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ValueError("无法载入 Python 文件")
        module = importlib.util.module_from_spec(spec)
        # 模块中的 dataclass 等定义可能需要从 sys.modules 找回自身。
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        function = getattr(module, function_name)
        if not callable(function):
            raise ValueError(f"{function_name} 不是函数")
        return function
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise DataPreparationError("TEMPLATE_FUNCTION_LOAD_ERROR", f"载入 {specification} 失败：{exc}") from exc


@dataclass(frozen=True)
class ReplayStep:
    source_index: int
    action: str
    kind: str                         # bash 或 submit，submit 不作为命令发送
    delay_time: float | None           # 秒；缺失仍为 None
    delay_ms: int | None               # 派生毫秒；不在 L1 进行等待
    original: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {**self.original, "source_index": self.source_index,
                "action": self.action, "kind": self.kind,
                "delay_time": self.delay_time, "delay_ms": self.delay_ms}


@dataclass(frozen=True)
class TrajectoryData:
    trajectory: tuple[ReplayStep, ...]
    environment: str
    metadata: dict[str, Any]
    raw_steps: int
    submit_index: int | None
    ignored_after_submit: int
    warnings: tuple[str, ...]


def load_trajectory(replay_path: str | Path, expected_instance_id: str,
                    *, require_submit: bool = True) -> TrajectoryData:
    """按用户的 replay JSON 格式加载；保留首次 submit，截断它之后的记录。"""
    path = Path(replay_path).expanduser().resolve()
    data = _read_json(path)
    if not isinstance(data, dict) or not isinstance(data.get("trajectory"), list):
        raise DataPreparationError("INVALID_TRAJECTORY", f"{path} 必须包含 trajectory 数组")
    info = data.get("info")
    if not isinstance(info, dict) or info.get("instance_id") != expected_instance_id:
        raise DataPreparationError("INSTANCE_ID_MISMATCH", f"{path} 的 info.instance_id 与 {expected_instance_id} 不一致")
    environment = _text(data.get("environment", "main"), "replay.environment")
    result: list[ReplayStep] = []
    warnings: list[str] = []
    submit_index: int | None = None
    for index, item in enumerate(data["trajectory"]):
        if not isinstance(item, dict) or not isinstance(item.get("action"), str) or not item["action"].strip():
            raise DataPreparationError("INVALID_ACTION", f"{path} trajectory[{index}].action 必须是非空字符串")
        action = item["action"]  # 保留 Bash 命令中的原始空白、换行和引号。
        delay = item.get("delay_time")
        if delay is None:
            seconds, milliseconds = None, None
            warnings.append(f"trajectory[{index}] 的 delay_time 缺失或为 null，保留为未知")
        else:
            if isinstance(delay, bool) or not isinstance(delay, (int, float)):
                raise DataPreparationError("INVALID_DELAY", f"trajectory[{index}].delay_time 必须是非负有限数或 null")
            try:
                seconds = float(delay)
                if not math.isfinite(seconds) or seconds < 0:
                    raise ValueError("非负有限数")
                milliseconds = int((Decimal(str(delay)) * 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            except (ValueError, OverflowError, ArithmeticError) as exc:
                raise DataPreparationError("INVALID_DELAY", f"trajectory[{index}].delay_time 必须是非负有限数") from exc
        kind = "submit" if action.strip() == "submit" else "bash"
        result.append(ReplayStep(index, action, kind, seconds, milliseconds, dict(item)))
        if kind == "submit":
            submit_index = index
            break
    if not result:
        raise DataPreparationError("EMPTY_TRAJECTORY", f"{path} trajectory 为空")
    if require_submit and submit_index is None:
        raise DataPreparationError("SUBMIT_NOT_FOUND", f"{path} 没有 submit 结束标记")
    if submit_index is None:
        warnings.append("未找到 submit，按文件末尾结束；是否允许回放由上层决定")
    ignored = len(data["trajectory"]) - len(result)
    if ignored:
        warnings.append(f"首次 submit 后有 {ignored} 条记录，已从回放序列中排除")
    return TrajectoryData(tuple(result), environment,
                          {key: value for key, value in data.items() if key != "trajectory"},
                          len(data["trajectory"]), submit_index, ignored, tuple(warnings))


@dataclass(frozen=True)
class PreparedTask:
    instance_id: str
    image: str
    template: str | None
    template_source: str
    workdir: str
    language: str
    architecture: str | None
    replay_file: str
    replay_path: Path
    manifest_row: dict[str, str]
    manifest_steps: int | None
    trace: TrajectoryData
    warnings: tuple[str, ...]

    @property
    def trajectory(self) -> tuple[ReplayStep, ...]:
        return self.trace.trajectory

    def require_template(self) -> str:
        """进入 L2 创建请求前调用；L1 解析本身允许模板待补齐。"""
        if self.template is None:
            raise DataPreparationError("TEMPLATE_UNRESOLVED", f"{self.instance_id} 尚无模板：补 template_id、提供 image-map 或 template_name 函数")
        return self.template

    def to_dict(self) -> dict[str, Any]:
        return {"instance_id": self.instance_id, "image": self.image,
                "template": self.template, "template_source": self.template_source,
                "template_resolved": self.template is not None,
                "workdir": self.workdir, "language": self.language,
                "architecture": self.architecture, "environment": self.trace.environment,
                "trajectory": [step.to_dict() for step in self.trace.trajectory],
                "replay_file": self.replay_file, "replay_path": str(self.replay_path),
                "source": self.manifest_row.get("source") or None,
                "network_category": self.manifest_row.get("network_category") or None,
                "manifest_steps": self.manifest_steps, "json_steps": self.trace.raw_steps,
                "command_steps": sum(step.kind == "bash" for step in self.trace.trajectory),
                "submit_index": self.trace.submit_index,
                "ignored_after_submit": self.trace.ignored_after_submit,
                "trace_metadata": self.trace.metadata,
                "manifest_row": dict(self.manifest_row), "warnings": list(self.warnings)}


class ReplayDataLoader:
    """L1 可复用入口：不创建沙箱，不组装 pb，不执行 action 或 sleep。"""

    def __init__(self, manifest_path: str | Path, *, replay_root: str | Path | None = None,
                 architecture: str | None = None, image_map_path: str | Path | None = None,
                 template_name_fn: Callable[[str], str] | None = None,
                 template_override: str | None = None, require_submit: bool = True,
                 strict_step_count: bool = False):
        self.manifest = ManifestIndex(manifest_path)
        self.replay_root = (Path(replay_root).expanduser().resolve() if replay_root is not None
                            else self.manifest.path.parent)
        self.architecture = _architecture(architecture)
        self.image_map = load_image_map(image_map_path) if image_map_path is not None else {}
        self.template_name_fn = template_name_fn
        self.template_override = (_text(template_override, "template_override")
                                  if template_override is not None else None)
        self.require_submit = require_submit
        self.strict_step_count = strict_step_count

    def _resolve_template(self, row: dict[str, str], image: str) -> tuple[str | None, str]:
        if self.template_override is not None:
            return self.template_override, "override"
        explicit = _manifest_template(row)
        if explicit is not None:
            return explicit, "manifest"
        if image in self.image_map:
            return self.image_map[image], "image_map"
        if self.template_name_fn is not None:
            try:
                template = _text(self.template_name_fn(image), "template_name 返回值")
            except Exception as exc:
                raise DataPreparationError("TEMPLATE_RESOLUTION_ERROR", f"解析 {image} 的模板失败：{exc}") from exc
            return template, "template_function"
        return None, "unresolved"

    def load(self, instance_id: str) -> PreparedTask:
        row = self.manifest.get(instance_id)
        instance_id = row["instance_id"]
        image, architecture = select_image(row, self.architecture)
        workdir = _text(row["workdir"], "manifest.workdir")
        if not PurePosixPath(workdir).is_absolute():
            raise DataPreparationError("INVALID_WORKDIR", f"workdir 必须为沙箱内的绝对路径：{workdir}")
        language = _text(row["language"], "manifest.language")
        replay_file = _text(row["replay_file"], "manifest.replay_file")
        path = Path(replay_file).expanduser()
        replay_path = (path if path.is_absolute() else self.replay_root / path).resolve()
        trace = load_trajectory(replay_path, instance_id, require_submit=self.require_submit)
        template, template_source = self._resolve_template(row, image)
        warnings = list(trace.warnings)
        if template is None:
            warnings.append("模板尚未解析；可以加载数据，调用创建接口前需补齐模板")
        count_text = row.get("steps", "")
        if count_text and (not count_text.isascii() or not count_text.isdecimal()):
            raise DataPreparationError("INVALID_STEP_COUNT", f"manifest.steps 必须是非负整数：{count_text}")
        manifest_steps = int(count_text) if count_text else None
        if manifest_steps is not None and manifest_steps != trace.raw_steps:
            message = f"manifest.steps={manifest_steps}，JSON trajectory 条数={trace.raw_steps}；原表计数保留，不作为工具调用数"
            if self.strict_step_count:
                raise DataPreparationError("STEP_COUNT_MISMATCH", message)
            warnings.append(message)
        return PreparedTask(instance_id, image, template, template_source, workdir, language,
                            architecture, replay_file, replay_path, row, manifest_steps,
                            trace, tuple(warnings))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="L1 数据准备：按 instance_id 读取 manifest 和回放 JSON")
    parser.add_argument("--manifest", required=True, help="manifest.csv 路径")
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--replay-root", help="相对 replay_file 的根目录，默认 CSV 所在目录")
    parser.add_argument("--arch", choices=["arm64", "aarch64", "x86", "x86_64", "amd64"])
    parser.add_argument("--image-map", help="JSON 或 CSV 镜像到模板映射")
    parser.add_argument("--template-id", help="显式覆盖模板 ID 或别名")
    parser.add_argument("--template-function", help="实际命名函数，例如 /repo/import_2u4g.py:template_name")
    parser.add_argument("--require-template", action="store_true", help="模板未解析时失败，进入 L2 前使用")
    parser.add_argument("--allow-missing-submit", action="store_true")
    parser.add_argument("--strict-step-count", action="store_true")
    parser.add_argument("--output", help="同时写入整理后的 JSON 文件")
    args = parser.parse_args(argv)
    try:
        # 外部命名函数可能输出日志。CLI 的 stdout 始终只保留结果 JSON。
        with contextlib.redirect_stdout(sys.stderr):
            resolver = load_template_function(args.template_function) if args.template_function else None
            loader = ReplayDataLoader(args.manifest, replay_root=args.replay_root,
                                      architecture=args.arch, image_map_path=args.image_map,
                                      template_name_fn=resolver, template_override=args.template_id,
                                      require_submit=not args.allow_missing_submit,
                                      strict_step_count=args.strict_step_count)
            task = loader.load(args.instance_id)
        if args.require_template:
            task.require_template()
        output = json.dumps(task.to_dict(), ensure_ascii=False, indent=2, allow_nan=False)
        if args.output:
            target = Path(args.output).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(output + "\n", encoding="utf-8")
        print(output)
        return 0
    except (DataPreparationError, OSError) as exc:
        error = exc.to_dict() if isinstance(exc, DataPreparationError) else {"code": "OUTPUT_WRITE_ERROR", "message": str(exc)}
        print(json.dumps({"error": error}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
