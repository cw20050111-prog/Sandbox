"""接口消息：组装六个 RPC 请求并解析响应；实际发送由通信层完成。"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import uuid4

from google.protobuf.message import DecodeError, Message
from . import agent_proxy_pb2 as pb


class ProtocolError(ValueError):
    """本地输入或对端响应不符合契约；响应错误不能当作后端未执行。"""
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProtocolError("INVALID_INPUT", f"{name} 必须是非空字符串")
    return value


def _integer(value: Any, name: str, minimum: int = 1, maximum: int = 2**32 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ProtocolError("INVALID_INPUT", f"{name} 必须为 {minimum}..{maximum} 的整数")
    return value


def _mapping(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise ProtocolError("INVALID_INPUT", "需要 L1 的 PreparedTask/ReplayStep 或对应字典")
    return dict(value)


RPCS = {
    "CreateSandbox": (pb.CreateSandboxRequest, pb.CreateSandboxResponse, pb.OPERATION_CREATE),
    "RunStep": (pb.RunStepRequest, pb.RunStepResponse, pb.OPERATION_RUN_STEP),
    "PauseSandbox": (pb.PauseSandboxRequest, pb.PauseSandboxResponse, pb.OPERATION_PAUSE),
    "ResumeSandbox": (pb.ResumeSandboxRequest, pb.ResumeSandboxResponse, pb.OPERATION_RESUME),
    "DeleteSandbox": (pb.DeleteSandboxRequest, pb.DeleteSandboxResponse, pb.OPERATION_DELETE),
    "GetStatus": (pb.GetStatusRequest, pb.GetStatusResponse, None),
}
FINAL_FIELDS = {pb.OPERATION_CREATE: "create_result", pb.OPERATION_RUN_STEP: "step_result",
                pb.OPERATION_PAUSE: "pause_result", pb.OPERATION_RESUME: "resume_result",
                pb.OPERATION_DELETE: "delete_result"}
UNFINISHED = {pb.OPERATION_ACCEPTED, pb.OPERATION_RUNNING, pb.OPERATION_UNKNOWN}


@dataclass(frozen=True)
class ReplayContext:
    run_id: str
    task_id: str
    task_instance_id: str = field(default_factory=lambda: "task_" + uuid4().hex)
    trace_id: str = ""

    def __post_init__(self):
        for name in ("run_id", "task_id", "task_instance_id"):
            _text(getattr(self, name), name)
        if not isinstance(self.trace_id, str):
            raise ProtocolError("INVALID_INPUT", "trace_id 必须是字符串")

    def meta(self, phase: int, request_id: str | None = None,
             round_id: str | None = None, call_id: str | None = None) -> pb.RequestMeta:
        _integer(phase, "phase", maximum=7)
        if isinstance(phase, bool) or phase not in pb.Phase.values() or phase == pb.PHASE_UNSPECIFIED:
            raise ProtocolError("INVALID_INPUT", "phase 必须是有效阶段")
        for name, value in (("round_id", round_id), ("call_id", call_id)):
            if value is not None and not isinstance(value, str):
                raise ProtocolError("INVALID_INPUT", f"{name} 必须保留为字符串或 None")
        return pb.RequestMeta(request_id=_text(request_id, "request_id") if request_id is not None
                              else "req_" + uuid4().hex,
                              run_id=self.run_id, task_id=self.task_id,
                              task_instance_id=self.task_instance_id, trace_id=self.trace_id,
                              phase=phase, round_id=round_id or "", call_id=call_id or "")


@dataclass(frozen=True)
class SandboxSettings:
    backend: str
    cpu_count: int
    memory_mib: int
    ttl_seconds: int
    create_timeout_ms: int = 60000
    step_timeout_ms: int = 60000
    lifecycle_timeout_ms: int = 30000
    rpc_margin_ms: int = 5000
    status_rpc_timeout_ms: int = 5000

    def __post_init__(self):
        _text(self.backend, "backend")
        _integer(self.cpu_count, "cpu_count")
        _integer(self.memory_mib, "memory_mib", maximum=2**64 - 1)
        _integer(self.ttl_seconds, "ttl_seconds")
        for name in ("create_timeout_ms", "step_timeout_ms", "lifecycle_timeout_ms"):
            _integer(getattr(self, name), name, maximum=3600000)
        _integer(self.rpc_margin_ms, "rpc_margin_ms", minimum=0, maximum=3600000)
        _integer(self.status_rpc_timeout_ms, "status_rpc_timeout_ms", maximum=3600000)


@dataclass(frozen=True)
class PreparedCall:
    method: str
    request_bytes: bytes
    rpc_timeout_ms: int
    original_call: PreparedCall | None = None

    def __post_init__(self):
        if self.method not in RPCS or not isinstance(self.request_bytes, bytes):
            raise ProtocolError("INVALID_INPUT", "需要有效 RPC 方法和 PB 请求字节")
        _integer(self.rpc_timeout_ms, "rpc_timeout_ms", maximum=2**31 - 1)
        try:
            message = RPCS[self.method][0].FromString(self.request_bytes)
        except DecodeError as exc:
            raise ProtocolError("INVALID_INPUT", "请求 PB 字节无法解码") from exc
        for name in ("request_id", "run_id", "task_id", "task_instance_id"):
            _text(getattr(message.meta, name), f"meta.{name}")

    @property
    def request(self) -> Message:
        """返回独立 PB 对象，修改它不会改变已准备的字节或关联 ID。"""
        return RPCS[self.method][0].FromString(self.request_bytes)

    @property
    def request_id(self) -> str:
        return self.request.meta.request_id


@dataclass(frozen=True)
class StepPlan:
    delay_ms: int | None
    is_submit: bool
    call: PreparedCall | None
    source_index: int | None


class RequestFactory:
    """一个任务实例的请求构造器，内部不维护连接、执行循环或共享步号。"""
    def __init__(self, context: ReplayContext, settings: SandboxSettings):
        self.context = context
        self.settings = settings

    def _pack(self, method: str, request: Message, timeout_ms: int,
              original_call: PreparedCall | None = None) -> PreparedCall:
        return PreparedCall(method, request.SerializeToString(), timeout_ms, original_call)

    def _task(self, task: Any) -> dict[str, Any]:
        data = _mapping(task)
        if data.get("instance_id") != self.context.task_id:
            raise ProtocolError("TASK_MISMATCH", "L1 instance_id 与本次 ReplayContext.task_id 不一致")
        return data

    def create(self, task: Any, *, request_id: str | None = None) -> PreparedCall:
        data = self._task(task)
        template = _text(data.get("template"), "L1 template；需先补齐模板")
        cfg = self.settings
        request = pb.CreateSandboxRequest(template=template, backend=cfg.backend,
            spec=pb.SandboxSpec(cpu_count=cfg.cpu_count, memory_mib=cfg.memory_mib),
            ttl_seconds=cfg.ttl_seconds, timeout_ms=cfg.create_timeout_ms,
            meta=self.context.meta(pb.PHASE_CREATE, request_id))
        return self._pack("CreateSandbox", request, cfg.create_timeout_ms + cfg.rpc_margin_ms)

    def run_step(self, sandbox_id: str, step_id: int, tool: str, arguments: Mapping[str, Any],
                 *, phase: int = pb.PHASE_REPLAY, timeout_ms: int | None = None,
                 request_id: str | None = None, round_id: str | None = None,
                 call_id: str | None = None) -> PreparedCall:
        _text(tool, "tool")
        if phase not in (pb.PHASE_INITIALIZE, pb.PHASE_REPLAY, pb.PHASE_VALIDATE):
            raise ProtocolError("INVALID_INPUT", "RunStep 仅允许 INITIALIZE/REPLAY/VALIDATE")
        if tool not in {"bash.exec", "fs.read", "fs.write", "session.poll", "session.stop"}:
            raise ProtocolError("UNSUPPORTED_TOOL", f"工具未注册：{tool}")
        if not isinstance(arguments, Mapping):
            raise ProtocolError("INVALID_INPUT", "arguments 必须是对象")
        args = dict(arguments)
        if tool == "bash.exec":
            command = _text(args.get("command"), "command")
            if command.strip() == "submit":
                raise ProtocolError("SUBMIT_IS_CONTROL", "submit 是结束标记，不发送为 Bash")
            cwd = _text(args.get("cwd"), "cwd")
            if not cwd.startswith("/"):
                raise ProtocolError("INVALID_INPUT", "cwd 必须是沙箱内绝对路径")
            env = args.get("env", {})
            if not isinstance(env, dict) or any(not isinstance(k, str) or not isinstance(v, str)
                                               for k, v in env.items()):
                raise ProtocolError("INVALID_INPUT", "env 必须是字符串到字符串的对象")
            if not isinstance(args.get("background", False), bool):
                raise ProtocolError("INVALID_INPUT", "background 必须是布尔值")
            if args.get("background"):
                _text(args.get("logical_session"), "logical_session")
        elif tool.startswith("fs."):
            if not _text(args.get("path"), "path").startswith("/"):
                raise ProtocolError("INVALID_INPUT", "文件 path 必须是沙箱内绝对路径")
            if args.get("encoding") not in ("utf8", "base64"):
                raise ProtocolError("INVALID_INPUT", "encoding 必须为 utf8 或 base64")
            if tool == "fs.write" and not isinstance(args.get("content"), str):
                raise ProtocolError("INVALID_INPUT", "content 必须是字符串，允许空内容")
        else:
            _text(args.get("logical_session"), "logical_session")
        try:
            text = json.dumps(args, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ProtocolError("INVALID_INPUT", f"工具参数无法序列化：{exc}") from exc
        if len(text.encode("utf-8")) > 256 * 1024:
            raise ProtocolError("ARGUMENTS_TOO_LARGE", "arguments_json 超过 256 KiB")
        timeout = self.settings.step_timeout_ms if timeout_ms is None else timeout_ms
        _integer(timeout, "timeout_ms", maximum=3600000)
        request = pb.RunStepRequest(sandbox_id=_text(sandbox_id, "sandbox_id"),
            step_id=_integer(step_id, "step_id", maximum=2**64 - 1), tool=tool,
            arguments_json=text, timeout_ms=timeout,
            meta=self.context.meta(phase, request_id, round_id, call_id))
        return self._pack("RunStep", request, timeout + self.settings.rpc_margin_ms)

    def plan_step(self, task: Any, step: Any, sandbox_id: str, step_id: int,
                  *, phase: int = pb.PHASE_REPLAY) -> StepPlan:
        data, item = self._task(task), _mapping(step)
        action = _text(item.get("action"), "action")
        if "delay_ms" not in item:
            raise ProtocolError("INVALID_INPUT", "应传入 L1 归一化步骤，缺少 delay_ms")
        delay_ms = item["delay_ms"]
        if delay_ms is not None:
            _integer(delay_ms, "delay_ms", minimum=0, maximum=2**63 - 1)
        submit = action.strip() == "submit"
        if item.get("kind") not in ("bash", "submit") or (item["kind"] == "submit") != submit:
            raise ProtocolError("INVALID_INPUT", "L1 kind 与 action 不一致")
        if submit:
            return StepPlan(delay_ms, True, None, item.get("source_index"))
        call = self.run_step(sandbox_id, step_id, "bash.exec",
            {"command": action, "cwd": data.get("workdir"), "env": {}, "background": False},
            phase=phase, round_id=item.get("round_id"), call_id=item.get("call_id"))
        return StepPlan(delay_ms, False, call, item.get("source_index"))

    def _lifecycle(self, method: str, sandbox_id: str, request_id: str | None) -> PreparedCall:
        timeout = self.settings.lifecycle_timeout_ms
        request = RPCS[method][0](sandbox_id=_text(sandbox_id, "sandbox_id"), timeout_ms=timeout,
                                 meta=self.context.meta(pb.PHASE_WAIT, request_id))
        return self._pack(method, request, timeout + self.settings.rpc_margin_ms)

    def pause(self, sandbox_id: str, *, request_id: str | None = None) -> PreparedCall:
        return self._lifecycle("PauseSandbox", sandbox_id, request_id)

    def resume(self, sandbox_id: str, *, request_id: str | None = None) -> PreparedCall:
        return self._lifecycle("ResumeSandbox", sandbox_id, request_id)

    def delete(self, *, sandbox_id: str | None = None, create_request_id: str | None = None,
               request_id: str | None = None) -> PreparedCall:
        if (sandbox_id is None) == (create_request_id is None):
            raise ProtocolError("INVALID_INPUT", "删除必须指定 sandbox_id 或 create_request_id 之一")
        timeout = self.settings.lifecycle_timeout_ms
        request = pb.DeleteSandboxRequest(timeout_ms=timeout,
            meta=self.context.meta(pb.PHASE_CLEANUP, request_id))
        if sandbox_id is not None:
            request.sandbox_id = _text(sandbox_id, "sandbox_id")
        else:
            request.create_request_id = _text(create_request_id, "create_request_id")
        return self._pack("DeleteSandbox", request, timeout + self.settings.rpc_margin_ms)

    def status(self, *, sandbox_id: str | None = None, operation_request_id: str | None = None,
               original_call: PreparedCall | None = None, request_id: str | None = None) -> PreparedCall:
        if original_call is not None:
            if original_call.method == "GetStatus":
                raise ProtocolError("INVALID_INPUT", "original_call 必须是原变更请求")
            original_meta = original_call.request.meta
            for name in ("run_id", "task_id", "task_instance_id"):
                if getattr(original_meta, name) != getattr(self.context, name):
                    raise ProtocolError("TASK_MISMATCH", "原操作不属于本次任务实例")
            if operation_request_id is not None and operation_request_id != original_call.request_id:
                raise ProtocolError("INVALID_INPUT", "查询目标与原请求 ID 不一致")
            operation_request_id = original_call.request_id
        if (sandbox_id is None) == (operation_request_id is None):
            raise ProtocolError("INVALID_INPUT", "查询必须指定 sandbox_id 或 operation_request_id 之一")
        request = pb.GetStatusRequest(meta=self.context.meta(pb.PHASE_RECONCILE, request_id))
        if sandbox_id is not None:
            request.sandbox_id = _text(sandbox_id, "sandbox_id")
        else:
            request.operation_request_id = _text(operation_request_id, "operation_request_id")
            if request.operation_request_id == request.meta.request_id:
                raise ProtocolError("INVALID_INPUT", "查询自身 ID 必须与原操作 ID 不同")
        return self._pack("GetStatus", request, self.settings.status_rpc_timeout_ms, original_call)


@dataclass(frozen=True)
class ParsedResponse:
    method: str
    request_id: str
    success: bool
    error_code: str | None
    error_message: str
    backend_code: str | None = None
    step_id: int | None = None
    operation_state: str | None = None
    dispatch_state: str | None = None
    sandbox_id: str | None = None
    sandbox_state: str | None = None
    sandbox_backend: str | None = None
    sandbox_template: str | None = None
    capabilities: tuple[str, ...] = ()
    effective_spec: dict[str, int] | None = None
    effective_ttl_seconds: int | None = None
    tool_result: dict[str, Any] | None = None
    proxy_elapsed_ms: int | None = None
    cleanup_confirmed: bool = False
    requires_reconciliation: bool = False
    can_use_result: bool = False
    process_finished: bool | None = None
    final_response: ParsedResponse | None = None
    stored_response: bytes | None = None


def _contract(condition: bool, message: str) -> None:
    if not condition:
        raise ProtocolError("INVALID_RESPONSE", message)


def _enum_name(wrapper: Any, value: int, field_name: str) -> str:
    _contract(value in wrapper.values() and value != 0, f"{field_name} 未填写或不属于当前契约")
    return wrapper.Name(value)


def _result_json(text: str, tool: str) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"重复字段 {key}")
            result[key] = value
        return result
    def constant(value):
        raise ValueError(f"非有限数 {value}")
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("非有限数")
        return result
    _contract(len(text.encode("utf-8")) <= 1024 * 1024, "result_json 超过 1 MiB")
    try:
        result = json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("INVALID_RESPONSE", f"result_json 无效：{exc}") from exc
    _contract(isinstance(result, dict), "result_json 必须是对象")
    _contract(result.get("schema_version") == "1.0", "result_json.schema_version 必须为 1.0")
    expected_kind = "command" if tool == "bash.exec" else "file" if tool.startswith("fs.") else "session"
    _contract(result.get("kind") == expected_kind, "工具与 result_json.kind 不一致")
    _contract(result.get("state") in ("completed", "running", "stopped"), "工具结果 state 无效")
    for name in ("stdout", "stderr"):
        _contract(isinstance(result.get(name), str), f"{name} 必须是字符串")
    for name in ("stdout_truncated", "stderr_truncated"):
        _contract(isinstance(result.get(name), bool), f"{name} 必须是布尔值")
    for name in ("stdout_ref", "stderr_ref", "logical_session", "handle"):
        result.setdefault(name, None)
        _contract(result[name] is None or isinstance(result[name], str), f"{name} 必须是字符串或 null")
    result.setdefault("exit_code", None)
    result.setdefault("elapsed_ms", None)
    result.setdefault("data", None)
    exit_code, elapsed = result["exit_code"], result["elapsed_ms"]
    _contract(exit_code is None or (isinstance(exit_code, int) and not isinstance(exit_code, bool)),
              "exit_code 必须是整数或 null")
    _contract(elapsed is None or (isinstance(elapsed, (int, float)) and not isinstance(elapsed, bool)
                                 and math.isfinite(elapsed) and elapsed >= 0),
              "elapsed_ms 必须是非负有限数或 null")
    _contract(result["data"] is None or isinstance(result["data"], dict), "data 必须是对象或 null")
    if result["state"] == "running":
        _contract(exit_code is None, "后台进程运行中不能填写最终退出码")
    if expected_kind == "file":
        _contract(result["state"] == "completed" and exit_code is None, "文件工具应 completed，退出码为 null")
    return result


def _sandbox(info: pb.SandboxInfo, expected_id: str | None = None) -> tuple[str, str]:
    _contract(bool(info.sandbox_id), "沙箱响应缺少 sandbox_id")
    if expected_id is not None:
        _contract(info.sandbox_id == expected_id, "沙箱响应属于另一个 sandbox_id")
    return info.sandbox_id, _enum_name(pb.SandboxState, info.state, "sandbox.state")


def _sandbox_details(info: pb.SandboxInfo, expected_id: str | None = None) -> dict[str, Any]:
    sandbox_id, state = _sandbox(info, expected_id)
    spec = ({"cpu_count": info.effective_spec.cpu_count, "memory_mib": info.effective_spec.memory_mib}
            if info.HasField("effective_spec") else None)
    return dict(sandbox_id=sandbox_id, sandbox_state=state, sandbox_backend=info.backend or None,
                sandbox_template=info.template or None, capabilities=tuple(info.capabilities),
                effective_spec=spec, effective_ttl_seconds=info.effective_ttl_seconds or None)


def _operation(info: pb.OperationInfo, request_id: str, kind: int | None = None,
               expected_sandbox: str | None = None) -> tuple[str, str]:
    _contract(info.request_id == request_id, "操作响应 request_id 不匹配")
    _enum_name(pb.OperationKind, info.kind, "operation.kind")
    if kind is not None:
        _contract(info.kind == kind, "操作类型不匹配")
    if expected_sandbox is not None and info.sandbox_id:
        _contract(info.sandbox_id == expected_sandbox, "operation.sandbox_id 不匹配")
    return (_enum_name(pb.OperationState, info.state, "operation.state"),
            _enum_name(pb.DispatchState, info.dispatch_state, "operation.dispatch_state"))


def parse_response(call: PreparedCall, response: Message | bytes) -> ParsedResponse:
    """response 为 L3 解出的 PB 消息或纯 PB payload，不能传入完整 bRPC 帧。"""
    response_class = RPCS[call.method][1]
    if isinstance(response, bytes):
        try:
            response = response_class.FromString(response)
        except DecodeError as exc:
            raise ProtocolError("INVALID_RESPONSE", "响应 PB 字节无法解码") from exc
    _contract(isinstance(response, response_class), f"需要 {response_class.__name__}")
    request = call.request
    _contract(response.HasField("meta") and response.meta.request_id == request.meta.request_id,
              "外层响应 meta.request_id 不匹配")
    if request.meta.trace_id:
        _contract(response.meta.trace_id == request.meta.trace_id, "响应 trace_id 不匹配")
    _contract(response.HasField("error"), "响应必须显式填写 ErrorDetail")
    error_name = _enum_name(pb.ErrorCode, response.error.code, "error.code")
    _contract(response.success == (response.error.code == pb.ERROR_OK), "success 与 error.code 不一致")
    if response.success:
        _contract(response.error.message == "", "成功响应的 error.message 必须为空")
    measured = response.meta.proxy_elapsed.value_ms if response.meta.HasField("proxy_elapsed") else None
    common = dict(method=call.method, request_id=request.meta.request_id,
                  success=response.success, error_code=error_name,
                  backend_code=response.error.backend_code or None,
                  error_message=response.error.message, proxy_elapsed_ms=measured)
    if call.method == "GetStatus":
        return _parse_status(call, request, response, common)

    _contract(response.HasField("operation"), "变更响应缺少 operation")
    conflict = response.error.code == pb.ERROR_REQUEST_ID_CONFLICT
    expected_sandbox = getattr(request, "sandbox_id", "") or None
    state, dispatched = _operation(response.operation, call.request_id,
                                   None if conflict else RPCS[call.method][2],
                                   None if conflict else expected_sandbox)
    if response.success:
        _contract(response.operation.state == pb.OPERATION_SUCCEEDED,
                  "变更 success=true 只能对应 OPERATION_SUCCEEDED")
    elif response.error.code not in (pb.ERROR_DUPLICATE_REQUEST, pb.ERROR_REQUEST_ID_CONFLICT):
        _contract(response.operation.state in (pb.OPERATION_FAILED, pb.OPERATION_UNKNOWN),
                  "失败响应必须明确 FAILED 或 UNKNOWN")
    reconciliation = (response.operation.state in UNFINISHED or
                      response.error.code in (pb.ERROR_DUPLICATE_REQUEST, pb.ERROR_REQUEST_ID_CONFLICT))
    fields: dict[str, Any] = dict(operation_state=state, dispatch_state=dispatched,
        sandbox_id=response.operation.sandbox_id or None, requires_reconciliation=reconciliation,
        can_use_result=response.success)
    if call.method == "RunStep":
        _contract(response.step_id == request.step_id, "RunStep step_id 不匹配")
        _contract(response.error_message == response.error.message, "RunStep 两个错误文本不一致")
        fields["step_id"] = response.step_id
        if response.success:
            _contract(bool(response.result_json), "成功 RunStep 缺少 result_json")
        if response.result_json:
            _contract(response.operation.state in (pb.OPERATION_SUCCEEDED, pb.OPERATION_FAILED),
                      "结果未知时不能声明工具最终结果")
            result = _result_json(response.result_json, request.tool)
            fields["tool_result"] = result
            if result["kind"] != "file":
                fields["process_finished"] = result["state"] in ("completed", "stopped")
    elif call.method == "DeleteSandbox":
        _contract(response.success == response.cleanup_confirmed,
                  "DeleteSandbox success 必须与 cleanup_confirmed 一致")
        if request.WhichOneof("target") == "create_request_id":
            _contract(response.operation.target_create_request_id == request.create_request_id,
                      "创建超时清理的原创建 ID 不匹配")
        if response.sandbox_id:
            if expected_sandbox:
                _contract(response.sandbox_id == expected_sandbox, "删除响应 sandbox_id 不匹配")
            if response.operation.sandbox_id:
                _contract(response.sandbox_id == response.operation.sandbox_id, "删除响应两个沙箱 ID 不一致")
            fields["sandbox_id"] = response.sandbox_id
        fields["cleanup_confirmed"] = response.cleanup_confirmed
    else:
        if response.HasField("sandbox"):
            sandbox_id, sandbox_state = _sandbox(response.sandbox, expected_sandbox)
            if response.operation.sandbox_id:
                _contract(sandbox_id == response.operation.sandbox_id, "生命周期响应两个沙箱 ID 不一致")
            fields.update(_sandbox_details(response.sandbox, expected_sandbox))
        if response.success:
            _contract(response.HasField("sandbox"), "成功生命周期响应缺少 sandbox")
            target_state = pb.SANDBOX_PAUSED if call.method == "PauseSandbox" else pb.SANDBOX_READY
            _contract(response.sandbox.state == target_state, "生命周期 success 与沙箱状态不一致")
            if call.method == "CreateSandbox":
                _contract(response.sandbox.backend == request.backend, "创建响应改变了所选后端")
                _contract(bool(response.sandbox.template) and response.sandbox.HasField("effective_spec"),
                          "创建响应缺少实际模板或规格")
                _contract(response.sandbox.effective_spec == request.spec, "实际规格与请求不一致")
                _contract(response.sandbox.effective_ttl_seconds > 0, "创建响应缺少实际 TTL")
    return ParsedResponse(**common, **fields)


def _parse_status(call: PreparedCall, request: pb.GetStatusRequest,
                  response: pb.GetStatusResponse, common: dict[str, Any]) -> ParsedResponse:
    target, selected = request.WhichOneof("target"), response.WhichOneof("result")
    if response.success:
        _contract(selected is not None, "成功 GetStatus 缺少 result")
    if selected is None:
        return ParsedResponse(**common, requires_reconciliation=True)
    if selected == "sandbox":
        _contract(target == "sandbox_id", "按操作查询却返回沙箱快照")
        sandbox_id, state = _sandbox(response.sandbox, request.sandbox_id)
        return ParsedResponse(**common, **_sandbox_details(response.sandbox, request.sandbox_id),
            requires_reconciliation=not response.success or response.sandbox.state == pb.SANDBOX_UNKNOWN)

    _contract(target == "operation_request_id", "按沙箱查询却返回操作记录")
    record = response.operation
    _contract(record.HasField("info"), "操作记录缺少 info")
    state, dispatched = _operation(record.info, request.operation_request_id)
    fields: dict[str, Any] = dict(operation_state=state, dispatch_state=dispatched,
        sandbox_id=record.info.sandbox_id or None,
        requires_reconciliation=not response.success or record.info.state in UNFINISHED)
    if not response.success:
        return ParsedResponse(**common, **fields)  # 失败查询中的快照不能作为原操作完成证明。
    _contract(record.HasField("original_meta"), "操作记录缺少原请求元数据")
    _contract(record.original_meta.request_id == request.operation_request_id, "原操作元数据 ID 不匹配")
    for name in ("run_id", "task_id", "task_instance_id"):
        _contract(getattr(record.original_meta, name) == getattr(request.meta, name), "操作记录属于另一任务实例")
    final_field = record.WhichOneof("final_response")
    if record.info.state in UNFINISHED:
        _contract(final_field is None, "未完成或未知操作不能带 final_response")
        return ParsedResponse(**common, **fields)
    _contract(final_field == FINAL_FIELDS.get(record.info.kind), "终态缺少对应 final_response 或类型错误")
    original_response = getattr(record, final_field)
    _contract(original_response.HasField("meta") and original_response.meta.request_id == request.operation_request_id,
              "保存的原响应 ID 不匹配")
    _contract(original_response.HasField("operation"), "保存的原响应缺少 operation")
    for name in ("request_id", "kind", "state", "dispatch_state", "sandbox_id", "target_create_request_id"):
        _contract(getattr(original_response.operation, name) == getattr(record.info, name),
                  "原响应与操作记录状态不一致")
    fields["stored_response"] = original_response.SerializeToString()
    if call.original_call is not None:
        _contract(record.original_meta == call.original_call.request.meta, "原请求元数据与本地请求不一致")
        _contract(RPCS[call.original_call.method][2] == record.info.kind, "原请求方法与记录不一致")
        decoded = parse_response(call.original_call, original_response)
        fields.update(final_response=decoded, tool_result=decoded.tool_result,
                      sandbox_id=decoded.sandbox_id, sandbox_state=decoded.sandbox_state,
                      step_id=decoded.step_id, sandbox_backend=decoded.sandbox_backend,
                      sandbox_template=decoded.sandbox_template, capabilities=decoded.capabilities,
                      effective_spec=decoded.effective_spec, effective_ttl_seconds=decoded.effective_ttl_seconds,
                      can_use_result=decoded.can_use_result,
                      cleanup_confirmed=decoded.cleanup_confirmed,
                      process_finished=decoded.process_finished,
                      requires_reconciliation=decoded.requires_reconciliation)
    # 没有原请求时仅提供记录与原响应字节，不自动认为步骤结果已正确关联。
    return ParsedResponse(**common, **fields)


def transport_failure(call: PreparedCall, message: str) -> ParsedResponse:
    """由 L3 在连接失败/RPC 超时时调用；不虚构业务错误码或后端完成状态。"""
    return ParsedResponse(method=call.method, request_id=call.request_id, success=False,
        error_code=None, error_message=message, operation_state="OPERATION_UNKNOWN",
        requires_reconciliation=True)
