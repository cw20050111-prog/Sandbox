"""Client 任务入口：接收调度分配，读取任务数据，再准备创建消息。

本模块不实现任务队列、调度通信或 bRPC 发送。任务来源是调度组件，
不是 Proxy；接入实际调度通道后，将其消息交给 prepare_assignment 即可。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .load_task import PreparedTask, ReplayDataLoader
from ..message.request_response import PreparedCall, ReplayContext, RequestFactory, SandboxSettings


class AssignmentError(ValueError):
    """调度分配消息缺少必要关联字段或字段类型不正确。"""
    code = "INVALID_ASSIGNMENT"

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


@dataclass(frozen=True)
class TaskAssignment:
    """由调度组件提供；同一次分配的 task_instance_id 必须保持不变。"""
    instance_id: str
    run_id: str
    task_instance_id: str
    trace_id: str = ""

    def __post_init__(self):
        for name in ("instance_id", "run_id", "task_instance_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise AssignmentError(f"调度消息 {name} 必须是非空字符串")
        if not isinstance(self.trace_id, str):
            raise AssignmentError("调度消息 trace_id 必须是字符串")

    @classmethod
    def from_mapping(cls, message: Mapping[str, Any]) -> TaskAssignment:
        if not isinstance(message, Mapping):
            raise AssignmentError("调度分配消息必须是对象")
        return cls(instance_id=message.get("instance_id"), run_id=message.get("run_id"),
                   task_instance_id=message.get("task_instance_id"),
                   trace_id=message.get("trace_id", ""))

    def to_dict(self) -> dict[str, str]:
        return {"instance_id": self.instance_id, "run_id": self.run_id,
                "task_instance_id": self.task_instance_id, "trace_id": self.trace_id}

    def to_context(self) -> ReplayContext:
        # 沿用既有 RequestMeta；数据集 instance_id 对应其中的 task_id。
        return ReplayContext(run_id=self.run_id, task_id=self.instance_id,
                             task_instance_id=self.task_instance_id, trace_id=self.trace_id)


@dataclass(frozen=True)
class PreparedAssignment:
    assignment: TaskAssignment
    task: PreparedTask
    factory: RequestFactory
    create_call: PreparedCall

    @property
    def context(self) -> ReplayContext:
        return self.factory.context


def prepare_assignment(assignment: TaskAssignment | Mapping[str, Any], *,
                       loader: ReplayDataLoader,
                       settings: SandboxSettings) -> PreparedAssignment:
    """在发送任何 Proxy RPC 前验证分配、读取数据并检查模板。

    失败时抛 AssignmentError、L1 DataPreparationError 或 L2 ProtocolError。
    不发送消息，不创建沙箱；调用方应记录失败并通过自己的调度通道上报。
    返回的 create_call 供 L3 发送。重复调用不保证任务去重，不应因此重发 RPC。
    """
    if not isinstance(assignment, TaskAssignment):
        assignment = TaskAssignment.from_mapping(assignment)
    task = loader.load(assignment.instance_id)
    task.require_template()
    factory = RequestFactory(assignment.to_context(), settings)
    create_call = factory.create(task)
    return PreparedAssignment(assignment=assignment, task=task,
                              factory=factory, create_call=create_call)
