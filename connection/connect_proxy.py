"""接入真实 Proxy 通信；此文件定义接入函数，通信实现由调用方传入。

业务协议保持原样。L3 必须返回纯 PB payload 或对应响应 PB 对象，
并实际遵守 PreparedCall.rpc_timeout_ms。不要将完整 bRPC 帧传入。
"""
from __future__ import annotations

from ..task.receive_task import prepare_assignment
from ..timing.record_calls import JsonlSink, MeasuredClient
from ..timing.timer import PerfBackend


def build_sync_client(l3_transport, *, log_path=None, native_path=None):
    """每个 Client 初始化一次；同一进程的线程可以共享。

    l3_transport(call) 根据 call.method 发送 call.request_bytes，
    超时使用 call.rpc_timeout_ms，收到响应返回 PB 字节。
    log_path=None 时不写 JSONL，仍执行真正的 SpDiag PerfPoint。
    不同进程使用不同日志路径；线程共享一个 sink 即可。
    """
    backend = PerfBackend(mode='native', native_path=native_path)
    sink = JsonlSink(log_path) if log_path is not None else None
    return MeasuredClient(l3_transport, backend=backend, sink=sink)


def create_assigned_sandbox(assignment, *, loader, settings, client):
    """调度分配 instance_id → L1 校验 → 构造 create → 带打点发送。

    数据校验失败不会发出请求；该本地校验没有被伪装成 Create RPC。
    successful_response.sandbox_id 是校验过的真实返回标识。
    业务失败正常返回 ParsedResponse.success=False；由任务主循环处理。
    通信或协议错误重新抛原异常，exc.measured_response 保留 UNKNOWN。
    """
    prepared = prepare_assignment(assignment, loader=loader, settings=settings)
    created = client.call(prepared.create_call)
    return prepared, created


def begin_async_call(client, call):
    """紧挨 L3 异步 dispatch 之前调用，并由 L3 保留返回的 pending。

    pending = begin_async_call(client, prepared_call)
    # 发起真实 RPC，成功回调执行：
    parsed = pending.finish(response_pb_payload)
    # 通信失败回调执行：
    unknown = pending.fail(original_exception)
    # 到达真正的通信截止时间时执行：
    unknown = pending.fail(TimeoutError('RPC deadline exceeded'))

    finish/fail/abandon 的第一个终结事件生效；迟到或重复回调返回 None。
    本包装不创建截止时间计时器，也不停止底层 RPC；这些由 L3 负责。
    超时后的后端操作仍可能执行，应使用 GetStatus 进行对账。
    回调可能抛协议或观测异常，需要由 L3 回调错误处理接住；
    pending.result / pending.exception / pending.observability_error 保留结果。
    """
    return client.begin(call)


def close_client_log(client):
    """等待所有在途调用结束后再关闭日志；SDK 仍由进程生命周期管理。"""
    if client.sink is not None:
        client.sink.close()
