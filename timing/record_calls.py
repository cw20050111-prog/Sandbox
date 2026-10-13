"""Measure actual L3 transport calls, then validate their L2 responses.

No transport, timeout scheduler, retry, or reconciliation loop is implemented
here. Supply a callable that sends one PreparedCall and returns PB bytes/message.
For async transports call begin immediately before dispatch, then finish from
the callback or fail on a transport error/deadline/cancellation. The native span
uses global_perf, so start and completion may happen on different threads.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import CancelledError as FutureCancelledError
import json
from pathlib import Path
import threading
import time
from typing import Callable

from google.protobuf.message import Message
from ..message import agent_proxy_pb2 as pb
from ..message.request_response import PreparedCall, ParsedResponse, ProtocolError, parse_response, transport_failure
from .timer import METHOD_KEYS, PerfBackend, ReturnCode


class JsonlSink:
    """Optional line-atomic JSONL output, outside native PerfPoint timing.

    One sink can be shared by threads within a process. Give different processes
    distinct files; this lock does not coordinate separate processes.
    """
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, record: dict):
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
        with self._lock:
            self._file.write(line)
            self._file.flush()

    def close(self):
        with self._lock:
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


def _classification(exc: BaseException, *, parsing: bool = False):
    if isinstance(exc, (asyncio.CancelledError, FutureCancelledError, KeyboardInterrupt, SystemExit)):
        return ReturnCode.CANCELLED, "cancelled"
    if isinstance(exc, TimeoutError):
        return ReturnCode.TIMEOUT, "timeout"
    if isinstance(exc, ProtocolError):
        return ReturnCode.PROTOCOL_ERROR, "protocol_error"
    if parsing:
        return ReturnCode.INTERNAL_ERROR, "internal_error"
    return ReturnCode.TRANSPORT_ERROR, "transport_error"


class PendingCall:
    """One in-flight request; the first finish/fail/abandon wins.

    Duplicate or late completion returns None and never emits another PerfPoint
    or line. Calling fail(TimeoutError()) is the async transport owner's duty.
    The object must be retained by the transport until completion/cancellation.
    """
    def __init__(self, call: PreparedCall, *, backend: PerfBackend, sink=None):
        self.call = call
        self.backend = backend
        self.sink = sink
        self._lock = threading.Lock()
        self._done = False
        self.result: ParsedResponse | None = None
        self.exception: BaseException | None = None
        self.observability_error: BaseException | None = None
        self.record: dict | None = None
        request = call.request
        meta = request.meta
        # Metadata extraction and aligned buffer allocation occur before t0.
        self._base = {
            "schema_version": "1.0", "method": call.method,
            "perf_key": METHOD_KEYS[call.method].name, "perf_backend": backend.mode,
            "run_id": meta.run_id, "task_id": meta.task_id,
            "task_instance_id": meta.task_instance_id, "request_id": meta.request_id,
            "trace_id": meta.trace_id or None, "round_id": meta.round_id or None,
            "call_id": meta.call_id or None, "phase": pb.Phase.Name(meta.phase),
            "step_id": getattr(request, "step_id", None), "tool": getattr(request, "tool", None),
            "target_sandbox_id": getattr(request, "sandbox_id", "") or None,
            "target_operation_request_id": getattr(request, "operation_request_id", "") or None,
            "target_create_request_id": getattr(request, "create_request_id", "") or None,
            "rpc_timeout_ms": call.rpc_timeout_ms,
        }
        self._span = backend.make_span(METHOD_KEYS[call.method])
        self._span.start()
        # Native interval is slightly wider than the Python clock interval.
        # All latency subtraction uses perf_counter_ns on this client only.
        self._t0_unix = time.time_ns()
        self._t0 = time.perf_counter_ns()

    @property
    def done(self) -> bool:
        with self._lock:
            return self._done

    def _attach(self, exc: BaseException):
        # Helpful local recovery metadata without replacing the original error.
        try:
            exc.measured_response = self.result
            exc.perf_record = self.record
            exc.perf_observability_error_type = (type(self.observability_error).__name__
                                                 if self.observability_error is not None else None)
        except (AttributeError, TypeError):
            pass

    def _complete(self, *, rc, outcome, t_received=None, t_received_unix=None,
                  t1=None, t1_unix=None, parsed=None, exc=None):
        self._done = True
        self.result = parsed
        self.exception = exc
        native_error = None
        try:
            if rc is None:
                self._span.abandon()  # Deliberately no End; SDK records Abandon.
            else:
                self._span.close(int(rc))  # Explicit native End for every terminal result.
        except BaseException as error:
            native_error = error
            self.observability_error = error
        original = parsed.final_response if parsed is not None else None
        tool_result = parsed.tool_result if parsed is not None else None
        transport_ns = t_received - self._t0 if t_received is not None else None
        self.record = {
            **self._base,
            "t0_monotonic_ns": self._t0, "t_received_monotonic_ns": t_received,
            "t1_monotonic_ns": t1, "t0_unix_ns": self._t0_unix,
            "t_received_unix_ns": t_received_unix, "t1_unix_ns": t1_unix,
            "transport_ns": transport_ns,
            "parse_ns": t1 - t_received if t_received is not None and t1 is not None else None,
            "total_ns": t1 - self._t0 if t1 is not None else None,
            "rc": int(rc) if rc is not None else None, "outcome": outcome,
            "response_success": parsed.success if parsed is not None and exc is None else None,
            "error_code": (exc.code if isinstance(exc, ProtocolError) else
                           parsed.error_code if parsed is not None and exc is None else None),
            "exception_type": type(exc).__name__ if exc is not None else None,
            "sandbox_id": (parsed.sandbox_id if parsed is not None else None) or self._base["target_sandbox_id"],
            "operation_state": parsed.operation_state if parsed is not None else None,
            "dispatch_state": parsed.dispatch_state if parsed is not None else None,
            "requires_reconciliation": parsed.requires_reconciliation if parsed is not None else True,
            "original_operation_state": (parsed.operation_state
                if self.call.method == "GetStatus" and parsed is not None else None),
            "original_response_success": original.success if original is not None else None,
            "original_error_code": original.error_code if original is not None else None,
            "tool_exit_code": tool_result.get("exit_code") if tool_result is not None else None,
            "process_finished": parsed.process_finished if parsed is not None else None,
            "cleanup_confirmed": parsed.cleanup_confirmed if parsed is not None else False,
            "proxy_elapsed_ms": parsed.proxy_elapsed_ms if parsed is not None else None,
            "perf_native_error_type": type(native_error).__name__ if native_error is not None else None,
        }
        # JSON serialization, disk writes and sink locks are excluded from span.
        if self.sink is not None:
            try:
                self.sink.write(self.record)
            except BaseException as error:
                self.observability_error = error
        if exc is not None:
            self._attach(exc)

    def finish(self, response: Message | bytes) -> ParsedResponse | None:
        # Capture raw-return boundary before parsing, locks, or correlation checks.
        received = time.perf_counter_ns()
        received_unix = time.time_ns()
        with self._lock:
            if self._done:
                return None
            try:
                parsed = parse_response(self.call, response)
            except BaseException as exc:
                t1 = time.perf_counter_ns()
                t1_unix = time.time_ns()
                rc, outcome = _classification(exc, parsing=True)
                unknown = transport_failure(self.call, "response validation failed; reconcile original request")
                self._complete(rc=rc, outcome=outcome, t_received=received,
                               t_received_unix=received_unix, t1=t1, t1_unix=t1_unix,
                               parsed=unknown, exc=exc)
                raise
            t1 = time.perf_counter_ns()
            t1_unix = time.time_ns()
            rc = ReturnCode.SUCCESS if parsed.success else ReturnCode.BUSINESS_ERROR
            self._complete(rc=rc, outcome="success" if parsed.success else "business_error",
                           t_received=received, t_received_unix=received_unix,
                           t1=t1, t1_unix=t1_unix, parsed=parsed)
            if self.observability_error is not None:
                self._attach(self.observability_error)
                raise self.observability_error
            return parsed

    def fail(self, exc: BaseException) -> ParsedResponse | None:
        """Complete an error/deadline; unknown does not prove backend nonexecution."""
        if not isinstance(exc, BaseException):
            raise TypeError("fail 需要原始异常对象")
        with self._lock:
            if self._done:
                return None
            t1 = time.perf_counter_ns()
            t1_unix = time.time_ns()
            rc, outcome = _classification(exc)
            unknown = transport_failure(self.call, "transport did not yield a validated result; reconcile original request")
            self._complete(rc=rc, outcome=outcome, t1=t1, t1_unix=t1_unix, parsed=unknown, exc=exc)
            # Do not replace the caller's original timeout/transport exception.
            return unknown

    def abandon(self) -> bool:
        """Only for genuinely forgotten measurements, never known error/timeout."""
        with self._lock:
            if self._done:
                return False
            self._complete(rc=None, outcome="abandon")
            return True

    def __del__(self):
        try:
            if hasattr(self, "_span") and not self._done:
                self.abandon()
        except BaseException:
            pass


class MeasuredClient:
    def __init__(self, transport: Callable[[PreparedCall], Message | bytes] | None = None, *,
                 backend: PerfBackend | None = None, sink=None):
        self.transport = transport
        self.backend = backend if backend is not None else PerfBackend()
        self.sink = sink

    def begin(self, call: PreparedCall) -> PendingCall:
        return PendingCall(call, backend=self.backend, sink=self.sink)

    def call(self, call: PreparedCall) -> ParsedResponse:
        if self.transport is None:
            raise ValueError("同步 call 需要 transport；异步通道请使用 begin/finish/fail")
        pending = self.begin(call)
        try:
            response = self.transport(call)
        except BaseException as exc:
            pending.fail(exc)
            raise
        return pending.finish(response)
