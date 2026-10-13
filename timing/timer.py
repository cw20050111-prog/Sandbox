"""SpDiag C ABI adapter; clock_only is an explicit offline-only alternative.

The native SDK uses fixed enum keys. Python object creation, ctypes calls, locks,
clock reads and optional JSONL output still have overhead; the native SDK's O(1)
hot-path claim does not apply to this entire Python adapter.
"""
from __future__ import annotations

import ctypes
from enum import IntEnum
from pathlib import Path


class PerfKey(IntEnum):
    RPC_CREATE_SANDBOX = 0
    RPC_RUN_STEP = 1
    RPC_PAUSE_SANDBOX = 2
    RPC_RESUME_SANDBOX = 3
    RPC_DELETE_SANDBOX = 4
    RPC_GET_STATUS = 5


class ReturnCode(IntEnum):
    SUCCESS = 0
    BUSINESS_ERROR = 1
    TRANSPORT_ERROR = 2
    TIMEOUT = 3
    PROTOCOL_ERROR = 4
    CANCELLED = 5
    INTERNAL_ERROR = 6


METHOD_KEYS = {
    "CreateSandbox": PerfKey.RPC_CREATE_SANDBOX,
    "RunStep": PerfKey.RPC_RUN_STEP,
    "PauseSandbox": PerfKey.RPC_PAUSE_SANDBOX,
    "ResumeSandbox": PerfKey.RPC_RESUME_SANDBOX,
    "DeleteSandbox": PerfKey.RPC_DELETE_SANDBOX,
    "GetStatus": PerfKey.RPC_GET_STATUS,
}


class PerfBackendError(RuntimeError):
    """SDK integration failed; this is never converted to clock_only silently."""


class _ClockOnlySpan:
    """No shared memory and no emulated native statistics."""
    def __init__(self):
        self.started = False
        self.closed = False

    def start(self):
        if self.started or self.closed:
            raise PerfBackendError("span 已启动或已关闭")
        self.started = True

    def close(self, rc: int):
        if not self.started or self.closed:
            raise PerfBackendError("span 未启动或重复结束")
        self.closed = True

    def abandon(self):
        self.closed = True


class _NativeSpan:
    def __init__(self, backend: "PerfBackend", key: PerfKey):
        self.backend = backend
        self.key = key
        # One caller-owned aligned buffer for each in-flight measurement.
        self._buffer = ctypes.create_string_buffer(backend.span_size + backend.span_alignment - 1)
        address = ctypes.addressof(self._buffer)
        address = (address + backend.span_alignment - 1) & ~(backend.span_alignment - 1)
        self.storage = ctypes.c_void_p(address)
        self.started = False
        self.closed = False

    def start(self):
        if self.started or self.closed:
            raise PerfBackendError("span 已启动或已关闭")
        status = self.backend.lib.spdiag_bridge_begin(self.storage, self.backend.span_size, int(self.key))
        if status != 0:
            raise PerfBackendError(f"spdiag_bridge_begin 失败，status={status}")
        self.started = True

    def close(self, rc: int):
        if not self.started or self.closed:
            raise PerfBackendError("span 未启动或重复结束")
        end_status = self.backend.lib.spdiag_bridge_end(self.storage, int(rc))
        destroy_status = self.backend.lib.spdiag_bridge_destroy(self.storage)
        # Keep the owning buffer alive through both C calls.
        self.closed = destroy_status == 0
        if end_status != 0 or destroy_status != 0:
            raise PerfBackendError(f"SpDiag End/destroy 失败，end={end_status}, destroy={destroy_status}")

    def abandon(self):
        if self.closed:
            return
        if self.started:
            status = self.backend.lib.spdiag_bridge_destroy(self.storage)
            if status != 0:
                raise PerfBackendError(f"spdiag_bridge_destroy 失败，status={status}")
        self.closed = True

    def __del__(self):
        # A truly forgotten span has no End: native destruction records Abandon.
        try:
            self.abandon()
        except BaseException:
            pass


class PerfBackend:
    """Default mode requires the real SpDiag bridge and validates its key ABI.

    SDK no-op when spdiag start has not been run is SDK behavior, not a fallback
    backend. Inspect spdiag status/show to establish that collection is enabled.
    """
    def __init__(self, *, mode: str = "native", native_path: str | Path | None = None,
                 allow_test_sdk: bool = False):
        if mode not in ("native", "clock_only"):
            raise ValueError("mode 必须为 native 或 clock_only")
        self.mode = mode
        if mode == "clock_only":
            if native_path is not None or allow_test_sdk:
                raise ValueError("clock_only 不加载 native_path/test SDK")
            return
        path = Path(native_path) if native_path is not None else (
            Path(__file__).resolve().parent / "spdiag/build/libspdiag_bridge.so")
        try:
            self.lib = ctypes.CDLL(str(path))
        except OSError as exc:
            raise PerfBackendError(
                f"无法加载 SpDiag bridge: {path}；先安装 SpDiag 并执行 scripts/build_spdiag_bridge.sh 进行 CMake 构建；"
                "离线 mock 可显式选择 mode='clock_only'。") from exc
        marker = getattr(self.lib, "spdiag_bridge_test_only_sdk", None)
        if marker is not None:
            marker.argtypes = []
            marker.restype = ctypes.c_int
            if marker() == 1:
                if not allow_test_sdk:
                    raise PerfBackendError("拒绝在 native 模式加载 TEST ONLY SDK；使用真实 SpDiag 构建")
                self.mode = "test_sdk"
        if allow_test_sdk and self.mode != "test_sdk":
            raise PerfBackendError("allow_test_sdk 仅用于明确标记的 TEST ONLY bridge")
        signatures = {
            "abi_version": ([], ctypes.c_uint32),
            "key_count": ([], ctypes.c_uint32),
            "key_manifest": ([], ctypes.c_char_p),
            "span_size": ([], ctypes.c_size_t),
            "span_alignment": ([], ctypes.c_size_t),
            "begin": ([ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32], ctypes.c_int),
            "end": ([ctypes.c_void_p, ctypes.c_int32], ctypes.c_int),
            "destroy": ([ctypes.c_void_p], ctypes.c_int),
        }
        try:
            for name, (arguments, result) in signatures.items():
                function = getattr(self.lib, "spdiag_bridge_" + name)
                function.argtypes = arguments
                function.restype = result
            manifest = self.lib.spdiag_bridge_key_manifest()
            expected = "|".join(key.name for key in PerfKey).encode("ascii")
            if (self.lib.spdiag_bridge_abi_version() != 1 or
                    self.lib.spdiag_bridge_key_count() != len(PerfKey) or manifest != expected):
                raise PerfBackendError("SpDiag bridge ABI 或 PerfKey 顺序不匹配；请重新构建同版本 bridge")
            self.span_size = self.lib.spdiag_bridge_span_size()
            self.span_alignment = self.lib.spdiag_bridge_span_alignment()
            if (not 0 < self.span_size <= 64 * 1024 * 1024 or
                    not 0 < self.span_alignment <= 1024 * 1024 or
                    self.span_alignment & (self.span_alignment - 1)):
                raise PerfBackendError("SpDiag bridge 返回非法 span size/alignment")
        except AttributeError as exc:
            raise PerfBackendError("SpDiag bridge 缺少 ABI 函数；请重新构建同版本 bridge") from exc

    def make_span(self, key: PerfKey):
        key = PerfKey(key)
        return _NativeSpan(self, key) if self.mode in ("native", "test_sdk") else _ClockOnlySpan()
