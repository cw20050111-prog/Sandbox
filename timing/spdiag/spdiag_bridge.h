#ifndef AGENTCLIENT_SPDIAG_BRIDGE_H
#define AGENTCLIENT_SPDIAG_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define SPDIAG_BRIDGE_API __declspec(dllexport)
#elif defined(__GNUC__)
#define SPDIAG_BRIDGE_API __attribute__((visibility("default")))
#else
#define SPDIAG_BRIDGE_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

enum SpDiagBridgeResult {
    SPDIAG_BRIDGE_OK = 0,
    SPDIAG_BRIDGE_ALREADY_COMPLETED = 1,
    SPDIAG_BRIDGE_INVALID_ARGUMENT = -1,
    SPDIAG_BRIDGE_INSUFFICIENT_CAPACITY = -2,
    SPDIAG_BRIDGE_MISALIGNED_STORAGE = -3,
    SPDIAG_BRIDGE_INVALID_KEY = -4,
    SPDIAG_BRIDGE_SDK_EXCEPTION = -5,
    SPDIAG_BRIDGE_BUSY = -6,
    SPDIAG_BRIDGE_INVALID_STATE = -7
};

SPDIAG_BRIDGE_API uint32_t spdiag_bridge_abi_version(void);
SPDIAG_BRIDGE_API uint32_t spdiag_bridge_key_count(void);
// Compile-time names joined by '|', without a trailing separator.
SPDIAG_BRIDGE_API const char* spdiag_bridge_key_manifest(void);
SPDIAG_BRIDGE_API size_t spdiag_bridge_span_size(void);
SPDIAG_BRIDGE_API size_t spdiag_bridge_span_alignment(void);

// Storage belongs to the caller. It must be at least span_size() bytes and
// aligned to span_alignment(). Begin once; do not begin over a live span.
// No heap allocation, dynamic key lookup, or mutex is added by the bridge.
// key is a logical Client index 0..5, translated to the SDK enum by name.
SPDIAG_BRIDGE_API int spdiag_bridge_begin(void* storage, size_t capacity,
                                        uint32_t key);
// End may run on another thread. Exactly one concurrent End wins; duplicates
// return ALREADY_COMPLETED while the span is live. The winning rc is passed
// unchanged to SpDiag::PerfPoint::End (zero means success).
SPDIAG_BRIDGE_API int spdiag_bridge_end(void* storage, int32_t rc);
// Destroy once, after all End calls have returned. Destroy without End lets
// the SDK record Abandon. BUSY means an End is still running; retry afterwards.
// Destruction/reuse must be coordinated by the caller: this is not a refcount.
// If Begin returns an error, no live span remains and Destroy must not be used.
SPDIAG_BRIDGE_API int spdiag_bridge_destroy(void* storage);

#ifdef __cplusplus
}
#endif

#endif
