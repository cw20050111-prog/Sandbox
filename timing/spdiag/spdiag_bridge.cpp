#include "spdiag_bridge.h"

#define SPDIAG_PROGRAM_NAME "agentclient"
#include "spdiag/auto_perf.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <new>

namespace {

constexpr uint32_t kAbiVersion = 1;
constexpr uint32_t kSpanMagic = 0x53504447U;

// The C ABI uses logical keys 0..5; SDK enums may reserve values or have gaps.
// Generate an O(1) translation table from the same ordered definition manifest.
constexpr PerfKey kSdkKeys[] = {
#define PERF_KEY_DEF(enum_name, module_name, point_name) PerfKey::enum_name,
#include "perf_points.def"
#undef PERF_KEY_DEF
};
constexpr uint32_t kBridgeKeyCount = sizeof(kSdkKeys) / sizeof(kSdkKeys[0]);
static_assert(kBridgeKeyCount == 6, "Client C ABI must expose six logical keys.");

constexpr bool SdkKeysAreValidAndUnique() {
    for (uint32_t i = 0; i < kBridgeKeyCount; ++i) {
        if (static_cast<uint32_t>(kSdkKeys[i]) >= static_cast<uint32_t>(PerfKey::COUNT)) {
            return false;
        }
        for (uint32_t j = 0; j < i; ++j) {
            if (kSdkKeys[i] == kSdkKeys[j]) {
                return false;
            }
        }
    }
    return true;
}
static_assert(SdkKeysAreValidAndUnique(), "Invalid or duplicate SDK performance keys.");
static_assert(std::atomic<uint32_t>::is_always_lock_free,
              "The bridge requires lock-free 32-bit atomics.");

constexpr char kManifestWithTrailingSeparator[] =
#define PERF_KEY_DEF(enum_name, module_name, point_name) #enum_name "|"
#include "perf_points.def"
#undef PERF_KEY_DEF
    ;

template <size_t N>
constexpr std::array<char, N - 1> WithoutTrailingSeparator(const char (&s)[N]) {
    std::array<char, N - 1> result{};
    for (size_t i = 0; i < N - 2; ++i) {
        result[i] = s[i];
    }
    result[N - 2] = '\0';
    return result;
}

constexpr auto kManifest = WithoutTrailingSeparator(kManifestWithTrailingSeparator);

enum class SpanState : uint32_t {
    Active = 0,
    Ending = 1,
    Ended = 2,
    Failed = 3,
    Destroying = 4
};

struct Span {
    uint32_t magic;
    std::atomic<uint32_t> state;
    SpDiag::PerfPoint point;

    explicit Span(PerfKey key)
        : magic(kSpanMagic),
          state(static_cast<uint32_t>(SpanState::Active)),
          point(key, SpDiag::global_perf, SpDiag::PerfLevel::KEY_MODULE) {}

    ~Span() noexcept(false) = default;
};

int ValidateStorage(const void* storage) noexcept {
    if (storage == nullptr) {
        return SPDIAG_BRIDGE_INVALID_ARGUMENT;
    }
    if (reinterpret_cast<uintptr_t>(storage) % alignof(Span) != 0) {
        return SPDIAG_BRIDGE_MISALIGNED_STORAGE;
    }
    return SPDIAG_BRIDGE_OK;
}

}  // namespace

extern "C" {

uint32_t spdiag_bridge_abi_version(void) { return kAbiVersion; }

uint32_t spdiag_bridge_key_count(void) {
    return kBridgeKeyCount;
}

const char* spdiag_bridge_key_manifest(void) { return kManifest.data(); }

size_t spdiag_bridge_span_size(void) { return sizeof(Span); }

size_t spdiag_bridge_span_alignment(void) { return alignof(Span); }

int spdiag_bridge_begin(void* storage, size_t capacity, uint32_t key) {
    const int validation = ValidateStorage(storage);
    if (validation != SPDIAG_BRIDGE_OK) {
        return validation;
    }
    if (capacity < sizeof(Span)) {
        return SPDIAG_BRIDGE_INSUFFICIENT_CAPACITY;
    }
    if (key >= kBridgeKeyCount) {
        return SPDIAG_BRIDGE_INVALID_KEY;
    }
    Span* span = nullptr;
    try {
        // Placement new constructs PerfPoint before Start in caller storage.
        span = new (storage) Span(kSdkKeys[key]);
        span->point.Start();
        return SPDIAG_BRIDGE_OK;
    } catch (...) {
        // Constructors unwind their members. If Start threw instead, destroy
        // the completed object explicitly; no exception crosses the C ABI.
        if (span != nullptr) {
            try {
                span->magic = 0;
                span->~Span();
            } catch (...) {
                // The operation still reports an SDK failure to the caller.
            }
        }
        return SPDIAG_BRIDGE_SDK_EXCEPTION;
    }
}

int spdiag_bridge_end(void* storage, int32_t rc) {
    const int validation = ValidateStorage(storage);
    if (validation != SPDIAG_BRIDGE_OK) {
        return validation;
    }
    Span* span = static_cast<Span*>(storage);
    if (span->magic != kSpanMagic) {
        return SPDIAG_BRIDGE_INVALID_STATE;
    }
    uint32_t expected = static_cast<uint32_t>(SpanState::Active);
    if (!span->state.compare_exchange_strong(
            expected, static_cast<uint32_t>(SpanState::Ending),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (expected == static_cast<uint32_t>(SpanState::Ending) ||
            expected == static_cast<uint32_t>(SpanState::Ended)) {
            return SPDIAG_BRIDGE_ALREADY_COMPLETED;
        }
        if (expected == static_cast<uint32_t>(SpanState::Failed)) {
            return SPDIAG_BRIDGE_SDK_EXCEPTION;
        }
        return SPDIAG_BRIDGE_INVALID_STATE;
    }
    try {
        span->point.End(rc);
        span->state.store(static_cast<uint32_t>(SpanState::Ended),
                          std::memory_order_release);
        return SPDIAG_BRIDGE_OK;
    } catch (...) {
        span->state.store(static_cast<uint32_t>(SpanState::Failed),
                          std::memory_order_release);
        return SPDIAG_BRIDGE_SDK_EXCEPTION;
    }
}

int spdiag_bridge_destroy(void* storage) {
    const int validation = ValidateStorage(storage);
    if (validation != SPDIAG_BRIDGE_OK) {
        return validation;
    }
    Span* span = static_cast<Span*>(storage);
    if (span->magic != kSpanMagic) {
        return SPDIAG_BRIDGE_INVALID_STATE;
    }
    uint32_t expected = span->state.load(std::memory_order_acquire);
    for (;;) {
        if (expected == static_cast<uint32_t>(SpanState::Ending)) {
            return SPDIAG_BRIDGE_BUSY;
        }
        if (expected == static_cast<uint32_t>(SpanState::Destroying)) {
            return SPDIAG_BRIDGE_INVALID_STATE;
        }
        if (span->state.compare_exchange_weak(
                expected, static_cast<uint32_t>(SpanState::Destroying),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    try {
        span->magic = 0;
        // An unfinished point is intentionally abandoned, never End(0).
        span->~Span();
        return SPDIAG_BRIDGE_OK;
    } catch (...) {
        return SPDIAG_BRIDGE_SDK_EXCEPTION;
    }
}

}  // extern "C"
