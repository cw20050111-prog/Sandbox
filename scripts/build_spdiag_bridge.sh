#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "$script_dir/.." && pwd)"
build_dir="${SPDIAG_BRIDGE_BUILD_DIR:-$project_dir/timing/spdiag/build}"

if ! command -v cmake >/dev/null 2>&1; then
    echo "CMake 3.16+ is required; install it before building the real SpDiag bridge." >&2
    exit 1
fi

# Set CMAKE_PREFIX_PATH or pass -DSpDiag_DIR=/path/to/SpDiag/cmake if the SDK
# package is installed outside the default search path. No SDK stub fallback.
cmake -S "$project_dir/timing/spdiag" -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release "$@"
cmake --build "$build_dir" --parallel "${SPDIAG_BRIDGE_BUILD_JOBS:-2}"
echo "SpDiag bridge: $build_dir/libspdiag_bridge.so"
