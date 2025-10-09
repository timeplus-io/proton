#!/usr/bin/env bash

set -o errexit
set -o pipefail

: "${BSONCXX_POLYFILL:?}"
: "${CXX_STANDARD:?}"

mongoc_prefix="$(pwd)/../mongoc"

# shellcheck source=/dev/null
. "${mongoc_prefix:?}/.evergreen/scripts/find-cmake-latest.sh"
export cmake_binary
cmake_binary="$(find_cmake_latest)"
command -v "$cmake_binary"

# scan-build binary is available in different locations depending on the distro.
# Search for a match in order of preference as listed.
declare -a scan_build_directories

scan_build_directories+=(
  # Prefer toolchain scan-build if available.
  "/opt/mongodbtoolchain/v4/bin"
  "/opt/mongodbtoolchain/v3/bin"
)

# Use system scan-build otherwise.
IFS=: read -ra sys_dirs <<< "${PATH:-}"
scan_build_directories+=("${sys_dirs[@]:-}")

declare scan_build_binary
for dir in "${scan_build_directories[@]}"; do
  if command -v "${dir}/scan-build" && command -v "${dir}/clang" && command -v "${dir}/clang++"; then
    # Ensure compilers are consistent with scan-build binary. All three binaries
    # should be present in the same directory.
    scan_build_binary="${dir}/scan-build"
    CC="${dir}/clang"
    CXX="${dir}/clang++"
    break
  fi
done
: "${scan_build_binary:?"could not find a scan-build binary!"}"
export CC
export CXX

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi
CMAKE_BUILD_PARALLEL_LEVEL="$(nproc)"
export CMAKE_BUILD_PARALLEL_LEVEL

cmake_flags=(
  -D CMAKE_BUILD_TYPE=Debug
  -D "CMAKE_CXX_STANDARD=${CXX_STANDARD:?}"
  -D CMAKE_CXX_STANDARD_REQUIRED=ON
  -D ENABLE_TESTS=OFF
)

scan_build_flags=(
  --use-cc "${CC:?}"
  --use-c++ "${CXX:?}"
  -analyze-headers
  --exclude "$(pwd)/build/src/bsoncxx/third_party/_deps" # mnmlstc
  --exclude "$(pwd)/build/_deps"                         # mongoc
)

case "${BSONCXX_POLYFILL:?}" in
mnmlstc) cmake_flags+=(-D "BSONCXX_POLY_USE_MNMLSTC=ON") ;;
boost) cmake_flags+=(-D "BSONCXX_POLY_USE_BOOST=ON") ;;
impls) cmake_flags+=(-D "BSONCXX_POLY_USE_IMPLS=ON") ;;
std) cmake_flags+=(-D "BSONCXX_POLY_USE_STD=ON") ;;
esac

echo "Configuring with CMake flags: ${cmake_flags[*]}"

# Configure via scan-build for consistency.
CCCACHE_DISABLE=1 "${scan_build_binary}" "${scan_build_flags[@]}" "${cmake_binary:?}" -S . -B build "${cmake_flags[@]}"

# If scan-build emits warnings, continue the task and upload scan results before marking task as a failure.
declare -r continue_command='{"status":"failed", "type":"test", "should_continue":true, "desc":"scan-build emitted one or more warnings or errors"}'

# Put clang static analyzer results in scan/ and fail build if warnings found.
"${scan_build_binary}" "${scan_build_flags[@]}" -o scan --status-bugs "${cmake_binary:?}" --build build ||
  curl -sS -d "${continue_command}" -H "Content-Type: application/json" -X POST localhost:2285/task_status
