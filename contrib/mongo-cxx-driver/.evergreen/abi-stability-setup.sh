#!/usr/bin/env bash

set -o errexit
set -o pipefail

: "${cxx_standard:?}" # Set by abi-stability-checks-* build variant definition.

command -V git >/dev/null

# Files prepared by EVG config.
[[ -d "mongoc" ]] || {
  echo "missing mongoc" 1>&2
  exit 1
}
[[ -d "mongo-cxx-driver" ]] || {
  echo "missing mongo-cxx-driver" 1>&2
  exit 1
}

declare working_dir
working_dir="$(pwd)"

declare cmake_binary
# shellcheck source=/dev/null
. ./mongoc/.evergreen/scripts/find-cmake-latest.sh
cmake_binary="$(find_cmake_latest)"
command -V "${cmake_binary:?}"

# To use a different base commit, replace `--abbrev 0` with the intended commit.
# Note: EVG treat all changes relative to the EVG base commit as staged changes!
declare base current
base="$(git -C mongo-cxx-driver describe --tags --abbrev=0)"
current="$(git -C mongo-cxx-driver describe --tags)"

echo "Old Version (Base): ${base:?}"
echo "New Version (Current): ${current:?}"

printf "%s" "${base:?}" >base-commit.txt
printf "%s" "${current:?}" >current-commit.txt

# Remove 'r' prefix in version string.
declare old_ver new_ver
old_ver="${base:1}"
new_ver="${current:1}"

declare parallel_level
parallel_level="$(("$(nproc)" + 1))"

# Use Ninja if available.
if command -V ninja; then
  export CMAKE_GENERATOR
  CMAKE_GENERATOR="Ninja"
else
  export CMAKE_BUILD_PARALLEL_LEVEL
  CMAKE_BUILD_PARALLEL_LEVEL="${parallel_level:?}"
fi

# Use ccache if available.
if command -V ccache 2>/dev/null; then
  export CMAKE_C_COMPILER_LAUNCHER=ccache
  export CMAKE_CXX_COMPILER_LAUNCHER=ccache

  # Allow reuse of ccache compilation results between different build directories.
  export CCACHE_BASEDIR CCACHE_NOHASHDIR
  CCACHE_BASEDIR="$(pwd)/mongo-cxx-driver"
  CCACHE_NOHASHDIR=1
fi

# Install prefix to use for ABI compatibility scripts.
mkdir -p "${working_dir}/install"

# As encouraged by ABI compatibility checkers.
export CFLAGS CXXFLAGS
CFLAGS="-g -Og"
CXXFLAGS="-g -Og"

# Build and install the base commit first.
git -C mongo-cxx-driver stash push -u
git -C mongo-cxx-driver reset --hard "${base:?}"

# Install old (base) to install/old.
echo "Building old libraries..."
{
  "${cmake_binary:?}" \
    -S mongo-cxx-driver \
    -B build/old \
    -DCMAKE_INSTALL_PREFIX="install/old" \
    -DCMAKE_PREFIX_PATH="${working_dir:?}/mongoc" \
    -DBUILD_VERSION="${old_ver:?}-base" \
    -DCMAKE_CXX_STANDARD="${cxx_standard:?}"
  "${cmake_binary:?}" --build build/old
  "${cmake_binary:?}" --install build/old
} &>old.log || {
  cat old.log 1>&2
  exit 1
}
echo "Building old libraries... done."

# Restore all pending changes.
git -C mongo-cxx-driver reset --hard "HEAD@{1}"
git -C mongo-cxx-driver stash pop -q || true # Only patch builds have stashed changes.

# Install new (current) to install/new.
echo "Building new libraries..."
{
  "${cmake_binary:?}" \
    -S mongo-cxx-driver \
    -B build/new \
    -DCMAKE_INSTALL_PREFIX="install/new" \
    -DCMAKE_PREFIX_PATH="${working_dir:?}/mongoc" \
    -DBUILD_VERSION="${new_ver:?}-current" \
    -DCMAKE_CXX_STANDARD="${cxx_standard:?}"
  "${cmake_binary:?}" --build build/new
  "${cmake_binary:?}" --install build/new
} &>new.log || {
  cat new.log 1>&2
  exit 1
}
echo "Building new libraries... done."
