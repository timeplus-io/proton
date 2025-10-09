#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_opt CC
check_var_opt MARCH

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare install_dir="${mongoc_dir}/install-dir"
declare openssl_install_dir="${mongoc_dir}/openssl-install-dir"

declare cmake_prefix_path="${install_dir}"
if [[ -n "${EXTRA_CMAKE_PREFIX_PATH:-}" ]]; then
  cmake_prefix_path+=";${EXTRA_CMAKE_PREFIX_PATH}"
fi

declare -a configure_flags

configure_flags_append() {
  configure_flags+=("${@:?}")
}

configure_flags_append_if_not_null() {
  declare var="${1:?}"
  if [[ -n "${!var:-}" ]]; then
    shift
    configure_flags+=("${@:?}")
  fi
}

configure_flags_append "-DCMAKE_SKIP_RPATH=TRUE" # Avoid hardcoding absolute paths to dependency libraries.
configure_flags_append "-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF"
configure_flags_append "-DENABLE_MAINTAINER_FLAGS=ON"
configure_flags_append "-DENABLE_SSL=OPENSSL"
configure_flags_append "-DOPENSSL_USE_STATIC_LIBS=ON"

declare -a flags

case "${MARCH}" in
i686)
  flags+=("-m32" "-march=i386")
  ;;
esac

case "${HOSTTYPE}" in
s390x)
  flags+=("-march=z196" "-mtune=zEC12")
  ;;
x86_64)
  flags+=("-m64" "-march=x86-64")
  ;;
powerpc64le)
  flags+=("-mcpu=power8" "-mtune=power8" "-mcmodel=medium")
  ;;
esac

# CMake and compiler environment variables.
export CC
export CXX
export CFLAGS
export CXXFLAGS

CFLAGS+=" ${flags+${flags[*]}}"
CXXFLAGS+=" ${flags+${flags[*]}}"

if [[ "${OSTYPE}" == darwin* ]]; then
  CFLAGS+=" -Wno-unknown-pragmas"
fi

case "${CC}" in
clang)
  CXX=clang++
  ;;
gcc)
  CXX=g++
  ;;
esac

if [[ "${OSTYPE}" == darwin* && "${HOSTTYPE}" == "arm64" ]]; then
  configure_flags_append "-DCMAKE_OSX_ARCHITECTURES=arm64"
fi

# Ensure find-cmake-latest.sh is sourced *before* add-build-dirs-to-paths.sh
# to avoid interfering with potential CMake build configuration.
# shellcheck source=.evergreen/scripts/find-cmake-latest.sh
. "${script_dir}/find-cmake-latest.sh" # ${CMAKE}
CMAKE=$(find_cmake_latest)

# shellcheck source=.evergreen/scripts/add-build-dirs-to-paths.sh
. "${script_dir}/add-build-dirs-to-paths.sh"

export PKG_CONFIG_PATH
PKG_CONFIG_PATH="${install_dir}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$(pwd)"
CCACHE_NOHASHDIR=1

"${CMAKE}" "${configure_flags[@]}" .
"${CMAKE}" --build . -- -j "$(nproc)"
