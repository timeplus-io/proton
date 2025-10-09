#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_req CC

check_var_opt C_STD_VERSION
check_var_opt CFLAGS
check_var_opt MARCH

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare mongoc_dir
mongoc_dir="$(to_absolute "${script_dir}/../..")"

declare install_dir="${mongoc_dir}/install-dir"

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

configure_flags_append "-DCMAKE_PREFIX_PATH=${install_dir}"
configure_flags_append "-DCMAKE_SKIP_RPATH=TRUE" # Avoid hardcoding absolute paths to dependency libraries.
configure_flags_append "-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF"
configure_flags_append "-DENABLE_CLIENT_SIDE_ENCRYPTION=ON"
configure_flags_append "-DENABLE_DEBUG_ASSERTIONS=ON"
configure_flags_append "-DENABLE_MAINTAINER_FLAGS=ON"

configure_flags_append_if_not_null C_STD_VERSION "-DCMAKE_C_STANDARD=${C_STD_VERSION}"

if [[ "${OSTYPE}" == darwin* && "${HOSTTYPE}" == "arm64" ]]; then
  configure_flags_append "-DCMAKE_OSX_ARCHITECTURES=arm64"
fi

if [[ "${CC}" =~ ^"Visual Studio " ]]; then
  # Avoid C standard conformance issues with Windows 10 SDK headers.
  # See: https://developercommunity.visualstudio.com/t/stdc17-generates-warning-compiling-windowsh/1249671#T-N1257345
  configure_flags_append "-DCMAKE_SYSTEM_VERSION=10.0.20348.0"
fi

declare -a flags

if [[ ! "${CC}" =~ ^"Visual Studio " ]]; then
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
fi

if [[ "${CC}" =~ ^"Visual Studio " ]]; then
  # Even with -DCMAKE_SYSTEM_VERSION=10.0.20348.0, winbase.h emits conformance warnings.
  flags+=('/wd5105')
fi

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

# Ensure find-cmake-latest.sh is sourced *before* add-build-dirs-to-paths.sh
# to avoid interfering with potential CMake build configuration.
# shellcheck source=.evergreen/scripts/find-cmake-latest.sh
. "${script_dir}/find-cmake-latest.sh"
declare cmake_binary
cmake_binary="$(find_cmake_latest)"

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

echo "Installing libmongocrypt..."
# shellcheck source=.evergreen/scripts/compile-libmongocrypt.sh
"${script_dir}/compile-libmongocrypt.sh" "${cmake_binary}" "${mongoc_dir}" "${install_dir}" &>output.txt || {
  cat output.txt 1>&2
  exit 1
}
echo "Installing libmongocrypt... done."

echo "CFLAGS: ${CFLAGS}"
echo "configure_flags: ${configure_flags[*]}"

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$(pwd)"
CCACHE_NOHASHDIR=1

"${cmake_binary}" "${configure_flags[@]}" .
"${cmake_binary}" --build .
