#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_opt BYPASS_FIND_CMAKE "OFF"
check_var_opt C_STD_VERSION # CMake default: 99.
check_var_opt CC
check_var_opt CFLAGS
check_var_opt CHECK_LOG "OFF"
check_var_opt COMPILE_LIBMONGOCRYPT "OFF"
check_var_opt COVERAGE # CMake default: OFF.
check_var_opt CXXFLAGS
check_var_opt DEBUG "OFF"
check_var_opt ENABLE_SHM_COUNTERS # CMake default: AUTO.
check_var_opt EXTRA_CMAKE_PREFIX_PATH
check_var_opt EXTRA_CONFIGURE_FLAGS
check_var_opt ENABLE_RDTSCP "OFF"
check_var_opt MARCH
check_var_opt RELEASE "OFF"
check_var_opt SANITIZE
check_var_opt SASL "OFF"     # CMake default: AUTO.
check_var_opt SNAPPY         # CMake default: AUTO.
check_var_opt SRV            # CMake default: AUTO.
check_var_opt SSL "OFF"      # CMake default: AUTO.
check_var_opt TRACING        # CMake default: OFF.
check_var_opt ZLIB "BUNDLED" # CMake default: AUTO.
check_var_opt ZSTD           # CMake default: AUTO.

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

## * Note: For additional configure-time context, the following lines can be
## * uncommented to enable CMake's debug output:
# configure_flags_append --log-level=debug
# configure_flags_append --log-context

configure_flags_append "-DCMAKE_INSTALL_PREFIX=${install_dir}"
configure_flags_append "-DCMAKE_PREFIX_PATH=${cmake_prefix_path}"
configure_flags_append "-DCMAKE_SKIP_RPATH=TRUE" # Avoid hardcoding absolute paths to dependency libraries.
configure_flags_append "-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF"
configure_flags_append "-DENABLE_HTML_DOCS=OFF"
configure_flags_append "-DENABLE_MAINTAINER_FLAGS=ON"
configure_flags_append "-DENABLE_MAN_PAGES=OFF"

configure_flags_append_if_not_null C_STD_VERSION "-DCMAKE_C_STANDARD=${C_STD_VERSION}"
configure_flags_append_if_not_null ENABLE_RDTSCP "-DENABLE_RDTSCP=${ENABLE_RDTSCP}"
configure_flags_append_if_not_null ENABLE_SHM_COUNTERS "-DENABLE_SHM_COUNTERS=${ENABLE_SHM_COUNTERS}"
configure_flags_append_if_not_null SANITIZE "-DMONGO_SANITIZE=${SANITIZE}"
configure_flags_append_if_not_null SASL "-DENABLE_SASL=${SASL}"
configure_flags_append_if_not_null SNAPPY "-DENABLE_SNAPPY=${SNAPPY}"
configure_flags_append_if_not_null SRV "-DENABLE_SRV=${SRV}"
configure_flags_append_if_not_null TRACING "-DENABLE_TRACING=${TRACING}"
configure_flags_append_if_not_null ZLIB "-DENABLE_ZLIB=${ZLIB}"

if [[ "${DEBUG}" == "ON" ]]; then
  configure_flags_append "-DCMAKE_BUILD_TYPE=Debug"
else
  configure_flags_append "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
fi

if [[ "${SSL}" == "OPENSSL_STATIC" ]]; then
  configure_flags_append "-DENABLE_SSL=OPENSSL" "-DOPENSSL_USE_STATIC_LIBS=ON"
else
  configure_flags_append_if_not_null SSL "-DENABLE_SSL=${SSL}"
fi

if [[ "${COVERAGE}" == "ON" ]]; then
  configure_flags_append "-DENABLE_COVERAGE=ON" "-DENABLE_EXAMPLES=OFF"
fi

if [[ "${RELEASE}" != "ON" ]]; then
  configure_flags_append "-DENABLE_DEBUG_ASSERTIONS=ON"
fi

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

if [[ "${OSTYPE}" == darwin* && "${HOSTTYPE}" == "arm64" ]]; then
  configure_flags_append "-DCMAKE_OSX_ARCHITECTURES=arm64"
fi

case "${CC}" in
clang)
  CXX=clang++
  ;;
gcc)
  CXX=g++
  ;;
esac

declare cmake_binary
if [[ "${BYPASS_FIND_CMAKE}" == "OFF" ]]; then
  # Ensure find-cmake-latest.sh is sourced *before* add-build-dirs-to-paths.sh
  # to avoid interfering with potential CMake build configuration.
  # shellcheck source=.evergreen/scripts/find-cmake-latest.sh
  . "${script_dir}/find-cmake-latest.sh"
  cmake_binary="$(find_cmake_latest)"
else
  cmake_binary="cmake"
fi

"${cmake_binary:?}" --version

# shellcheck source=.evergreen/scripts/add-build-dirs-to-paths.sh
. "${script_dir}/add-build-dirs-to-paths.sh"

export PKG_CONFIG_PATH
PKG_CONFIG_PATH="${install_dir}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

if [[ -d "${openssl_install_dir}" ]]; then
  # Use custom SSL library if present.
  configure_flags_append "-DOPENSSL_ROOT_DIR=${openssl_install_dir}"
  PKG_CONFIG_PATH="${openssl_install_dir}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

echo "SSL Version: $(pkg-config --modversion libssl 2>/dev/null || echo "N/A")"

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi

declare -a extra_configure_flags
IFS=' ' read -ra extra_configure_flags <<<"${EXTRA_CONFIGURE_FLAGS:-}"

if [[ "${COMPILE_LIBMONGOCRYPT}" == "ON" ]]; then
  echo "Installing libmongocrypt..."
  # shellcheck source=.evergreen/scripts/compile-libmongocrypt.sh
  "${script_dir}/compile-libmongocrypt.sh" "${cmake_binary}" "${mongoc_dir}" "${install_dir}" &>output.txt || {
    cat output.txt 1>&2
    exit 1
  }
  echo "Installing libmongocrypt... done."

  # Fail if the C driver is unable to find the installed libmongocrypt.
  configure_flags_append "-DENABLE_CLIENT_SIDE_ENCRYPTION=ON"
else
  # Avoid symbol collisions with libmongocrypt installed via apt/yum.
  # Note: may be overwritten by ${EXTRA_CONFIGURE_FLAGS}.
  configure_flags_append "-DENABLE_CLIENT_SIDE_ENCRYPTION=OFF"
fi

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$(pwd)"
CCACHE_NOHASHDIR=1

"${cmake_binary}" "${configure_flags[@]}" ${extra_configure_flags[@]+"${extra_configure_flags[@]}"} .
"${cmake_binary}" --build . -- -j "$(nproc)"
"${cmake_binary}" --build . --target install
