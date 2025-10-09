#!/bin/bash

# Runs cmake and compiles the standard build targets (all, install, examples).  Any arguments passed
# to this script will be forwarded on as flags passed to cmake.
#
# This script should be run from the root of the repository.  This script will run the build from
# the default build directory './build'.  The following environment variables will change the
# behavior of this script:
# - build_type: must be set to "Release" or "Debug"

set -o errexit
set -o pipefail

: "${branch_name:?}"
: "${build_type:?}"
: "${distro_id:?}" # Required by find-cmake-latest.sh.

: "${COMPILE_MACRO_GUARD_TESTS:-}"
: "${ENABLE_CODE_COVERAGE:-}"
: "${ENABLE_TESTS:-}"
: "${generator:-}"
: "${platform:-}"
: "${REQUIRED_CXX_STANDARD:-}"
: "${RUN_DISTCHECK:-}"
: "${USE_POLYFILL_BOOST:-}"
: "${USE_SANITIZER_ASAN:-}"
: "${USE_SANITIZER_UBSAN:-}"
: "${USE_STATIC_LIBS:-}"

mongoc_prefix="$(pwd)/../mongoc"
echo "mongoc_prefix=${mongoc_prefix:?}"

if [[ "${OSTYPE:?}" =~ cygwin ]]; then
  mongoc_prefix=$(cygpath -m "${mongoc_prefix:?}")
fi

# shellcheck source=/dev/null
. "${mongoc_prefix:?}/.evergreen/scripts/find-cmake-latest.sh"
export cmake_binary
cmake_binary="$(find_cmake_latest)"
command -v "$cmake_binary"

if [ ! -d ../drivers-evergreen-tools ]; then
  git clone --depth 1 git@github.com:mongodb-labs/drivers-evergreen-tools.git ../drivers-evergreen-tools
fi
# shellcheck source=/dev/null
. ../drivers-evergreen-tools/.evergreen/find-python3.sh
# shellcheck source=/dev/null
. ../drivers-evergreen-tools/.evergreen/venv-utils.sh

venvcreate "$(find_python3)" venv
python -m pip install GitPython

if [[ "${build_type:?}" != "Debug" && "${build_type:?}" != "Release" ]]; then
  echo "$0: expected build_type environment variable to be set to 'Debug' or 'Release'" >&2
  exit 1
fi

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi
CMAKE_BUILD_PARALLEL_LEVEL="$(nproc)"
export CMAKE_BUILD_PARALLEL_LEVEL

# Use ccache if available.
if command -V ccache 2>/dev/null; then
  export CMAKE_CXX_COMPILER_LAUNCHER=ccache

  # Allow reuse of ccache compilation results between different build directories.
  export CCACHE_BASEDIR CCACHE_NOHASHDIR
  if [[ "${OSTYPE:?}" == "cygwin" ]]; then
    CCACHE_BASEDIR="$(cygpath -aw "$(pwd)")"
  else
    CCACHE_BASEDIR="$(pwd)"
  fi
  CCACHE_NOHASHDIR=1
fi

cmake_build_opts=()
case "${OSTYPE:?}" in
cygwin)
  cmake_build_opts+=("/verbosity:minimal")
  cmake_examples_target="examples/examples"
  ;;

darwin* | linux*)
  cmake_examples_target="examples"
  ;;

*)
  echo "unrecognized operating system ${OSTYPE:?}" 1>&2
  exit 1
  ;;
esac
: "${cmake_examples_target:?}"

# Create a VERSION_CURRENT file in the build directory to include in the dist tarball.
python ./etc/calc_release_version.py >./build/VERSION_CURRENT
cd build

cmake_flags=(
  "-DCMAKE_BUILD_TYPE=${build_type:?}"
  "-DCMAKE_PREFIX_PATH=${mongoc_prefix:?}"
  -DBUILD_TESTING=ON
  -DMONGOCXX_ENABLE_SLOW_TESTS=ON
  -DCMAKE_INSTALL_PREFIX=install
  -DENABLE_UNINSTALL=ON
)

_RUN_DISTCHECK=""
case "${OSTYPE:?}" in
cygwin)
  case "${generator:-}" in
  *2015*) cmake_flags+=("-DBOOST_ROOT=C:/local/boost_1_60_0") ;;
  *2017* | *2019*) cmake_flags+=("-DCMAKE_CXX_STANDARD=17") ;;
  *)
    echo "missing explicit CMake Generator on Windows distro" 1>&2
    exit 1
    ;;
  esac
  ;;
darwin* | linux*)
  : "${generator:="Unix Makefiles"}"

  # If enabled, limit distcheck to Unix-like systems only.
  _RUN_DISTCHECK="${RUN_DISTCHECK:-}"
  ;;
*)
  echo "unrecognized operating system ${OSTYPE:?}" 1>&2
  exit 1
  ;;
esac
export CMAKE_GENERATOR="${generator:?}"
export CMAKE_GENERATOR_PLATFORM="${platform:-}"

if [[ "${USE_POLYFILL_BOOST:-}" == "ON" ]]; then
  cmake_flags+=("-DBSONCXX_POLY_USE_BOOST=ON")
fi

cc_flags_init=(-Wall -Wextra -Wno-attributes -Werror -Wno-missing-field-initializers)
cxx_flags_init=(-Wall -Wextra -Wconversion -Wnarrowing -pedantic -Werror)
cc_flags=()
cxx_flags=()

case "${OSTYPE:?}" in
cygwin)
  # Most compiler flags are not applicable to builds on Windows distros.
  ;;
darwin*)
  cc_flags+=("${cc_flags_init[@]}")
  cxx_flags+=("${cxx_flags_init[@]}" -stdlib=libc++)
  ;;
linux*)
  cc_flags+=("${cc_flags_init[@]}")
  cxx_flags+=("${cxx_flags_init[@]}" -Wno-expansion-to-defined -Wno-missing-field-initializers)
  ;;
*)
  echo "unrecognized operating system ${OSTYPE:?}" 1>&2
  exit 1
  ;;
esac

# Most compiler flags are not applicable to builds on Windows distros.
if [[ "${OSTYPE:?}" != cygwin ]]; then
  # Sanitizers overwrite the usual compiler flags.
  if [[ "${USE_SANITIZER_ASAN:-}" == "ON" ]]; then
    cxx_flags=(
      "${cxx_flags_init[@]}"
      -D_GLIBCXX_USE_CXX11_ABI=0
      -fsanitize=address
      -O1 -g -fno-omit-frame-pointer
    )
  fi

  # Sanitizers overwrite the usual compiler flags.
  if [[ "${USE_SANITIZER_UBSAN:-}" == "ON" ]]; then
    cxx_flags=(
      "${cxx_flags_init[@]}"
      -D_GLIBCXX_USE_CXX11_ABI=0
      -fsanitize=undefined
      -fsanitize-blacklist="$(pwd)/../etc/ubsan.ignorelist"
      -fno-sanitize-recover=undefined
      -O1 -g -fno-omit-frame-pointer
    )
  fi

  # Ignore warnings generated by core::optional in mnmlstc/core.
  if [[ "${OSTYPE:?}" == linux* && "${HOSTTYPE:?}" == powerpc64le ]]; then
    cxx_flags+=(-Wno-error=maybe-uninitialized)
  fi

  # Ignore deprecation warnings when building on a release branch.
  if [[ "$(echo "${branch_name:?}" | cut -f2 -d'/')" != "${branch_name:?}" ]]; then
    cc_flags+=(-Wno-deprecated-declarations)
    cxx_flags+=(-Wno-deprecated-declarations)
  fi
fi

if [[ "${#cc_flags[@]}" -gt 0 ]]; then
  cmake_flags+=("-DCMAKE_C_FLAGS=${cc_flags[*]}")
fi

if [[ "${#cxx_flags[@]}" -gt 0 ]]; then
  cmake_flags+=("-DCMAKE_CXX_FLAGS=${cxx_flags[*]}")
fi

if [[ "${ENABLE_CODE_COVERAGE:-}" == "ON" ]]; then
  cmake_flags+=("-DENABLE_CODE_COVERAGE=ON")
fi

if [ "${USE_STATIC_LIBS:-}" ]; then
  cmake_flags+=("-DBUILD_SHARED_LIBS=OFF")
fi

if [ "${ENABLE_TESTS:-}" = "OFF" ]; then
  cmake_flags+=("-DENABLE_TESTS=OFF")
fi

if [[ -n "${REQUIRED_CXX_STANDARD:-}" ]]; then
  cmake_flags+=("-DCMAKE_CXX_STANDARD=${REQUIRED_CXX_STANDARD:?}")
  cmake_flags+=("-DCMAKE_CXX_STANDARD_REQUIRED=ON")
fi

echo "Configuring with CMake flags: ${cmake_flags[*]}"

"${cmake_binary}" "${cmake_flags[@]}" ..

if [[ "${COMPILE_MACRO_GUARD_TESTS:-"OFF"}" == "ON" ]]; then
  # We only need to compile the macro guard tests.
  "${cmake_binary}" -DENABLE_MACRO_GUARD_TESTS=ON ..
  "${cmake_binary}" --build . --config "${build_type:?}" --target test_bsoncxx_macro_guards test_mongocxx_macro_guards -- "${cmake_build_opts[@]}"
  exit # Nothing else to be done.
fi

# Regular build and install routine.
"${cmake_binary}" --build . --config "${build_type:?}" -- "${cmake_build_opts[@]}"
"${cmake_binary}" --build . --config "${build_type:?}" --target install -- "${cmake_build_opts[@]}"
"${cmake_binary}" --build . --config "${build_type:?}" --target "${cmake_examples_target:?}" -- "${cmake_build_opts[@]}"

if [[ "${_RUN_DISTCHECK:-}" ]]; then
  "${cmake_binary}" --build . --config "${build_type:?}" --target distcheck
fi
