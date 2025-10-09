#!/usr/bin/env bash

set -o errexit
set -o pipefail

declare -r mongoc_version="${mongoc_version:-"${mongoc_version_minimum:?"missing mongoc version"}"}"
: "${mongoc_version:?}"

# Usage:
#   to_windows_path "./some/unix/style/path"
#   to_windows_path "/some/unix/style/path"
to_windows_path() {
  cygpath -aw "${1:?"to_windows_path requires a path to convert"}"
}

declare mongoc_dir
mongoc_dir="$(pwd)/mongoc"

# "i" for "(platform-)independent".
declare mongoc_idir mongoc_install_idir
if [[ "${OSTYPE:?}" == "cygwin" ]]; then
  # CMake requires Windows paths for configuration variables on Windows.
  mongoc_idir="$(to_windows_path "${mongoc_dir}")"
  mongoc_install_idir="$(to_windows_path "${mongoc_dir}")"
else
  mongoc_idir="${mongoc_dir}"
  mongoc_install_idir="${mongoc_dir}"
fi
: "${mongoc_idir:?}"
: "${mongoc_install_idir:?}"

echo "libmongoc version: ${mongoc_version}"

# Download tarball from GitHub and extract into ${mongoc_dir}.
rm -rf "${mongoc_dir}"
mkdir "${mongoc_dir}"
curl -sS -o mongo-c-driver.tar.gz -L "https://api.github.com/repos/mongodb/mongo-c-driver/tarball/${mongoc_version}"
tar xzf mongo-c-driver.tar.gz --directory "${mongoc_dir}" --strip-components=1

# C Driver needs VERSION_CURRENT to compute BUILD_VERSION.
# RegEx pattern to match SemVer strings. See https://semver.org/.
declare -r semver_regex="^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
if echo "${mongoc_version}" | perl -ne "$(printf 'exit 1 unless /%s/' "${semver_regex}")"; then
  # If $VERSION is already SemVer compliant, use as-is.
  echo "${mongoc_version}" >|"${mongoc_dir}/VERSION_CURRENT"
else
  # Otherwise, use the tag name of the latest release to construct a prerelease version string.

  # Extract "tag_name" from latest Github release.
  declare build_version
  build_version=$(curl -sS -H "Accept: application/vnd.github.v3+json" https://api.github.com/repos/mongodb/mongo-c-driver/releases/latest | perl -ne 'print for /"tag_name": "(.+)"/')

  # Assert the tag name is a SemVer string via errexit.
  echo "${build_version}" | perl -ne "$(printf 'exit 1 unless /%s/' "${semver_regex}")"

  # Bump to the next minor version, e.g. 1.0.1 -> 1.1.0.
  build_version="$(echo "${build_version}" | perl -ne "$(printf '/%s/; print $+{major} . "." . ($+{minor}+1) . ".0"' "${semver_regex}")")"

  # Append a prerelease tag, e.g. 1.1.0-pre+<version>.
  build_version="$(printf "%s-pre+%s" "${build_version}" "${mongoc_version}")"

  # Use the constructed prerelease build version when building the C driver.
  echo "${build_version}" >|"${mongoc_dir}/VERSION_CURRENT"
fi

# shellcheck source=/dev/null
. "${mongoc_dir}/.evergreen/scripts/find-cmake-latest.sh"
declare cmake_binary
cmake_binary="$(find_cmake_latest)"
command -v "${cmake_binary:?}"

# Install libmongocrypt.
{
  echo "Installing libmongocrypt into ${mongoc_dir}..." 1>&2
  "${mongoc_dir}/.evergreen/scripts/compile-libmongocrypt.sh" "${cmake_binary}" "${mongoc_idir}" "${mongoc_install_idir}"
  echo "Installing libmongocrypt into ${mongoc_dir}... done." 1>&2
} >/dev/null

if [[ "${OSTYPE}" == darwin* ]]; then
  # MacOS does not have nproc.
  nproc() {
    sysctl -n hw.logicalcpu
  }
fi

# Default CMake generator to use if not already provided.
declare CMAKE_GENERATOR CMAKE_GENERATOR_PLATFORM
if [[ "${OSTYPE:?}" == "cygwin" ]]; then
  CMAKE_GENERATOR="${generator:-"Visual Studio 14 2015"}"
  CMAKE_GENERATOR_PLATFORM="${platform:-"x64"}"
else
  CMAKE_GENERATOR="${generator:-"Unix Makefiles"}"
  CMAKE_GENERATOR_PLATFORM="${platform:-""}"
fi
export CMAKE_GENERATOR CMAKE_GENERATOR_PLATFORM

declare -a configure_flags=(
  "-DCMAKE_BUILD_TYPE=Debug"
  "-DCMAKE_INSTALL_PREFIX=${mongoc_install_idir}"
  "-DCMAKE_PREFIX_PATH=${mongoc_idir}"
  "-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF"
  "-DENABLE_CLIENT_SIDE_ENCRYPTION=ON"
  "-DENABLE_EXAMPLES=OFF"
  "-DENABLE_SHM_COUNTERS=OFF"
  "-DENABLE_STATIC=ON"
  "-DENABLE_TESTS=OFF"
)

declare -a compile_flags

case "${OSTYPE:?}" in
cygwin)
  compile_flags+=("/maxcpucount:$(nproc)")
  ;;
darwin*)
  configure_flags+=("-DCMAKE_C_FLAGS=-fPIC")
  configure_flags+=("-DCMAKE_MACOSX_RPATH=ON")
  compile_flags+=("-j" "$(nproc)")
  ;;
*)
  configure_flags+=("-DCMAKE_C_FLAGS=-fPIC")
  compile_flags+=("-j" "$(nproc)")
  ;;
esac

if [[ "${BSON_EXTRA_ALIGNMENT:-}" == "1" ]]; then
  echo "Building C Driver with ENABLE_EXTRA_ALIGNMENT=ON"
  configure_flags+=("-DENABLE_EXTRA_ALIGNMENT=ON")
else
  configure_flags+=("-DENABLE_EXTRA_ALIGNMENT=OFF")
fi

# Use ccache if available.
if command -V ccache 2>/dev/null; then
  export CMAKE_C_COMPILER_LAUNCHER=ccache
  export CMAKE_CXX_COMPILER_LAUNCHER=ccache

  # Allow reuse of ccache compilation results between different build directories.
  export CCACHE_BASEDIR CCACHE_NOHASHDIR
  CCACHE_BASEDIR="${mongoc_idir}"
  CCACHE_NOHASHDIR=1
fi

# Install libmongoc.
{
  echo "Installing C Driver into ${mongoc_dir}..." 1>&2
  "${cmake_binary}" -S "${mongoc_idir}" -B "${mongoc_idir}" "${configure_flags[@]}"
  "${cmake_binary}" --build "${mongoc_idir}" --config Debug --target install -- "${compile_flags[@]}"
  echo "Installing C Driver into ${mongoc_dir}... done." 1>&2
} >/dev/null
