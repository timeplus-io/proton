#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Configure environment with toolchain components
if [[ -d /opt/mongo-c-toolchain ]]; then
  sudo rm -r /opt/mongo-c-toolchain
fi

sudo mkdir /opt/mongo-c-toolchain

declare toolchain_tar_gz
toolchain_tar_gz=$(readlink -f ../mongo-c-toolchain.tar.gz)
sudo tar -xf "${toolchain_tar_gz}" -C /opt/mongo-c-toolchain

echo "--- TOOLCHAIN MANIFEST BEGIN ---"
cat /opt/mongo-c-toolchain/MANIFEST.txt
echo "--- TOOLCHAIN MANIFEST END ---"

declare addl_path
addl_path="$(readlink -f /opt/mongo-c-toolchain/bin):${PATH:-}"

declare cmake_binary
cmake_binary="$(readlink -f /opt/mongo-c-toolchain/bin/cmake)"

if [[ ! -x "${cmake_binary}" ]]; then
  echo "CMake (${cmake_binary}) does not exist or is not executable" 1>&2
  exit 1
fi

declare toolchain_base_dir
toolchain_base_dir="$(readlink -f /opt/mongo-c-toolchain)"

declare toolchain_lib_dir="${toolchain_base_dir}/lib"

declare -a ssl_vers=(
  "libressl-2.5"
  "libressl-3.0"
  "openssl-1.0.1"
  "openssl-1.0.1-fips"
  "openssl-1.0.2"
  "openssl-1.1.0"
)

for ssl_ver in "${ssl_vers[@]}"; do
  echo "TESTING TOOLCHAIN COMPONENTS FOR ${ssl_ver}..."

  cp -a ../mongoc "../mongoc-${ssl_ver}"
  pushd "../mongoc-${ssl_ver}"

  declare new_path
  new_path="$(readlink -f "/opt/mongo-c-toolchain/${ssl_ver}/bin")"
  new_path+=":${addl_path}"

  declare ssl_base_dir
  ssl_base_dir="$(readlink -f "/opt/mongo-c-toolchain/${ssl_ver}")"

  # Output some information about our build environment
  "${cmake_binary}" --version

  declare ssl
  if [[ "${ssl_ver#*libressl}" != "${ssl_ver}" ]]; then
    ssl="LIBRESSL"
  else
    ssl="OPENSSL"
  fi

  declare output_file
  output_file="$(mktemp)"

  env \
    BYPASS_FIND_CMAKE="ON" \
    CFLAGS="-Wno-redundant-decls" \
    EXTRA_CMAKE_PREFIX_PATH="${ssl_base_dir};${toolchain_base_dir}" \
    EXTRA_CONFIGURE_FLAGS="-DCMAKE_VERBOSE_MAKEFILE=ON" \
    LD_LIBRARY_PATH="${toolchain_lib_dir}" \
    PATH="${new_path}" \
    SSL="${ssl}" \
    bash .evergreen/scripts/compile-unix.sh 2>&1 >|"${output_file}"

  # Verify that the toolchain components were used
  if grep -Ec "[-]I/opt/mongo-c-toolchain/include" "${output_file}" >/dev/null &&
    grep -Ec "[-]isystem /opt/mongo-c-toolchain/${ssl_ver}/include" "${output_file}" >/dev/null &&
    grep -Ec "[-]L/opt/mongo-c-toolchain/lib" "${output_file}" >/dev/null &&
    grep -Ec "/opt/mongo-c-toolchain/${ssl_ver}/lib" "${output_file}" >/dev/null; then
    echo "TOOLCHAIN COMPONENTS FOR ${ssl_ver} DETECTED IN BUILD OUTPUT."
  else
    echo "TOOLCHAIN COMPONENTS FOR ${ssl_ver} NOT DETECTED IN BUILD OUTPUT! ABORTING!" 1>&2
    echo "BUILD OUTPUT:"
    cat "${output_file}"
    exit 1
  fi

  rm -f "${output_file}"

  popd # "mongoc-${ssl_ver}"

  echo "TESTING TOOLCHAIN COMPONENTS FOR ${ssl_ver}... DONE."
done
