#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

# Working directory is expected to be mongo-c-driver repo.
ROOT=$(pwd)
INSTALL_DIR=$ROOT/install
. .evergreen/scripts/find-cmake-latest.sh
declare cmake_binary
cmake_binary="$(find_cmake_latest)"
echo "Installing libmongocrypt ... begin"
.evergreen/scripts/compile-libmongocrypt.sh "${cmake_binary}" "$ROOT" "$INSTALL_DIR" &>output.txt || {
  cat output.txt 1>&2
  exit 1
}
echo "Installing libmongocrypt ... end"

# Allow reuse of ccache compilation results between different build directories.
export CCACHE_BASEDIR CCACHE_NOHASHDIR
CCACHE_BASEDIR="$(pwd)"
CCACHE_NOHASHDIR=1

echo "Compile test-azurekms ... begin"
# Disable unnecessary dependencies. test-azurekms is copied to a remote host for testing, which may not have all dependent libraries.
"${cmake_binary}" \
  -DENABLE_SASL=OFF \
  -DENABLE_SNAPPY=OFF \
  -DENABLE_ZSTD=OFF \
  -DENABLE_ZLIB=OFF \
  -DENABLE_SRV=OFF \
  -DENABLE_CLIENT_SIDE_ENCRYPTION=ON \
  -DCMAKE_PREFIX_PATH="$INSTALL_DIR" \
  .
"${cmake_binary}" --build . --target test-azurekms
echo "Compile test-azurekms ... end"
