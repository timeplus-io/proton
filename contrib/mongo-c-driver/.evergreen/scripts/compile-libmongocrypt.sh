#!/usr/bin/env bash

compile_libmongocrypt() {
  declare -r cmake_binary="${1:?}"
  declare -r mongoc_dir="${2:?}"
  declare -r install_dir="${3:?}"

  # When updating libmongocrypt, consider also updating the copy of
  # libmongocrypt's kms-message in `src/kms-message`. Run
  # `.evergreen/scripts/kms-divergence-check.sh` to ensure that there is no
  # divergence in the copied files.

  # TODO: once 1.10.0 is released (containing MONGOCRYPT-605) replace the following with:
  # git clone -q --depth=1 https://github.com/mongodb/libmongocrypt --branch 1.10.0 || return
  {
    git clone -q https://github.com/mongodb/libmongocrypt || return
    # Check out commit containing MONGOCRYPT-614
    git -C libmongocrypt checkout 9ce5db1ca353a4b82788724257d460a9ce67a3e9
  }

  declare -a crypt_cmake_flags=(
    "-DMONGOCRYPT_MONGOC_DIR=${mongoc_dir}"
    "-DBUILD_TESTING=OFF"
    "-DENABLE_ONLINE_TESTS=OFF"
    "-DENABLE_MONGOC=OFF"
    "-DBUILD_VERSION=1.10.0-pre"
  )

  DEBUG="0" \
    CMAKE_EXE="${cmake_binary}" \
    MONGOCRYPT_INSTALL_PREFIX=${install_dir} \
    DEFAULT_BUILD_ONLY=true \
    LIBMONGOCRYPT_EXTRA_CMAKE_FLAGS="${crypt_cmake_flags[*]}" \
    ./libmongocrypt/.evergreen/compile.sh || return
}

: "${1:?"missing path to CMake binary"}"
: "${2:?"missing path to mongoc directory"}"
: "${3:?"missing path to install directory"}"

compile_libmongocrypt "${1}" "${2}" "${3}"
