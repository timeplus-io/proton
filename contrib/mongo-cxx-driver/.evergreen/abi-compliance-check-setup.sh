#!/usr/bin/env bash

set -o errexit
set -o pipefail

declare working_dir
working_dir="$(pwd)"

export PATH
PATH="${working_dir:?}/install/bin:${PATH:-}"

# Install prefix to use for ABI compatibility scripts.
[[ -d "${working_dir}/install" ]]

declare parallel_level
parallel_level="$(("$(nproc)" + 1))"

# Obtain abi-compliance-checker.
echo "Fetching abi-compliance-checker..."
[[ -d checker ]] || {
  git clone -b "2.3" --depth 1 https://github.com/lvc/abi-compliance-checker.git checker
  pushd checker
  make -j "${parallel_level:?}" --no-print-directory install prefix="${working_dir:?}/install"
  popd # checker
} >/dev/null
echo "Fetching abi-compliance-checker... done."

# Obtain ctags.
echo "Fetching ctags..."
[[ -d ctags ]] || {
  git clone -b "v6.0.0" --depth 1 https://github.com/universal-ctags/ctags.git ctags
  pushd ctags
  ./autogen.sh
  ./configure --prefix="${working_dir}/install"
  make -j "${parallel_level:?}"
  make install
  popd # ctags
} >/dev/null
echo "Fetching ctags... done."

command -V abi-compliance-checker
