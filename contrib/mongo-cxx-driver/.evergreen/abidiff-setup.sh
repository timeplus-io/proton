#!/usr/bin/env bash

set -o errexit
set -o pipefail

declare working_dir
working_dir="$(pwd)"

export PATH
PATH="${working_dir:?}/install/bin:${PATH:-}"

# Install prefix to use for ABI compatibility scripts.
[[ -d "${working_dir}/install" ]]

if command -V abidiff 2>/dev/null; then
  exit # Already available.
fi

# Expected to be run on Ubuntu.
echo "Installing abigail-tools..."
sudo apt-get install -q -y abigail-tools >/dev/null
echo "Installing abigail-tools... done."

command -V abidiff
