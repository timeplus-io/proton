#!/usr/bin/env bash
#
# clang-format-all.sh
#
# Usage:
#   ./tools/poetry.sh install --with=dev
#   ./tools/poetry.sh run .evergreen/scripts/clang-format-all.sh
#   DRYRUN=1 ./tools/poetry.sh run .evergreen/scripts/clang-format-all.sh
#   ./tools/poetry.sh run env DRYRUN=1 .evergreen/scripts/clang-format-all.sh
#
# This script is meant to be run from the project root directory.

: "${DRYRUN:-}"

set -o errexit
set -o pipefail

clang-format --version

source_dirs=(
  src/common
  src/libbson
  src/libmongoc
)

exclude_patterns=(
  -e src/libbson/src/jsonsl
)

mapfile -t source_files < <(
  find "${source_dirs[@]:?}" -name '*.[ch]' |
    grep -E -v "${exclude_patterns[@]}"
)

if [[ -n "${DRYRUN:-}" ]]; then
  clang-format --dry-run -Werror "${source_files[@]:?}"
else
  clang-format --verbose -i "${source_files[@]:?}"
fi
