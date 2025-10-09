#!/usr/bin/env bash

set -o errexit
set -o pipefail

command -V nm >/dev/null

declare -a libs
libs=(
  install/new/lib/libbsoncxx.so
  install/new/lib/libmongocxx.so
)

for lib in "${libs[@]}"; do
  [[ -f "${lib:?}" ]] || {
    echo "error: missing ${lib:?}"
    exit 1
  } 1>&2
done

# Patterns for library symbols to check.
match_pattern=(
  -e '(bsoncxx|mongocxx)::'
)

# Patterns for bad symbols.
bad_pattern=(
  -e '(bsoncxx|mongocxx)::(.+::)?detail::'
  -e '(bsoncxx|mongocxx)::(.+::)?test::'
)

# Ensure implementation details do not leak into the ABI.
mapfile -t bad_symbols < <(
  nm --demangle --dynamic --defined-only --extern-only --just-symbols "${libs[@]}" |
    grep -E "${match_pattern[@]}" |
    grep -E "${bad_pattern[@]}"
)

# Print list of bad symbols.
(("${#bad_symbols[*]}" == 0)) || {
  echo "error: found ${#bad_symbols[@]} prohibited symbols in exported ABI:"
  printf " - %s\n" "${bad_symbols[@]}"
  exit 1
} 1>&2
