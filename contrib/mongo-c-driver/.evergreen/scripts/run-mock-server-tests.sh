#!/usr/bin/env bash

set -o errexit
set -o pipefail

# shellcheck source=.evergreen/scripts/env-var-utils.sh
. "$(dirname "${BASH_SOURCE[0]}")/env-var-utils.sh"
. "$(dirname "${BASH_SOURCE[0]}")/use-tools.sh" paths

check_var_opt ASAN "OFF"
check_var_opt CC
check_var_opt MARCH
check_var_opt DNS "nodns"

declare script_dir
script_dir="$(to_absolute "$(dirname "${BASH_SOURCE[0]}")")"

declare -a test_args=(
  "-d"
  "-F"
  "test-results.json"
  "--skip-tests"
  ".evergreen/etc/skip-tests.txt"
)

# AddressSanitizer configuration
export ASAN_OPTIONS="detect_leaks=1 abort_on_error=1 symbolize=1"
export ASAN_SYMBOLIZER_PATH="/usr/lib/llvm-3.4/bin/llvm-symbolizer"

export MONGOC_TEST_FUTURE_TIMEOUT_MS=30000
export MONGOC_TEST_SERVER_LOG="json"
export MONGOC_TEST_SKIP_MOCK="off"
export MONGOC_TEST_SKIP_LIVE="on"
export MONGOC_TEST_SKIP_SLOW="on"
export MONGOC_TEST_IPV4_AND_IPV6_HOST="ipv4_and_ipv6.test.build.10gen.cc"

# shellcheck source=.evergreen/scripts/add-build-dirs-to-paths.sh
. "${script_dir}/add-build-dirs-to-paths.sh"
# shellcheck source=.evergreen/scripts/bypass-dlclose.sh
. "${script_dir}/bypass-dlclose.sh"

declare ld_preload="${LD_PRELOAD:-}"
if [[ "${ASAN}" == "on" ]]; then
  ld_preload="$(bypass_dlclose):${ld_preload}"
fi

case "${OSTYPE}" in
cygwin)
  LD_PRELOAD="${ld_preload:-}" ./src/libmongoc/test-libmongoc.exe "${test_args[@]}"
  ;;

*)
  ulimit -c unlimited || true

  LD_PRELOAD="${ld_preload:-}" ./src/libmongoc/test-libmongoc --no-fork "${test_args[@]}"
  ;;
esac
