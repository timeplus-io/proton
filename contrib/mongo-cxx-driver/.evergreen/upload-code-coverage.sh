#!/usr/bin/env bash

: "${codecov_token:?}"

: "${ENABLE_CODE_COVERAGE:-}"

set -o errexit
set -o pipefail

# Nothing to do if code coverage was not enabled.
if [[ "${ENABLE_CODE_COVERAGE:-}" != "ON" ]]; then
  exit 0
fi

# Note: coverage is currently only enabled on the ubuntu-1804 distro.
# This script does not support MacOS, Windows, or non-x86_64 distros.
# Update accordingly if code coverage is expanded to other distros.
curl -Os https://uploader.codecov.io/latest/linux/codecov
chmod +x codecov

# -Z: Exit with a non-zero value if error.
# -g: Run with gcov support.
# -t: Codecov upload token.
# perl: filter verbose "Found" list and "Processing" messages.
./codecov -Zgt "${codecov_token:?}" | perl -lne 'print if not m|(^.*\.gcov(\.\.\.)?$)|'
