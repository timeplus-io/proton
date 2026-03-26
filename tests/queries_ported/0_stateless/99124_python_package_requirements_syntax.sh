#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -euo pipefail

tmp_dir="$CURDIR"/../../../tmp
mkdir -p "$tmp_dir"
tmp_err="$tmp_dir"/99124_python_package_requirements_syntax.err
trap 'rm -f "$tmp_err"' EXIT

expect_fail_contains() {
    local query="$1"
    local expected_substring="$2"
    local ok_label="$3"

    if $CLICKHOUSE_CLIENT -q "$query" > /dev/null 2>"$tmp_err"; then
        echo "Unexpected success: $ok_label"
        exit 1
    fi

    if ! grep -qF "$expected_substring" "$tmp_err"; then
        echo "Missing expected error substring for: $ok_label"
        cat "$tmp_err"
        exit 1
    fi

    echo "$ok_label"
}

expect_fail_contains \
    "SYSTEM INSTALL PYTHON PACKAGE 'requests' INDEX_URL 'not-a-url'" \
    "Invalid --index-url value" \
    "OK index_url_validation"

expect_fail_contains \
    "SYSTEM INSTALL PYTHON PACKAGE 'requests' EXTRA_INDEX_URL 'not-a-url'" \
    "Invalid --extra-index-url value" \
    "OK extra_index_url_validation"

expect_fail_contains \
    "SYSTEM INSTALL PYTHON PACKAGE REQUIREMENTS '-r nested.txt'" \
    "Requirements line '-r nested.txt' is not supported" \
    "OK requirements_text_validation"

expect_fail_contains \
    "SYSTEM INSTALL PYTHON PACKAGE REQUIREMENTS 'requests==2.32.3' '2.0.0'" \
    "Syntax error" \
    "OK requirements_plus_version_rejected"
