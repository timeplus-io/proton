#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Implementation specific behaviour on overflow. We may return error or produce empty string.
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(nan);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(inf);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(-inf);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(1e300);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(-123.456);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(-1);" >/dev/null 2>&1 ||:

${CLICKHOUSE_CLIENT} --query="SELECT random_printable_ascii(0), 'Ok';"
