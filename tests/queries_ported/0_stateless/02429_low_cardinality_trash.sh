#!/usr/bin/env bash
# Tags: long, no-backward-compatibility-check

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -sS --data-binary "SELECT rand64(1) % 10 AS k, group_array(to_string(64 - floor(log2(rand64(2) + 1))))::array(low_cardinality(string)) FROM numbers(100) GROUP BY k FORMAT RawBLOB" | grep -o -F 'NOT_IMPLEMENTED'
