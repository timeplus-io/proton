#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Should correctly handle error.

${CLICKHOUSE_LOCAL} --query "SELECT to_string(number) AS a, to_string(number) AS a FROM numbers(10)" --output-format Native |
    ${CLICKHOUSE_LOCAL} --query "SELECT * FROM table" --input-format Native --structure 'a low_cardinality(string)' 2>&1 |
    grep -c -F Exception
