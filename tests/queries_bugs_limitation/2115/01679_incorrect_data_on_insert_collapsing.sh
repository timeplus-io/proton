#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS collapsing_merge_tree"

${CLICKHOUSE_CLIENT} --query "CREATE STREAM collapsing_merge_tree (key uint32, sign int8, date Datetime) ENGINE=CollapsingMergeTree(sign) PARTITION BY date ORDER BY key"

${CLICKHOUSE_CLIENT} --query "INSERT INTO collapsing_merge_tree VALUES (1, -117, '2020-01-01')" 2>&1 | grep -q 'Incorrect data: Sign = -117' && echo 'OK' || echo 'FAIL'; 

${CLICKHOUSE_CLIENT} --query "DROP STREAM collapsing_merge_tree;"

