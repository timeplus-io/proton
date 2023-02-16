#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS stream_with_empty_part"

${CLICKHOUSE_CLIENT} --query="CREATE STREAM stream_with_empty_part
(
    id uint64,
    value uint64
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id
SETTINGS vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0, remove_empty_parts = 0
"


${CLICKHOUSE_CLIENT} --query="INSERT INTO stream_with_empty_part VALUES (1, 1)"

${CLICKHOUSE_CLIENT} --query="INSERT INTO stream_with_empty_part VALUES (2, 2)"

${CLICKHOUSE_CLIENT} --mutations_sync=2 --query="ALTER STREAM stream_with_empty_part DELETE WHERE id % 2 == 0"

${CLICKHOUSE_CLIENT} --query="SELECT count(DISTINCT value) FROM stream_with_empty_part"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM stream_with_empty_part MODIFY COLUMN value nullable(uint64)"

${CLICKHOUSE_CLIENT} --query="SELECT count(distinct value) FROM stream_with_empty_part"

${CLICKHOUSE_CLIENT} --query="OPTIMIZE TABLE stream_with_empty_part FINAL"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS stream_with_empty_part"
