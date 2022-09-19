#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS ordinary_00682"
${CLICKHOUSE_CLIENT} --query="create stream ordinary_00682(k uint32) ENGINE MergeTree ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary_00682(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO ordinary_00682(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM ordinary_00682 DELETE WHERE k = 1" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM ordinary_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM ordinary_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM ordinary_00682"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Vertical merge ***'"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS vertical_00682"
${CLICKHOUSE_CLIENT} --query="create stream vertical_00682(k uint32, v uint32) ENGINE MergeTree ORDER BY k \
    SETTINGS enable_vertical_merge_algorithm=1, remove_empty_parts=0, \
             vertical_merge_algorithm_min_rows_to_activate=0, \
             vertical_merge_algorithm_min_columns_to_activate=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical_00682(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO vertical_00682(k, v) VALUES (2, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM vertical_00682 DELETE WHERE k = 1" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM vertical_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM vertical_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM vertical_00682"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS summing_00682"
${CLICKHOUSE_CLIENT} --query="create stream summing_00682(k uint32, v uint32) ENGINE SummingMergeTree ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO summing_00682(k, v) VALUES (1, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO summing_00682(k, v) VALUES (1, 2)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM summing_00682 DELETE WHERE k = 1" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM summing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM summing_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM summing_00682"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS aggregating_00682"
${CLICKHOUSE_CLIENT} --query="create stream aggregating_00682(k uint32, v aggregate_function(count)) ENGINE AggregatingMergeTree ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating_00682(k) VALUES (1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO aggregating_00682(k) VALUES (1)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM aggregating_00682 DELETE WHERE k = 1" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM aggregating_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM aggregating_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM aggregating_00682"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS replacing_00682"
${CLICKHOUSE_CLIENT} --query="create stream replacing_00682(k uint32, v string) ENGINE ReplacingMergeTree ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing_00682(k, v) VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query="INSERT INTO replacing_00682(k, v) VALUES (1, 'b')"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM replacing_00682 DELETE WHERE k = 1" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM replacing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM replacing_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM replacing_00682"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS collapsing_00682"
${CLICKHOUSE_CLIENT} --query="create stream collapsing_00682(k uint32, v string, s int8) ENGINE CollapsingMergeTree(s) ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing_00682(k, v, s) VALUES (1, 'a', 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO collapsing_00682(k, v, s) VALUES (2, 'b', 1)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM collapsing_00682 DELETE WHERE k IN (1, 2)" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM collapsing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM collapsing_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM collapsing_00682"


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS versioned_collapsing_00682"
${CLICKHOUSE_CLIENT} --query="create stream versioned_collapsing_00682(k uint32, val string, ver uint32, s int8) ENGINE VersionedCollapsingMergeTree(s, ver) ORDER BY k SETTINGS remove_empty_parts=0"

${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing_00682(k, val, ver, s) VALUES (1, 'a', 0, 1)"
${CLICKHOUSE_CLIENT} --query="INSERT INTO versioned_collapsing_00682(k, val, ver, s) VALUES (2, 'b', 0, 1)"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM versioned_collapsing_00682 DELETE WHERE k IN (1, 2)" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="OPTIMIZE STREAM versioned_collapsing_00682 PARTITION tuple() FINAL"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM versioned_collapsing_00682"

${CLICKHOUSE_CLIENT} --query="DROP STREAM versioned_collapsing_00682"
