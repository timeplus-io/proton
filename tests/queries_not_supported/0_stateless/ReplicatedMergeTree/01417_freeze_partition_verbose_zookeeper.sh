#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-parallel
# Tag no-replicated-database: Fails due to additional replicas or shards

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ALTER_OUT_STRUCTURE='command_type string, partition_id string, part_name string'
ATTACH_OUT_STRUCTURE='old_part_name string'
FREEZE_OUT_STRUCTURE='backup_name string, backup_path string , part_backup_path string'

# setup

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze_replicated;"
${CLICKHOUSE_CLIENT} --query "create stream table_for_freeze_replicated (key uint64, value string) ENGINE = ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_for_freeze_replicated', '1') ORDER BY key PARTITION BY key % 10;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze_replicated SELECT number, to_string(number) from numbers(10);"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated FREEZE WITH NAME 'test_01417' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated FREEZE PARTITION '3' WITH NAME 'test_01417_single_part' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated DETACH PARTITION '3';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze_replicated VALUES (3, '3');"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated ATTACH PARTITION '3' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, old_part_name FROM table"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated DETACH PARTITION '5';"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_replicated FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7', ATTACH PART '5_0_0_0' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name, old_part_name FROM table"

# teardown
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze_replicated;"
