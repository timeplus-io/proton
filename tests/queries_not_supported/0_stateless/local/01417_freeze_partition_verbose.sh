#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel
# Tag no-replicated-database: Unsupported type of ALTER query

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ALTER_OUT_STRUCTURE='command_type string, partition_id string, part_name string'
ATTACH_OUT_STRUCTURE='old_part_name string'
FREEZE_OUT_STRUCTURE='backup_name string, backup_path string , part_backup_path string'

# setup
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze;"
${CLICKHOUSE_CLIENT} --query "create stream table_for_freeze (key uint64, value string) ENGINE = MergeTree() ORDER BY key PARTITION BY key % 10;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze SELECT number, to_string(number) from numbers(10);"

# also for old syntax
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze_old_syntax;"
${CLICKHOUSE_CLIENT} --query "create stream table_for_freeze_old_syntax (dt date, value string) ENGINE = MergeTree(dt, (value), 8192);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze_old_syntax SELECT to_date('2021-03-01'), to_string(number) from numbers(10);"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze FREEZE WITH NAME 'test_01417' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze FREEZE PARTITION '3' WITH NAME 'test_01417_single_part' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze DETACH PARTITION '3';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze VALUES (3, '3');"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze ATTACH PARTITION '3' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, old_part_name FROM table"


${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze DETACH PARTITION '5';"

${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7', ATTACH PART '5_6_6_0' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name, old_part_name FROM table"

# Unfreeze partition
${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze UNFREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

# Freeze partition with old syntax
${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_old_syntax FREEZE PARTITION '202103' WITH NAME 'test_01417_single_part_old_syntax' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

# Unfreeze partition with old syntax
${CLICKHOUSE_CLIENT} --query "ALTER STREAM table_for_freeze_old_syntax UNFREEZE PARTITION '202103' WITH NAME 'test_01417_single_part_old_syntax' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

# teardown
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze;"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS table_for_freeze_old_syntax;"
