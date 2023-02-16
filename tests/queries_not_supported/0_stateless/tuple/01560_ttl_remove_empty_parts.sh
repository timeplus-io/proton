#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

${CLICKHOUSE_CLIENT} -q 'DROP STREAM IF EXISTS ttl_empty_parts'

${CLICKHOUSE_CLIENT} -q '
    CREATE STREAM ttl_empty_parts (id uint32, d Date) ENGINE = MergeTree ORDER BY tuple() PARTITION BY id SETTINGS old_parts_lifetime=5
'

${CLICKHOUSE_CLIENT} -q "INSERT INTO ttl_empty_parts SELECT 0, to_date('2005-01-01') + number from numbers(500);"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ttl_empty_parts SELECT 1, to_date('2050-01-01') + number from numbers(500);"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ttl_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE stream = 'ttl_empty_parts' AND database = current_database() AND active;"

${CLICKHOUSE_CLIENT} -q "ALTER STREAM ttl_empty_parts MODIFY TTL d SETTINGS mutations_sync = 1;"

# To be sure, that task, which clears outdated parts executed.
timeout 60 bash -c 'wait_for_delete_empty_parts ttl_empty_parts'

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ttl_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE stream = 'ttl_empty_parts' AND database = current_database() AND active;"

${CLICKHOUSE_CLIENT} -q "DROP STREAM ttl_empty_parts;"
