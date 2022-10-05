#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in $(seq -w 0 2 20); do
    $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS merge_item_$i"
    $CLICKHOUSE_CLIENT -q "create stream merge_item_$i (d int8) "
    $CLICKHOUSE_CLIENT -q "INSERT INTO merge_item_$i(d) VALUES ($i)"
    sleep 2
done

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS merge_storage"
$CLICKHOUSE_CLIENT -q "create stream merge_storage (d int8) ENGINE = Merge('${CLICKHOUSE_DATABASE}', '^merge_item_')"
$CLICKHOUSE_CLIENT --max_threads=1 -q "SELECT _table, d FROM table(merge_storage) WHERE _table LIKE 'merge_item_1%' ORDER BY _table"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS merge_storage"

for i in $(seq -w 0 2 20); do $CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS merge_item_$i"; done
