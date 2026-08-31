#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_DIR="$CURDIR"/data_avro
SCHEMASDIR="$CURDIR"/format_schemas

# Arrays split into several blocks with negative item count + byte size (Java BlockingBinaryEncoder style).

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS stateless_99932_avro_negative_block_size"
$CLICKHOUSE_CLIENT -q "CREATE STREAM stateless_99932_avro_negative_block_size (id int64, tags array(string)) ENGINE = Memory"

echo '=== container file'
$CLICKHOUSE_CLIENT -q "INSERT INTO stateless_99932_avro_negative_block_size FORMAT Avro" < "$DATA_DIR"/negative_block_size.avro
$CLICKHOUSE_CLIENT -q "SELECT id, length(tags), tags[1], tags[-1] FROM stateless_99932_avro_negative_block_size ORDER BY id"

echo '=== raw datum with format_schema'
$CLICKHOUSE_CLIENT -q "TRUNCATE STREAM stateless_99932_avro_negative_block_size"
$CLICKHOUSE_CLIENT -q "INSERT INTO stateless_99932_avro_negative_block_size SETTINGS format_schema='$SCHEMASDIR/99932_avro_negative_block_size.avsc' FORMAT Avro" < "$DATA_DIR"/negative_block_size.raw.avro
$CLICKHOUSE_CLIENT -q "SELECT id, length(tags), tags[1], tags[-1] FROM stateless_99932_avro_negative_block_size ORDER BY id"

$CLICKHOUSE_CLIENT -q "DROP STREAM stateless_99932_avro_negative_block_size"
