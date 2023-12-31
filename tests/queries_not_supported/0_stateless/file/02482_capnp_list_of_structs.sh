#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
touch $USER_FILES_PATH/data.capnp

SCHEMADIR=$(clickhouse-client --query "select * from file('data.capnp', 'CapnProto', 'val1 char') settings format_schema='nonexist:Message'" 2>&1 | grep Exception | grep -oP "file \K.*(?=/nonexist.capnp)")
CLIENT_SCHEMADIR=$CURDIR/format_schemas
SERVER_SCHEMADIR=test_02482
mkdir -p $SCHEMADIR/$SERVER_SCHEMADIR
cp -r $CLIENT_SCHEMADIR/02482_* $SCHEMADIR/$SERVER_SCHEMADIR/


$CLICKHOUSE_CLIENT -q "insert into function file(02482_data.capnp, auto, 'nested Nested(x int64, y int64)') select [1,2], [3,4] settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested', engine_file_truncate_on_insert=1"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp) settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, 'nested Nested(x int64, y int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, '\`nested.x\` array(int64), \`nested.y\` array(int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"
$CLICKHOUSE_CLIENT -q "select * from file(02482_data.capnp, auto, '\`nested.x\` array(int64)') settings format_schema='$SERVER_SCHEMADIR/02482_list_of_structs.capnp:Nested'"

rm $USER_FILES_PATH/data.capnp
rm $USER_FILES_PATH/02482_data.capnp
