#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..

DATA_FILE=$CUR_DIR/data_arrow/test.arrow

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_load"
${CLICKHOUSE_CLIENT} --query="create stream arrow_load (bool uint8, int8 int8, int16 Int16, int32 int32, int64 int64, uint8 uint8, uint16 uint16, uint32 uint32, uint64 uint64, halffloat Float32, float Float32, double float64, string string, date32 date, date64 datetime('Europe/Moscow'), timestamp datetime('Europe/Moscow')) "
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into arrow_load format Arrow"
${CLICKHOUSE_CLIENT} --query="select * from arrow_load"

$CLICKHOUSE_CLIENT -q "DROP STREAM arrow_load"
