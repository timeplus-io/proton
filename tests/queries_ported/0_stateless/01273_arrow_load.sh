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
${CLICKHOUSE_CLIENT} --query="CREATE STREAM arrow_load (bool uint8, int8 int8, int16 int16, int32 int32, int64 int64, uint8 uint8, uint16 uint16, uint32 uint32, uint64 uint64, halffloat float32, float float32, double float64, string string, date32 Date, date64 DateTime('Asia/Istanbul'), timestamp DateTime('Asia/Istanbul')) ENGINE = Memory"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into arrow_load format Arrow"
${CLICKHOUSE_CLIENT} --query="select * from arrow_load"

$CLICKHOUSE_CLIENT -q "DROP STREAM arrow_load"
