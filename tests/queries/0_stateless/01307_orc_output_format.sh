#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS orc";

$CLICKHOUSE_CLIENT --query="create stream orc (uint8 uint8, uint16 uint16, uint32 uint32, uint64 uint64, int8 int8, int16 int16, int32 int32, int64 int64, float float32, double float64, string string, fixed fixed_string(4), date date, datetime datetime('Europe/Moscow'), decimal32 Decimal32(4), decimal64 Decimal64(10), decimal128 Decimal128(20), nullable nullable(int32)) ";

$CLICKHOUSE_CLIENT --query="INSERT INTO orc VALUES (255, 65535, 4294967295, 100000000000, -128, -32768, -2147483648, -100000000000, 2.02, 10000.0000001, 'string', '2020', 18980, 1639872000, 1.0001, 1.00000001, 100000.00000000000001, 1), (4, 1234, 3244467295, 500000000000, -1, -256, -14741221, -7000000000, 100.1, 14321.032141201, 'Another string', '2000', 20000, 1839882000, 34.1234, 123123.123123123, 123123123.123123123123123, NULL), (42, 42, 42, 42, 42, 42, 42, 42, 42.42, 42.42, '42', '4242', 42, 42, 42.42, 42.42424242, 424242.42424242424242, 42)";

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc FORMAT ORC" > "$CURDIR"/tmp_orc_test_all_types.orc;

cat "$CURDIR/tmp_orc_test_all_types.orc" | $CLICKHOUSE_CLIENT --query="INSERT INTO orc FORMAT ORC";

rm "$CURDIR/tmp_orc_test_all_types.orc"

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc";

$CLICKHOUSE_CLIENT --query="DROP STREAM orc";
