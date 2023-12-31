#!/usr/bin/env bash
# Tags: no-debug, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_DIR="/tmp"

declare -a SearchTypes=("POLYGON_INDEX_EACH" "POLYGON_INDEX_CELL")

tar -xf "${CURDIR}"/01037_test_data_perf.tar.gz -C "${CURDIR}"

$CLICKHOUSE_CLIENT -n --query="
DROP DATABASE IF EXISTS test_01037;
CREATE DATABASE test_01037;
DROP STREAM IF EXISTS test_01037.points;
create stream test_01037.points (x float64, y float64) ;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.points FORMAT TSV" --max_insert_block_size=100000 < "${CURDIR}/01037_point_data"

rm "${CURDIR}"/01037_point_data

$CLICKHOUSE_CLIENT -n --query="
DROP STREAM IF EXISTS test_01037.polygons_array;

create stream test_01037.polygons_array
(
   key array(array(array(array(float64)))),
   name string,
   value uint64
)
;
"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_01037.polygons_array FORMAT JSONEachRow" --max_insert_block_size=100000 < "${CURDIR}/01037_polygon_data"

rm "${CURDIR}"/01037_polygon_data

for type in "${SearchTypes[@]}";
do
   outputFile="${TMP_DIR}/results${type}.out"

   $CLICKHOUSE_CLIENT -n --query="
   DROP DICTIONARY IF EXISTS test_01037.dict_array;

   CREATE DICTIONARY test_01037.dict_array
   (
   key array(array(array(array(float64)))),
   name string DEFAULT 'qqq',
   value uint64 DEFAULT 101
   )
   PRIMARY KEY key
   SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'polygons_array' PASSWORD '' DB 'test_01037'))
   LIFETIME(0)
   LAYOUT($type());

   select 'dictGet', 'test_01037.dict_array' as dict_name, tuple(x, y) as key,
      dictGet(dict_name, 'value', key) from test_01037.points order by x, y;
   " > "$outputFile"

   diff -q "${CURDIR}/01037_polygon_dicts_correctness_fast.ans" "$outputFile"
done

$CLICKHOUSE_CLIENT -n --query="
DROP STREAM test_01037.points;
DROP DATABASE test_01037;
"
