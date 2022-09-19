#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS maps"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET allow_experimental_map_type = 1;
create stream maps (m1 Map(uint32, uint32), m2 Map(string, string), m3 Map(uint32, tuple(uint32, uint32)), m4 Map(uint32, array(uint32)), m5 array(Map(uint32, uint32)), m6 tuple(Map(uint32, uint32), Map(string, string)), m7 array(Map(uint32, array(tuple(Map(uint32, uint32), tuple(uint32)))))) ENGINE=Memory();
EOF

${CLICKHOUSE_CLIENT} --query="INSERT INTO maps VALUES ({1 : 2, 2 : 3}, {'1' : 'a', '2' : 'b'}, {1 : (1, 2), 2 : (3, 4)}, {1 : [1, 2], 2 : [3, 4]}, [{1 : 2, 2 : 3}, {3 : 4, 4 : 5}], ({1 : 2, 2 : 3}, {'a' : 'b', 'c' : 'd'}), [{1 : [({1 : 2}, (1)), ({2 : 3}, (2))]}, {2 : [({3 : 4}, (3)), ({4 : 5}, (4))]}])"


formats="Arrow Parquet ORC";


for format in ${formats}; do
    echo $format
    
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM maps FORMAT $format" > "${CLICKHOUSE_TMP}"/maps
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE maps"
    cat "${CLICKHOUSE_TMP}"/maps | ${CLICKHOUSE_CLIENT} -q "INSERT INTO maps FORMAT $format"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM maps"
done

${CLICKHOUSE_CLIENT} --query="DROP STREAM maps"
