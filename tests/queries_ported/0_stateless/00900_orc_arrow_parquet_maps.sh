#!/usr/bin/env bash
# Tags: no-fasttest, no-python

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS maps"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET allow_experimental_map_type = 1;
CREATE STREAM maps (m1 map(uint32, uint32), m2 map(string, string), m3 map(uint32, tuple(uint32, uint32)), m4 map(uint32, array(uint32)), m5 array(map(uint32, uint32)), m6 tuple(map(uint32, uint32), map(string, string)), m7 array(map(uint32, array(tuple(map(uint32, uint32), tuple(uint32)))))) ENGINE=Memory();
EOF

${CLICKHOUSE_CLIENT} --query="INSERT INTO maps VALUES ({1 : 2, 2 : 3}, {'1' : 'a', '2' : 'b'}, {1 : (1, 2), 2 : (3, 4)}, {1 : [1, 2], 2 : [3, 4]}, [{1 : 2, 2 : 3}, {3 : 4, 4 : 5}], ({1 : 2, 2 : 3}, {'a' : 'b', 'c' : 'd'}), [{1 : [({1 : 2}, (1)), ({2 : 3}, (2))]}, {2 : [({3 : 4}, (3)), ({4 : 5}, (4))]}])"


formats="Arrow Parquet ORC";


for format in ${formats}; do
    echo $format
    
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM maps FORMAT $format" > "${CLICKHOUSE_TMP}"/maps
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE STREAM maps"
    cat "${CLICKHOUSE_TMP}"/maps | ${CLICKHOUSE_CLIENT} -q "INSERT INTO maps FORMAT $format"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM maps"
done

${CLICKHOUSE_CLIENT} --query="DROP STREAM maps"
