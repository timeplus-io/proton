#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_arrays"
${CLICKHOUSE_CLIENT} --query="create stream arrow_arrays (arr1 array(int8), arr2 array(uint8), arr3 array(int16), arr4 array(uint16), arr5 array(int32), arr6 array(uint32), arr7 array(int64), arr8 array(uint64), arr9 array(string), arr10 array(fixed_string(4)), arr11 array(float32), arr12 array(float64), arr13 array(date), arr14 array(Datetime)) ENGINE=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_arrays VALUES ([1,-2,3],[1,2,3],[100,-200,300],[100,200,300],[10000000,-20000000,30000000],[10000000,2000000,3000000],[100000000000000,-200000000000,3000000000000],[100000000000000,20000000000000,3000000000000],['Some string','Some string','Some string'],['0000','1111','2222'],[42.42,424.2,0.4242],[424242.424242,4242042420.242424,42],['2000-01-01','2001-01-01','2002-01-01'],['2000-01-01 00:00:00','2001-01-01 00:00:00','2002-01-01 00:00:00']),([],[],[],[],[],[],[],[],[],[],[],[],[],[])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_arrays FORMAT Arrow" > "${CLICKHOUSE_TMP}"/arrays.arrow

cat "${CLICKHOUSE_TMP}"/arrays.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_arrays FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_arrays"
${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_arrays"
