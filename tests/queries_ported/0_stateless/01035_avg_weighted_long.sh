#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT array_join([(1, 5), (2, 4), (3, 3), (4, 2), (5, 1)]) AS t));"
${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, weight) FROM (SELECT t.1 AS x, t.2 AS weight FROM (SELECT array_join([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)]) AS t));"
${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, y) FROM (select to_decimal256(1, 0) as x, to_decimal256(1, 1) as y);"
${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, y) FROM (select to_decimal32(1, 0) as x, to_decimal256(1, 1) as y);"

types=("int8" "int16" "int32" "int64" "uint8" "uint16" "uint32" "uint64" "float32" "float64")

for left in "${types[@]}"
do
    for right in "${types[@]}"
    do
        ${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, w) FROM values('x ${left}, w ${right}', (4, 1), (1, 0), (10, 2))"
        ${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(x, w) FROM values('x ${left}, w ${right}', (0, 0), (1, 0))"
    done
done

exttypes=("int128" "int256" "uint256")

for left in "${exttypes[@]}"
do
    for right in "${exttypes[@]}"
    do
        ${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(to_${left}(1), to_${right}(2))"
    done
done

# decimal types
dtypes=("32" "64" "128" "256")

for left in "${dtypes[@]}"
do
    for right in "${dtypes[@]}"
    do
        ${CLICKHOUSE_CLIENT} --query="SELECT avg_weighted(to_decimal${left}(2, 4), to_decimal${right}(1, 4))"
    done
done

echo "$(${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --query="SELECT avg_weighted(['string'], to_float64(0))" 2>&1)" \
  | grep -c 'Code: 43. DB::Exception: .* DB::Exception:.* Types .* are non-conforming as arguments for aggregate function avg_weighted'
