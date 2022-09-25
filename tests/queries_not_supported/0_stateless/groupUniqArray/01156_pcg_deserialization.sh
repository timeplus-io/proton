#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a functions=("groupArraySample" "groupUniqArray")
declare -a engines=("Memory" "MergeTree order by n" "Log")

for func in "${functions[@]}"
do
  for engine in "${engines[@]}"
  do
    $CLICKHOUSE_CLIENT -q "drop stream if exists t";
    $CLICKHOUSE_CLIENT -q "create stream t (n uint8, a1 aggregate_function($func(1), uint8)) engine=$engine"
    $CLICKHOUSE_CLIENT -q "insert into t select number % 5 as n, ${func}State(1)(to_uint8(number)) from numbers(10) group by n"

    $CLICKHOUSE_CLIENT -q "select * from t format TSV" | $CLICKHOUSE_CLIENT -q "insert into t format TSV"
    $CLICKHOUSE_CLIENT -q "select countDistinct(n), countDistinct(a1) from t"

    $CLICKHOUSE_CLIENT -q "drop stream t";
  done
done
