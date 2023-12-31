#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT -q "drop stream if exists t_avro_$i"
    $CLICKHOUSE_CLIENT -q "create stream t_avro_$i (x uint32, s string) engine=File(Avro)"
done

for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT -q "insert into t_avro_$i select number, 'str' from numbers(1000) settings engine_file_truncate_on_insert=1" > /dev/null &
done

sleep 5

for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT-q "drop stream t_avro_$i"
done

