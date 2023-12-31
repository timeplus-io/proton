#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS data_a_01019;"
$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS data_b_01019;"

$CLICKHOUSE_CLIENT --query="CREATE STREAM data_a_01019 (x uint64) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query="CREATE STREAM data_b_01019 (x uint64) ENGINE = Memory;"

function thread1()
{
        for _ in {1..10}
        do
                seq 1 500000 | $CLICKHOUSE_CLIENT --query_id=11 --query="INSERT INTO data_a_01019 FORMAT TSV" &
                while true; do
                        $CLICKHOUSE_CLIENT --query="KILL QUERY WHERE query_id='11' SYNC" | grep -q "cant_cancel" && sleep .1 || break ||:
                done
                while true; do
                        $CLICKHOUSE_CLIENT --query="SELECT count(*)>0 FROM system.processes WHERE query_id='11'" | grep -q "1" && sleep .1 || break ||:
                done
        done
}

function thread2()
{
        for _ in {1..10}
        do
                seq 1 500000 | $CLICKHOUSE_CLIENT --query_id=22 --query="INSERT INTO data_b_01019 FORMAT TSV" &
                while true; do
                        $CLICKHOUSE_CLIENT --query="KILL QUERY WHERE query_id='22' SYNC" | grep -q "cant_cancel" && sleep .1 || break ||:
                done
                while true; do
                        $CLICKHOUSE_CLIENT --query="SELECT count(*)>0 FROM system.processes WHERE query_id='22'" | grep -q "1" && sleep .1 || break ||:
                done
        done
}

export -f thread1;
export -f thread2;

bash -c thread1 > /dev/null 2>&1 &
bash -c thread2 > /dev/null 2>&1 &

wait
echo OK

$CLICKHOUSE_CLIENT --query "DROP STREAM data_a_01019"
$CLICKHOUSE_CLIENT --query "DROP STREAM data_b_01019"
