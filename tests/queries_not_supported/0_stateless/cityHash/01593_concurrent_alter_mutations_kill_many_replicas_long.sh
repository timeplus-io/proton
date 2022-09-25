#!/usr/bin/env bash
# Tags: long, replica, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./replication.lib
. "$CURDIR"/replication.lib

REPLICAS=5

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS concurrent_kill_$i"
done

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "create stream concurrent_kill_$i (key uint64, value string) ENGINE =
    ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{shard}', '{replica}$i') ORDER BY key
    SETTINGS max_replicated_mutations_in_queue=1000, number_of_free_entries_in_pool_to_execute_mutation=0,max_replicated_merges_in_queue=1000"

done

$CLICKHOUSE_CLIENT --query "INSERT INTO concurrent_kill_1 SELECT number, to_string(number) FROM numbers(1000000)"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA concurrent_kill_$i"
    $CLICKHOUSE_CLIENT --query "SELECT sum(to_uint64(value)) FROM concurrent_kill_$i"
done

function alter_thread
{
    while true; do
        REPLICA=$(($RANDOM % 5 + 1))
        TYPE=$($CLICKHOUSE_CLIENT --query "SELECT type FROM system.columns WHERE table='concurrent_kill_$REPLICA' and database='${CLICKHOUSE_DATABASE}' and name='value'")
        if [ "$TYPE" == "string" ]; then
            $CLICKHOUSE_CLIENT --query "ALTER STREAM concurrent_kill_$REPLICA MODIFY COLUMN value uint64 SETTINGS replication_alter_partitions_sync=2"
        else
            $CLICKHOUSE_CLIENT --query "ALTER STREAM concurrent_kill_$REPLICA MODIFY COLUMN value string SETTINGS replication_alter_partitions_sync=2"
        fi
    done
}

function kill_mutation_thread
{
    while true; do
        # find any mutation and kill it
        mutation_id=$($CLICKHOUSE_CLIENT --query "SELECT mutation_id FROM system.mutations WHERE is_done = 0 and table like 'concurrent_kill_%' and database='${CLICKHOUSE_DATABASE}' LIMIT 1")
        if [ ! -z "$mutation_id" ]; then
            $CLICKHOUSE_CLIENT --query "KILL MUTATION WHERE mutation_id='$mutation_id' and table like 'concurrent_kill_%' and database='${CLICKHOUSE_DATABASE}'" 1> /dev/null
            sleep 1
        fi
    done
}

export -f alter_thread;
export -f kill_mutation_thread;

TIMEOUT=30

timeout $TIMEOUT bash -c alter_thread 2> /dev/null &
timeout $TIMEOUT bash -c kill_mutation_thread 2> /dev/null &

wait

# with timeout alter query can be not finished yet, so to execute new alter
# we use retries
counter=0
while true; do
    if $CLICKHOUSE_CLIENT --query "ALTER STREAM concurrent_kill_1 MODIFY COLUMN value int64 SETTINGS replication_alter_partitions_sync=2" 2> /dev/null ; then
        break
    fi

    if [ "$counter" -gt 120 ]
    then
        break
    fi
    sleep 0.5
    counter=$(($counter + 1))
done


metadata_version=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/s1/replicas/r11/' and name = 'metadata_version'")
for i in $(seq $REPLICAS); do
    replica_metadata_version=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/s1/replicas/r1$i/' and name = 'metadata_version'")

    if [ "$metadata_version" != "$replica_metadata_version" ]; then
        echo "Metadata version on replica $i differs from the first replica, FAIL"
    else
        echo "Metadata version on replica $i equal with first replica, OK"
    fi

    $CLICKHOUSE_CLIENT --query "SHOW create stream concurrent_kill_$i"
done

$CLICKHOUSE_CLIENT --query "SELECT sum(value) FROM concurrent_kill_1"

check_replication_consistency "concurrent_kill_" "count(), sum(key), sum(cityHash64(value))"

for i in $(seq $REPLICAS); do
    $CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS concurrent_kill_$i"
done
