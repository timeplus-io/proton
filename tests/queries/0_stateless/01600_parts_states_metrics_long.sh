#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
verify_sql="SELECT
    (SELECT sum_if(value, metric = 'PartsActive'), sum_if(value, metric = 'PartsOutdated') FROM system.metrics)
    = (SELECT sum(active), sum(NOT active) FROM system.parts)"

# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition. But it should get expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    while true
    do
        result=$( $CLICKHOUSE_CLIENT -m --query="$verify_sql" )
        [ "$result" = "1" ] && break
        sleep 0.1
    done
    echo 1
}

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP STREAM IF EXISTS test_table"
$CLICKHOUSE_CLIENT --query="create stream test_table(data date) ENGINE = MergeTree  PARTITION BY to_year(data) ORDER BY data;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-01')"
verify

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-02')"
verify

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE test_table FINAL"
verify

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP STREAM test_table"
verify
