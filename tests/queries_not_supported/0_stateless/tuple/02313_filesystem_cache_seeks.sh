#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-s3-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


for STORAGE_POLICY in 's3_cache' 'local_cache' 's3_cache_multi'; do
    echo "Using storage policy: $STORAGE_POLICY"
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"

    $CLICKHOUSE_CLIENT --query "DROP STREAM IF EXISTS test_02313"

    $CLICKHOUSE_CLIENT --query "CREATE STREAM test_02313 (id int32, val string)
    ENGINE = MergeTree()
    ORDER BY tuple()
    SETTINGS storage_policy = '$STORAGE_POLICY'"

    $CLICKHOUSE_CLIENT --enable_filesystem_cache_on_write_operations=0 -n --query "INSERT INTO test_02313
    SELECT * FROM
        generateRandom('id int32, val string')
    LIMIT 100000"

    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SELECT * FROM test_02313 WHERE val LIKE concat('%', randomPrintableASCII(3), '%') FORMAT Null"

    $CLICKHOUSE_CLIENT --query "DROP STREAM test_02313"

done
