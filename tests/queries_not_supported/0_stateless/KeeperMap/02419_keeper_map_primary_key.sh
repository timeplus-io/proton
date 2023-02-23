#!/usr/bin/env bash
# Tags: no-ordinary-database, no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS 02419_test SYNC;"

test_primary_key()
{
    $CLICKHOUSE_CLIENT -nm -q "
    CREATE STREAM 02419_test (key uint64, value float64) Engine=KeeperMap('/' || current_database() || '/test2418', 3) PRIMARY KEY($1);
    INSERT INTO 02419_test VALUES (1, 1.1), (2, 2.2);
    SELECT value FROM 02419_test WHERE key = 1;
    SELECT value FROM 02419_test WHERE key IN (2, 3);
    DROP STREAM 02419_test SYNC;
    "
}

test_primary_key "sipHash64(key + 42) * 12212121212121"
test_primary_key "reverse(concat(CAST(key, 'string'), 'some string'))"
test_primary_key "hex(to_float32(key))"
