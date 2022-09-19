#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    DROP DATABASE IF EXISTS 01280_db;
    CREATE DATABASE 01280_db Engine = Ordinary;
    DROP STREAM IF EXISTS 01280_db.table_for_dict;
    create stream 01280_db.table_for_dict
    (
        k1 string,
        k2 int32,
        a uint64,
        b int32,
        c string
    )
    ENGINE = MergeTree()
    ORDER BY (k1, k2);

    INSERT INTO 01280_db.table_for_dict VALUES (to_string(1), 3, 100, -100, 'clickhouse'), (to_string(2), -1, 3, 4, 'database'), (to_string(5), -3, 6, 7, 'columns'), (to_string(10), -20, 9, 8, '');
    INSERT INTO 01280_db.table_for_dict SELECT to_string(number), number + 1, 0, -1, 'a' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370;
    INSERT INTO 01280_db.table_for_dict SELECT to_string(number), number + 10, 0, -1, 'b' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 370, 370;
    INSERT INTO 01280_db.table_for_dict SELECT to_string(number), number + 100, 0, -1, 'c' FROM system.numbers WHERE number NOT IN (1, 2, 5, 10) LIMIT 700, 370;

    DROP DICTIONARY IF EXISTS 01280_db.ssd_dict;
    CREATE DICTIONARY 01280_db.ssd_dict
    (
        k1 string,
        k2 int32,
        a uint64 DEFAULT 0,
        b int32 DEFAULT -1,
        c string DEFAULT 'none'
    )
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01280_db'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));"

$CLICKHOUSE_CLIENT --testmode -nq "SELECT dictHas('01280_db.ssd_dict', 'a', tuple('1')); -- { serverError 43 }"

$CLICKHOUSE_CLIENT -n --query="
    SELECT 'TEST_SMALL';
    SELECT 'VALUE FROM RAM BUFFER';
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('1', to_int32(3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('1', to_int32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('1', to_int32(3)));
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('1', to_int32(3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('1', to_int32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('1', to_int32(3)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('2', to_int32(-1)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('2', to_int32(-1)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('2', to_int32(-1)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('5', to_int32(-3)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('5', to_int32(-3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('5', to_int32(-3)));

    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple('10', to_int32(-20)));
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', tuple('10', to_int32(-20)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', tuple('10', to_int32(-20)));"

$CLICKHOUSE_CLIENT --testmode -nq "SELECT dictGetUInt64('01280_db.ssd_dict', 'a', tuple(to_int32(3))); -- { serverError 53 }"

$CLICKHOUSE_CLIENT -n --query="DROP DICTIONARY 01280_db.ssd_dict;
    DROP STREAM IF EXISTS 01280_db.keys_table;
    create stream 01280_db.keys_table
    (
        k1 string,
        k2 int32
    )
    ENGINE = StripeLog();

    INSERT INTO 01280_db.keys_table VALUES ('1', 3);
    INSERT INTO 01280_db.keys_table SELECT to_string(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370;
    INSERT INTO 01280_db.keys_table VALUES ('2', -1);
    INSERT INTO 01280_db.keys_table SELECT to_string(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 370, 370;
    INSERT INTO 01280_db.keys_table VALUES ('5', -3);
    INSERT INTO 01280_db.keys_table SELECT to_string(intHash64(number + 1) % 1200), 11 + intHash64(number) % 1200 FROM system.numbers LIMIT 700, 370;
    INSERT INTO 01280_db.keys_table VALUES ('10', -20);

    DROP DICTIONARY IF EXISTS 01280_db.ssd_dict;CREATE DICTIONARY 01280_db.ssd_dict
    (
        k1 string,
        k2 int32,
        a uint64 DEFAULT 0,
        b int32 DEFAULT -1,
        c string DEFAULT 'none'
    )
    PRIMARY KEY k1, k2
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '01280_db'))
    LIFETIME(MIN 1000 MAX 2000)
    LAYOUT(COMPLEX_KEY_SSD_CACHE(FILE_SIZE 8192 PATH '$USER_FILES_PATH/1d' BLOCK_SIZE 512 WRITE_BUFFER_SIZE 4096));

    SELECT 'UPDATE DICTIONARY';

    SELECT sum(dictGetUInt64('01280_db.ssd_dict', 'a', (k1, k2))) FROM 01280_db.keys_table;

    SELECT 'VALUE FROM DISK';
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', ('1', to_int32(3)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', ('1', to_int32(3)));

    SELECT 'VALUE FROM RAM BUFFER';
    SELECT dictGetInt32('01280_db.ssd_dict', 'b', ('10', to_int32(-20)));
    SELECT dictGetString('01280_db.ssd_dict', 'c', ('10', to_int32(-20)));

    SELECT 'VALUES FROM DISK AND RAM BUFFER';
    SELECT sum(dictGetUInt64('01280_db.ssd_dict', 'a', (k1, k2))) FROM 01280_db.keys_table;

    SELECT 'HAS';
    SELECT count() FROM 01280_db.keys_table WHERE dictHas('01280_db.ssd_dict', (k1, k2));

    SELECT 'VALUES NOT FROM TABLE';
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', ('unknown', to_int32(0))), dictGetInt32('01280_db.ssd_dict', 'b', ('unknown', to_int32(0))), dictGetString('01280_db.ssd_dict', 'c', ('unknown', to_int32(0)));
    SELECT dictGetUInt64('01280_db.ssd_dict', 'a', ('unknown', to_int32(0))), dictGetInt32('01280_db.ssd_dict', 'b', ('unknown', to_int32(0))), dictGetString('01280_db.ssd_dict', 'c', ('unknown', to_int32(0)));

    SELECT 'DUPLICATE KEYS';
    SELECT array_join([('1', to_int32(3)), ('2', to_int32(-1)), ('', to_int32(0)), ('', to_int32(0)), ('2', to_int32(-1)), ('1', to_int32(3))]) AS keys, dictGetInt32('01280_db.ssd_dict', 'b', keys);
    DROP DICTIONARY IF EXISTS database_for_dict.ssd_dict;
    DROP STREAM IF EXISTS database_for_dict.keys_table;"

$CLICKHOUSE_CLIENT -n --query="DROP DATABASE IF EXISTS 01280_db;"
