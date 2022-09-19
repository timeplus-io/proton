#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

$CLICKHOUSE_CLIENT -n --query="
    SET allow_experimental_bigint_types = 1;

    DROP STREAM IF EXISTS dictionary_decimal_source_table;
    create stream dictionary_decimal_source_table
    (
        id uint64,
        decimal_value Decimal256(5)
    ) ;

    INSERT INTO dictionary_decimal_source_table VALUES (1, 5.0);

    DROP DICTIONARY IF EXISTS ssd_cache_dictionary;
    CREATE DICTIONARY ssd_cache_dictionary
    (
        id uint64,
        decimal_value Decimal256(5)
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'dictionary_decimal_source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '$USER_FILES_PATH/0d'));

    SELECT 'SSDCache dictionary';
    SELECT dictGet('ssd_cache_dictionary', 'decimal_value', to_uint64(1));

    DROP DICTIONARY ssd_cache_dictionary;"
