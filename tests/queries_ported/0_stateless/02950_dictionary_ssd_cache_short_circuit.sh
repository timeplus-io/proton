#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="
    CREATE STREAM source_table
    (
    id uint64,
    v1 string,
    v2 nullable(string),
    v3 nullable(uint64)
    )
    ENGINE = Memory;

    INSERT INTO source_table VALUES (0, 'zero', 'zero', 0), (1, 'one', null, 1);

    CREATE DICTIONARY ssd_cache_dictionary
    (
    id uint64,
    v1 string,
    v2 nullable(string) DEFAULT NULL,
    v3 nullable(uint64)
    )
    PRIMARY KEY id
    SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' STREAM 'source_table'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 8192 PATH '/var/lib/proton/user_files/${CLICKHOUSE_DATABASE}_ssd_dic'));

    SELECT dict_get_or_default('ssd_cache_dictionary', ('v1', 'v2'), 0, (int_div(1, id), int_div(1, id))) FROM source_table;
    SELECT dict_get_or_default('ssd_cache_dictionary', 'v2', id+1, int_div(null, id)) FROM source_table;
    SELECT dict_get_or_default('ssd_cache_dictionary', 'v3', id+1, int_div(null, id)) FROM source_table;

    DROP DICTIONARY ssd_cache_dictionary;
    DROP STREAM source_table;"
