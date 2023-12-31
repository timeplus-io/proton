#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest
# Tag no-replicated-database: grep -c

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF  EXISTS dict1"

# Simple layout, but with two keys
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY key1, key2
    LAYOUT(HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c 'Primary key for simple dictionary must contain exactly one element'


# Simple layout, but with non existing key
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY non_existing_column
    LAYOUT(HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c "Unknown key attribute 'non_existing_column'"

# Complex layout, with non existing key
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY non_existing_column, key1
    LAYOUT(COMPLEX_KEY_HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c "Unknown key attribute 'non_existing_column'"

# No layout
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY key2, key1
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c "Cannot create dictionary with empty layout"

# No PK
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    LAYOUT(COMPLEX_KEY_HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c "Cannot create dictionary without primary key"

# No lifetime
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY key2, key1
    LAYOUT(COMPLEX_KEY_HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
" 2>&1 | grep -c "Cannot create dictionary with empty lifetime"

# No source
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY non_existing_column, key1
    LAYOUT(COMPLEX_KEY_HASHED())
    LIFETIME(MIN 1 MAX 10)
" 2>&1 | grep -c "Cannot create dictionary with empty source"


# Complex layout, but with one key
$CLICKHOUSE_CLIENT -q "
    CREATE DICTIONARY dict1
    (
        key1 uint64,
        key2 uint64,
        value string
    )
    PRIMARY KEY key1
    LAYOUT(COMPLEX_KEY_HASHED())
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcp_port() USER 'default' STREAM 'table_for_dict1' DB '$CLICKHOUSE_DATABASE'))
    LIFETIME(MIN 1 MAX 10)
" || exit 1


$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF  EXISTS dict1"
