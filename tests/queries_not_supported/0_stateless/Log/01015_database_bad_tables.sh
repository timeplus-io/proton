#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

db="db_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db;"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db;"
$CLICKHOUSE_CLIENT -q "CREATE STREAM $db.\`таблица_со_странным_названием\` (a uint64, b uint64) ENGINE = Log;"
$CLICKHOUSE_CLIENT -q "INSERT INTO $db.\`таблица_со_странным_названием\` VALUES (1, 1);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $db;"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $db;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP STREAM $db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $db;"

lazy_db="lazy_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $lazy_db;"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $lazy_db ENGINE = Lazy(1);"
$CLICKHOUSE_CLIENT -q "CREATE STREAM $lazy_db.\`таблица_со_странным_названием\` (a uint64, b uint64) ENGINE = Log;"
$CLICKHOUSE_CLIENT -q "INSERT INTO $lazy_db.\`таблица_со_странным_названием\` VALUES (1, 1);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $lazy_db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $lazy_db;"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $lazy_db;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $lazy_db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP STREAM $lazy_db.\`таблица_со_странным_названием\`;"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $lazy_db;"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $lazy_db ENGINE = Lazy(10);"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS $lazy_db.test;"
$CLICKHOUSE_CLIENT -q "CREATE STREAM IF NOT EXISTS $lazy_db.test (a uint64, b uint64) ENGINE = Log;"
$CLICKHOUSE_CLIENT -q "CREATE STREAM IF NOT EXISTS $lazy_db.test (a uint64, b uint64) ENGINE = Log;"
$CLICKHOUSE_CLIENT -q "INSERT INTO $lazy_db.test VALUES (1, 1);"
$CLICKHOUSE_CLIENT -q "SELECT * FROM $lazy_db.test;"
$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS $lazy_db.test;"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $lazy_db;"
