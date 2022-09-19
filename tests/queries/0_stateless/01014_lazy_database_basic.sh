#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n -q "DROP DATABASE IF EXISTS testlazy"

${CLICKHOUSE_CLIENT} -n -q "
    CREATE DATABASE testlazy ENGINE = Lazy(1);
    create stream testlazy.log (a uint64, b uint64)  ;
    create stream testlazy.slog (a uint64, b uint64) ENGINE = StripeLog;
    create stream testlazy.tlog (a uint64, b uint64) ;
"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.parts WHERE database = 'testlazy'";

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name, create_table_query FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -n -q "
    SELECT * FROM testlazy.log LIMIT 0; -- drop testlazy.log from cache
    RENAME TABLE testlazy.log TO testlazy.log2;
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -n -q "
    INSERT INTO testlazy.log2 VALUES (1, 1);
    INSERT INTO testlazy.slog VALUES (2, 2);
    INSERT INTO testlazy.tlog VALUES (3, 3);
    SELECT * FROM testlazy.log2;
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -n -q "
    SELECT * FROM testlazy.log2 LIMIT 0; -- drop testlazy.log2 from cache
    DROP STREAM testlazy.log2;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -n -q "
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE testlazy;
"
