#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -n -q "
        DROP STREAM IF EXISTS matview_exception_a_to_c;
        DROP STREAM IF EXISTS matview_exception_a_to_b;
        DROP STREAM IF EXISTS table_exception_c;
        DROP STREAM IF EXISTS table_exception_b;
        DROP STREAM IF EXISTS table_exception_a;
        ";
}

function setup()
{
    ${CLICKHOUSE_CLIENT} -n -q "
        create stream table_exception_a (a string, b int64) ENGINE = MergeTree ORDER BY b;
        create stream table_exception_b (a float64,  b int64) ENGINE = MergeTree ORDER BY tuple();
        create stream table_exception_c (a float64) ENGINE = MergeTree ORDER BY a;

        CREATE MATERIALIZED VIEW matview_exception_a_to_b TO table_exception_b AS SELECT toFloat64(a) AS a, b FROM table_exception_a;
        CREATE MATERIALIZED VIEW matview_exception_a_to_c TO table_exception_c AS SELECT b AS a FROM table_exception_a;
      ";
}

function test()
{
    echo "$@";
    # We are going to insert an invalid number into table_exception_a. This will fail when inserting into
    # table_exception_b via matview_exception_a_to_b, and will work ok when inserting into table_exception_c
    ${CLICKHOUSE_CLIENT} "$@" --log_queries=1 --log_query_views=1 -q "INSERT INTO table_exception_a VALUES ('0.Aa234', 22)" > /dev/null 2>&1 || true;
    ${CLICKHOUSE_CLIENT} -q "
        SELECT * FROM
        (
          SELECT 'table_exception_a' as name, count() AS c FROM table_exception_a UNION ALL
          SELECT 'table_exception_b' as name, count() AS c FROM table_exception_b UNION ALL
          SELECT 'table_exception_c' as name, count() AS c FROM table_exception_c
        )
        ORDER BY name ASC
        FORMAT TSV";

    ${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS';

    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            replaceOne(CAST(type AS string), 'ExceptionWhileProcessing', 'Excep****WhileProcessing')
            exception_code
        FROM system.query_log
        WHERE
              query LIKE 'INSERT INTO table_exception_a%' AND
              type > 0 AND
              event_date >= yesterday() AND
              current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        FORMAT TSV";

    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            view_name,
            replaceOne(CAST(status AS string), 'ExceptionWhileProcessing', 'Excep****WhileProcessing'),
            exception_code,
            view_target,
            view_query
        FROM system.query_views_log
        WHERE initial_query_id =
            (
                SELECT query_id
                FROM system.query_log
                WHERE
                      current_database = '${CLICKHOUSE_DATABASE}' AND
                      query LIKE 'INSERT INTO table_exception_a%' AND
                      type > 0 AND
                      event_date >= yesterday() AND
                      current_database = currentDatabase()
                ORDER BY event_time_microseconds DESC
                LIMIT 1
            )
        ORDER BY view_name ASC
        ";
}

trap cleanup EXIT;
cleanup;
setup;

test --parallel_view_processing 0;
test --parallel_view_processing 1;

exit 0
