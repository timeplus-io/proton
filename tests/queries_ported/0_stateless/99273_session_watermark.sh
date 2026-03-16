#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS 99273_mv"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_session_wm"

${CLICKHOUSE_CLIENT} --query "
    CREATE STREAM test_session_wm
    (
        value int32,
        ts datetime64(3, 'UTC')
    )
    SETTINGS event_time_column='ts'
"

${CLICKHOUSE_CLIENT} --query "SELECT sleep(1) FORMAT Null"

${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW 99273_mv AS
    SELECT sum(cnt) AS total
    FROM
    (
        SELECT count() AS cnt, window_start AS ws, window_end AS we
        FROM session(test_session_wm, ts, 60s)
        GROUP BY ws, we
        EMIT PERIODIC 50ms
    )
    SETTINGS default_hash_table='memory'
"

${CLICKHOUSE_CLIENT} --query "SELECT sleep(1) FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"



BASE_TS="2026-03-10 00:00:00.000"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_session_wm (value, ts)
    VALUES (0, to_datetime64('${BASE_TS}', 3, 'UTC'))
"

${CLICKHOUSE_CLIENT} --query "SELECT sleep(1) FORMAT Null"

for i in $(seq 1 80); do
    offset_ms=$((i * 3000))
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO test_session_wm (value, ts)
        VALUES (
            $i,
            add_milliseconds(to_datetime64('${BASE_TS}', 3, 'UTC'), ${offset_ms})
        )
    "
    sleep 0.15
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() = 0
    FROM table(system.proton_err_log)
    WHERE _tp_time > now() - 10s
        AND raw LIKE '%outdated watermark%'
"

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS 99273_mv"
${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS test_session_wm"
