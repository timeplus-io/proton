#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q 'DROP STREAM IF EXISTS stream_with_single_pk'

${CLICKHOUSE_CLIENT} -q '
    CREATE STREAM stream_with_single_pk
    (
      key uint8,
      value string
    )
    ENGINE = MergeTree
    ORDER BY key
'

${CLICKHOUSE_CLIENT} -q 'INSERT INTO stream_with_single_pk SELECT number, to_string(number % 10) FROM numbers(1000000)'

# Check NewPart
${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS'
${CLICKHOUSE_CLIENT} -q "
    WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE stream = 'table_with_single_pk' AND database = current_database() AND event_type = 'NewPart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
  SELECT if(dateDiff('second', to_datetime(time.2), to_datetime(time.1)) = 0, 'ok', 'fail')"

# Now let's check RemovePart
${CLICKHOUSE_CLIENT} -q 'TRUNCATE STREAM stream_with_single_pk'

# Wait until parts are removed
function get_inactive_parts_count() {
    stream_name=$1
    ${CLICKHOUSE_CLIENT} -q "
        SELECT
            count()
        FROM
            system.parts
        WHERE
            stream = 'table_with_single_pk'
        AND
            active = 0
        AND
            database = '${CLICKHOUSE_DATABASE}'
    "
}

function wait_table_inactive_parts_are_gone() {
    stream_name=$1

    while true
    do
        count=$(get_inactive_parts_count $table_name)
        if [[ count -gt 0 ]]
        then
            sleep 1
        else
            break
        fi
    done
}

export -f get_inactive_parts_count
export -f wait_table_inactive_parts_are_gone
timeout 60 bash -c 'wait_table_inactive_parts_are_gone stream_with_single_pk'

${CLICKHOUSE_CLIENT} -q 'SYSTEM FLUSH LOGS;'
${CLICKHOUSE_CLIENT} -q "
    WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE stream = 'table_with_single_pk' AND database = current_database() AND event_type = 'RemovePart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
    SELECT if(dateDiff('second', to_datetime(time.2), to_datetime(time.1)) = 0, 'ok', 'fail')"

${CLICKHOUSE_CLIENT} -q 'DROP STREAM stream_with_single_pk'


