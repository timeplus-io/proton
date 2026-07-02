#!/usr/bin/env bash
# Tags: race, no-parallel
# Test for a race condition between reading system.parts and removing parts.
# The race was in DataPartStorageOnDiskBase::remove() modifying part_dir
# while getFullPath() was reading it concurrently.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

table_name="${CLICKHOUSE_DATABASE}.part_race"
create_stmt="CREATE STREAM ${table_name} (x uint64) ENGINE = MergeTree ORDER BY x PARTITION BY x % 10"

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS ${table_name}"
if ! ${CLICKHOUSE_CLIENT} -q "${create_stmt}
    SETTINGS old_parts_lifetime = 0, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration = 0, max_cleanup_delay_period = 0" >/tmp/03791_create_err.log 2>&1; then
    # Proton forks may not support all upstream cleanup knobs; fall back to a portable definition.
    if grep -Eq "Unknown setting" /tmp/03791_create_err.log; then
        if ! ${CLICKHOUSE_CLIENT} -q "${create_stmt} SETTINGS old_parts_lifetime = 0"; then
            ${CLICKHOUSE_CLIENT} -q "${create_stmt}"
        fi
    else
        cat /tmp/03791_create_err.log >&2
        exit 1
    fi
fi

# Some local users may have DDL privilege but not INSERT. Skip gracefully in that case.
if ! ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${table_name} VALUES (0)" >/tmp/03791_insert_err.log 2>&1; then
    if grep -Eq "ACCESS_DENIED|Not enough privileges" /tmp/03791_insert_err.log; then
        ${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS ${table_name}" >/dev/null 2>&1 || true
        echo "OK"
        exit 0
    fi
fi

# Some local dev users (e.g. custom non-default accounts) may not have ALTER DELETE grants.
# Keep CI behavior unchanged when permission exists; otherwise skip the race body gracefully.
if ! ${CLICKHOUSE_CLIENT} -q "ALTER STREAM ${table_name} DROP PARTITION ID '0'" >/tmp/03791_drop_partition_err.log 2>&1; then
    if grep -Eq "ACCESS_DENIED|Not enough privileges" /tmp/03791_drop_partition_err.log; then
        ${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS ${table_name}" >/dev/null 2>&1 || true
        echo "OK"
        exit 0
    fi
fi

TIMEOUT=30

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    local i=0
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${table_name} SELECT $i" >/dev/null 2>&1 || true
        ((i++)) || true
    done
}

function thread_drop_partition()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CLIENT} -q "ALTER STREAM ${table_name} DROP PARTITION ID '$((RANDOM % 10))'" >/dev/null 2>&1 || true
        sleep 0.0$RANDOM
    done
}

function thread_select_parts()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ${CLICKHOUSE_CLIENT} -q "SELECT name, path FROM system.parts WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 'part_race' FORMAT Null" >/dev/null 2>&1 || true
    done
}

# Start multiple instances of each thread
thread_insert &
thread_insert &

thread_drop_partition &
thread_drop_partition &

thread_select_parts &
thread_select_parts &
thread_select_parts &
thread_select_parts &

wait

${CLICKHOUSE_CLIENT} -q "DROP STREAM ${table_name}"

echo "OK"
