#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


stream_name="test_99055"
database="test_99055"
disk_path="/var/lib/proton/disks"
disk_name="99055_d1"
TMP_QUERY_FILE=$(mktemp /tmp/tmp_query.log.XXXXXX)

function executeQuery()
{
    ## Execute query (provided via heredoc or herestring) and print query in case of error.
    trap 'rm -f ${TMP_QUERY_FILE}; trap - ERR RETURN' RETURN

    cat - > "${TMP_QUERY_FILE}"
    ! ${CLICKHOUSE_CLIENT} "${@}" --queries-file "${TMP_QUERY_FILE}"
}

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS 99055_hcs;"
$CLICKHOUSE_CLIENT -q "DROP STORAGE POLICY IF EXISTS 99055_p1;"
$CLICKHOUSE_CLIENT -q "DROP DISK IF EXISTS 99055_d1;"

$CLICKHOUSE_CLIENT -q "CREATE DISK IF NOT EXISTS 99055_d1 disk(type = local, path = '${disk_path}/d1/')";

executeQuery <<EOF
create STORAGE POLICY IF NOT EXISTS 99055_p1 as \$\$
volumes:
    hot:
        disk: default
    cold:
        disk: 99055_d1
\$\$;
EOF

executeQuery <<EOF
CREATE STREAM 99055_hcs
(
    i32 int32, s string, _tp_time datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4), INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 2
)
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
TTL to_start_of_day(_tp_time) + interval 7 day to volume 'cold'
SETTINGS storage_policy = '99055_p1';
EOF

echo "--- After create disk and storage policy"
# ignore free_space, total_space, keep_free_space
$CLICKHOUSE_CLIENT -q "show disks" | awk '{print $1, $2, $6, $7, $8, $9, $10, $11, $12, $13, $14}';
$CLICKHOUSE_CLIENT -q "show storage policies" | grep '99055_p1';


$CLICKHOUSE_CLIENT -q "DROP STORAGE POLICY IF EXISTS 99055_p1" 2>&1 | grep Exception | sed 's/Exception//g'
$CLICKHOUSE_CLIENT -q "DROP STORAGE POLICY IF EXISTS default" 2>&1 | grep Exception | sed 's/Exception//g'

$CLICKHOUSE_CLIENT -q "DROP DISK IF EXISTS 99055_d1" 2>&1 | grep Exception | sed 's/Exception//g'
$CLICKHOUSE_CLIENT -q "DROP DISK IF EXISTS default" 2>&1 | grep Exception | sed 's/Exception//g'

$CLICKHOUSE_CLIENT -q "DROP STREAM 99055_hcs"
$CLICKHOUSE_CLIENT -q "DROP STORAGE POLICY IF EXISTS 99055_p1"
$CLICKHOUSE_CLIENT -q "DROP DISK IF EXISTS 99055_d1"

echo "--- After drop disk and storage policy"
$CLICKHOUSE_CLIENT -q "show disks" | awk '{print $1, $2, $6, $7, $8, $9, $10, $11, $12, $13, $14}'
$CLICKHOUSE_CLIENT -q "show storage policies" | awk '{print $1, $2, $3, $4, $5, $6, $7, $8}'
