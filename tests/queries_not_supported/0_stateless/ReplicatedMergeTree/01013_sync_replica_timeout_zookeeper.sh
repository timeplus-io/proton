#!/usr/bin/env bash
# Tags: replica, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

R1=table_1013_1
R2=table_1013_2

${CLICKHOUSE_CLIENT} -n -q "
    DROP STREAM IF EXISTS $R1;
    DROP STREAM IF EXISTS $R2;

    create stream $R1 (x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_1013', 'r1') ORDER BY x;
    create stream $R2 (x uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_1013', 'r2') ORDER BY x;

    SYSTEM STOP FETCHES $R2;
    INSERT INTO $R1 VALUES (1)
"

timeout 10s ${CLICKHOUSE_CLIENT} -n -q "
    SET receive_timeout=1;
    SYSTEM SYNC REPLICA $R2
" 2>&1 | grep -F -q "Code: 159. DB::Exception" && echo 'OK' || echo 'Failed!'

# By dropping tables all related SYNC REPLICA queries would be terminated as well
${CLICKHOUSE_CLIENT} -n -q "
    DROP STREAM IF EXISTS $R2;
    DROP STREAM IF EXISTS $R1;
"
