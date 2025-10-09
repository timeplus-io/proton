#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

while :; do
    $CLICKHOUSE_CLIENT -q "select sleep_Each_Row(0.1) from numbers(100) settings max_block_size=1 format Null" &
    pid=$!
    sleep 1.5
    duration="$($CLICKHOUSE_CLIENT -q "select floor(elapsed) from system.processes where current_database = current_Database() and query not like '%system.processes%'")"
    # The process might not exist at this point in some exception situations
    # maybe it was killed by OOM?
    # It safe to skip this iteration.
    if ! kill -INT $pid > /dev/null 2>&1; then
        continue
    fi
    wait
    $CLICKHOUSE_CLIENT -q "kill query where current_database = current_Database() sync format Null"
    if [[ $duration -eq 1 ]]; then
        echo "OK"
        break
    fi
done
