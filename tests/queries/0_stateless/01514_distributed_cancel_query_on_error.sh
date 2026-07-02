#!/usr/bin/env bash
# Tags: distributed

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# _shard_num:
#   1 on 127.2
#   2 on 127.3
# max_block_size to fail faster
# max_memory_usage/_shard_num/repeat() will allow failure on the first shard earlier.
opts=(
    "--max_memory_usage=1G"
    "--max_block_size=50"
    "--max_threads=1"
    "--max_distributed_connections=2"
)
out=$(${CLICKHOUSE_CLIENT} "${opts[@]}" -q "SELECT groupArray(repeat('a', if(_shard_num == 2, 100000, 1))), number%100000 k from remote('127.{2,3}', system.numbers) GROUP BY k LIMIT 10e6" 2>&1 || true)

# The query should fail earlier on 127.3 and 127.2 should not even reach memory limit exceeded error.
#
# Proton/ClickHouse wording is not stable across backports, so accept both patterns.
echo "$out" | grep -Eq "DB::Exception: Received from 127\\.3(:${CLICKHOUSE_PORT_TCP})?\\. DB::Exception: (Query memory limit exceeded|Memory limit \\(for query\\) exceeded):" || {
    echo "$out"
    exit 1
}
