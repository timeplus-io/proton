#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on brotli and bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP STREAM IF EXISTS file"
${CLICKHOUSE_CLIENT} --query "CREATE STREAM file (x uint64) ENGINE = File(TSV, '${CLICKHOUSE_DATABASE}/data.tsv.bz2')"
${CLICKHOUSE_CLIENT} --query "TRUNCATE STREAM file"
${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(1000000)"
${CLICKHOUSE_CLIENT} --max_read_buffer_size=2 --query "SELECT count(), max(x) FROM file"
${CLICKHOUSE_CLIENT} --query "DROP STREAM file"

