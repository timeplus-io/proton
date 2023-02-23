#!/usr/bin/env bash
# Tags: no-s3-storage, no-random-settings, no-parallel
set -eo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpret_as_string(generate_uuidv4()))))")

${CLICKHOUSE_CLIENT} -q "DROP STREAM IF EXISTS lazy_mark_test;"
${CLICKHOUSE_CLIENT} <<EOF
CREATE STREAM lazy_mark_test
(
  n0 uint64,
  n1 uint64,
  n2 uint64,
  n3 uint64,
  n4 uint64,
  n5 uint64,
  n6 uint64,
  n7 uint64,
  n8 uint64,
  n9 uint64
)
ENGINE = MergeTree
ORDER BY n0 SETTINGS min_bytes_for_wide_part = 0;
EOF

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES lazy_mark_test"
${CLICKHOUSE_CLIENT} -q "INSERT INTO lazy_mark_test select number, number % 3, number % 5, number % 10, number % 13, number % 15, number % 17, number % 18, number % 22, number % 25 from numbers(1000000)"
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP MARK CACHE"
${CLICKHOUSE_CLIENT} --log_queries=1 --query_id "${QUERY_ID}" -q "SELECT * FROM lazy_mark_test WHERE n3==11 SETTINGS load_marks_asynchronously=0"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} -q "select ProfileEvents['FileOpen'] from system.query_log where query_id = '${QUERY_ID}' and type = 'QueryFinish' and current_database = current_database()"
