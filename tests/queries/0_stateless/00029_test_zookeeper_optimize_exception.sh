#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test_optimize_exception"
${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS test_optimize_exception_replicated"

${CLICKHOUSE_CLIENT} --query="create stream test_optimize_exception (date date) ENGINE=MergeTree() PARTITION BY toYYYYMM(date) ORDER BY date"
${CLICKHOUSE_CLIENT} --query="create stream test_optimize_exception_replicated (date date) ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/optimize', 'r1') PARTITION BY toYYYYMM(date) ORDER BY date"

${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception VALUES (to_date('2017-09-09')), (to_date('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception VALUES (to_date('2017-09-09')), (to_date('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception_replicated VALUES (to_date('2017-09-09')), (to_date('2017-09-10'))"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_optimize_exception_replicated VALUES (to_date('2017-09-09')), (to_date('2017-09-10'))"

${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="OPTIMIZE STREAM test_optimize_exception PARTITION 201709 FINAL"
${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --query="OPTIMIZE STREAM test_optimize_exception_replicated PARTITION 201709 FINAL"

echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="OPTIMIZE STREAM test_optimize_exception PARTITION 201710" 2>&1)" \
  | grep -c 'Code: 388. DB::Exception: .* DB::Exception: .* Cannot select parts for optimization'
echo "$(${CLICKHOUSE_CLIENT} --optimize_throw_if_noop 1 --server_logs_file=/dev/null --query="OPTIMIZE STREAM test_optimize_exception_replicated PARTITION 201710" 2>&1)" \
  | grep -c 'Code: 388. DB::Exception: .* DB::Exception:.* Cannot select parts for optimization'

${CLICKHOUSE_CLIENT} --query="DROP STREAM test_optimize_exception"
${CLICKHOUSE_CLIENT} --query="DROP STREAM test_optimize_exception_replicated"
