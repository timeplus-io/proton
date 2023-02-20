#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS json_test"

${CLICKHOUSE_CLIENT} --query="CREATE STREAM json_test (id uint32, metadata string) ENGINE = MergeTree() ORDER BY id"

${CLICKHOUSE_CLIENT} --query="INSERT INTO json_test VALUES (1, '{\"date\": \"2018-01-01\", \"task_id\": \"billing_history__billing_history.load_history_payments_data__20180101\"}'), (2, '{\"date\": \"2018-01-02\", \"task_id\": \"billing_history__billing_history.load_history_payments_data__20180101\"}')"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM json_test"

${CLICKHOUSE_CLIENT} --query="ALTER STREAM json_test DELETE WHERE json_extract_string(metadata, 'date') = '2018-01-01'" --mutations_sync=1

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM json_test"

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS json_test"
