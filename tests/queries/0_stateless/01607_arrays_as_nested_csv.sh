#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP STREAM IF EXISTS test;
create stream test (a array(string)) ;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test FORMAT CSV" <<END
"['Hello', 'world', '42"" TV']"
END

${CLICKHOUSE_CLIENT} --format_csv_allow_single_quotes 0 --query "INSERT INTO test FORMAT CSV" <<END
"'Hello', 'world', '42"" TV'"
END

${CLICKHOUSE_CLIENT} --input_format_csv_arrays_as_nested_csv 1 --query "INSERT INTO test FORMAT CSV" <<END
"[""Hello"", ""world"", ""42"""" TV""]"
"""Hello"", ""world"", ""42"""" TV"""
END

${CLICKHOUSE_CLIENT} --multiquery --query "
SELECT * FROM test;
DROP STREAM IF EXISTS test;
"
