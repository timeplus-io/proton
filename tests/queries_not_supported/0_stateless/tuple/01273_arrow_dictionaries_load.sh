#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP STREAM IF EXISTS arrow_dicts"
${CLICKHOUSE_CLIENT} --query="create stream arrow_dicts (a low_cardinality(string), b array(low_cardinality(string)), c tuple(low_cardinality(string), low_cardinality(string))) ENGINE=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts VALUES ('1', ['a', 'b', 'c'], ('z', '6')), ('2', ['d', 'e'], ('x', '9'))"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_dicts FORMAT Arrow SETTINGS output_format_arrow_low_cardinality_as_dictionary=1" > "${CLICKHOUSE_TMP}"/dicts.arrow

cat "${CLICKHOUSE_TMP}"/dicts.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_dicts FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_dicts"
${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_dicts"

${CLICKHOUSE_CLIENT} --query="create stream arrow_dicts (a low_cardinality(string)) ENGINE=Memory()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_dicts SELECT to_string(number % 500) from numbers(10000000)"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_dicts FORMAT Arrow SETTINGS output_format_arrow_low_cardinality_as_dictionary=1" > "${CLICKHOUSE_TMP}"/dicts.arrow

cat "${CLICKHOUSE_TMP}"/dicts.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_dicts FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM arrow_dicts"

${CLICKHOUSE_CLIENT} --query="DROP STREAM arrow_dicts"

