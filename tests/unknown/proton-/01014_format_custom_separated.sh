#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS custom_separated"
$CLICKHOUSE_CLIENT --query="CREATE STREAM custom_separated (n uint64, d Date, s string) ENGINE = Memory()"
$CLICKHOUSE_CLIENT --query="INSERT INTO custom_separated VALUES (0, '2019-09-24', 'hello'), (1, '2019-09-25', 'world'), (2, '2019-09-26', 'custom'), (3, '2019-09-27', 'separated')"

$CLICKHOUSE_CLIENT --query="SELECT * FROM custom_separated ORDER BY n FORMAT CustomSeparated SETTINGS \
format_custom_escaping_rule = 'CSV', \
format_custom_field_delimiter = '\t|\t', \
format_custom_row_before_delimiter = '||', \
format_custom_row_after_delimiter = '\t||', \
format_custom_row_between_delimiter = '\n', \
format_custom_result_before_delimiter = '========== result ==========\n', \
format_custom_result_after_delimiter = '\n============================\n'"

$CLICKHOUSE_CLIENT --query="TRUNCATE STREAM custom_separated"

echo '0, "2019-09-24", "hello"
1, 2019-09-25, "world"
2, "2019-09-26", custom
3, 2019-09-27, separated
end' | $CLICKHOUSE_CLIENT --query="INSERT INTO custom_separated SETTINGS \
format_custom_escaping_rule = 'CSV', \
format_custom_field_delimiter = ', ', \
format_custom_row_after_delimiter = '\n', \
format_custom_row_between_delimiter = '', \
format_custom_result_after_delimiter = 'end\n'
FORMAT CustomSeparated"

$CLICKHOUSE_CLIENT --query="SELECT * FROM custom_separated ORDER BY n FORMAT CSV"

$CLICKHOUSE_CLIENT --query="DROP STREAM custom_separated"

echo -ne "a,b\nc,d\n" | $CLICKHOUSE_LOCAL --structure "a string, b string"  \
  --input-format CustomSeparated --format_custom_escaping_rule=Escaped \
  --format_custom_field_delimiter=',' --format_custom_row_after_delimiter=$'\n' -q 'select * from stream' \
  2>&1| grep -Fac "'Escaped' serialization requires delimiter"
