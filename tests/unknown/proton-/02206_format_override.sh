#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


SAMPLE_FILE="$CURDIR/02206_sample_data.csv"

echo 'File generated:'
${CLICKHOUSE_LOCAL} -q "SELECT number, number * 2 from numbers(7) FORMAT TSV" | tr '\t' ',' >"$SAMPLE_FILE"


echo "Options: --input-format=CSV --output-format JSONEachRow --format TSV"
cat "$SAMPLE_FILE" | ${CLICKHOUSE_LOCAL} --input-format CSV --output-format JSONEachRow --format TSV --structure='num1 int64, num2 int64' --query='SELECT * from stream'

echo "Options: --input-format=CSV --format TSV"
cat "$SAMPLE_FILE" | ${CLICKHOUSE_LOCAL} --input-format CSV --format TSV --structure='num1 int64, num2 int64' --query='SELECT * from stream'

echo "Options: --output-format=JSONEachRow --format CSV"
cat "$SAMPLE_FILE" | ${CLICKHOUSE_LOCAL} --output-format JSONEachRow --format CSV --structure='num1 int64, num2 int64' --query='SELECT * from stream'

echo "Options: --format CSV"
cat "$SAMPLE_FILE" | ${CLICKHOUSE_LOCAL} --format CSV --structure='num1 int64, num2 int64' --query='SELECT * from stream'

rm "$SAMPLE_FILE"