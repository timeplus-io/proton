#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS nullable_low_cardinality_tsv_test;";
$CLICKHOUSE_CLIENT --multiquery --query="create stream nullable_low_cardinality_tsv_test
(
    A date,
    S LowCardinality(Nullable(string)),
    X int32,
    S1 LowCardinality(Nullable(string)),
    S2 array(string)
) ";

printf '2020-01-01\t\N\t32\t\N\n' | $CLICKHOUSE_CLIENT -q 'insert into nullable_low_cardinality_tsv_test format TSV' 2>&1 \
    | grep -q "Code: 27"

echo $?;

$CLICKHOUSE_CLIENT --query="DROP STREAM nullable_low_cardinality_tsv_test";
