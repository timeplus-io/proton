#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS tsv_custom_null";
$CLICKHOUSE_CLIENT --query="create stream tsv_custom_null (id Nullable(uint32)) ";

$CLICKHOUSE_CLIENT --query="INSERT INTO tsv_custom_null VALUES (NULL)";

$CLICKHOUSE_CLIENT --format_tsv_null_representation='MyNull' --query="SELECT * FROM tsv_custom_null FORMAT TSV";

$CLICKHOUSE_CLIENT --query="DROP STREAM tsv_custom_null";

