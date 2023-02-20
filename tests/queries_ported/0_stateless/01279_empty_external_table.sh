#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

touch "${CLICKHOUSE_TMP}"/empty.tsv
$CLICKHOUSE_CLIENT --query="SELECT count() FROM data" --external --file="${CLICKHOUSE_TMP}"/empty.tsv --name=data --types=uint32
rm "${CLICKHOUSE_TMP}"/empty.tsv

echo -n | $CLICKHOUSE_CLIENT --query="SELECT count() FROM data" --external --file=- --name=data --types=uint32
echo | $CLICKHOUSE_CLIENT --query="SELECT count() FROM data" --external --file=- --name=data --types=string
