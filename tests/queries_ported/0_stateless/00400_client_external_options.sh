#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file=- --types=int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file - --types int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file=- --types int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query="SELECT * FROM _data" --external --file - --types=int8;
echo -ne "1\n2\n3\n" | $CLICKHOUSE_CLIENT --query "SELECT * FROM _data" --external --file=- --types=int8;
