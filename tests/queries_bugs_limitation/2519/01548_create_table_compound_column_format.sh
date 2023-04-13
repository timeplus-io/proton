#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "CREATE STREAM test(a int64, b NESTED(a int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT

echo "CREATE STREAM test(a int64, b TUPLE(a int64)) ENGINE=TinyLog" | $CLICKHOUSE_FORMAT