#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS orc";

$CLICKHOUSE_CLIENT --query="create stream orc (array1 array(int32), array2 array(array(int32))) ";

$CLICKHOUSE_CLIENT --query="INSERT INTO orc VALUES ([1,2,3,4,5], [[1,2], [3,4], [5]]), ([42], [[42, 42], [42]])";

$CLICKHOUSE_CLIENT --query="SELECT * FROM orc FORMAT ORC";

$CLICKHOUSE_CLIENT --query="DROP STREAM orc";

