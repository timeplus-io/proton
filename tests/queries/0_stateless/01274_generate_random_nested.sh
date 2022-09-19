#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d "SELECT * FROM generateRandom('\"ParsedParams.Key1\" array(string), \"ParsedParams.Key2\" array(float64), x string', 1, 10, 2) LIMIT 10" > /dev/null;
