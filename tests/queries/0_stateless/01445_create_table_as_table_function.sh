#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
${CLICKHOUSE_CLIENT} --query "create stream system.columns AS numbers(10);" 2>&1 | grep -F "Code: 57" > /dev/null && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query "create stream system.columns engine=Memory AS numbers(10);" 2>&1 | grep -F "Code: 62" > /dev/null && echo 'OK' || echo 'FAIL'
${CLICKHOUSE_CLIENT} --query "create stream system.columns AS numbers(10) engine=Memory;" 2>&1 | grep -F "Code: 62" > /dev/null && echo 'OK' || echo 'FAIL'
