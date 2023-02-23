#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT delta_sum_timestamp(1, now64());" 2>&1 | grep -q "Code: 43.*Illegal type datetime64" && echo 'OK' || echo 'FAIL';

