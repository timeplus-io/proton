#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# bug: https://github.com/timeplus-io/proton/issues/523
formatted_sql=`$CLICKHOUSE_FORMAT --query "select trim(BOTH '\"' from '"test"') == 'test'"`
$CLICKHOUSE_CLIENT -q "$formatted_sql"
