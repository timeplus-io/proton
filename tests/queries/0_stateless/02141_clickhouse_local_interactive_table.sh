#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --file /dev/null --structure "key string" --input-format TSVWithNamesAndTypes --interactive --send_logs_level=trace <<<'show create stream table'
