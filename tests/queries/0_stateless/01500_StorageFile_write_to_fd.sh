#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The following command will execute:
#     create stream table (key uint32) ENGINE = File(TSV, stdin);
#     INSERT INTO `table` SELECT key FROM input('key uint32') FORMAT TSV
${CLICKHOUSE_LOCAL} -S 'key uint32' -q "INSERT INTO \`table\` SELECT key FROM input('key uint32') FORMAT TSV" < /dev/null 2>&1 \
    | grep -q "No data to insert" || echo "Fail"
