#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

echo "
    DROP STREAM IF EXISTS two_blocks;
    create stream two_blocks (d date) ENGINE = MergeTree(d, d, 1);
    INSERT INTO two_blocks VALUES ('2000-01-01');
    INSERT INTO two_blocks VALUES ('2000-01-02');
" | $CLICKHOUSE_CLIENT -n

for _ in {1..10}; do seq 1 100 | sed 's/.*/SELECT count() FROM (SELECT * FROM two_blocks);/' | $CLICKHOUSE_CLIENT -n | grep -vE '^2$' && echo 'Fail!' && break; echo -n '.'; done; echo

echo "DROP STREAM two_blocks;" | $CLICKHOUSE_CLIENT -n
