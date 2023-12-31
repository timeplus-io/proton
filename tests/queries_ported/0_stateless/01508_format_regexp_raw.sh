#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n --query "
DROP STREAM IF EXISTS t;
CREATE STREAM t (a string, b string) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --format_regexp_escaping_rule 'Raw' --format_regexp '^(.+?) separator (.+?)$' --query '
INSERT INTO t FORMAT Regexp abc\ separator Hello, world!'

${CLICKHOUSE_CLIENT} -n --query "
SELECT * FROM t;
DROP STREAM t;
"
