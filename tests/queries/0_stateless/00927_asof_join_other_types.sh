#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for typename in "uint32" "uint64" "float64" "float32" "datetime('Europe/Moscow')" "Decimal32(5)" "Decimal64(5)" "Decimal128(5)" "DateTime64(3, 'Europe/Moscow')"
do
    $CLICKHOUSE_CLIENT -mn <<EOF
DROP STREAM IF EXISTS A;
DROP STREAM IF EXISTS B;

create stream A(k uint32, t ${typename}, a float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (2,1,1),(2,3,3),(2,5,5);

create stream B(k uint32, t ${typename}, b float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (2,3,3);

SELECT k, t, a, b FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (k,t);

DROP STREAM A;
DROP STREAM B;
EOF

done
