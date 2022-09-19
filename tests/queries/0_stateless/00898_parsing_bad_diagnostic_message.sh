#!/usr/bin/env bash

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo -ne '0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ta' | $CLICKHOUSE_LOCAL --structure 'c0 uint8, c1 uint8, c2 uint8, c3 uint8, c4 uint8, c5 uint8, c6 uint8, c7 uint8, c8 uint8, c9 uint8, c10 uint8, c11 uint8' --input-format TSV --query 'SELECT * FROM table' 2>&1 | grep -F 'Column 11'
