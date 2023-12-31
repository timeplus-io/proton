#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --allow_suspicious_fixed_string_types 1 --structure 'x low_cardinality(fixed_string(2454139))' --input-format Values --output-format TSV --query "SELECT * FROM stream" <<< '(1)' | wc -c
