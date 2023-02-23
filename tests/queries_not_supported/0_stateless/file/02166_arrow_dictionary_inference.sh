#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "insert into stream function file('arrow.dict', 'Arrow', 'x low_cardinality(uint64)') select number from numbers(10) settings output_format_arrow_low_cardinality_as_dictionary=1, engine_file_truncate_on_insert=1, allow_suspicious_low_cardinality_types=1"

$CLICKHOUSE_CLIENT -q "desc file('arrow.dict', 'Arrow')"

