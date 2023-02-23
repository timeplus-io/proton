#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS t"

$CLICKHOUSE_CLIENT --allow_deprecated_syntax_for_merge_tree=1 --query="CREATE STREAM t (CounterID uint32, StartDate Date, UserID uint32, VisitID uint32, NestedColumn Nested(A uint8, S string), ToDrop uint32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

$CLICKHOUSE_CLIENT --query="ALTER STREAM t DROP COLUMN ToDro" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER STREAM t MODIFY COLUMN ToDro uint64" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER STREAM t RENAME COLUMN ToDro to ToDropp" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP STREAM t"
