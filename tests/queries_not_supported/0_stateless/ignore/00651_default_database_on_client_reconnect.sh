#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --ignore-error --multiquery --query "DROP STREAM IF EXISTS tab_00651; create stream tab_00651 (val uint64) engine = Memory; SHOW create stream tab_00651 format abcd; DESC tab_00651; DROP STREAM tab_00651;" 2>/dev/null ||:
