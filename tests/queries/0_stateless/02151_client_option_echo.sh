#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# shellcheck source=../shell_config.sh

. "$CURDIR"/../shell_config.sh

# single query echo on
${CLICKHOUSE_CLIENT} --echo --query="drop stream IF EXISTS echo_test_0"
# single query echo off
${CLICKHOUSE_CLIENT} --query="drop stream IF EXISTS echo_test_1"
# multi query echo on
${CLICKHOUSE_CLIENT} --echo --multiquery --query="drop stream IF EXISTS echo_test_2;drop stream IF EXISTS echo_test_3"
# multi query echo off
${CLICKHOUSE_CLIENT} --multiquery --query="drop stream IF EXISTS echo_test_4;drop stream IF EXISTS echo_test_5"
