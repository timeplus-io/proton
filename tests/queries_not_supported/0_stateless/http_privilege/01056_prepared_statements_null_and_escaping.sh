#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%20World" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%5CtWorld" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%5CnWorld" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%5C%09World" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%5C%0AWorld" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=%5CN" \
    -d "SELECT {x:nullable(string)}";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%09World" \
    -d "SELECT {x:nullable(string)}" 2>&1 | grep -oF '457' | head -n1;

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_x=Hello,%0AWorld" \
    -d "SELECT {x:nullable(string)}" 2>&1 | grep -oF '457' | head -n1;
