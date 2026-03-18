#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RESPONSE=$(${CLICKHOUSE_CURL} -sS -X POST "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/timeplusd/v1/sqlanalyzer" \
    -H "Content-Type: application/json" \
    -d '{"query":"select abc"}' \
    -w '\n%{http_code}')

BODY=$(printf '%s\n' "$RESPONSE" | sed '$d')
HTTP_CODE=$(printf '%s\n' "$RESPONSE" | tail -n 1)

echo "$HTTP_CODE"
printf '%s\n' "$BODY" | jq -r '.code'
