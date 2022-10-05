#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: Requires investigation

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&session_id=id_${CLICKHOUSE_DATABASE}"

echo "DROP STREAM IF EXISTS table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
echo "create stream table (a string) ENGINE Memory()" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-

# NOTE: suppose that curl sends everything in a single chunk - there are no options to force the chunk-size.
echo "SET max_query_size=44" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
echo -ne "INSERT INTO TABLE table FORMAT TabSeparated 1234567890 1234567890 1234567890 1234567890\n" | ${CLICKHOUSE_CURL} -H "Transfer-Encoding: chunked" -sS "${URL}" --data-binary @-

echo "SELECT * from table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
echo "DROP STREAM table" | ${CLICKHOUSE_CURL} -sSg "${URL}" -d @-
