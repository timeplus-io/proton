#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS test_role_02306;"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS user_test_02306;"
${CLICKHOUSE_CLIENT} -q "CREATE USER user_test_02306 IDENTIFIED WITH plaintext_password BY 'user_test_02306';"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE test_role_02306;"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE,INSERT,SELECT,DROP ON *.* TO test_role_02306;"
${CLICKHOUSE_CLIENT} -q "GRANT test_role_02306 TO user_test_02306;"

echo "DROP STREAM IF EXISTS table_with_uint64" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}&user=user_test_02306&password=user_test_02306"
echo "CREATE STREAM table_with_uint64(no uint64) ENGINE = MergeTree ORDER BY no" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}&user=user_test_02306&password=user_test_02306"
echo -en '\xef\xbb\xbf\x00\xab\x3b\xec\x16' | ${CLICKHOUSE_CURL} --data-binary @- "${CLICKHOUSE_URL}&query=INSERT+INTO+table_with_uint64(no)+FORMAT+RowBinary&user=user_test_02306&password=user_test_02306"
echo "SELECT * FROM table_with_uint64" | ${CLICKHOUSE_CURL} -d@- -sS "${CLICKHOUSE_URL}&user=user_test_02306&password=user_test_02306"

${CLICKHOUSE_CLIENT} -q "DROP USER user_test_02306;"
${CLICKHOUSE_CLIENT} -q "DROP ROLE test_role_02306;"
