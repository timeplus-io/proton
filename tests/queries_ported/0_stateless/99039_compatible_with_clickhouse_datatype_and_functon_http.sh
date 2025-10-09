#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# type conversion functions
echo "SELECT toTime(toDateTime64('1970-12-10 01:20:30.3000',3));" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toInt('3');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toFloat('3.1415926');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toDecimal('3.1415926',2);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toString(3);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toBool(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT CAST('1', 'integer');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT toTypeName(123);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-

# process text functions
echo "SELECT lower('Hello');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT upper('Hello');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT format('{}', 'Hello');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT concat('a', 'b') == 'ab';" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT substr('HelloHello', 2, -2);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT substring('HelloHello', 2, -2);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT startsWith('Hello', 'He');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT endsWith('Hello', 'lo');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT trim('  Hello ');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT match('Hello', '');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT multiSearchAny(arrayJoin(['hello', 'world', 'hello, world', 'abc']), ['hello', 'world']);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT replaceOne('abca','a','z');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT replace('abca','a','z');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-

# aggregation functions
echo "SELECT count(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT countDistinct(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT countIf(number > 5) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT unique(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT number % 2 AS even, uniqueExact(even, number) FROM numbers(10) GROUP BY even;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT uniqueExactIf(number, number > 2) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT min(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT max(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT sum(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT avg(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT median(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT quantile(0.9)(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT p90(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT p95(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT p99(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT topK(number,2) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT minK(number,2) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT maxK(number,2) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupArray(5)(number+1) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupArrayLast(5)(number+1) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupArraySorted(5)(number+1) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupArraySample(5)(number+1) FROM numbers(10) format Null;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupUniqArray(5)(number+1) FROM numbers(10)" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT groupConcat(',')(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT any(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT first_value(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT last_value(number) FROM numbers(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-

# login functions
echo "SELECT if(1=2, 'a', 'b');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT multiIf(1=2, 'a', 1=3, 'b', 'c');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT sleep(1) format Null;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-

# math functions
echo "SELECT abs(-1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT round(1.5);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT e();" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT pi();" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT exp(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT exp2(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT exp10(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT log(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT log2(2);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT log10(10);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT sqrt(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT cbrt(1);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT lgamma(5);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT tgamma(5);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT sin(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT cos(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT tan(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT asin(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT acos(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT atan(0);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT pow(1,2.5);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-
echo "SELECT power(1,2.5);" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" --data-binary @-

# ddl
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS test_role_99039;"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS user_test_99039;"
${CLICKHOUSE_CLIENT} -q "CREATE USER user_test_99039 IDENTIFIED WITH plaintext_password BY 'user_test_99039';"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE test_role_99039;"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE,INSERT,SELECT,DROP ON *.* TO test_role_99039;"
${CLICKHOUSE_CLIENT} -q "GRANT test_role_99039 TO user_test_99039;"

echo "DROP STREAM IF EXISTS test_99039" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-
echo "CREATE STREAM test_99039(id UInt32, profile String) ENGINE = MergeTree() ORDER BY id;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-
echo "INSERT INTO test_99039  (id, profile) VALUES (1, 'Admin'), (2, 'Guest'), (3, 'Member');" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-
echo "SELECT * FROM test_99039  order by id FORMAT TabSeparatedWithNamesAndTypes;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-
echo "SHOW STREAMS WHERE name ='test_99039';" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-
echo "SHOW create test_99039;" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=user_test_99039&password=user_test_99039" --data-binary @-

${CLICKHOUSE_CLIENT} -q "DROP USER user_test_99039;"
${CLICKHOUSE_CLIENT} -q "DROP ROLE test_role_99039;"
