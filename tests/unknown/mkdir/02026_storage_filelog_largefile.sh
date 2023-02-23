#!/usr/bin/env bash
# Tags: long, no-parallel

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.
# Now we can get the user_files_path by use the stream file function for trick. also we can get it by query as:
#  "insert into function file('exist.txt', 'CSV', 'val1 char') values ('aaaa'); select _path from file('exist.txt', 'CSV', 'val1 char')"
user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*

chmod 777 ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

for i in {1..10}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k uint32, v uint32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "drop stream if exists file_log;"
${CLICKHOUSE_CLIENT} --query "create stream file_log(k uint32, v uint32) engine=FileLog('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/', 'CSV');"

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

for i in {11..20}
do
	${CLICKHOUSE_CLIENT} --query "insert into function file('${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/test$i.csv', 'CSV', 'k uint32, v uint32') select number, number from numbers(10000);"
done

${CLICKHOUSE_CLIENT} --query "select count() from file_log settings stream_like_engine_allow_direct_select=1;"

${CLICKHOUSE_CLIENT} --query "drop stream file_log;"

rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}
