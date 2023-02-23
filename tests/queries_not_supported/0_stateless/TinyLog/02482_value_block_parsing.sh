#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="
  CREATE STREAM simple_key_dictionary_source_table__fuzz_48
  (
      id nullable(int8),
      value array(Date),
      value_nullable uuid
  )
  ENGINE = TinyLog;"

echo "INSERT INTO simple_key_dictionary_source_table__fuzz_48 FORMAT Values (null, [], '61f0c404-5cb3-11e7-907b-a6006ad3dba0')
( -- Bu        " | ${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" --data-binary @- -v 2>&1 | grep -c 'X-ClickHouse-Exception-Code: 62'


echo "INSERT INTO simple_key_dictionary_source_table__fuzz_48 FORMAT Values
                  (!Invalid" | ${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" --data-binary @- -v 2>&1 | grep -c 'X-ClickHouse-Exception-Code: 62'

echo "INSERT INTO simple_key_dictionary_source_table__fuzz_48 FORMAT Values    (null, [], '61f0c404-5cb3-11e7-907b-a6006ad3dba0')
          ,(null, [], '61f0c404-5cb3-11e7-907b-a6006ad3dba0'),
          (!!!!!!3adas
      )" | ${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}" --data-binary @- -v 2>&1  | grep -c 'X-ClickHouse-Exception-Code: 62'
