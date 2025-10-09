#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


stream_name="test_990012_shard_expr"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS default.${stream_name}"


${CLICKHOUSE_CURL} -sS -X "POST" "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/ddl/streams" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d "{
    \"columns\": [
        {\"name\": \"id\", \"type\": \"int32\"}, 
        {\"name\": \"val\", \"type\": \"int32\"}, 
        {\"name\": \"nowhere\", \"type\": \"int32\"}
    ],
    \"name\": \"${stream_name}\",
    \"primary_key\": \"(id,val)\"
}" &> /dev/null


weak_shard_expr=$($CLICKHOUSE_CLIENT -q "SHOW CREATE default.${stream_name} FORMAT CSV " | grep -o "ENGINE = .*")

if [ "$weak_shard_expr" = "ENGINE = Stream(1, weak_hash32((id, val)))" ] ; then
  echo "True"
else
  echo "Default shard expr for primary key based stream is weak_hash, but get unexpected result"
  exit 1
fi


stream_name="test_990012_shard_expr_no_primary_key"

$CLICKHOUSE_CLIENT -q "DROP STREAM IF EXISTS default.${stream_name}"

${CLICKHOUSE_CURL} -sS -X "POST" "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/ddl/streams" \
     -H 'Content-Type: application/json; charset=utf-8' \
     -d "{
    \"columns\": [
        {\"name\": \"id\", \"type\": \"int32\"}, 
        {\"name\": \"val\", \"type\": \"int32\"}, 
        {\"name\": \"nowhere\", \"type\": \"int32\"}
    ],
    \"name\": \"${stream_name}\"
}" &> /dev/null


rand_shard_expr=$($CLICKHOUSE_CLIENT -q "SHOW CREATE default.${stream_name} FORMAT CSV " | grep -o "ENGINE = .*")

if [ "$rand_shard_expr" = "ENGINE = Stream(1, rand())" ] ; then
  echo "True"
else
  echo "Default shard expr for non-primary key based stream is rand(), but get unexpected result"
  exit 1
fi
