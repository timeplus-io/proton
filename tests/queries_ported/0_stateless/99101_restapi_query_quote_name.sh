#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REST_ENDPOINT="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v2/ddl/db%2799101/streams"

# Quote the heredoc tag to prevent command/param expansion inside
$CLICKHOUSE_CLIENT --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS `db'99101`;
SELECT sleep(1) FORMAT Null;
CREATE STREAM IF NOT EXISTS `db'99101`.source(id int);
SELECT sleep(1) FORMAT Null;
CREATE MATERIALIZED VIEW IF NOT EXISTS `db'99101`.`m'v` AS SELECT * FROM `db'99101`.source;
CREATE MATERIALIZED VIEW IF NOT EXISTS `db'99101`.`'`   AS SELECT * FROM `db'99101`.source;
SQL

sleep 2

# Keep the backslash at EOL; quote the jq program; -r for raw output
${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/m%27v" -H "Content-Type: application/json" \
  -u "proton:proton@t+" | jq -r '.data[0].target_stream'

${CLICKHOUSE_CURL} -sS -X GET "${REST_ENDPOINT}/%27" -H "Content-Type: application/json" \
  -u "proton:proton@t+" | jq -r '.data[0].target_stream'

# Tear down
$CLICKHOUSE_CLIENT --multiquery <<'SQL'
DROP VIEW IF EXISTS `db'99101`.`m'v`;
DROP VIEW IF EXISTS `db'99101`.`'`;
DROP STREAM IF EXISTS `db'99101`.source;
DROP DATABASE IF EXISTS `db'99101`;
SQL
