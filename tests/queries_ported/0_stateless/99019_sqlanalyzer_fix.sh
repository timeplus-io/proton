#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q  '
CREATE OR REPLACE FUNCTION determineSymbolStatus(platformParas string, symbol string) RETURNS string LANGUAGE JAVASCRIPT AS $$ 
function  determineSymbolStatus(platformParas, symbols)
{
    let i = 0;
	return platformParas.map( platform=>{
	    const paras = JSON.parse(platformParas);
        const symbol = symbols[i];
        i++;
        if (paras[symbol] === true) {
            return 'RUNNING';
        } else if (paras[symbol] === false) {
            return 'SUSPEND';
        } else {
            return 'STOP';
        }
	  }
	)
}
$$;'

# sleep 1seconds to waiting mutation work
sleep 1

$CLICKHOUSE_CLIENT -q '
CREATE OR REPLACE FORMAT SCHEMA schema_test AS
$$

              syntax = "proto3";

              message SearchRequest {
                string query = 1;
                int32 page_number = 2;
                int32 results_per_page = 3;
              }
              
$$
TYPE Protobuf'

# sleep 1seconds to waiting mutation work
sleep 1

## analyzer
${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW FORMAT SCHEMAS"}' | jq .

${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW FUNCTIONS"}' | jq .

${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW CREATE FORMAT SCHEMA schema_test"}' | jq .

${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW CREATE FUNCTION determineSymbolStatus"}' | jq .

### alerts
${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "CREATE ALERT test CALL some_udf AS SELECT * FROM some_stream"}' | jq .
${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW ALERTS"}' | jq .
${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "SHOW CREATE ALERT test"}' | jq .
${CLICKHOUSE_CURL} -sS -X POST http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v1/sqlanalyzer -H "Content-Type: text/plain; charset=utf-8" -d $'{ "query": "DROP ALERT test"}' | jq .

$CLICKHOUSE_CLIENT -q "drop function if exists determineSymbolStatus;"
