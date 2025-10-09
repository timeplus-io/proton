#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Test case for Memory DB and MySQL DB integration

# Part 1: Memory Engine Test
echo "==== Testing Memory Engine ===="
# Create a database and a stream in the Memory engine
$CLICKHOUSE_CLIENT -q "CREATE DATABASE 99056Mem ENGINE = Memory();"
sleep 1
$CLICKHOUSE_CLIENT -q "CREATE STREAM 99056Mem.u (id INT) ENGINE = Memory();"
sleep 1

# Query the stream from the REST API
response=$(${CLICKHOUSE_CURL} -sS -X "GET" "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v2/ddl/99056Mem/streams/u" \
    -H "Content-Type: application/json" \
    -u "proton:proton@t+")

# echo "Memory DB Response: $response"

# Extract and validate database and stream name
database=$(echo $response | jq -r '.data[0].database')
stream_name=$(echo $response | jq -r '.data[0].name')
column_name=$(echo $response | jq -r '.data[0].columns[0].name')
engine=$(echo $response | jq -r '.data[0].engine')

echo "Extracted values:"
echo "  - Database: $database"
echo "  - Stream Name: $stream_name"
echo "  - Column Name: $column_name"
echo "  - Engine: $engine"

# Validation
if [ "$database" != "99056Mem" ]; then
    echo "Error: Expected database '99056Mem' but got '$database'"
    exit 1
fi

if [ "$stream_name" != "u" ]; then
    echo "Error: Expected stream name 'u' but got '$stream_name'"
    exit 1
fi

if [ "$column_name" != "id" ]; then
    echo "Error: Expected column name 'id' but got '$column_name'"
    exit 1
fi

if [ "$engine" != "Memory" ]; then
    echo "Error: Expected engine 'Memory' but got '$engine'"
    exit 1
fi

echo "Memory DB test passed successfully!"

# Part 2: MySQL DB Test
echo -e "\n==== Testing MySQL Integration ===="
# Create a database connection to MySQL
$CLICKHOUSE_CLIENT -q "CREATE DATABASE mysql_tpch SETTINGS type = 'mysql', \
    address = 'mysql-timeplus.g.aivencloud.com:28851', \
    user = 'avnadmin', \
    password = 'AVNS_1Bp64RDeSwm_mo3-BmN', \
    database = 'tpch';"
sleep 1

# Query the region table from the REST API
mysql_response=$(${CLICKHOUSE_CURL} -sS -X "GET" "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/proton/v2/ddl/mysql_tpch/streams/region" \
    -H "Content-Type: application/json" \
    -u "proton:proton@t+")

# echo "MySQL DB Response: $mysql_response"

# Extract and validate MySQL database and table information
mysql_database=$(echo $mysql_response | jq -r '.data[0].database')
mysql_table=$(echo $mysql_response | jq -r '.data[0].name')
mysql_engine=$(echo $mysql_response | jq -r '.data[0].engine')
column_count=$(echo $mysql_response | jq -r '.data[0].columns | length')

echo "Extracted MySQL values:"
echo "  - Database: $mysql_database"
echo "  - Table: $mysql_table"
echo "  - Engine: $mysql_engine"
echo "  - Column Count: $column_count"

# Expected columns in the region table
expected_columns=("r_regionkey" "r_name" "r_comment")

# Validate column names
for i in "${!expected_columns[@]}"; do
    col_name=$(echo $mysql_response | jq -r ".data[0].columns[$i].name")
    if [ "$col_name" != "${expected_columns[$i]}" ]; then
        echo "Error: Expected column '${expected_columns[$i]}' but got '$col_name'"
        exit 1
    fi
done

# Validate database and table name
if [ "$mysql_database" != "mysql_tpch" ]; then
    echo "Error: Expected database 'mysql_tpch' but got '$mysql_database'"
    exit 1
fi

if [ "$mysql_table" != "region" ]; then
    echo "Error: Expected table 'region' but got '$mysql_table'"
    exit 1
fi

if [ "$mysql_engine" != "MySQL" ]; then
    echo "Error: Expected engine 'MySQL' but got '$mysql_engine'"
    exit 1
fi

echo "MySQL DB test passed successfully!"
echo -e "\nAll tests completed successfully!"
