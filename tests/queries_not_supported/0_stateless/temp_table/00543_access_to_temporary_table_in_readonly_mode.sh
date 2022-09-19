#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="
    DROP STREAM IF EXISTS test_readonly;
    create stream test_readonly (
        ID int
    ) Engine=Memory;
";

################
# readonly = 1 #
################

# Try to create temporary table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 1;
    CREATE TEMPORARY STREAM readonly (
        ID int
    ) Engine=Memory;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "164" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to insert into exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 1;
    INSERT INTO test_readonly (ID) VALUES (1);
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "164" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to drop exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 1;
    DROP STREAM test_readonly;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "164" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

################
# readonly = 2 #
################

# Try to create temporary table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 2;
    CREATE TEMPORARY STREAM readonly (
        ID int
    ) Engine=Memory;
    INSERT INTO readonly (ID) VALUES (1);
    DROP TEMPORARY TABLE readonly;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to insert into exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 2;
    INSERT INTO test_readonly (ID) VALUES (1);
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "164" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to drop exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 2;
    DROP STREAM test_readonly;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "164" ] && [ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

################
# readonly = 0 #
################

# Try to create temporary table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 0;
    CREATE TEMPORARY STREAM readonly (
        ID int
    ) Engine=Memory;
    INSERT INTO readonly (ID) VALUES (1);
    DROP TEMPORARY TABLE readonly;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to insert into exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 0;
    INSERT INTO test_readonly (ID) VALUES (1);
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

# Try to drop exists (non temporary) table
$CLICKHOUSE_CLIENT -n --query="
    SET readonly = 0;
    DROP STREAM test_readonly;
" 2> /dev/null;
CODE=$?;
[ "$CODE" -ne "0" ] && echo "Fail" && exit $CODE;

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS test_readonly;";

