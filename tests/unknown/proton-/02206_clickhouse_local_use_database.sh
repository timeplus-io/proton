#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL --echo --multiline --multiquery -q """
SHOW STREAMS;
CREATE DATABASE test1;
CREATE STREAM test1.table1 (a int32) ENGINE=Memory;
USE test1;
SHOW STREAMS;
CREATE DATABASE test2;
USE test2;
SHOW STREAMS;
USE test1;
SHOW STREAMS;
"""
