#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FILE=${CURDIR}/../file_02112
echo "drop stream if exists t;create stream t(i int32) engine=Memory; insert into t(i) select 1" > "$FILE"
