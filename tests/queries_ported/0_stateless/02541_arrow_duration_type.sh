#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_arrow

USER_FILES_PATH=$(${CLICKHOUSE_CLIENT} --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 string')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

cp -f $DATA_DIR/duration.arrow $USER_FILES_PATH/

$CLICKHOUSE_CLIENT -q "desc file('duration.arrow')"
$CLICKHOUSE_CLIENT -q "select count() from file('duration.arrow')"
