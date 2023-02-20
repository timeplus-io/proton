#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

exception_pattern='DB::Exception:'

function check()
{
  ${CLICKHOUSE_CLIENT} -q "$1" |& {
      if [[ `grep -F  $exception_pattern  | wc -l` -gt 0 ]]
      then
        echo 'Not supported'
      fi
  }
}

check "SELECT javaHash(to_uint8(1))"
check "SELECT javaHash(to_uint16(1))"
check "SELECT javaHash(to_uint32(1))"
check "SELECT javaHash(to_uint64(1))"
