#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT xyzgarbage 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -xyzgarbage 2>&1 | grep -q "UNRECOGNIZED_ARGUMENTS" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --xyzgarbage 2>&1 | grep -q "UNRECOGNIZED_ARGUMENTS" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login string, unused string, uid uint16, gid uint16, comment string, home string, shell string' xyzgarbage 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external -xyzgarbage --file=- --name=passwd --structure='login string, unused string, uid uint16, gid uint16, comment string, home string, shell string' 2>&1 | grep -q "UNRECOGNIZED_ARGUMENTS" && echo 'OK' || echo 'FAIL'

cat /etc/passwd | sed 's/:/\t/g' | $CLICKHOUSE_CLIENT --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --xyzgarbage --file=- --name=passwd --structure='login string, unused string, uid uint16, gid uint16, comment string, home string, shell string' 2>&1 | grep -q "UNRECOGNIZED_ARGUMENTS" && echo 'OK' || echo 'FAIL'
