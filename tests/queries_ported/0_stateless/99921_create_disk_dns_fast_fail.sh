#!/usr/bin/env bash
# Tags: long, no-fasttest
#
# Regression for https://github.com/timeplus-io/proton-enterprise/issues/10048
#
# DNSResolver previously threw plain DB::Exception(DNS_ERROR), which was lost
# at PocoHTTPClient's catch (NetException) and fell through to catch (...)
# where it was misclassified as NETWORK_CONNECTION and entered the AWS SDK
# retry loop with exponential backoff (~25s wall time).
#
# Aligning DNSResolver with upstream (DB::NetException(DNS_ERROR)) lets the
# existing catch (const NetException &) branch in PocoHTTPClient classify it
# as ENDPOINT_RESOLUTION_FAILURE, which is non-retryable, so the failure
# surfaces immediately.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_NAME="d_10048_${CLICKHOUSE_DATABASE}_$$"

start=$(date +%s)
output=$($CLICKHOUSE_CLIENT --query "
    CREATE DISK ${DISK_NAME} disk(
        type='s3',
        endpoint='http://nonexistent-host-xyz.invalid:9001/bucket/path/',
        access_key_id='k',
        secret_access_key='s')
" 2>&1 || true)
elapsed=$(( $(date +%s) - start ))

$CLICKHOUSE_CLIENT --query "DROP DISK IF EXISTS ${DISK_NAME}" >/dev/null 2>&1 || true

if ! echo "$output" | grep -qE "CANNOT_CREATE_DISK|DNS_ERROR|Not found address"; then
    echo "FAIL: expected DNS-related error, got: ${output}"
    exit 1
fi

# Pre-fix: ~25s wall time. Post-fix: <1s. Threshold leaves CI headroom.
if [ "${elapsed}" -gt 5 ]; then
    echo "FAIL: CREATE DISK against unresolvable host took ${elapsed}s (>5s, regression of #10048)"
    exit 1
fi

echo OK
