#!/usr/bin/env bash
# set -x

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/config.xml"

echo "
<proton>
    <show_addresses_in_stack_traces>false</show_addresses_in_stack_traces>
    <profiles>
        <default></default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</proton>
" > "${CONFIG}"

# Use CLICKHOUSE_CLIENT instead of CLICKHOUSE_LOCAL
${CLICKHOUSE_CLIENT} --query "SELECT throw_if(1)" --stacktrace --config-file "${CONFIG}" 2>&1 | grep -c -F '@ 0x'

if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' 's/<show_addresses_in_stack_traces>false/<show_addresses_in_stack_traces>true/' "${CONFIG}"
else
    sed -i 's/<show_addresses_in_stack_traces>false/<show_addresses_in_stack_traces>true/' "${CONFIG}"
fi

${CLICKHOUSE_CLIENT} --query "SELECT throw_if(1)" --stacktrace --config-file "${CONFIG}" 2>&1 | grep -c -F '@ 0x' | grep -c -v '^0$'
rm "${CONFIG}"
