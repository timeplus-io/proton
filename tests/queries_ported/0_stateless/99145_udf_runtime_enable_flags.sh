#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG_BACKUP="${CLICKHOUSE_CONFIG}.99145.bak"
PYTHON_UDF_AVAILABLE=1

reload_config() {
    if $CLICKHOUSE_CLIENT -q "SYSTEM RELOAD CONFIG" >/dev/null 2>&1; then
        return 0
    fi

    $CLICKHOUSE_CLIENT -q "SYSTEM RELOAD CONFIGS" >/dev/null 2>&1
}

restore_config() {
    if [[ -f "${CONFIG_BACKUP}" ]]; then
        cp "${CONFIG_BACKUP}" "${CLICKHOUSE_CONFIG}"
        reload_config
        rm -f "${CONFIG_BACKUP}"
    fi
}

cleanup() {
    restore_config
}

expect_error() {
    local output=$1
    local expected=$2

    if [[ "$output" != *"$expected"* ]]; then
        echo "$output"
        exit 1
    fi
}

set_udf_config() {
    local enabled=$1

    perl -0pi -e "s|<enable_javascript_udf>.*?</enable_javascript_udf>|<enable_javascript_udf>${enabled}</enable_javascript_udf>|s; s|<enable_python_udf>.*?</enable_python_udf>|<enable_python_udf>${enabled}</enable_python_udf>|s" "${CLICKHOUSE_CONFIG}"
    reload_config
}

if [[ ! -f "${CLICKHOUSE_CONFIG}" ]]; then
    echo "@@SKIP@@ writable CLICKHOUSE_CONFIG is unavailable"
    exit 0
fi

trap cleanup EXIT
cleanup
cp "${CLICKHOUSE_CONFIG}" "${CONFIG_BACKUP}"

set_udf_config false

js_create_output=$($CLICKHOUSE_CLIENT -q "CREATE FUNCTION js_disabled_create_99145(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$ function js_disabled_create_99145(value) { return value; } $$" 2>&1 || true)
expect_error "$js_create_output" 'JavaScript UDF creation is disabled by server config `enable_javascript_udf`'

js_aggr_create_output=$($CLICKHOUSE_CLIENT -q "CREATE AGGREGATE FUNCTION js_disabled_create_aggr_99145(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$ { process: function(values) {}, finalize: function() { return 0; } } $$" 2>&1 || true)
expect_error "$js_aggr_create_output" 'JavaScript UDF creation is disabled by server config `enable_javascript_udf`'

py_create_disabled_output=$($CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_disabled_create_99145(value float32) RETURNS float32 LANGUAGE PYTHON AS $$ def py_disabled_create_99145(values): return values $$" 2>&1 || true)
if [[ "$py_create_disabled_output" == *"Embedded Python interpreter is not initialized"* ]] || [[ "$py_create_disabled_output" == *"Syntax error"* ]]; then
    PYTHON_UDF_AVAILABLE=0
fi

if [[ "${PYTHON_UDF_AVAILABLE}" -eq 1 ]]; then
    expect_error "$py_create_disabled_output" 'Python UDF creation is disabled by server config `enable_python_udf`'

    py_aggr_create_disabled_output=$($CLICKHOUSE_CLIENT -q "CREATE AGGREGATE FUNCTION py_disabled_create_aggr_99145(value float32) RETURNS float32 LANGUAGE PYTHON AS $$ class py_disabled_create_aggr_99145: pass $$" 2>&1 || true)
    expect_error "$py_aggr_create_disabled_output" 'Python UDF creation is disabled by server config `enable_python_udf`'
fi
