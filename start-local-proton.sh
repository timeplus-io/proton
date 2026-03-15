#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"

EMBEDDED_TEMPLATE="${REPO_ROOT}/programs/server/embedded.xml"

# ---------------------------------------------------------------------------
# Project identity — override via environment to adapt for other binaries.
#   CLICKHOUSE_BINARY    Binary name, e.g. proton or timeplusd  (default: proton)
#   ENV_PREFIX           Env var prefix for port overrides       (default: uppercase CLICKHOUSE_BINARY)
#   WATCHDOG_ENV_VAR     Watchdog disable env var name           (default: ${ENV_PREFIX}_WATCHDOG_ENABLE)
#   EMBEDDED_DATA_DIR    Default data dir string in embedded.xml (default: ./${CLICKHOUSE_BINARY}-data)
# ---------------------------------------------------------------------------
CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-proton}"
ENV_PREFIX="${ENV_PREFIX:-$(echo "${CLICKHOUSE_BINARY}" | tr '[:lower:]' '[:upper:]')}"
WATCHDOG_ENV_VAR="${WATCHDOG_ENV_VAR:-${ENV_PREFIX}_WATCHDOG_ENABLE}"
EMBEDDED_DATA_DIR="${EMBEDDED_DATA_DIR:-./${CLICKHOUSE_BINARY}-data}"
SERVER_LOG_DIR="${SERVER_LOG_DIR:-${CLICKHOUSE_BINARY}-server}"

# Resolve default ports from env vars using indirect reference (bash 3+)
_v="${ENV_PREFIX}_NODE_TCP_PORT";    DEFAULT_NODE_TCP_PORT="${!_v:-8463}"
_v="${ENV_PREFIX}_NODE_HTTP_PORT";   DEFAULT_NODE_HTTP_PORT="${!_v:-3218}"
_v="${ENV_PREFIX}_TABLE_TCP_PORT";   DEFAULT_TABLE_TCP_PORT="${!_v:-7587}"
_v="${ENV_PREFIX}_TABLE_HTTP_PORT";  DEFAULT_TABLE_HTTP_PORT="${!_v:-8123}"
_v="${ENV_PREFIX}_POSTGRESQL_PORT";  DEFAULT_POSTGRESQL_PORT="${!_v:-5432}"
_v="${ENV_PREFIX}_PROMETHEUS_PORT";  DEFAULT_PROMETHEUS_PORT="${!_v:-9363}"
_v="${ENV_PREFIX}_MAX_PORT_SEARCH";  MAX_PORT_SEARCH="${!_v:-200}"
unset _v

is_process_alive() {
    local pid_file="$1"
    [[ -f "$pid_file" ]] || return 1
    local pid=$(cat "$pid_file" 2>/dev/null || true)
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && ps -p "$pid" -o args= 2>/dev/null | grep -q "${CLICKHOUSE_BINARY} server"
}

if [[ -x "${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}"
elif [[ -x "${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}"
else
    echo "Cannot find ${CLICKHOUSE_BINARY} binary under build/programs." >&2
    exit 1
fi

if [[ ! -f "${EMBEDDED_TEMPLATE}" ]]; then
    echo "Template config not found: ${EMBEDDED_TEMPLATE}" >&2
    exit 1
fi

is_port_busy() {
    local port="$1"
    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    elif command -v ss >/dev/null 2>&1; then
        ss -ltn "( sport = :${port} )" | grep -q "${port}"
    else
        netstat -an 2>/dev/null | grep -E "LISTEN|LISTENING" | grep -q "[\.:]${port}[[:space:]]"
    fi
}

choose_ports() {
    local i port ports_free

    # Step by 10 to give each instance a non-overlapping block of ports.
    local step=10
    for (( i = 0; i <= MAX_PORT_SEARCH; i++ )); do
        NODE_TCP_PORT=$((DEFAULT_NODE_TCP_PORT + i * step))
        NODE_HTTP_PORT=$((DEFAULT_NODE_HTTP_PORT + i * step))
        TABLE_TCP_PORT=$((DEFAULT_TABLE_TCP_PORT + i * step))
        TABLE_HTTP_PORT=$((DEFAULT_TABLE_HTTP_PORT + i * step))
        POSTGRESQL_PORT=$((DEFAULT_POSTGRESQL_PORT + i * step))
        PROMETHEUS_PORT=$((DEFAULT_PROMETHEUS_PORT + i * step))

        # Skip port sets whose runtime dir already exists in this repo (fast path).
        [[ -e "${REPO_ROOT}/tmp_data_${NODE_TCP_PORT}" ]] && continue

        # Check if all ports are free
        ports_free=true
        for port in "$NODE_TCP_PORT" "$NODE_HTTP_PORT" "$TABLE_TCP_PORT" "$TABLE_HTTP_PORT" \
                    "$POSTGRESQL_PORT" "$PROMETHEUS_PORT"; do
            if is_port_busy "$port"; then
                ports_free=false
                break
            fi
        done

        $ports_free && return 0
    done

    echo "No available port set found after ${MAX_PORT_SEARCH} increments." >&2
    return 1
}

wait_ready() {
    local retries=120
    local interval=1

    echo "  Waiting for ${CLICKHOUSE_BINARY} to be ready on tcp=${NODE_TCP_PORT} (${RUNTIME_ROOT}) …" >&2
    for (( i = 1; i <= retries; i++ )); do
        if "${SERVER_BIN}" client --port "${NODE_TCP_PORT}" --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        echo "  ... ${i}/${retries} (${RUNTIME_ROOT})" >&2
        sleep "${interval}"
    done

    return 1
}

is_bind_error() {
    grep -qiE "Address already in use|Cannot bind|EADDRINUSE" "${1}" 2>/dev/null
}

# Main loop: port selection + atomic claim + server start.
# Fallback: if the server loses a race with another process (from a different
# repo root) and fails to bind, is_bind_error detects it from the startup log
# and the loop retries with the next available port.
max_retries=20
server_pid=""
for (( retry = 0; retry < max_retries; retry++ )); do
    choose_ports || break

    RUNTIME_ROOT="${REPO_ROOT}/tmp_data_${NODE_TCP_PORT}"
    DATA_DIR="${RUNTIME_ROOT}/data"
    PID_FILE="${RUNTIME_ROOT}/${CLICKHOUSE_BINARY}.pid"
    CONFIG_FILE="${RUNTIME_ROOT}/config.xml"
    STARTUP_LOG="${RUNTIME_ROOT}/startup.log"

    # Atomic same-repo claim: mkdir without -p fails if dir already exists.
    if ! mkdir "${RUNTIME_ROOT}" 2>/dev/null; then
        # No PID file yet → another process is still setting up this port set.
        if [[ ! -f "${PID_FILE}" ]]; then
            continue
        fi
        # PID file exists but process is dead → cleanup and reclaim.
        if is_process_alive "${PID_FILE}"; then
            continue  # Process alive, try next port
        fi
        CLEANUP_DIR="${RUNTIME_ROOT}.cleanup_$$_${RANDOM}"
        if mv "${RUNTIME_ROOT}" "${CLEANUP_DIR}" 2>/dev/null; then
            rm -rf "${CLEANUP_DIR}" &  # Async cleanup
            mkdir "${RUNTIME_ROOT}" 2>/dev/null || continue
        else
            continue  # Another process claimed it
        fi
    fi

    mkdir -p "${DATA_DIR}" "${DATA_DIR}/store" "${DATA_DIR}/var/log/${SERVER_LOG_DIR}"
    echo "  Claimed port set: tcp=${NODE_TCP_PORT} http=${NODE_HTTP_PORT} (${RUNTIME_ROOT})" >&2

    # Configure
    cp "${EMBEDDED_TEMPLATE}" "${CONFIG_FILE}"
    DATA_DIR="${DATA_DIR}" REPO_ROOT="${REPO_ROOT}" \
    EMBEDDED_DATA_DIR="${EMBEDDED_DATA_DIR}" \
    NODE_HTTP_PORT="${NODE_HTTP_PORT}" NODE_TCP_PORT="${NODE_TCP_PORT}" \
    TABLE_TCP_PORT="${TABLE_TCP_PORT}" TABLE_HTTP_PORT="${TABLE_HTTP_PORT}" \
    POSTGRESQL_PORT="${POSTGRESQL_PORT}" PROMETHEUS_PORT="${PROMETHEUS_PORT}" \
    perl -0777 -i -pe '
        s|\Q$ENV{EMBEDDED_DATA_DIR}\E|$ENV{DATA_DIR}|g;
        s|grok-patterns|$ENV{REPO_ROOT}/programs/server/grok-patterns|g;
        s|(<node>.*?<http>\s*<port>)\d+(</port>)|${1}$ENV{NODE_HTTP_PORT}${2}|s;
        s|(<node>.*?<tcp>\s*<port>)\d+(</port>)|${1}$ENV{NODE_TCP_PORT}${2}|s;
        s|(<node>.*?<table_tcp>\s*<port>)\d+(</port>)|${1}$ENV{TABLE_TCP_PORT}${2}|s;
        s|(<node>.*?<table_http>\s*<port>)\d+(</port>)|${1}$ENV{TABLE_HTTP_PORT}${2}|s;
        s|(<node>.*?<postgresql>\s*<port>)\d+(</port>)|${1}$ENV{POSTGRESQL_PORT}${2}|s;
        s|(<prometheus>.*?<port>)\d+(</port>)|${1}$ENV{PROMETHEUS_PORT}${2}|s;
    ' "${CONFIG_FILE}"

    # Launch
    echo "  Starting ${CLICKHOUSE_BINARY} daemon (${RUNTIME_ROOT}) …" >&2
    env "${WATCHDOG_ENV_VAR}=0" "${SERVER_BIN}" server \
        --daemon --pid-file="${PID_FILE}" --config-file="${CONFIG_FILE}" \
        >> "${STARTUP_LOG}" 2>&1

    # Wait for PID file with running process
    server_pid=""
    for (( i = 1; i <= 300; i++ )); do
        [[ -s "${PID_FILE}" ]] || { sleep 0.1; continue; }
        server_pid=$(cat "${PID_FILE}" 2>/dev/null || true)
        [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null && break
        sleep 0.1
    done

    # Server exited before writing PID — check if it lost the port race
    if [[ -z "${server_pid}" ]] || ! kill -0 "${server_pid}" 2>/dev/null; then
        if is_bind_error "${STARTUP_LOG}"; then
            echo "  Port ${NODE_TCP_PORT} bind conflict detected, retrying next slot …" >&2
            rm -rf "${RUNTIME_ROOT}"
            server_pid=""
            continue
        fi
        echo "Failed to obtain server pid from ${PID_FILE}" >&2
        echo "startup log: ${STARTUP_LOG}" >&2
        tail -n 50 "${STARTUP_LOG}" >&2 2>/dev/null
        exit 1
    fi

    if ! wait_ready; then
        if is_bind_error "${STARTUP_LOG}"; then
            echo "  Port ${NODE_TCP_PORT} bind conflict detected (late), retrying next slot …" >&2
            kill "${server_pid}" 2>/dev/null || true
            rm -rf "${RUNTIME_ROOT}"
            server_pid=""
            continue
        fi
        cat >&2 <<-EOF
	${CLICKHOUSE_BINARY} failed to become ready on tcp_port=${NODE_TCP_PORT}
	  pid file: ${PID_FILE}
	  pid: ${server_pid}
	EOF
        kill -0 "${server_pid}" 2>/dev/null || echo "  process ${server_pid} is not running" >&2
        echo "  startup log: ${STARTUP_LOG}" >&2
        echo "  error log: ${DATA_DIR}/var/log/${SERVER_LOG_DIR}/${CLICKHOUSE_BINARY}-server.err.log" >&2
        tail -n 50 "${STARTUP_LOG}" >&2 2>/dev/null
        tail -n 50 "${DATA_DIR}/var/log/${SERVER_LOG_DIR}/${CLICKHOUSE_BINARY}-server.err.log" >&2 2>/dev/null
        kill "${server_pid}" 2>/dev/null || true
        exit 1
    fi

    break  # Successfully started
done

if [[ -z "${server_pid}" ]]; then
    echo "Failed to start ${CLICKHOUSE_BINARY} after ${max_retries} attempts." >&2
    exit 1
fi

cat <<-EOF
	${CLICKHOUSE_BINARY} started
	  binary: ${SERVER_BIN}
	  pid:    $(cat "${PID_FILE}" 2>/dev/null || echo unknown)
	  tcp:    ${NODE_TCP_PORT}
	  node_http: ${NODE_HTTP_PORT}
	  table_http: ${TABLE_HTTP_PORT}
	  table_tcp: ${TABLE_TCP_PORT}
	  postgresql: ${POSTGRESQL_PORT}
	  data:   ${DATA_DIR}
	  log:    ${DATA_DIR}/var/log/${SERVER_LOG_DIR}/${CLICKHOUSE_BINARY}-server.log
	  err:    ${DATA_DIR}/var/log/${SERVER_LOG_DIR}/${CLICKHOUSE_BINARY}-server.err.log
	  startup: ${STARTUP_LOG}
	  config: ${CONFIG_FILE}
	client command:
	  ${SERVER_BIN} client --port ${NODE_TCP_PORT}
	EOF
