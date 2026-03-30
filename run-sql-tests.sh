#!/usr/bin/env bash
# run-sql-tests.sh — Run stateless SQL tests against a local proton instance.
#
# Quick Start:
#   ./run-sql-tests.sh                           # All stateless tests (auto-starts server)
#   ./run-sql-tests.sh 00001_select_1            # Single test case by name
#   ./run-sql-tests.sh '0003_*'                  # Wildcard prefix (quote the glob)
#   ./run-sql-tests.sh --reuse-server 0003_*     # Skip server start/stop
#   ./run-sql-tests.sh --port 8463 0003_*        # Explicit port (implies --reuse-server)
#
# Options:
#   --port TCP         Connect to an already-running server on this TCP port
#                      (implies --reuse-server; skips server start/stop)
#   --reuse-server     Auto-detect a running server from tmp_data_* directories
#   -j N               Parallel test jobs (default: nproc/2, min 1)
#   -t N               Per-test timeout in seconds (default: 90)
#   -o DIR             Write xUnit report to DIR
#                      (default: tmp_data_<tcp>/logs/sql/ when server is owned,
#                               ./tmp/sql_reports/ when reusing an external server)
#   --stop-on-error    Stop on first failure
#
# Environment overrides:
#   CLICKHOUSE_BINARY  Binary name               (default: proton)
#   CLICKHOUSE_USER    Auth user                 (default: proton)
#   CLICKHOUSE_PASSWORD Auth password            (default: proton@t+)
#
# Exit code:
#   0   All tests passed
#   1   One or more failures, or setup failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"

CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-proton}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-proton}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-proton@t+}"

# ---------------------------------------------------------------------------
# Resolve server binary
# ---------------------------------------------------------------------------
if [[ -x "${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}"
elif [[ -x "${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}"
else
    echo "[sql] ERROR: ${CLICKHOUSE_BINARY} binary not found. Build first: cd build && ninja" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
OVERRIDE_PORT=""
REUSE_SERVER=0
JOBS=""
TIMEOUT=90
OUTPUT_DIR=""
STOP_ON_ERROR=0
TEST_PATTERN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)           OVERRIDE_PORT="$2"; REUSE_SERVER=1; shift 2 ;;
        --reuse-server)   REUSE_SERVER=1;                     shift   ;;
        -j|--jobs)        JOBS="$2";                          shift 2 ;;
        -t|--timeout)     TIMEOUT="$2";                       shift 2 ;;
        -o|--output)      OUTPUT_DIR="$2";                    shift 2 ;;
        --stop-on-error)  STOP_ON_ERROR=1;                    shift   ;;
        -h|--help)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        -*)
            echo "[sql] Unknown option: $1" >&2; exit 1 ;;
        *)
            TEST_PATTERN="$1"; shift ;;
    esac
done

# Default jobs: nproc/2 (min 1)
if [[ -z "${JOBS}" ]]; then
    NPROC="$(sysctl -n hw.logicalcpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
    JOBS=$(( NPROC / 2 > 1 ? NPROC / 2 : 1 ))
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { echo "[sql] $*"; }
die() { echo "[sql] ERROR: $*" >&2; exit 1; }

# Derive table_http port from tcp port (same step=10 scheme as start-local-proton.sh)
_companion_http() {
    local tcp="$1"
    echo $(( 8123 + (tcp - 8463) / 10 * 10 ))
}

# ---------------------------------------------------------------------------
# Server state (set by _start_server or _resolve_reuse_port)
# ---------------------------------------------------------------------------
SQL_SERVER_PID=""
SQL_RUNTIME_ROOT=""
SQL_TCP_PORT=""
SQL_TABLE_HTTP=""
SERVER_OWNED=0   # 1 = we started it, 0 = pre-existing (don't stop on exit)

# ---------------------------------------------------------------------------
# Server start / stop
# ---------------------------------------------------------------------------
_start_server() {
    log "Starting ${CLICKHOUSE_BINARY} …"

    # Tee start-local-proton.sh output to a temp file so we can both show
    # it in real time AND parse ports from it after the fact.
    local tmp_out
    tmp_out="$(mktemp "${REPO_ROOT}/tmp/sql_start.XXXXXX")"

    set +e
    "${REPO_ROOT}/start-local-proton.sh" 2>&1 | tee "${tmp_out}"
    local rc="${PIPESTATUS[0]}"
    set -e

    if [[ "${rc}" -ne 0 ]]; then
        rm -f "${tmp_out}"
        die "start-local-proton.sh exited with code ${rc}"
    fi

    SQL_TCP_PORT="$(grep -E '^\s*tcp:' "${tmp_out}"      | head -1 | grep -oE '[0-9]+')"
    SQL_TABLE_HTTP="$(grep -E '^\s*table_http:' "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    rm -f "${tmp_out}"

    [[ -n "${SQL_TCP_PORT}" ]]   || die "Could not parse tcp port from start-local-proton.sh output"
    [[ -n "${SQL_TABLE_HTTP}" ]] || SQL_TABLE_HTTP="$(_companion_http "${SQL_TCP_PORT}")"

    SQL_RUNTIME_ROOT="${REPO_ROOT}/tmp_data_${SQL_TCP_PORT}"
    local pid_file="${SQL_RUNTIME_ROOT}/${CLICKHOUSE_BINARY}.pid"
    SQL_SERVER_PID="$(cat "${pid_file}" 2>/dev/null || true)"

    if [[ -z "${SQL_SERVER_PID}" ]] || ! kill -0 "${SQL_SERVER_PID}" 2>/dev/null; then
        # Server started but PID is unreadable; clean up the data dir we would own
        rm -rf "${SQL_RUNTIME_ROOT}"
        die "Server started but PID not readable from ${pid_file}"
    fi

    SERVER_OWNED=1
    log "Server ready: tcp=${SQL_TCP_PORT}  table_http=${SQL_TABLE_HTTP}  pid=${SQL_SERVER_PID}"
}

_stop_server() {
    # Kill the process
    if [[ -n "${SQL_SERVER_PID}" ]]; then
        log "Stopping ${CLICKHOUSE_BINARY} (pid=${SQL_SERVER_PID}) …"
        kill -TERM "${SQL_SERVER_PID}" 2>/dev/null || true
        local i
        for (( i = 1; i <= 30; i++ )); do
            kill -0 "${SQL_SERVER_PID}" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "${SQL_SERVER_PID}" 2>/dev/null || true
        SQL_SERVER_PID=""
    fi
    # Remove the data dir (only if we created it)
    if [[ "${SERVER_OWNED}" -eq 1 && -n "${SQL_RUNTIME_ROOT}" && -d "${SQL_RUNTIME_ROOT}" ]]; then
        rm -rf "${SQL_RUNTIME_ROOT}"
        SQL_RUNTIME_ROOT=""
    fi
}

_resolve_reuse_port() {
    if [[ -n "${OVERRIDE_PORT}" ]]; then
        SQL_TCP_PORT="${OVERRIDE_PORT}"
    else
        local found=""
        for dir in "${REPO_ROOT}"/tmp_data_*/; do
            [[ -d "${dir}" ]] || continue
            local tcp="${dir%/}"; tcp="${tcp##*_}"
            local pid_file="${dir}/${CLICKHOUSE_BINARY}.pid"
            local pid; pid="$(cat "${pid_file}" 2>/dev/null || true)"
            if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
                found="${tcp}"; SQL_SERVER_PID="${pid}"
                break
            fi
        done
        [[ -n "${found}" ]] || \
            die "No live ${CLICKHOUSE_BINARY} found. Start one with ./start-local-proton.sh"
        SQL_TCP_PORT="${found}"
    fi

    SQL_TABLE_HTTP="$(_companion_http "${SQL_TCP_PORT}")"
    # SERVER_OWNED stays 0; we never stop or clean up a pre-existing server

    "${SERVER_BIN}" client --port "${SQL_TCP_PORT}" --query "SELECT 1" >/dev/null 2>&1 || \
        die "${CLICKHOUSE_BINARY} not responding on tcp=${SQL_TCP_PORT}"

    log "Reusing server: tcp=${SQL_TCP_PORT}  table_http=${SQL_TABLE_HTTP}"
}

# ---------------------------------------------------------------------------
# Cleanup trap — only removes what we own
# ---------------------------------------------------------------------------
_cleanup() {
    local rc=$?
    if [[ "${SERVER_OWNED}" -eq 1 ]]; then
        _stop_server
    fi
    exit "${rc}"
}
trap _cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Start or attach
# ---------------------------------------------------------------------------
mkdir -p "${REPO_ROOT}/tmp"

if [[ "${REUSE_SERVER}" -eq 1 ]]; then
    _resolve_reuse_port
else
    _start_server
fi

# Resolve output directory.
# When we own the server, co-locate reports with its runtime dir (tmp_data_<tcp>/logs/sql/).
# When reusing an external server (--port/--reuse-server), fall back to ./tmp/sql_reports/
# to avoid creating directories that don't belong to that server.
if [[ -z "${OUTPUT_DIR}" ]]; then
    if [[ "${SERVER_OWNED}" -eq 1 ]]; then
        OUTPUT_DIR="${REPO_ROOT}/tmp_data_${SQL_TCP_PORT}/logs/sql"
    else
        OUTPUT_DIR="${REPO_ROOT}/tmp/sql_reports"
    fi
fi
mkdir -p "${OUTPUT_DIR}"

# ---------------------------------------------------------------------------
# Build and run the test command
# ---------------------------------------------------------------------------
CMD=(
    python3 "${REPO_ROOT}/tests/ported-clickhouse-test.py"
    --binary  "${SERVER_BIN}"
    --queries "${REPO_ROOT}/tests/queries_ported"
    --jobs    "${JOBS}"
    --timeout "${TIMEOUT}"
    --no-stateful
)
[[ "${STOP_ON_ERROR}" -eq 1 ]] && CMD+=(--stop)
CMD+=(-o "${OUTPUT_DIR}")
[[ -n "${TEST_PATTERN}" ]] && CMD+=("${TEST_PATTERN}")

log "Binary:  ${SERVER_BIN}"
log "Port:    tcp=${SQL_TCP_PORT}  table_http=${SQL_TABLE_HTTP}"
log "Jobs:    ${JOBS}  timeout: ${TIMEOUT}s"
log "Reports: ${OUTPUT_DIR}"
[[ -n "${TEST_PATTERN}" ]] && log "Pattern: ${TEST_PATTERN}"
echo ""

CLICKHOUSE_PORT_TCP="${SQL_TCP_PORT}" \
CLICKHOUSE_PORT_HTTP="${SQL_TABLE_HTTP}" \
CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
    "${CMD[@]}"
