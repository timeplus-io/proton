#!/usr/bin/env bash
# run-smoke-tests.sh — Run probe-based smoke tests against a local proton instance.
#
# Quick Start:
#   ./run-smoke-tests.sh                         # Run all smoke cases (blacklist: bug,todo,skip)
#   ./run-smoke-tests.sh -s tumble_window        # Run a single suite
#   ./run-smoke-tests.sh -k 00_tumble_window_max # Run a single case
#   ./run-smoke-tests.sh --collect-only          # List matching cases without running
#   ./run-smoke-tests.sh --reuse-server          # Skip start/stop; connect to an already-running instance
#
# Options:
#   -s SUITE       Limit to a named suite (repeatable)
#   -k CASE        Limit to a named case  (repeatable)
#   -b BLACKLIST   Comma-separated tag blacklist   (default: bug,todo,skip)
#   --nodes N      Set probe variable nodes=N       (default: 1)
#   --timeout N    Per-case timeout in seconds      (default: 120)
#   --collect-only List matching cases and exit
#   --reuse-server Use an already-running server instead of starting one
#                  (reads port from the first tmp_data_* directory found)
#   --port TCP     Override the TCP port to connect to (implies --reuse-server)
#
# Environment overrides:
#   CLICKHOUSE_BINARY   Binary name (default: proton)
#   PROBE_BIN           Path to probe binary (default: auto-detected)
#   SMOKE_LOG_DIR       Directory for log output
#                       (default: tmp_data_<tcp>/logs/smoke/ when server is owned,
#                                ./tmp/smoke_logs/ when reusing an external server)
#
# Logs: tmp_data_<tcp>/logs/smoke/probe_smoke_results.log
#
# Exit code:
#   0   All cases passed (or --collect-only succeeded)
#   1   One or more cases failed, or setup failed
#
# Probe binary resolution (in order):
#   1. $PROBE_BIN env var
#   2. probe in PATH
#   3. Extract from timeplus/probe:latest Docker image (requires docker)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-proton}"
SMOKE_CASE_DIR="${REPO_ROOT}/tests/cluster/smoke"
# SMOKE_LOG_DIR is resolved after server start (co-located with tmp_data_<tcp>/).
# Override with SMOKE_LOG_DIR env var if you need a fixed location.
SMOKE_LOG_DIR_OVERRIDE="${SMOKE_LOG_DIR:-}"

# Argument defaults
SUITES=()
CASES=()
BLACKLIST="bug,todo,skip"
NODES=1
TIMEOUT=120
COLLECT_ONLY=0
REUSE_SERVER=0
OVERRIDE_PORT=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--suite)    SUITES+=("$2");    shift 2 ;;
        -k|--case)     CASES+=("$2");     shift 2 ;;
        -b|--blacklist) BLACKLIST="$2";   shift 2 ;;
        --nodes)       NODES="$2";        shift 2 ;;
        --timeout)     TIMEOUT="$2";      shift 2 ;;
        --collect-only) COLLECT_ONLY=1;   shift ;;
        --reuse-server) REUSE_SERVER=1;   shift ;;
        --port)        OVERRIDE_PORT="$2"; REUSE_SERVER=1; shift 2 ;;
        -h|--help)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [-s suite] [-k case] [-b blacklist] [--nodes N] [--timeout N]" >&2
            echo "          [--collect-only] [--reuse-server] [--port TCP]" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[smoke] $*"; }
warn() { echo "[smoke] WARNING: $*" >&2; }
die()  { echo "[smoke] ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Resolve server binary
# ---------------------------------------------------------------------------
if [[ -x "${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/stripped/bin/${CLICKHOUSE_BINARY}"
elif [[ -x "${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}" ]]; then
    SERVER_BIN="${REPO_ROOT}/build/programs/${CLICKHOUSE_BINARY}"
else
    die "Cannot find ${CLICKHOUSE_BINARY} binary. Build first: cd build && ninja"
fi
log "Using binary: ${SERVER_BIN}"

# ---------------------------------------------------------------------------
# Resolve probe binary
# ---------------------------------------------------------------------------
PROBE_EXTRACTED=0

_get_probe() {
    # 1. Explicit env override
    if [[ -n "${PROBE_BIN:-}" ]]; then
        [[ -x "${PROBE_BIN}" ]] || die "PROBE_BIN=${PROBE_BIN} is not executable"
        echo "${PROBE_BIN}"
        return 0
    fi
    # 2. probe in PATH
    if command -v probe >/dev/null 2>&1; then
        echo "$(command -v probe)"
        return 0
    fi
    # 3. Extract from Docker image
    if command -v docker >/dev/null 2>&1; then
        log "probe not in PATH; extracting from timeplus/probe:latest …"
        mkdir -p "${REPO_ROOT}/tmp"
        local tmp_bin
        tmp_bin="$(mktemp "${REPO_ROOT}/tmp/probe.XXXXXX")"
        if docker pull timeplus/probe:latest >/dev/null 2>&1 && \
           docker run --rm --entrypoint cat timeplus/probe:latest /usr/local/bin/probe \
               > "${tmp_bin}" 2>/dev/null && \
           [[ -s "${tmp_bin}" ]]; then
            chmod +x "${tmp_bin}"
            PROBE_EXTRACTED=1
            log "probe extracted from Docker image."
            echo "${tmp_bin}"
            return 0
        fi
        rm -f "${tmp_bin}"
    fi
    return 1
}

if ! PROBE_BIN="$(_get_probe)"; then
    die "probe binary not found. Install it (add to PATH) or ensure Docker is available."
fi
log "Using probe: ${PROBE_BIN} ($(${PROBE_BIN} --version 2>/dev/null || echo 'unknown version'))"

# ---------------------------------------------------------------------------
# Validate smoke case directory
# ---------------------------------------------------------------------------
[[ -d "${SMOKE_CASE_DIR}" ]] || die "Smoke case directory not found: ${SMOKE_CASE_DIR}"

# ---------------------------------------------------------------------------
# Detect OS / arch for probe filtering
# ---------------------------------------------------------------------------
PROBE_OS="$(uname | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
    arm64|aarch64) PROBE_ARCH="aarch64" ;;
    x86_64|amd64)  PROBE_ARCH="x86_64"  ;;
    *)             PROBE_ARCH="$(uname -m)" ;;
esac

# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------
SMOKE_SERVER_PID=""
SMOKE_RUNTIME_ROOT=""
SERVER_TCP_PORT=""
SERVER_HTTP_PORT=""
SERVER_TABLE_HTTP=""
SERVER_TABLE_TCP=""
SERVER_PGSQL_PORT=""
SERVER_PROM_PORT=""
SERVER_OWNED=0   # 1 = we started it; 0 = pre-existing (don't stop/clean on exit)

_wait_server_ready() {
    local port="$1" retries=60
    for (( i = 1; i <= retries; i++ )); do
        if "${SERVER_BIN}" client --port "${port}" --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Derive companion ports from tcp port (step=10 scheme)
_companion_ports() {
    local tcp="$1"
    local offset=$(( (tcp - 8463) / 10 ))
    SERVER_HTTP_PORT=$(( 3218 + offset * 10 ))
    SERVER_TABLE_TCP=$(( 7587 + offset * 10 ))
    SERVER_TABLE_HTTP=$(( 8123 + offset * 10 ))
    SERVER_PGSQL_PORT=$(( 5432 + offset * 10 ))
    SERVER_PROM_PORT=$(( 9363  + offset * 10 ))
}

_start_server() {
    log "Starting ${CLICKHOUSE_BINARY} for smoke tests …"

    # Tee start-local-proton.sh output so it's visible in real time AND
    # parseable for port values after the fact.
    local tmp_out
    tmp_out="$(mktemp "${REPO_ROOT}/tmp/smoke_start.XXXXXX")"

    set +e
    "${REPO_ROOT}/start-local-proton.sh" 2>&1 | tee "${tmp_out}"
    local rc="${PIPESTATUS[0]}"
    set -e

    if [[ "${rc}" -ne 0 ]]; then
        rm -f "${tmp_out}"
        die "start-local-proton.sh exited with code ${rc}"
    fi

    SERVER_TCP_PORT="$(grep -E '^\s*tcp:'       "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    SERVER_HTTP_PORT="$(grep -E '^\s*node_http:' "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    SERVER_TABLE_HTTP="$(grep -E '^\s*table_http:' "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    SERVER_TABLE_TCP="$(grep -E '^\s*table_tcp:'   "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    SERVER_PGSQL_PORT="$(grep -E '^\s*postgresql:'  "${tmp_out}" | head -1 | grep -oE '[0-9]+')"
    rm -f "${tmp_out}"

    [[ -n "${SERVER_TCP_PORT}" ]] || die "Could not parse tcp port from start-local-proton.sh output"
    # Fall back to computed companion ports if grep missed any field
    [[ -n "${SERVER_TABLE_HTTP}" ]] || _companion_ports "${SERVER_TCP_PORT}"

    SMOKE_RUNTIME_ROOT="${REPO_ROOT}/tmp_data_${SERVER_TCP_PORT}"
    local pid_file="${SMOKE_RUNTIME_ROOT}/${CLICKHOUSE_BINARY}.pid"
    SMOKE_SERVER_PID="$(cat "${pid_file}" 2>/dev/null || true)"

    if [[ -z "${SMOKE_SERVER_PID}" ]] || ! kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null; then
        rm -rf "${SMOKE_RUNTIME_ROOT}"
        die "Server started but PID not readable from ${pid_file}"
    fi

    SERVER_OWNED=1
    log "Server ready: tcp=${SERVER_TCP_PORT}  table_http=${SERVER_TABLE_HTTP}  pid=${SMOKE_SERVER_PID}"
}

_stop_server() {
    if [[ -n "${SMOKE_SERVER_PID}" ]]; then
        log "Stopping ${CLICKHOUSE_BINARY} (pid=${SMOKE_SERVER_PID}) …"
        kill -TERM "${SMOKE_SERVER_PID}" 2>/dev/null || true
        local i
        for (( i = 1; i <= 30; i++ )); do
            kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null || break
            sleep 1
        done
        kill -KILL "${SMOKE_SERVER_PID}" 2>/dev/null || true
        SMOKE_SERVER_PID=""
    fi
    # Only clean up the data dir if we created it
    if [[ "${SERVER_OWNED}" -eq 1 && -n "${SMOKE_RUNTIME_ROOT}" && -d "${SMOKE_RUNTIME_ROOT}" ]]; then
        rm -rf "${SMOKE_RUNTIME_ROOT}"
        SMOKE_RUNTIME_ROOT=""
    fi
}

_resolve_reuse_port() {
    if [[ -n "${OVERRIDE_PORT}" ]]; then
        SERVER_TCP_PORT="${OVERRIDE_PORT}"
    else
        local found=""
        for dir in "${REPO_ROOT}"/tmp_data_*/; do
            [[ -d "${dir}" ]] || continue
            local tcp="${dir%/}"; tcp="${tcp##*_}"
            local pid_file="${dir}/${CLICKHOUSE_BINARY}.pid"
            local pid; pid="$(cat "${pid_file}" 2>/dev/null || true)"
            if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
                found="${tcp}"; SMOKE_SERVER_PID="${pid}"
                break
            fi
        done
        [[ -n "${found}" ]] || \
            die "No live ${CLICKHOUSE_BINARY} instance found. Start one with ./start-local-proton.sh"
        SERVER_TCP_PORT="${found}"
    fi

    _companion_ports "${SERVER_TCP_PORT}"
    # SERVER_OWNED stays 0; never stop or clean up a pre-existing server

    log "Reusing server: tcp=${SERVER_TCP_PORT}  table_http=${SERVER_TABLE_HTTP}"
    _wait_server_ready "${SERVER_TCP_PORT}" || \
        die "${CLICKHOUSE_BINARY} not responding on tcp=${SERVER_TCP_PORT}"
}

# ---------------------------------------------------------------------------
# Build dynamic deployment YAML for probe
# ---------------------------------------------------------------------------
_make_deploy_yaml() {
    local deploy_dir="$1"
    mkdir -p "${deploy_dir}"
    cat > "${deploy_dir}/p1k1.yaml" << EOF
p1:
  host: localhost
  proton:
    cluster_id:
    role:
      - metadata
      - data
    username: default
    password:
    database: default
    tcp_port: ${SERVER_TCP_PORT}
    http_port: ${SERVER_HTTP_PORT}
    tcp_snap_port: ${SERVER_TABLE_TCP}
    http_snap_port: ${SERVER_TABLE_HTTP}
    postgres_port: ${SERVER_PGSQL_PORT}
    prometheus_port: ${SERVER_PROM_PORT:-9363}
EOF
}

# ---------------------------------------------------------------------------
# Cleanup trap — only removes resources this script created
# ---------------------------------------------------------------------------
_cleanup() {
    local rc=$?
    # Stop server only if we started it (SERVER_OWNED=1)
    if [[ "${SERVER_OWNED}" -eq 1 ]]; then
        _stop_server
    fi
    # Remove extracted probe binary if we downloaded it
    if [[ "${PROBE_EXTRACTED}" -eq 1 ]]; then
        rm -f "${PROBE_BIN}"
    fi
    # Remove deploy dir (safe to remove on any exit including Ctrl-C)
    [[ -n "${DEPLOY_DIR:-}" && -d "${DEPLOY_DIR}" ]] && rm -rf "${DEPLOY_DIR}"
    exit "${rc}"
}
trap _cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
mkdir -p "${REPO_ROOT}/tmp"

# Start or attach to server
if [[ "${REUSE_SERVER}" -eq 1 ]]; then
    _resolve_reuse_port
else
    _start_server
fi

# Resolve log directory now that we know the server port.
# When we own the server, co-locate logs with its runtime dir (tmp_data_<tcp>/logs/smoke/).
# When reusing an external server (--port/--reuse-server), fall back to ./tmp/smoke_logs/
# to avoid creating directories that don't belong to that server.
# Either path can be overridden by setting SMOKE_LOG_DIR env var before invoking.
if [[ -n "${SMOKE_LOG_DIR_OVERRIDE}" ]]; then
    SMOKE_LOG_DIR="${SMOKE_LOG_DIR_OVERRIDE}"
elif [[ "${SERVER_OWNED}" -eq 1 ]]; then
    SMOKE_LOG_DIR="${REPO_ROOT}/tmp_data_${SERVER_TCP_PORT}/logs/smoke"
else
    SMOKE_LOG_DIR="${REPO_ROOT}/tmp/smoke_logs"
fi
mkdir -p "${SMOKE_LOG_DIR}"

# Build deployment yaml pointing at the actual server ports
DEPLOY_DIR="${SMOKE_LOG_DIR}/deploy"
_make_deploy_yaml "${DEPLOY_DIR}"

# Build probe command
PROBE_CMD=(
    "${PROBE_BIN}" smoke "${SMOKE_CASE_DIR}"
    -v
    -d "${DEPLOY_DIR}"
    -c p1k1
    --variable "nodes=${NODES}"
    -b "${BLACKLIST}"
    -o "${PROBE_OS}"
    -a "${PROBE_ARCH}"
    --timeout "${TIMEOUT}"
)
for s in "${SUITES[@]}";  do PROBE_CMD+=(-s "$s"); done
for k in "${CASES[@]}";   do PROBE_CMD+=(-k "$k"); done
[[ "${COLLECT_ONLY}" -eq 1 ]] && PROBE_CMD+=(--collect-only)

# Run
PROBE_LOG="${SMOKE_LOG_DIR}/probe_smoke_results.log"
log "Running probe smoke tests …"
log "Log: ${PROBE_LOG}"
log "Cmd: ${PROBE_CMD[*]}"
echo ""

set +e
"${PROBE_CMD[@]}" 2>&1 | tee "${PROBE_LOG}"
PROBE_RC="${PIPESTATUS[0]}"
set -e

echo ""
if [[ "${PROBE_RC}" -eq 0 ]]; then
    log "Smoke tests PASSED."
else
    log "Smoke tests FAILED (exit code ${PROBE_RC}). See: ${PROBE_LOG}"
fi

exit "${PROBE_RC}"
