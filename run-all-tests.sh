#!/usr/bin/env bash
# run-all-tests.sh — Run the full local test suite: unit → SQL → smoke.
#
# Quick Start:
#   ./run-all-tests.sh            # All three suites in sequence
#   ./run-all-tests.sh --skip-smoke   # Skip smoke tests (faster pre-commit pass)
#   ./run-all-tests.sh --only unit    # Run one suite only: unit | sql | smoke
#   ./run-all-tests.sh --port <tcp>   # Reuse running server for SQL and smoke
#
# Options:
#   --only SUITE       Run one suite only: unit | sql | smoke
#   --skip-unit        Skip unit tests
#   --skip-sql         Skip stateless SQL tests
#   --skip-smoke       Skip smoke tests
#   --port TCP         Reuse a running server for SQL and smoke (implies --reuse-server)
#   --stop-on-error    Stop after first suite failure (default: run all, report at end)
#   -h, --help         Show this help
#
# Exit code:
#   0   All enabled suites passed
#   1   One or more suites failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RUN_UNIT=1
RUN_SQL=1
RUN_SMOKE=1
PORT_ARG=""
STOP_ON_ERROR=0

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)
            case "$2" in
                unit)  RUN_SQL=0;  RUN_SMOKE=0 ;;
                sql)   RUN_UNIT=0; RUN_SMOKE=0 ;;
                smoke) RUN_UNIT=0; RUN_SQL=0   ;;
                *) echo "[all] Unknown suite: $2 (expected: unit | sql | smoke)" >&2; exit 1 ;;
            esac
            shift 2 ;;
        --skip-unit)        RUN_UNIT=0;  shift ;;
        --skip-sql)         RUN_SQL=0;   shift ;;
        --skip-smoke)       RUN_SMOKE=0; shift ;;
        --port)             PORT_ARG="--port $2"; shift 2 ;;
        --stop-on-error)    STOP_ON_ERROR=1; shift ;;
        -h|--help)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *)
            echo "[all] Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[all] $*"; }
PASS=() FAIL=()

_run_suite() {
    local name="$1"; shift
    log "--- $name ---"
    # shellcheck disable=SC2068
    if "$@"; then
        PASS+=("$name")
        log "PASSED: $name"
    else
        FAIL+=("$name")
        log "FAILED: $name"
        if [[ "${STOP_ON_ERROR}" -eq 1 ]]; then
            _summary
            exit 1
        fi
    fi
    echo ""
}

_summary() {
    log "--- Summary ---"
    for s in "${PASS[@]+"${PASS[@]}"}"; do log "  PASS  $s"; done
    for s in "${FAIL[@]+"${FAIL[@]}"}"; do log "  FAIL  $s"; done
}

# ---------------------------------------------------------------------------
# Run suites
# ---------------------------------------------------------------------------
echo ""
log "Starting full local test run  (unit=${RUN_UNIT} sql=${RUN_SQL} smoke=${RUN_SMOKE})"
echo ""

if [[ "${RUN_UNIT}" -eq 1 ]]; then
    _run_suite "unit"  "${SCRIPT_DIR}/run-unit-tests.sh"
fi

if [[ "${RUN_SQL}" -eq 1 ]]; then
    # shellcheck disable=SC2086
    _run_suite "sql"   "${SCRIPT_DIR}/run-sql-tests.sh"   ${PORT_ARG}
fi

if [[ "${RUN_SMOKE}" -eq 1 ]]; then
    # shellcheck disable=SC2086
    _run_suite "smoke" "${SCRIPT_DIR}/run-smoke-tests.sh" ${PORT_ARG}
fi

# ---------------------------------------------------------------------------
# Final summary + exit
# ---------------------------------------------------------------------------
echo ""
_summary

if [[ "${#FAIL[@]}" -gt 0 ]]; then
    log "${#FAIL[@]} suite(s) failed."
    exit 1
fi
log "All suites passed."
