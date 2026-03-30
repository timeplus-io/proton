#!/usr/bin/env bash
# run-unit-tests.sh — Run unit_tests_dbms (C++ gtest suite).
#
# Quick Start:
#   ./run-unit-tests.sh                          # Run all unit tests
#   ./run-unit-tests.sh StorageStream            # Filter by class/prefix (prefix match)
#   ./run-unit-tests.sh "ParserStream.ParseDDL"  # Single test case (exact)
#   ./run-unit-tests.sh "Parser*"                # Glob pattern (quoted)
#   ./run-unit-tests.sh -l StorageStream         # List matching tests, don't run
#
# Options:
#   -f FILTER    Explicit gtest_filter expression (overrides positional arg)
#   -l           List matching tests without running (--gtest_list_tests)
#   -o DIR       Write JUnit XML report to DIR/unit_test_results.xml
#
# Environment overrides:
#   CLICKHOUSE_BINARY   Binary name         (default: proton)
#   UNIT_TEST_BIN       Override binary path entirely
#
# Exit code mirrors gtest: 0 = all pass, non-zero = failures.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-proton}"

# ---------------------------------------------------------------------------
# Resolve binary
# ---------------------------------------------------------------------------
if [[ -n "${UNIT_TEST_BIN:-}" ]]; then
    TEST_BIN="${UNIT_TEST_BIN}"
elif [[ -x "${REPO_ROOT}/build/src/stripped/bin/unit_tests_dbms" ]]; then
    TEST_BIN="${REPO_ROOT}/build/src/stripped/bin/unit_tests_dbms"
elif [[ -x "${REPO_ROOT}/build/src/unit_tests_dbms" ]]; then
    TEST_BIN="${REPO_ROOT}/build/src/unit_tests_dbms"
else
    echo "[unit] ERROR: unit_tests_dbms not found. Build first: cd build && ninja unit_tests_dbms" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
FILTER=""
LIST_ONLY=0
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--filter)  FILTER="$2";     shift 2 ;;
        -l|--list)    LIST_ONLY=1;      shift   ;;
        -o|--output)  OUTPUT_DIR="$2";  shift 2 ;;
        -h|--help)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        -*)
            echo "[unit] Unknown option: $1" >&2; exit 1 ;;
        *)
            # Positional: "Class.Case" or "Class:Case" → exact; bare word → prefix match
            if [[ -z "${FILTER}" ]]; then
                if [[ "$1" == *"*"* || "$1" == *"."* || "$1" == *":"* ]]; then
                    FILTER="$1"
                else
                    FILTER="${1}*"
                fi
            fi
            shift ;;
    esac
done

# ---------------------------------------------------------------------------
# Build command
# ---------------------------------------------------------------------------
CMD=("${TEST_BIN}")
[[ -n "${FILTER}" ]]       && CMD+=(--gtest_filter="${FILTER}")
[[ "${LIST_ONLY}" -eq 1 ]] && CMD+=(--gtest_list_tests)

if [[ -n "${OUTPUT_DIR}" ]]; then
    mkdir -p "${OUTPUT_DIR}"
    CMD+=(--gtest_output="xml:${OUTPUT_DIR}/unit_test_results.xml")
fi

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
echo "[unit] Binary: ${TEST_BIN}"
[[ -n "${FILTER}" ]] && echo "[unit] Filter:  ${FILTER}"
echo ""

"${CMD[@]}"
