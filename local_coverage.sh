#!/usr/bin/env bash
# local_coverage.sh — Build with LLVM instrumentation and generate code coverage
# reports for C++ code under src/.
#
# Quick Start:
#   ./local_coverage.sh                  # Run all tests (full mode: unit+smoke+stateless+stateful) + HTML
#   ./local_coverage.sh unit             # Run only unit tests
#   ./local_coverage.sh stateless        # Run only stateless SQL tests
#
# Usage:
#   ./local_coverage.sh [MODE] [OPTIONS]
#
# Test Modes:
#   full      (default) Run all: unit + smoke + stateless + stateful
#   unit      Run only unit_tests_dbms (C++ unit tests)
#   smoke     Run only smoke tests (basic SQL queries, quick validation)
#   stateless Run only stateless SQL tests (no external data required)
#   stateful  Run only stateful SQL tests (downloads ~300MB dataset first)
#
# Report-Only Modes (skip test execution):
#   llvm_html Generate native LLVM HTML report from existing merged.profdata
#   lcov_html Generate LCOV HTML report from existing coverage.info
#
# Options:
#   --rebuild   Force full CMake reconfigure + rebuild (removes build_coverage/)
#   --no-html   Skip HTML report generation (only generate text summary)
#
# Examples:
#   # Basic workflow - run all tests with HTML report (default: full mode)
#   ./local_coverage.sh
#
#   # Run only unit tests (faster, ~2-5 minutes)
#   ./local_coverage.sh unit
#
#   # Run only stateless SQL tests
#   ./local_coverage.sh stateless
#
#   # Incremental coverage - run tests separately, results accumulate
#   ./local_coverage.sh unit       # Generate unit_tests_*.profraw
#   ./local_coverage.sh stateless  # Add server_*.profraw, merge with unit coverage
#   ./local_coverage.sh stateful   # Update server_*.profraw, merge again
#   # Final report includes all three test suites
#
#   # Force rebuild and run all tests
#   ./local_coverage.sh --rebuild
#
#   # Run stateless tests only, skip HTML generation
#   ./local_coverage.sh stateless --no-html
#
#   # Just regenerate HTML reports from existing profraw data
#   ./local_coverage.sh llvm_html
#   ./local_coverage.sh lcov_html
#
# Prerequisites (macOS / Homebrew):
#   brew install llvm        # LLVM 16-19 (provides clang, llvm-profdata, llvm-cov)
#   brew install lcov        # genhtml for LCOV HTML reports
#   brew install coreutils   # GNU timeout for _long test scripts
#   brew install bash        # bash 5.x for test runner (|& syntax support)
#
# Prerequisites (Linux / apt):
#   apt install llvm clang lcov
#
# Output:
#   coverage_profraw/              Raw .profraw files (unit_tests_*, ${CLICKHOUSE_BINARY}_*)
#   coverage_report/merged.profdata   Merged coverage database
#   coverage_report/coverage_summary.txt   Text summary
#   coverage_report/llvm_html/index.html   Native LLVM HTML report
#   coverage_report/lcov_html/index.html   LCOV HTML report
#
# Incremental Coverage:
#   Each mode (unit/smoke/stateless/stateful) preserves other modes' .profraw files:
#   - unit mode: cleans unit_tests_*.profraw, keeps ${CLICKHOUSE_BINARY}_*.profraw
#   - SQL modes: clean ${CLICKHOUSE_BINARY}_*.profraw, keep unit_tests_*.profraw
#   - full mode: cleans all .profraw files
#   This allows running tests separately and merging coverage at the end.
#
# Optional Environment Variables:
#   LLVM_BIN              LLVM toolchain directory (auto-discovered if not set)
#   CC, CXX               Override C/C++ compiler (default: auto-discovered clang/clang++)
#   SQL_TEST_JOBS         Parallel jobs for SQL tests (default: NPROC/2)
#   SQL_TEST_TIMEOUT      Per-test timeout in seconds (default: 90, stateful: 180)
#   COVERAGE_HTML_SCOPE   HTML scope: src|all (default: src)
#   CLICKHOUSE_BINARY           Server binary name, e.g. proton or timeplusd (default: proton)
#   CLICKHOUSE_USER       SQL user for all tests (default: proton)
#   CLICKHOUSE_PASSWORD   SQL password for all tests (default: proton@t+)
#   ENV_PREFIX            Env var prefix for server options (default: uppercase CLICKHOUSE_BINARY)
#   WATCHDOG_ENV_VAR      Watchdog disable env var name (default: ${ENV_PREFIX}_WATCHDOG_ENABLE)

set -euo pipefail

# ---------------------------------------------------------------------------
# Project identity — override via environment to adapt for other binaries.
#   CLICKHOUSE_BINARY        Server binary name, e.g. proton or timeplusd (default: proton)
#   CLICKHOUSE_USER    SQL user for all tests (default: proton)
#   CLICKHOUSE_PASSWORD SQL password for all tests (default: proton@t+)
#   ENV_PREFIX         Env var prefix for server options (default: uppercase CLICKHOUSE_BINARY)
#   WATCHDOG_ENV_VAR   Watchdog disable env var (default: ${ENV_PREFIX}_WATCHDOG_ENABLE)
# ---------------------------------------------------------------------------
CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-proton}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-proton}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-proton@t+}"
ENV_PREFIX="${ENV_PREFIX:-$(echo "${CLICKHOUSE_BINARY}" | tr '[:lower:]' '[:upper:]')}"
WATCHDOG_ENV_VAR="${WATCHDOG_ENV_VAR:-${ENV_PREFIX}_WATCHDOG_ENABLE}"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
BUILD_DIR="${REPO_ROOT}/build_coverage"
PROFRAW_DIR="${REPO_ROOT}/coverage_profraw"
REPORT_DIR="${REPO_ROOT}/coverage_report"

# Default local server ports used by SQL and smoke tests.
COV_NODE_TCP=8463
COV_TABLE_HTTP=8123

# ---------------------------------------------------------------------------
# LLVM toolchain discovery
# Precedence:
#   1. LLVM_BIN env var (explicit override)
#   2. Unversioned llvm-profdata in PATH  (Homebrew with PATH configured)
#   3. Newest versioned llvm-profdata-N in PATH  (apt install llvm-N)
#   4. Newest under well-known install prefixes  (Homebrew keg-only / apt)
# Individual tool overrides: LLVM_PROFDATA and/or LLVM_COV.
# Note: sort -V is GNU-only (unsupported on macOS BSD sort); _pick_newest
#       uses POSIX awk which works on both Linux gawk/mawk and macOS awk.
# ---------------------------------------------------------------------------

# Pick the candidate with the highest embedded integer version number.
# Reads one path per line from stdin; prints the path with the largest N.
_pick_newest() {
    awk 'match($0, /[0-9]+/) {
             v = substr($0, RSTART, RLENGTH) + 0
             if (v > best) { best = v; winner = $0 }
         }
         END { if (winner != "") print winner }'
}

_discover_llvm_bin() {
    local bin newest
    # 2. Unversioned in PATH — covers Homebrew with PATH configured
    bin="$(command -v llvm-profdata 2>/dev/null || true)"
    [[ -x "${bin}" ]] && echo "$(dirname "${bin}")" && return 0
    # 3. Newest versioned in PATH — covers apt without manual PATH setup
    #    compgen -c is a bash built-in available on bash 3.2+ (Linux + macOS)
    newest="$(compgen -c 'llvm-profdata-' 2>/dev/null \
        | grep -E '^llvm-profdata-[0-9]+$' | _pick_newest || true)"
    if [[ -n "${newest}" ]]; then
        bin="$(command -v "${newest}" 2>/dev/null || true)"
        [[ -x "${bin}" ]] && echo "$(dirname "${bin}")" && return 0
    fi
    # 4. Known install prefixes — covers Homebrew keg-only and apt /usr/lib
    bin="$(ls /usr/lib/llvm-*/bin/llvm-profdata \
              /opt/homebrew/opt/llvm@*/bin/llvm-profdata \
              /usr/local/opt/llvm@*/bin/llvm-profdata 2>/dev/null \
           | _pick_newest || true)"
    [[ -x "${bin}" ]] && echo "$(dirname "${bin}")" && return 0
    return 1
}

if [[ -z "${LLVM_BIN:-}" ]]; then
    if ! LLVM_BIN="$(_discover_llvm_bin)"; then
        echo "ERROR: Cannot find an LLVM installation with llvm-profdata." >&2
        echo "  macOS:  brew install llvm" >&2
        echo "  Linux:  apt install llvm  (or set LLVM_BIN / LLVM_PROFDATA / LLVM_COV)" >&2
        exit 1
    fi
fi

# Allow individual tool overrides via env var (mirrors CI interface)
LLVM_PROFDATA="${LLVM_PROFDATA:-${LLVM_BIN}/llvm-profdata}"
LLVM_COV="${LLVM_COV:-${LLVM_BIN}/llvm-cov}"
GENHTML="$(command -v genhtml 2>/dev/null || echo "")"

_discover_clang() {
    local suffix="$1" bin newest
    # Exact pattern: clang-N or clang++-N only (avoids matching clang-format-N etc.)
    local pattern="^clang${suffix//+/\\+}-[0-9]+$"
    # 1. LLVM_BIN: unversioned (Homebrew — clang lives beside llvm-profdata)
    bin="${LLVM_BIN}/clang${suffix}"
    [[ -x "${bin}" ]] && echo "${bin}" && return 0
    # 2. LLVM_BIN: newest versioned clang[++]-N in same dir (apt layout)
    newest="$(ls "${LLVM_BIN}/clang${suffix}"-[0-9]* 2>/dev/null | _pick_newest || true)"
    [[ -x "${newest}" ]] && echo "${newest}" && return 0
    # 3. PATH: newest versioned clang[++]-N
    newest="$(compgen -c "clang${suffix}-" 2>/dev/null \
        | grep -E "${pattern}" | _pick_newest || true)"
    if [[ -n "${newest}" ]]; then
        bin="$(command -v "${newest}" 2>/dev/null || true)"
        [[ -x "${bin}" ]] && echo "${bin}" && return 0
    fi
    # 4. PATH: unversioned clang
    bin="$(command -v "clang${suffix}" 2>/dev/null || true)"
    [[ -x "${bin}" ]] && echo "${bin}" && return 0
    echo "clang${suffix}"  # last resort: let cmake fail with a clear message
}

CC_COMPILER="${CC:-$(_discover_clang "")}"
CXX_COMPILER="${CXX:-$(_discover_clang "++")}"

NPROC="$(sysctl -n hw.logicalcpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
HTML_SCOPE="${COVERAGE_HTML_SCOPE:-src}"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
MODE="full"
DO_REBUILD=0
DO_HTML=1  # Generate HTML reports by default

for arg in "$@"; do
    case "${arg}" in
        unit) MODE="unit" ;;
        smoke) MODE="smoke" ;;
        stateless) MODE="stateless" ;;
        stateful) MODE="stateful" ;;
        full) MODE="full" ;;
        llvm_html) MODE="llvm_html" ;;
        lcov_html) MODE="lcov_html" ;;
        --rebuild) DO_REBUILD=1 ;;
        --html) DO_HTML=1 ;;  # Kept for backward compatibility
        --no-html) DO_HTML=0 ;;
        *)
            echo "Unknown argument: ${arg}" >&2
            echo "Usage: $0 [unit|smoke|stateless|stateful|full|llvm_html|lcov_html] [--rebuild] [--no-html]" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
for tool in "${LLVM_PROFDATA}" "${LLVM_COV}"; do
    if [[ ! -x "${tool}" ]]; then
        echo "ERROR: Required tool not found or not executable: ${tool}" >&2
        echo "  macOS:  brew install llvm@19" >&2
        echo "  Linux:  apt install llvm" >&2
        echo "  or set: LLVM_BIN, LLVM_PROFDATA, LLVM_COV" >&2
        exit 1
    fi
done

if ! command -v cmake >/dev/null 2>&1; then
    echo "ERROR: cmake not found in PATH" >&2
    exit 1
fi

if [[ "${DO_HTML}" -eq 1 ]] && [[ -z "${GENHTML}" ]]; then
    echo "WARNING: genhtml not found (LCOV HTML reports will be skipped)" >&2
    echo "  Install with: brew install lcov" >&2
    echo "  LLVM HTML reports will still be generated." >&2
    echo ""
fi

if [[ "${MODE}" == "stateless" || "${MODE}" == "stateful" || "${MODE}" == "full" ]] \
    && ! command -v timeout >/dev/null 2>&1; then
    echo "WARNING: GNU 'timeout' not found — some _long test scripts will fail." >&2
    echo "Install with: brew install coreutils" >&2
fi

echo "=== Local Coverage Validation ==="
echo "  Mode    : ${MODE}"
echo "  Build   : ${BUILD_DIR}"
echo "  Reports : ${REPORT_DIR}"
echo ""

_build_llvm_cov_show_args() {
    COV_SHOW_MAIN_BIN=""
    COV_SHOW_EXTRA_FLAGS=()
    local -a bins=()
    local i=0

    for (( i = 0; i < ${#OBJECT_FLAGS[@]}; i += 2 )); do
        bins+=( "${OBJECT_FLAGS[$((i + 1))]}" )
    done
    if [[ "${#bins[@]}" -eq 0 ]]; then
        return 1
    fi

    COV_SHOW_MAIN_BIN="${bins[0]}"
    for (( i = 1; i < ${#bins[@]}; i++ )); do
        COV_SHOW_EXTRA_FLAGS+=( -object "${bins[$i]}" )
    done
}

if [[ "${MODE}" == "lcov_html" ]]; then
    LCOV_CLEAN="${REPORT_DIR}/coverage.info"
    LCOV_HTML_DIR="${REPORT_DIR}/lcov_html"

    if [[ ! -f "${LCOV_CLEAN}" ]]; then
        echo "WARNING: ${LCOV_CLEAN} not found. Run a coverage test first." >&2
        echo "Skipping LCOV HTML generation." >&2
        exit 0
    fi

    if [[ -z "${GENHTML}" ]]; then
        echo "WARNING: genhtml not found. Install with: brew install lcov" >&2
        echo "Skipping LCOV HTML generation." >&2
        exit 0
    fi

    echo "=== Generating LCOV HTML report ==="
    mkdir -p "${LCOV_HTML_DIR}"
    "${GENHTML}" \
        "${LCOV_CLEAN}" \
        --output-directory "${LCOV_HTML_DIR}" \
        --title "${CLICKHOUSE_BINARY} Coverage (LCOV)" \
        --show-details \
        --branch-coverage \
        --legend \
        --ignore-errors inconsistent,corrupt,unsupported \
        2>"${REPORT_DIR}/genhtml.log"
    
    if [[ -s "${REPORT_DIR}/genhtml.log" ]]; then
        _unexpected=$(grep -i "error\|warning" "${REPORT_DIR}/genhtml.log" \
                      | grep -v "(inconsistent)\|(unsupported)" || true)
        if [[ -n "${_unexpected}" ]]; then
            echo "  WARNING: genhtml reported unexpected issues (see ${REPORT_DIR}/genhtml.log):" >&2
            echo "${_unexpected}" | head -5 >&2
        fi
    fi
    echo "LCOV HTML report generated at: ${LCOV_HTML_DIR}/index.html"
    exit 0
fi

# Prefer stripped binaries (debug symbols split off, coverage sections intact)
# to reduce RSS and startup time. Fall back to the full binary if not present.
_resolve_bin_paths() {
    if [[ -x "${BUILD_DIR}/src/stripped/bin/unit_tests_dbms" ]]; then
        UNIT_TEST_BIN="${BUILD_DIR}/src/stripped/bin/unit_tests_dbms"
    else
        UNIT_TEST_BIN="${BUILD_DIR}/src/unit_tests_dbms"
    fi

    if [[ -x "${BUILD_DIR}/programs/stripped/bin/${CLICKHOUSE_BINARY}" ]]; then
        SERVER_BIN="${BUILD_DIR}/programs/stripped/bin/${CLICKHOUSE_BINARY}"
    else
        SERVER_BIN="${BUILD_DIR}/programs/${CLICKHOUSE_BINARY}"
    fi
}

if [[ "${MODE}" == "llvm_html" ]]; then
    PROFDATA="${REPORT_DIR}/merged.profdata"

    if [[ ! -f "${PROFDATA}" ]]; then
        echo "WARNING: ${PROFDATA} not found. Run a coverage test first." >&2
        echo "Skipping LLVM HTML generation." >&2
        exit 0
    fi

    if [[ ! -x "${LLVM_COV}" ]]; then
        echo "WARNING: llvm-cov not found at ${LLVM_COV}" >&2
        echo "Install with: brew install llvm" >&2
        echo "Skipping LLVM HTML generation." >&2
        exit 0
    fi

    # Resolve now so that early-exit modes (e.g. llvm_html) have the variables set.
    _resolve_bin_paths

    OBJECT_FLAGS=()
    if [[ -x "${UNIT_TEST_BIN}" ]]; then OBJECT_FLAGS+=(-object "${UNIT_TEST_BIN}"); fi
    if [[ -x "${SERVER_BIN}" ]]; then OBJECT_FLAGS+=(-object "${SERVER_BIN}"); fi

    if [[ "${#OBJECT_FLAGS[@]}" -eq 0 ]]; then
        echo "WARNING: No instrumented binaries found under ${BUILD_DIR}" >&2
        echo "Skipping LLVM HTML generation." >&2
        exit 0
    fi

    LLVM_HTML_DIR="${REPORT_DIR}/llvm_html"
    SRC_EXCLUDE_REGEX='(contrib|programs|base|cmake|utils|tests|debian|docker)/'

    echo "=== Generating native LLVM HTML coverage report (src/ only) ==="
    mkdir -p "${LLVM_HTML_DIR}"

    if ! _build_llvm_cov_show_args; then
        echo "WARNING: failed to prepare llvm-cov object flags" >&2
        echo "Skipping LLVM HTML generation." >&2
        exit 0
    fi

    "${LLVM_COV}" show \
        "${COV_SHOW_MAIN_BIN}" \
        ${COV_SHOW_EXTRA_FLAGS[@]+"${COV_SHOW_EXTRA_FLAGS[@]}"} \
        -instr-profile="${PROFDATA}" \
        --compilation-dir="${REPO_ROOT}" \
        --ignore-filename-regex="${SRC_EXCLUDE_REGEX}" \
        --format=html \
        --output-dir="${LLVM_HTML_DIR}" \
        --show-line-counts-or-regions \
        2>&1 || echo "Warning: HTML generation encountered errors (non-fatal)"

    echo "Native LLVM HTML report generated at: ${LLVM_HTML_DIR}/index.html"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 1: CMake configure (skipped if build already configured and --rebuild
#         was not requested)
# ---------------------------------------------------------------------------
NEEDS_CONFIGURE=0
if [[ ! -f "${BUILD_DIR}/CMakeCache.txt" ]]; then
    NEEDS_CONFIGURE=1
elif [[ "${DO_REBUILD}" -eq 1 ]]; then
    echo "  [--rebuild] Wiping ${BUILD_DIR} …"
    rm -rf "${BUILD_DIR}"
    NEEDS_CONFIGURE=1
else
    # Verify that the existing cache was configured with WITH_COVERAGE=ON
    if ! grep -q "WITH_COVERAGE:BOOL=ON" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null; then
        echo "  Existing build_coverage/ does not have WITH_COVERAGE=ON. Re-configuring …"
        NEEDS_CONFIGURE=1
    fi
fi

mkdir -p "${BUILD_DIR}"

if [[ "${NEEDS_CONFIGURE}" -eq 1 ]]; then
    echo "--- [1/4] Configuring CMake with WITH_COVERAGE=ON ---"
    cmake \
        -S "${REPO_ROOT}" \
        -B "${BUILD_DIR}" \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_CXX_COMPILER="${CXX_COMPILER}" \
        -DCMAKE_C_COMPILER="${CC_COMPILER}" \
        -DWITH_COVERAGE=ON \
        -DENABLE_PROTON_ALL=ON \
        -DENABLE_PROTON_SERVER=ON \
        -DENABLE_PROTON_CLIENT=ON \
        -DENABLE_PROTON_INSTALL=ON \
        -DENABLE_PROTON_NLOG=ON \
        -DENABLE_PROTON_PYTHON=ON \
        -DUSE_DEBUG_HELPERS=ON \
        -DENABLE_LIBRARIES=OFF \
        -DENABLE_BASE64=ON \
        -DENABLE_KAFKA=ON \
        -DENABLE_NURAFT=ON \
        -DENABLE_RAPIDJSON=ON \
        -DENABLE_YAML_CPP=ON \
        -DENABLE_SIMDJSON=ON \
        -DENABLE_ROCKSDB=ON \
        -DENABLE_JEMALLOC=OFF \
        -DENABLE_SSL=ON \
        -DENABLE_BZIP2=ON \
        -DENABLE_BROTLI=ON \
        -DENABLE_PROTOBUF=ON \
        -DENABLE_LIBURING=OFF \
        -DENABLE_UTILS=ON \
        -DENABLE_THINLTO=OFF \
        -DENABLE_CLANG_TIDY=OFF \
        -DENABLE_TESTS=ON \
        -DENABLE_EXAMPLES=OFF \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DENABLE_CCACHE=ON \
        -DENABLE_CHECK_HEAVY_BUILDS=OFF \
        -DENABLE_GRPC=ON \
        -DENABLE_MATH_FUNCS=ON \
        -DENABLE_GEO_FUNCS=ON \
        -DENABLE_HIGH_ORDER_ARRAY_FUNCS=ON \
        -DENABLE_PARQUET=ON \
        -DENABLE_THRIFT=ON \
        -DENABLE_CYRUS_SASL=ON \
        -DENABLE_KRB5=ON \
        -DSANITIZE= \
        -DENABLE_BITMAP_FUNCS=ON \
        -DENABLE_BINARY_REPR_FUNCS=ON \
        -DENABLE_IP_CODING_FUNCS=ON \
        -DENABLE_UUID_CODING_FUNCS=ON \
        -DENABLE_EXTERNAL_DICT_FUNCS=OFF \
        -DENABLE_FORMATTING_FUNCS=ON \
        -DENABLE_HASH_FUNCS=ON \
        -DENABLE_MISC_FUNCS=ON \
        -DENABLE_INTROSPECTION_FUNCS=ON \
        -DUSE_CONSISTENT_HASH_FUNCS=ON \
        -DENABLE_HAMMING_DISTANCE_FUNCS=ON \
        -DENABLE_SNOWFLAKE_FUNCS=ON \
        -DENABLE_ENCRYPT_DECRYPT_FUNCS=ON \
        -DENABLE_DEBUG_FUNCS=ON \
        -DENABLE_URL_FUNCS=ON \
        -DENABLE_ARG_MIN_MAX_FUNCS=OFF \
        -DENABLE_AVRO=ON \
        -DENABLE_PULSAR=ON \
        -DENABLE_CURL=ON \
        -DENABLE_PYTHON_UDF=ON \
        -DENABLE_AWS_S3=ON \
        -DENABLE_AWS_MSK_IAM=ON \
        -DENABLE_MYSQL=ON \
        -DENABLE_LIBPQXX=ON \
        -DENABLE_AZURE_BLOB_STORAGE=OFF \
        -DUSE_MONGODB=ON \
        -DSPLIT_DEBUG_SYMBOLS=ON
    echo ""
fi

# ---------------------------------------------------------------------------
# Step 2: Build targets
# ---------------------------------------------------------------------------
TARGETS=()
if [[ "${MODE}" == "unit" || "${MODE}" == "full" ]]; then
    TARGETS+=(unit_tests_dbms)
fi
if [[ "${MODE}" == "smoke" || "${MODE}" == "stateless" || "${MODE}" == "stateful" || "${MODE}" == "full" ]]; then
    TARGETS+=("${CLICKHOUSE_BINARY}")
fi

echo "--- [2/4] Building: ${TARGETS[*]} ---"
cmake --build "${BUILD_DIR}" --target "${TARGETS[@]}" -- -j"${NPROC}"
echo ""

# Re-resolve after build: the stripped binary may now exist for the first time.
_resolve_bin_paths

# ---------------------------------------------------------------------------
# Step 3: Run tests, collecting .profraw files
# ---------------------------------------------------------------------------
mkdir -p "${PROFRAW_DIR}" "${REPORT_DIR}"

# Selectively clean .profraw files based on mode to allow incremental runs:
# - unit: only clean unit_tests_*.profraw
# - smoke/stateless/stateful: only clean ${CLICKHOUSE_BINARY}_*.profraw
# - full: clean all *.profraw
# - llvm_html/lcov_html: don't clean (use existing data)
case "${MODE}" in
    unit)
        echo "--- [3/4] Running tests (unit mode - preserving ${CLICKHOUSE_BINARY}_*.profraw) ---"
        rm -f "${PROFRAW_DIR}"/unit_tests_*.profraw
        ;;
    smoke|stateless|stateful)
        echo "--- [3/4] Running tests (${MODE} mode - preserving unit_tests_*.profraw) ---"
        rm -f "${PROFRAW_DIR}"/${CLICKHOUSE_BINARY}_*.profraw
        ;;
    full)
        echo "--- [3/4] Running tests (full mode - cleaning all .profraw) ---"
        rm -f "${PROFRAW_DIR}"/*.profraw
        ;;
esac

# Remove stale artefacts from previous runs
rm -f "${REPORT_DIR}/llvm_cov_show.log"

_start_default_proton() {
    local server_tag="${1:-coverage}"
    local test_tz="${2:-UTC}"

    COV_RUNTIME_ROOT="${REPO_ROOT}/${CLICKHOUSE_BINARY}-data"
    COV_STARTUP_LOG="${COV_RUNTIME_ROOT}/startup.log"
    COV_SERVER_PID=""

    # Kill any stale process holding COV_NODE_TCP before starting.
    local stale_pid
    stale_pid="$(lsof -ti ":${COV_NODE_TCP}" 2>/dev/null || true)"
    if [[ -n "${stale_pid}" ]]; then
        echo "  Killing stale process on port ${COV_NODE_TCP} (pid=${stale_pid}) …"
        kill -TERM ${stale_pid} 2>/dev/null || true
        sleep 1
        kill -KILL ${stale_pid} 2>/dev/null || true
    fi

    # Remove stale data directory to get a clean MetaStore on every run.
    rm -rf "${COV_RUNTIME_ROOT}"
    mkdir -p "${COV_RUNTIME_ROOT}"

    echo "  Starting coverage-instrumented ${CLICKHOUSE_BINARY} on default tcp=${COV_NODE_TCP} (${server_tag}) …"
    TZ="${test_tz}" \
    LLVM_PROFILE_FILE="${PROFRAW_DIR}/${CLICKHOUSE_BINARY}_%p.profraw" \
    env "${WATCHDOG_ENV_VAR}=0" \
        "${SERVER_BIN}" server >> "${COV_STARTUP_LOG}" 2>&1 &
    COV_SERVER_PID="$!"

    for (( i = 1; i <= 60; i++ )); do
        if [[ -n "${COV_SERVER_PID}" ]] && kill -0 "${COV_SERVER_PID}" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done

    if [[ -z "${COV_SERVER_PID}" ]] || ! kill -0 "${COV_SERVER_PID}" 2>/dev/null; then
        echo "  WARNING: ${CLICKHOUSE_BINARY} failed to start on default tcp=${COV_NODE_TCP}; skipping ${server_tag}." >&2
        tail -n 30 "${COV_STARTUP_LOG}" >&2 || true
        rm -rf "${COV_RUNTIME_ROOT}"
        return 1
    fi

    for (( i = 1; i <= 60; i++ )); do
        if "${SERVER_BIN}" client --port "${COV_NODE_TCP}" --query "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "  WARNING: ${CLICKHOUSE_BINARY} not ready after 60 s on default tcp=${COV_NODE_TCP}; skipping ${server_tag}." >&2
    tail -n 30 "${COV_STARTUP_LOG}" >&2 || true
    _stop_local_proton "${COV_SERVER_PID}" "${COV_RUNTIME_ROOT}"
    return 1
}

_stop_local_proton() {
    local server_pid="${1:-}"
    local runtime_root="${2:-}"

    if [[ -n "${server_pid}" ]]; then
        echo "  Stopping ${CLICKHOUSE_BINARY} (pid=${server_pid}) …"
        kill -TERM "${server_pid}" 2>/dev/null || true
        for (( i = 1; i <= 30; i++ )); do
            if ! kill -0 "${server_pid}" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "${server_pid}" 2>/dev/null; then
            kill -KILL "${server_pid}" 2>/dev/null || true
        fi
    fi

    if [[ -n "${runtime_root}" && -d "${runtime_root}" ]]; then
        rm -rf "${runtime_root}"
    fi
}

_run_sql_tests() {
    local sql_kind="$1"
    local reuse_server="${2:-0}"
    local log_file="${REPORT_DIR}/${sql_kind}_test_results.log"
    local mode_flag=""
    local test_tz="${COVERAGE_TEST_TZ:-UTC}"
    local sql_jobs
    if [[ -n "${SQL_TEST_JOBS:-}" ]]; then
        sql_jobs="${SQL_TEST_JOBS}"
    else
        sql_jobs=$(( NPROC / 2 > 1 ? NPROC / 2 : 1 ))
    fi
    local sql_timeout="${SQL_TEST_TIMEOUT:-90}"
    local sql_global_time_limit="${SQL_TEST_GLOBAL_TIME_LIMIT:-1800}"

    # Stateful coverage on instrumented binaries is noticeably slower.
    if [[ ( "${sql_kind}" == "stateful" || "${sql_kind}" == "both" ) && -z "${SQL_TEST_TIMEOUT:-}" ]]; then
        sql_timeout=180
    fi

    if [[ "${sql_kind}" != "stateless" && "${sql_kind}" != "stateful" && "${sql_kind}" != "both" ]]; then
        echo "  WARNING: unknown SQL test kind '${sql_kind}', skipping." >&2
        return 0
    fi
    if [[ ! -x "${SERVER_BIN}" ]]; then
        echo "  WARNING: ${SERVER_BIN} not found; skipping ${sql_kind} SQL tests." >&2
        return 0
    fi

    if [[ "${sql_kind}" == "stateless" ]]; then
        mode_flag="--no-stateful"
    elif [[ "${sql_kind}" == "stateful" ]]; then
        mode_flag="--no-stateless"
    fi
    # For "both", mode_flag remains "" — run all suites (stateless + stateful)

    local runtime_root=""
    local server_pid=""

    _prepare_stateful_test_data() {
        local data_dir="${STATEFUL_DATA_DIR:-${REPORT_DIR}/stateful_dataset}"
        local auto_download="${STATEFUL_DATA_AUTO_DOWNLOAD:-1}"
        local force_refresh="${STATEFUL_DATA_FORCE_REFRESH:-0}"
        local run_ck_tests="${REPO_ROOT}/tests/run_ck_tests.sh"
        local visits_tsv=""
        local hits_tsv=""
        local visits_xz=""
        local hits_xz=""
        local wrapper_dir=""
        local wrapper_bin=""
        local provision_script=""
        local provision_log=""
        local hits_exists="0"
        local visits_exists="0"
        local -a loader_client_cmd=()
        local old_path="${PATH}"

        mkdir -p "${data_dir}"
        visits_tsv="${data_dir}/visits_v1.tsv"
        hits_tsv="${data_dir}/hits_v1.tsv"
        visits_xz="${visits_tsv}.xz"
        hits_xz="${hits_tsv}.xz"

        if [[ "${force_refresh}" == "1" ]]; then
            rm -f "${visits_tsv}" "${hits_tsv}" "${visits_xz}" "${hits_xz}"
        fi

        if [[ ! -f "${visits_tsv}" || ! -f "${hits_tsv}" ]]; then
            if [[ "${auto_download}" != "1" ]]; then
                echo "  ERROR: ${data_dir} does not contain visits_v1.tsv and hits_v1.tsv." >&2
                echo "           Enable auto download with STATEFUL_DATA_AUTO_DOWNLOAD=1." >&2
                return 1
            fi
            if ! command -v curl >/dev/null 2>&1; then
                echo "  ERROR: curl not found; cannot auto-download stateful dataset." >&2
                return 1
            fi
            if ! command -v unxz >/dev/null 2>&1 && ! command -v xz >/dev/null 2>&1; then
                echo "  ERROR: neither unxz nor xz found; cannot decompress stateful dataset." >&2
                return 1
            fi

            echo "  Downloading stateful dataset into ${data_dir} …"
            if [[ ! -f "${hits_tsv}" ]]; then
                curl -L "https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz" -o "${hits_xz}" || true
                if [[ -f "${hits_xz}" ]]; then
                    if command -v unxz >/dev/null 2>&1; then
                        unxz -f "${hits_xz}" || true
                    else
                        xz -d -f "${hits_xz}" || true
                    fi
                fi
            fi
            if [[ ! -f "${visits_tsv}" ]]; then
                curl -L "https://datasets.clickhouse.com/visits/tsv/visits_v1.tsv.xz" -o "${visits_xz}" || true
                if [[ -f "${visits_xz}" ]]; then
                    if command -v unxz >/dev/null 2>&1; then
                        unxz -f "${visits_xz}" || true
                    else
                        xz -d -f "${visits_xz}" || true
                    fi
                fi
            fi
        fi

        if [[ ! -f "${visits_tsv}" || ! -f "${hits_tsv}" ]]; then
            echo "  ERROR: stateful dataset files are still missing in ${data_dir}." >&2
            return 1
        fi
        if [[ ! -f "${run_ck_tests}" ]]; then
            echo "  ERROR: ${run_ck_tests} not found; cannot auto-load stateful dataset." >&2
            return 1
        fi

        wrapper_dir="${runtime_root}/stateful_loader_bin"
        wrapper_bin="${wrapper_dir}/${CLICKHOUSE_BINARY}-client"
        provision_script="${REPORT_DIR}/stateful_provision_${COV_NODE_TCP}.sh"
        provision_log="${REPORT_DIR}/stateful_provision_${COV_NODE_TCP}.log"
        mkdir -p "${wrapper_dir}"

        cat > "${wrapper_bin}" <<'EOF'
#!/usr/bin/env bash
set -e
if [[ -n "${CLICKHOUSE_PASSWORD:-}" ]]; then
    exec "${SERVER_BIN}" client --port "${COV_NODE_TCP}" --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" "$@"
else
    exec "${SERVER_BIN}" client --port "${COV_NODE_TCP}" --user "${CLICKHOUSE_USER}" "$@"
fi
EOF
        chmod +x "${wrapper_bin}"

        cat > "${provision_script}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
EOF
        awk '
            /^echo "Provisioning test database"/, /--query="insert into test\.hits format TSV"/ {
                print
            }
        ' "${run_ck_tests}" >> "${provision_script}"
        chmod +x "${provision_script}"

        if ! grep -q 'Provisioning test database' "${provision_script}" \
            || ! grep -q 'insert into test.hits format TSV' "${provision_script}"; then
            echo "  ERROR: failed to extract stateful provisioning commands from ${run_ck_tests}" >&2
            return 1
        fi

        echo "  Loading stateful dataset from ${data_dir} into tcp=${COV_NODE_TCP} …"
        if ! PATH="${wrapper_dir}:${PATH}" \
            SERVER_BIN="${SERVER_BIN}" \
            COV_NODE_TCP="${COV_NODE_TCP}" \
            CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
            CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
            hits_tsv="${hits_tsv}" \
            visits_tsv="${visits_tsv}" \
                bash "${provision_script}" 2>&1 | tee "${provision_log}"; then
            PATH="${old_path}"
            echo "  ERROR: stateful dataset provisioning failed. See ${provision_log}" >&2
            return 1
        fi
        PATH="${old_path}"

        if grep -q "ACCESS_DENIED" "${provision_log}"; then
            echo "  WARNING: stateful dataset provisioning failed with ACCESS_DENIED." >&2
            echo "           Check CLICKHOUSE_USER/CLICKHOUSE_PASSWORD." >&2
            return 1
        fi

        loader_client_cmd=( "${SERVER_BIN}" client --port "${COV_NODE_TCP}" --user "${CLICKHOUSE_USER}" )
        if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
            loader_client_cmd+=( --password "${CLICKHOUSE_PASSWORD}" )
        fi
        hits_exists="$("${loader_client_cmd[@]}" --query "EXISTS TABLE test.hits" 2>>"${provision_log}" | tr -d '[:space:]' || echo 0)"
        visits_exists="$("${loader_client_cmd[@]}" --query "EXISTS TABLE test.visits" 2>>"${provision_log}" | tr -d '[:space:]' || echo 0)"
        if [[ "${hits_exists}" != "1" || "${visits_exists}" != "1" ]]; then
            echo "  ERROR: stateful test tables were not created (hits=${hits_exists}, visits=${visits_exists})." >&2
            echo "         See ${provision_log}" >&2
            return 1
        fi
    }

    if [[ "${reuse_server}" == "1" ]]; then
        runtime_root="${COV_RUNTIME_ROOT:-}"
        server_pid="${COV_SERVER_PID:-}"
        if [[ -z "${server_pid}" ]] || ! kill -0 "${server_pid}" 2>/dev/null; then
            echo "  WARNING: no reusable ${CLICKHOUSE_BINARY} instance for ${sql_kind} SQL tests; skipping." >&2
            return 1
        fi
    else
        if ! _start_default_proton "${sql_kind}" "${test_tz}"; then
            return 0
        fi
        runtime_root="${COV_RUNTIME_ROOT}"
        server_pid="${COV_SERVER_PID}"
    fi

    echo "  Server ready. Running ${sql_kind} SQL tests …"
    echo "    node_tcp=${COV_NODE_TCP}  table_http=${COV_TABLE_HTTP}"
    if [[ "${sql_kind}" == "stateful" || "${sql_kind}" == "both" ]]; then
        if ! _prepare_stateful_test_data; then
            if [[ "${reuse_server}" != "1" ]]; then
                _stop_local_proton "${server_pid}" "${runtime_root}"
            fi
            return 1
        fi
    fi

    # On macOS, fork() can fail with EAGAIN (errno 35) when the
    # coverage-instrumented server exhausts Mach VM regions.
    # Run with requested jobs first; on each fork failure halve the job count
    # and retry until jobs=1 (which bypasses multiprocessing.Pool entirely).
    local _cur_jobs
    _cur_jobs="${sql_jobs}"
    while true; do
        CLICKHOUSE_PORT_HTTP="${COV_TABLE_HTTP}" \
        CLICKHOUSE_PORT_TCP="${COV_NODE_TCP}" \
        CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
        CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
        TZ="${test_tz}" \
        python3 "${REPO_ROOT}/tests/ported-clickhouse-test.py" \
            --binary "${SERVER_BIN}" \
            --queries "${REPO_ROOT}/tests/queries_ported" \
            --jobs "${_cur_jobs}" \
            --timeout "${sql_timeout}" \
            --global_time_limit "${sql_global_time_limit}" \
            ${mode_flag:+"${mode_flag}"} \
            2>&1 | tee "${log_file}" || true

        if [[ "$(uname)" == "Darwin" && "${_cur_jobs}" -gt 1 ]] \
            && grep -qE "BlockingIOError|fork: Resource temporarily unavailable|\[Errno 35\]" \
               "${log_file}" 2>/dev/null; then
            _cur_jobs=$(( _cur_jobs / 2 ))
            (( _cur_jobs < 1 )) && _cur_jobs=1
            echo "  WARNING: fork() EAGAIN detected; retrying with --jobs ${_cur_jobs} …" >&2
        else
            break
        fi
    done

    if [[ "${sql_kind}" == "stateful" || "${sql_kind}" == "both" ]] \
        && grep -q "Won't run stateful tests because test data wasn't loaded." "${log_file}"; then
        echo "  WARNING: stateful tests were skipped (missing test.hits/test.visits data)." >&2
        echo "           See ${log_file} and tests/run_ck_tests.sh for data load flow." >&2
    fi

    if [[ "${reuse_server}" != "1" ]]; then
        _stop_local_proton "${server_pid}" "${runtime_root}"
    fi
    echo "  ${sql_kind} SQL tests done."
}

_run_unit_tests() {
    echo "  Running unit_tests_dbms …"
    LLVM_PROFILE_FILE="${PROFRAW_DIR}/unit_tests_%p.profraw" \
        "${UNIT_TEST_BIN}" \
        --gtest_output="xml:${REPORT_DIR}/unit_test_results.xml" \
        || true   # don't abort on test failures; we still want coverage data
    echo "  unit_tests_dbms done."
    echo ""
}

if [[ "${MODE}" == "unit" ]]; then
    _run_unit_tests
fi

if [[ "${MODE}" == "stateless" ]]; then
    _run_sql_tests stateless
    echo ""
fi

if [[ "${MODE}" == "stateful" ]]; then
    _run_sql_tests stateful
    echo ""
fi

# ---- Probe smoke tests (smoke/full mode) ----
if [[ "${MODE}" == "smoke" || "${MODE}" == "full" ]]; then
    _run_probe_smoke_tests() {
        local reuse_server="${1:-0}"
        local probe_bin=""
        local probe_os=""
        local probe_arch=""
        local deploy_dir="${REPO_ROOT}/tests/cluster/deployment/local"
        local smoke_case_dir="${REPO_ROOT}/tests/cluster/smoke"
        local probe_log="${REPORT_DIR}/probe_smoke_results.log"
        local probe_blacklist="${PROBE_BLACKLIST:-bug,todo,skip}"
        local probe_suite="${PROBE_SUITE:-}"
        local probe_case="${PROBE_CASE:-}"
        local probe_nodes="${PROBE_NODES:-1}"
        local probe_rc=0
        local server_started=0

        local probe_extracted=0
        if command -v probe >/dev/null 2>&1; then
            probe_bin="$(command -v probe)"
        elif command -v docker >/dev/null 2>&1; then
            echo "  probe not found in PATH; pulling timeplus/probe:latest via Docker …"
            if docker pull timeplus/probe:latest >/dev/null 2>&1; then
                probe_bin="$(mktemp /tmp/probe.XXXXXX)"
                docker run --rm --entrypoint cat timeplus/probe:latest /usr/local/bin/probe \
                    > "${probe_bin}" 2>/dev/null
                chmod +x "${probe_bin}"
                if [[ ! -s "${probe_bin}" ]]; then
                    echo "  WARNING: failed to extract probe binary from Docker image; skipping." >&2
                    rm -f "${probe_bin}"
                    return 0
                fi
                probe_extracted=1
                echo "  probe extracted from timeplus/probe:latest."
            else
                echo "  WARNING: failed to pull timeplus/probe:latest; skipping probe smoke tests." >&2
                return 0
            fi
        fi
        if [[ -z "${probe_bin}" ]]; then
            echo "  WARNING: probe not found and docker unavailable; skipping probe smoke tests." >&2
            return 0
        fi

        if [[ ! -d "${smoke_case_dir}" ]]; then
            echo "  WARNING: smoke case directory not found; skipping probe smoke tests." >&2
            return 0
        fi

        probe_os="$(uname | tr '[:upper:]' '[:lower:]')"
        case "$(uname -m)" in
            arm64|aarch64) probe_arch="aarch64" ;;
            x86_64|amd64) probe_arch="x86_64" ;;
            *) probe_arch="$(uname -m)" ;;
        esac

        if [[ ! -x "${SERVER_BIN}" ]]; then
            echo "  WARNING: ${CLICKHOUSE_BINARY} client binary not found; skipping probe smoke tests." >&2
            return 0
        fi

        if [[ "${reuse_server}" == "1" ]]; then
            if [[ -z "${COV_SERVER_PID:-}" ]] || ! kill -0 "${COV_SERVER_PID}" 2>/dev/null; then
                echo "  WARNING: no reusable ${CLICKHOUSE_BINARY} instance for probe smoke tests; skipping." >&2
                return 1
            fi
        else
            if ! _start_default_proton "probe smoke" "${COVERAGE_TEST_TZ:-UTC}"; then
                echo "  WARNING: failed to start local ${CLICKHOUSE_BINARY} on default port; skipping probe smoke tests." >&2
                return 0
            fi
            server_started=1
        fi

        local probe_cmd=(
            "${probe_bin}" smoke "${smoke_case_dir}"
            -v
            -d "${deploy_dir}"
            -c p1k1
            --variable "nodes=${probe_nodes}"
            -b "${probe_blacklist}"
            -o "${probe_os}"
            -a "${probe_arch}"
        )
        if [[ -n "${probe_suite}" ]]; then
            probe_cmd+=(-s "${probe_suite}")
        fi
        if [[ -n "${probe_case}" ]]; then
            probe_cmd+=(-k "${probe_case}")
        fi

        echo "  Running local probe smoke tests (cluster=p1k1) …"
        set +e
        "${probe_cmd[@]}" 2>&1 | tee "${probe_log}"
        probe_rc="${PIPESTATUS[0]}"
        set -e
        if [[ "${probe_rc}" -ne 0 ]]; then
            echo "  WARNING: probe smoke exited with code ${probe_rc}. See ${probe_log}" >&2
        fi
        echo "  Probe smoke tests done. Log: ${probe_log}"

        if [[ "${server_started}" -eq 1 ]]; then
            _stop_local_proton "${COV_SERVER_PID}" "${COV_RUNTIME_ROOT}"
        fi
        if [[ "${probe_extracted}" -eq 1 ]]; then
            rm -f "${probe_bin}"
        fi
    }

    if [[ "${MODE}" == "smoke" ]]; then
        _run_probe_smoke_tests
    elif [[ "${MODE}" == "full" ]]; then
        unit_rc=0
        sql_rc=0
        smoke_rc=0
        shared_server_started=0

        echo "  Full mode sequential plan:"
        echo "    step-1: unit"
        echo "    step-2: start shared server"
        echo "    step-3: sql (stateless+stateful, shared server)"
        echo "    step-4: smoke (shared server)"

        set +e
        _run_unit_tests
        unit_rc=$?
        if [[ "${unit_rc}" -ne 0 ]]; then
            echo "  WARNING: unit step failed with code ${unit_rc}" >&2
        fi

        if _start_default_proton "full (sql+smoke)" "${COVERAGE_TEST_TZ:-UTC}"; then
            shared_server_started=1
        else
            echo "  WARNING: failed to start shared local ${CLICKHOUSE_BINARY} for full mode." >&2
            sql_rc=1
            smoke_rc=1
        fi

        if [[ "${shared_server_started}" -eq 1 ]]; then
            _run_sql_tests both 1
            sql_rc=$?
            if [[ "${sql_rc}" -ne 0 ]]; then
                echo "  WARNING: sql step failed with code ${sql_rc}" >&2
            fi
            echo ""

            _run_probe_smoke_tests 1
            smoke_rc=$?
            if [[ "${smoke_rc}" -ne 0 ]]; then
                echo "  WARNING: smoke step failed with code ${smoke_rc}" >&2
            fi
            _stop_local_proton "${COV_SERVER_PID}" "${COV_RUNTIME_ROOT}"
        fi
        set -e
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: Merge .profraw → .profdata and generate report
# ---------------------------------------------------------------------------
echo "--- [4/4] Generating coverage report ---"

PROFRAW_FILES=( "${PROFRAW_DIR}"/*.profraw )
if [[ ! -e "${PROFRAW_FILES[0]}" ]]; then
    echo "WARNING: No .profraw files found in ${PROFRAW_DIR}." >&2
    echo "  Tests were skipped or produced no coverage data." >&2
    echo "  Hint: ensure ${CLICKHOUSE_BINARY} can start on default tcp 8463 and smoke prerequisites are available." >&2
    echo "  No coverage report generated."
    exit 0
fi

echo "  Merging ${#PROFRAW_FILES[@]} .profraw file(s) …"
PROFDATA="${REPORT_DIR}/merged.profdata"
"${LLVM_PROFDATA}" merge -sparse "${PROFRAW_FILES[@]}" -o "${PROFDATA}"

# Auto-detect which binaries are needed based on existing .profraw files.
# This allows incremental runs (e.g., unit then stateless) to merge correctly.
OBJECT_FLAGS=()
echo "  Detecting coverage binaries from .profraw files:"
if compgen -G "${PROFRAW_DIR}/unit_tests_*.profraw" >/dev/null 2>&1 && [[ -x "${UNIT_TEST_BIN}" ]]; then
    OBJECT_FLAGS+=(-object "${UNIT_TEST_BIN}")
    echo "    ✓ unit_tests_dbms ($(ls "${PROFRAW_DIR}"/unit_tests_*.profraw 2>/dev/null | wc -l | tr -d ' ') files)"
fi
if compgen -G "${PROFRAW_DIR}/${CLICKHOUSE_BINARY}_*.profraw" >/dev/null 2>&1 && [[ -x "${SERVER_BIN}" ]]; then
    OBJECT_FLAGS+=(-object "${SERVER_BIN}")
    echo "    ✓ ${CLICKHOUSE_BINARY} ($(ls "${PROFRAW_DIR}"/${CLICKHOUSE_BINARY}_*.profraw 2>/dev/null | wc -l | tr -d ' ') files)"
fi
if [[ "${#OBJECT_FLAGS[@]}" -eq 0 ]]; then
    echo "ERROR: No coverage objects found under ${BUILD_DIR}" >&2
    exit 1
fi

# Exclude non-src directories; build stores paths without src/ prefix (e.g. Storages/..., Functions/...),
# so --ignore-filename-regex is the only filter needed. awk just accumulates the remaining entries.
SRC_EXCLUDE_REGEX='(contrib|programs|base|cmake|utils|tests|debian|docker)/'
echo ""
echo "=== Coverage Summary (src/ only) ==="
if ! _build_llvm_cov_show_args; then
    echo "ERROR: failed to prepare llvm-cov object flags for report" >&2
    exit 1
fi

"${LLVM_COV}" report \
    "${COV_SHOW_MAIN_BIN}" \
    ${COV_SHOW_EXTRA_FLAGS[@]+"${COV_SHOW_EXTRA_FLAGS[@]}"} \
    -instr-profile="${PROFDATA}" \
    --ignore-filename-regex="${SRC_EXCLUDE_REGEX}" \
    2>/dev/null \
  | awk '
    /^Filename/ || /^--/ { print; next }
    /^TOTAL/ { next }
    {
        print
        reg  += $2;  mreg += $3
        fun  += $5;  mfun += $6
        lin  += $8;  mlin += $9
        br   += $11; mbr  += $12
    }
    END {
        cvr_reg = (reg > 0) ? (reg - mreg) * 100.0 / reg : 0
        cvr_fun = (fun > 0) ? (fun - mfun) * 100.0 / fun : 0
        cvr_lin = (lin > 0) ? (lin - mlin) * 100.0 / lin : 0
        cvr_br  = (br  > 0) ? (br  - mbr)  * 100.0 / br  : 0
        printf "%-105s %8d %17d %8.2f%%   %9d %17d %8.2f%%   %9d %13d %8.2f%%   %9d %17d %8.2f%%\n",
            "TOTAL(src/)", reg, mreg, cvr_reg, fun, mfun, cvr_fun,
            lin, mlin, cvr_lin, br, mbr, cvr_br
    }
  ' | tee "${REPORT_DIR}/coverage_summary.txt"

if [[ "${DO_HTML}" -eq 1 ]]; then
    LCOV_RAW="${REPORT_DIR}/coverage.info.raw"
    LCOV_CLEAN="${REPORT_DIR}/coverage.info"
    REPORT_TITLE="${CLICKHOUSE_BINARY} Coverage (${MODE})"
    echo ""

    # Generate LLVM HTML if llvm-cov is available
    if [[ -x "${LLVM_COV}" ]]; then
        LLVM_HTML_DIR="${REPORT_DIR}/llvm_html"
        echo "  Generating native LLVM HTML report → ${LLVM_HTML_DIR} …"
        mkdir -p "${LLVM_HTML_DIR}"

        if _build_llvm_cov_show_args; then
            "${LLVM_COV}" show \
                "${COV_SHOW_MAIN_BIN}" \
                ${COV_SHOW_EXTRA_FLAGS[@]+"${COV_SHOW_EXTRA_FLAGS[@]}"} \
                -instr-profile="${PROFDATA}" \
                --compilation-dir="${REPO_ROOT}" \
                --ignore-filename-regex="${SRC_EXCLUDE_REGEX}" \
                --format=html \
                --output-dir="${LLVM_HTML_DIR}" \
                --show-line-counts-or-regions \
                2>&1 || echo "    Warning: LLVM HTML generation encountered errors (non-fatal)"
        else
            echo "    WARNING: failed to prepare llvm-cov object flags, skipping LLVM HTML" >&2
        fi
    else
        echo "  WARNING: llvm-cov not found, skipping LLVM HTML report" >&2
    fi

    # Generate LCOV HTML if genhtml is available
    if [[ -n "${GENHTML}" && -x "${LLVM_COV}" ]]; then
        # Step 1: Export LCOV — exclude base/, contrib/, and generated pb files
        # All stored paths start with build_coverage/; the ignore regex is a substring match.
        echo "    Exporting LCOV data …"
        "${LLVM_COV}" export \
            "${OBJECT_FLAGS[@]}" \
            -instr-profile="${PROFDATA}" \
            --format=lcov \
            --ignore-filename-regex='(contrib|programs|base|cmake|utils|tests|debian|docker|build_coverage/build_coverage)/' \
            2>/dev/null > "${LCOV_RAW}"

        # Step 2: Normalize source paths and optionally keep only src/ records.
        echo "    Cleaning paths …"
        awk -v build_dir="${BUILD_DIR}" -v scope="${HTML_SCOPE}" '
            BEGIN {
                keep = 0
                want_src = (scope != "all")
            }
            /^SF:/ {
                path = substr($0, 4)
                if (index(path, build_dir "/") == 1) {
                    path = substr(path, length(build_dir) + 2)
                }
                if (index(path, "build_coverage/") == 1) {
                    path = substr(path, length("build_coverage/") + 1)
                }
                keep = (!want_src || index(path, "src/") == 1)
                if (keep) {
                    print "SF:" path
                }
                next
            }
            {
                if (keep) {
                    print
                }
                if ($0 == "end_of_record") {
                    keep = 0
                }
            }
        ' "${LCOV_RAW}" > "${LCOV_CLEAN}"
        rm -f "${LCOV_RAW}"

        # Step 3: genhtml — renders directory hierarchy with collapsible navigation
        LCOV_HTML_DIR="${REPORT_DIR}/lcov_html"
        echo "    Rendering LCOV HTML report → ${LCOV_HTML_DIR} …"
        rm -rf "${LCOV_HTML_DIR}"
        "${GENHTML}" \
            "${LCOV_CLEAN}" \
            --output-directory "${LCOV_HTML_DIR}" \
            --title "${REPORT_TITLE}" \
            --show-details \
            --branch-coverage \
            --legend \
            --ignore-errors inconsistent,corrupt,unsupported \
            2>"${REPORT_DIR}/genhtml.log"

        if [[ -s "${REPORT_DIR}/genhtml.log" ]]; then
            # (inconsistent) and (unsupported) are expected with LLVM-generated LCOV data;
            # only surface genuine errors or unexpected warnings.
            _unexpected=$(grep -i "error\|warning" "${REPORT_DIR}/genhtml.log" \
                          | grep -v "(inconsistent)\|(unsupported)" || true)
            if [[ -n "${_unexpected}" ]]; then
                echo "  WARNING: genhtml reported unexpected issues (see ${REPORT_DIR}/genhtml.log):" >&2
                echo "${_unexpected}" | head -5 >&2
            fi
        fi
    else
        if [[ -z "${GENHTML}" ]]; then
            echo "  WARNING: genhtml not found, skipping LCOV HTML report" >&2
            echo "           Install with: brew install lcov" >&2
        fi
        if [[ ! -x "${LLVM_COV}" ]]; then
            echo "  WARNING: llvm-cov not found, skipping LCOV export" >&2
        fi
    fi
fi

echo ""
echo "=== Done ==="
echo "  .profraw files : ${PROFRAW_DIR}/"
echo "  .profdata      : ${PROFDATA}"
echo "  Text summary   : ${REPORT_DIR}/coverage_summary.txt"
if [[ "${DO_HTML}" -eq 1 ]]; then
    [[ -f "${REPORT_DIR}/coverage.info" ]] && \
        echo "  LCOV data      : ${REPORT_DIR}/coverage.info"
    [[ -f "${REPORT_DIR}/llvm_html/index.html" ]] && \
        echo "  LLVM HTML      : ${REPORT_DIR}/llvm_html/index.html"
    [[ -f "${REPORT_DIR}/lcov_html/index.html" ]] && \
        echo "  LCOV HTML      : ${REPORT_DIR}/lcov_html/index.html"
fi
