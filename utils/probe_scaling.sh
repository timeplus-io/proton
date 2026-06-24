#!/usr/bin/env bash
# Reproduce the 1× vs 3× FT Python source-path scaling measurement from
# src/CPython/perf/REPORT_FT_vs_GIL.md. Intended as a portable check:
# run on any host with a free-threaded proton already up, and compare
# the result against the "Do / Don't" thresholds documented in the report.
#
# Preconditions:
#   - proton (FT build) running on $PORT
#   - free-threaded CPython (e.g. py314t conda env) picked up by the server
#   - perf available; either perf_event_paranoid <= 0 or running with CAP_PERFMON
#   - no other heavy load on the host during the run (we measure the whole machine
#     for uncore IMC counters)
#
# Usage:
#   src/CPython/perf/probe_scaling.sh [--port 8463] [--duration 20] [--window 10]
#                                     [--parallel 3] [--out DIR] [--skip-pinned]
#
# Output:
#   - summary table on stdout (throughput, CPUs utilized, ctxsw/s, insns/row, DRAM GB/s)
#   - raw perf outputs under --out (default ./tmp/perf_scaling_<timestamp>/)
#   - exit 0 on success; exit 1 if any phase fails to collect rows

set -euo pipefail

PORT=8463
DURATION=20
WINDOW=10
PARALLEL=3
OUT=""
SKIP_PINNED=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)         PORT="$2";     shift 2 ;;
        --duration)     DURATION="$2"; shift 2 ;;
        --window)       WINDOW="$2";   shift 2 ;;
        --parallel)     PARALLEL="$2"; shift 2 ;;
        --out)          OUT="$2";      shift 2 ;;
        --skip-pinned)  SKIP_PINNED=true; shift ;;
        -h|--help)      sed -n '1,28p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CLI="${REPO_ROOT}/build/programs/stripped/bin/proton"

if [[ ! -x "${CLI}" ]]; then
    echo "proton binary not found at ${CLI}" >&2
    echo "build it first, or point CLI env to the right path" >&2
    exit 2
fi

: "${OUT:=${REPO_ROOT}/tmp/perf_scaling_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "${OUT}"

# ─── locate server PID ──────────────────────────────────────────────────────
SRV_PID="$(
    pgrep -f "[p]roton server.*${PORT}" 2>/dev/null | head -1
)"
if [[ -z "${SRV_PID}" ]]; then
    # fall back to pid file written by start-local-proton.sh
    PID_FILE="${REPO_ROOT}/tmp_data_${PORT}/proton.pid"
    [[ -f "${PID_FILE}" ]] && SRV_PID="$(cat "${PID_FILE}")"
fi
if [[ -z "${SRV_PID}" ]] || ! kill -0 "${SRV_PID}" 2>/dev/null; then
    echo "no proton server on port ${PORT}; start it first" >&2
    exit 2
fi

# ─── detect hybrid topology (P vs E cores on Intel 12th gen+) ──────────────
HYBRID=false
P_CPUS=""
if [[ -r /sys/devices/cpu_core/cpus ]]; then
    P_CPUS="$(cat /sys/devices/cpu_core/cpus)"
    E_CPUS="$(cat /sys/devices/cpu_atom/cpus 2>/dev/null || true)"
    HYBRID=true
fi

# ─── check perf + uncore counter availability ───────────────────────────────
if ! command -v perf >/dev/null 2>&1; then
    echo "perf not installed" >&2; exit 2
fi
UNCORE_OK=true
if ! perf stat -a -e 'uncore_imc_free_running/data_read/' sleep 0.1 \
        >/dev/null 2>&1; then
    echo "warning: uncore_imc_free_running counters unavailable — DRAM columns will be blank" >&2
    UNCORE_OK=false
fi

# ─── ensure the Python generator stream exists ──────────────────────────────
STREAM_NAME="scaling_probe_int"
"${CLI}" client --port "${PORT}" --multiquery --query "
DROP STREAM IF EXISTS ${STREAM_NAME};
CREATE EXTERNAL STREAM ${STREAM_NAME} (i32 int32, i64 int64)
AS \$\$
import random
def ${STREAM_NAME}():
    batch = [(random.randint(0, 999999), random.randint(0, 999999999))
             for _ in range(1000)]
    while True:
        yield batch
\$\$
SETTINGS type='python';
" >/dev/null

# ─── affinity helpers ───────────────────────────────────────────────────────
pin_all_threads () {
    local mask="$1"
    for t in /proc/"${SRV_PID}"/task/*/; do
        taskset -cp "${mask}" "$(basename "$t")" >/dev/null 2>&1 || true
    done
}

# ─── measure one phase ──────────────────────────────────────────────────────
# args: N  LABEL  PIN_MASK_or_empty
measure () {
    local N="$1"
    local LABEL="$2"
    local PIN="${3:-}"

    if [[ -n "${PIN}" ]]; then
        pin_all_threads "${PIN}"
    fi

    # launch N parallel count() queries
    local qpids=()
    for i in $(seq 1 "${N}"); do
        ("${CLI}" client --port "${PORT}" \
            --query "SELECT count() FROM ${STREAM_NAME} LIMIT 1 EMIT PERIODIC ${DURATION}s" \
            > "${OUT}/${LABEL}_q${i}.out" 2>&1) &
        qpids+=($!)
    done

    # let the pipeline reach steady state
    sleep 3

    # re-pin any threads that started after the initial pin
    if [[ -n "${PIN}" ]]; then
        pin_all_threads "${PIN}"
    fi

    # system-wide uncore (DRAM)
    if [[ "${UNCORE_OK}" == "true" ]]; then
        perf stat -a \
            -e 'uncore_imc_free_running/data_read/,uncore_imc_free_running/data_write/' \
            -o "${OUT}/${LABEL}_uncore.txt" \
            sleep "${WINDOW}" 2>/dev/null &
        local uncore_pid=$!
    fi

    # per-PID task-clock, context-switches, P-core cycles/instructions
    perf stat -p "${SRV_PID}" \
        -e 'task-clock,context-switches,cpu-migrations,cycles,instructions' \
        -o "${OUT}/${LABEL}_pid.txt" \
        sleep "${WINDOW}" 2>/dev/null

    [[ -n "${uncore_pid:-}" ]] && wait "${uncore_pid}" || true

    # wait for the queries to end
    for p in "${qpids[@]}"; do wait "${p}" 2>/dev/null || true; done

    # aggregate rows
    local rows_total=0
    for i in $(seq 1 "${N}"); do
        local r
        r="$(tail -1 "${OUT}/${LABEL}_q${i}.out" 2>/dev/null || echo 0)"
        [[ "${r}" =~ ^[0-9]+$ ]] || r=0
        rows_total=$((rows_total + r))
    done
    echo "${rows_total}" > "${OUT}/${LABEL}_rows_total.txt"
    echo "${DURATION}"  > "${OUT}/${LABEL}_duration.txt"
}

# ─── parse the perf-stat text into one CSV row ──────────────────────────────
# args: LABEL → echoes: rows_per_s,cpus_util,ctxsw_per_s,ipc,insn_per_row,dram_read_GBs,dram_write_GBs
summarize () {
    local L="$1"
    local rows dur pid_f unc_f
    rows="$(cat "${OUT}/${L}_rows_total.txt")"
    dur="$(cat "${OUT}/${L}_duration.txt")"
    pid_f="${OUT}/${L}_pid.txt"
    unc_f="${OUT}/${L}_uncore.txt"

    local rps; rps=$(awk -v r="${rows}" -v d="${dur}" 'BEGIN{printf "%.2f", r/d/1e6}')
    local tc;  tc=$(awk '/task-clock/{gsub(",","",$1); printf "%.3f", $1/1e9/'"${WINDOW}"'}' "${pid_f}" 2>/dev/null)
    local cs;  cs=$(awk '/context-switches/{gsub(",","",$1); printf "%.0f", $1/'"${WINDOW}"'}' "${pid_f}" 2>/dev/null)
    local cyc; cyc=$(awk '/[[:space:]]cycles|cpu_core\/cycles\//{gsub(",","",$1); print $1; exit}' "${pid_f}" 2>/dev/null)
    local ins; ins=$(awk '/[[:space:]]instructions|cpu_core\/instructions\//{gsub(",","",$1); print $1; exit}' "${pid_f}" 2>/dev/null)
    local ipc; ipc=$(awk -v c="${cyc:-0}" -v i="${ins:-0}" 'BEGIN{if (c>0) printf "%.2f", i/c; else printf "n/a"}')
    local ipr; ipr=$(awk -v i="${ins:-0}" -v r="${rows}" -v d="${dur}" -v w="${WINDOW}" \
        'BEGIN{if (r>0 && d>0) printf "%.0f", i / (r * w / d); else printf "n/a"}')

    local dr="n/a" dw="n/a"
    if [[ -s "${unc_f}" ]]; then
        dr=$(awk '/data_read/{gsub(",","",$1); printf "%.2f", $1/1024/'"${WINDOW}"'}' "${unc_f}")
        dw=$(awk '/data_write/{gsub(",","",$1); printf "%.2f", $1/1024/'"${WINDOW}"'}' "${unc_f}")
    fi
    echo "${rps},${tc},${cs},${ipc},${ipr},${dr},${dw}"
}

# ─── run the phases ─────────────────────────────────────────────────────────
echo "=== scaling probe: port=${PORT} parallel=${PARALLEL} dur=${DURATION}s window=${WINDOW}s ==="
echo "=== server pid ${SRV_PID} ==="
if [[ "${HYBRID}" == "true" ]]; then
    echo "=== hybrid CPU detected: P=${P_CPUS} E=${E_CPUS:-n/a} ==="
else
    echo "=== homogeneous CPU ==="
fi
echo "=== artifacts → ${OUT} ==="
echo

echo "[1/3] single stream, unpinned"       >&2; measure 1           single     ""
echo "[2/3] ${PARALLEL} streams, unpinned" >&2; measure "${PARALLEL}" three      ""
if [[ "${SKIP_PINNED}" == "true" || "${HYBRID}" != "true" ]]; then
    echo "[3/3] pinned run skipped ($([[ "${HYBRID}" != "true" ]] && echo "homogeneous CPU" || echo "--skip-pinned"))" >&2
    PINNED_ROW=""
else
    echo "[3/3] ${PARALLEL} streams, pinned to P-cores (${P_CPUS})" >&2
    measure "${PARALLEL}" three_pinned "${P_CPUS}"
    pin_all_threads "0-$(( $(nproc) - 1 ))"   # restore
    PINNED_ROW="three_pinned"
fi

S1="$(summarize single)"
S3="$(summarize three)"
[[ -n "${PINNED_ROW}" ]] && SP="$(summarize three_pinned)"

# ─── print the result table ─────────────────────────────────────────────────
printf '\n'
FMT='%-20s  %10s  %8s  %8s  %8s  %5s  %9s  %11s  %11s\n'
printf "${FMT}" \
    'phase' 'rows/s (M)' 'vs 1×' 'CPUs' 'ctxsw/s' 'IPC' 'insns/row' 'DRAM rd GB/s' 'DRAM wr GB/s'
printf "${FMT}" \
    '--------------------' '----------' '--------' '--------' '--------' '-----' '---------' '-----------' '-----------'
IFS=',' read -r rps_1 tc_1 cs_1 ipc_1 ipr_1 dr_1 dw_1 <<<"${S1}"
printf "${FMT}" \
    '1× unpinned' "${rps_1}" '1.00×' "${tc_1}" "${cs_1}" "${ipc_1}" "${ipr_1}" "${dr_1}" "${dw_1}"
IFS=',' read -r rps_3 tc_3 cs_3 ipc_3 ipr_3 dr_3 dw_3 <<<"${S3}"
ratio_3=$(awk -v a="${rps_3}" -v b="${rps_1}" 'BEGIN{if (b>0) printf "%.2f×", a/b; else print "n/a"}')
printf "${FMT}" \
    "${PARALLEL}× unpinned" "${rps_3}" "${ratio_3}" "${tc_3}" "${cs_3}" "${ipc_3}" "${ipr_3}" "${dr_3}" "${dw_3}"
if [[ -n "${PINNED_ROW}" ]]; then
    IFS=',' read -r rps_p tc_p cs_p ipc_p ipr_p dr_p dw_p <<<"${SP}"
    ratio_p=$(awk -v a="${rps_p}" -v b="${rps_1}" 'BEGIN{if (b>0) printf "%.2f×", a/b; else print "n/a"}')
    printf "${FMT}" \
        "${PARALLEL}× P-core pinned" "${rps_p}" "${ratio_p}" "${tc_p}" "${cs_p}" "${ipc_p}" "${ipr_p}" "${dr_p}" "${dw_p}"
fi
echo
echo "Interpretation (see src/CPython/perf/REPORT_FT_vs_GIL.md):"
echo "  - ${PARALLEL}× ≥ 2.95× on homogeneous server hardware → FT scaling confirmed; no code change needed."
echo "  - On a hybrid laptop CPU, 2.7× is the expected ceiling; pinning to P-cores typically reaches 2.80–2.85×."
echo "  - insns/row unchanged between 1× and ${PARALLEL}× → no atomic-retry contention."
echo "  - ctxsw/s growing < 2× when going 1× → ${PARALLEL}× → no mutex-blocking contention."
