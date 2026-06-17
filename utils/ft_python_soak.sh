#!/usr/bin/env bash
# Soak test for free-threaded (PEP 703) embedded CPython.
# Verifies:
#   - bounded RSS under sustained Python-source + passthrough load
#   - stable cancellation/teardown of streaming queries
#   - NonJemallocMemory trend (exposes mimalloc/Python heap drift)
#
# Usage:
#   utils/ft_python_soak.sh [DURATION_SEC] [PORT]
# Defaults: DURATION_SEC=3600 (1h), PORT from running server.

set -euo pipefail

DURATION="${1:-3600}"
PORT="${2:-}"

if [[ -z "$PORT" ]]; then
    PID_FILE=$(ls -1 tmp_data_*/proton.pid 2>/dev/null | head -n1 || true)
    [[ -n "$PID_FILE" ]] || { echo "No running proton; pass PORT as arg 2."; exit 2; }
    PORT="${PID_FILE#tmp_data_}"; PORT="${PORT%/proton.pid}"
fi

PROTON="${PROTON:-build/programs/stripped/bin/proton}"
OUT_DIR="tmp/ft_soak_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/metrics.csv"
ERRLOG="$OUT_DIR/errors.log"
echo "ts,rss,jemalloc_resident,non_jemalloc,memory_tracker,memory_tracker_peak" > "$LOG"

echo "soak: port=$PORT duration=${DURATION}s out=$OUT_DIR"

client() { "$PROTON" client --port "$PORT" --query "$1" 2>>"$ERRLOG"; }

# --- streams ---
client "DROP STREAM IF EXISTS soak_tgt"
client "CREATE STREAM soak_tgt (i int32, s string)"
client "DROP STREAM IF EXISTS soak_src"
cat > "$OUT_DIR/soak_src.sql" <<SQLEOF
CREATE EXTERNAL STREAM soak_src (i int32, s string) AS \$\$
_client = None
def _get():
    global _client
    if _client is None:
        from proton_driver import Client
        _client = Client(host='127.0.0.1', port=$PORT)
    return _client
def soak_write(i, s):
    _get().execute('INSERT INTO soak_tgt (i, s) VALUES', list(zip(i, s)))
\$\$
SETTINGS type='python', write_function_name='soak_write'
SQLEOF
"$PROTON" client --port "$PORT" --queries-file "$OUT_DIR/soak_src.sql" 2>>"$ERRLOG"

client "DROP STREAM IF EXISTS soak_gen"
client "CREATE RANDOM STREAM soak_gen (i int32 DEFAULT rand(), s string DEFAULT to_string(rand()))"

# --- load: source workload (Py->C++) ---
for i in 1 2 3; do
    "$PROTON" client --port "$PORT" \
      --query "SELECT count() FROM soak_gen EMIT PERIODIC 10s" \
      > "$OUT_DIR/src_${i}.log" 2>&1 &
done

# --- load: passthrough workload (C++->Py->proton) ---
"$PROTON" client --port "$PORT" \
  --query "INSERT INTO soak_src SELECT i, s FROM soak_gen SETTINGS eps=50000" \
  > "$OUT_DIR/passthru.log" 2>&1 &

# --- metrics poll loop ---
end=$(( $(date +%s) + DURATION ))
while [[ $(date +%s) -lt $end ]]; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    out=$(client "SELECT sum(if(metric='MemoryResident',value,0))::int64 rss,
                         sum(if(metric='jemalloc.resident',value,0))::int64 jer,
                         sum(if(metric='NonJemallocMemory',value,0))::int64 njm,
                         sum(if(metric='MemoryTracker.Amount',value,0))::int64 mt,
                         sum(if(metric='MemoryTracker.Peak',value,0))::int64 mtp
                  FROM system.asynchronous_metrics FORMAT CSV") || out="0,0,0,0,0"
    echo "$ts,$out" >> "$LOG"
    sleep 30
done

# --- teardown ---
pkill -INT -P $$ 2>/dev/null || true
sleep 2
client "DROP STREAM IF EXISTS soak_src"
client "DROP STREAM IF EXISTS soak_tgt"
client "DROP STREAM IF EXISTS soak_gen"

# --- summary ---
python3 - "$LOG" <<'PYEOF'
import csv, sys, statistics
rows = list(csv.DictReader(open(sys.argv[1])))
if len(rows) < 2: sys.exit("soak produced no samples")
rss=[int(r['rss']) for r in rows if r['rss'].isdigit()]
njm=[int(r['non_jemalloc']) for r in rows if r['non_jemalloc'].isdigit()]
print(f"samples         : {len(rows)}")
print(f"RSS start/max/end : {rss[0]/1e6:.1f} / {max(rss)/1e6:.1f} / {rss[-1]/1e6:.1f} MB")
print(f"NonJemalloc s/m/e : {njm[0]/1e6:.1f} / {max(njm)/1e6:.1f} / {njm[-1]/1e6:.1f} MB")
print(f"RSS drift (end-start): {(rss[-1]-rss[0])/1e6:+.1f} MB")
PYEOF

echo "soak done: $OUT_DIR"
