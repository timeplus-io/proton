#!/usr/bin/env bash
# Regression: after >= 2 MV recovery cycles, query memory_tracker peak must
# stay bounded — phantom residue would inflate it monotonically across cycles.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CURL="${CLICKHOUSE_CURL} -sS"
[[ -n "${CLICKHOUSE_USER:-}" && -n "${CLICKHOUSE_PASSWORD:-}" ]] \
    && CURL+=" -u ${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}"
CURL+=" ${CLICKHOUSE_URL}"

MV=99300_oom_mv
SRC=99300_src
SINK=99300_sink

run()   { ${CURL} --data-binary "$1" > /dev/null; }
query() { ${CURL} --data-binary "$1"; }

cleanup() {
  [[ -n "${INSERT_PID:-}" ]] && kill "$INSERT_PID" 2>/dev/null && wait "$INSERT_PID" 2>/dev/null
  run "DROP VIEW IF EXISTS ${MV}"
  run "DROP STREAM IF EXISTS ${SINK}"
  run "DROP STREAM IF EXISTS ${SRC}"
}
trap cleanup EXIT

run "DROP VIEW IF EXISTS ${MV}"
run "DROP STREAM IF EXISTS ${SINK}"
run "DROP STREAM IF EXISTS ${SRC}"
run "CREATE STREAM ${SRC}(k string, v int64)"
run "CREATE STREAM ${SINK}(k string, total int64)"
run "CREATE MATERIALIZED VIEW ${MV} INTO ${SINK} AS
  SELECT k, sum(v) AS total FROM ${SRC} GROUP BY k EMIT PERIODIC 100ms
  SETTINGS max_memory_usage = '64Mi'"
sleep 1

(
  for i in $(seq 1 30); do
    off=$(( i * 600000 ))
    run "INSERT INTO ${SRC}(k, v) SELECT to_string(number + $off) AS k, to_int64(number) AS v FROM numbers(600000)"
    sleep 1
  done
) &
INSERT_PID=$!

# Phase 1: gate on >= 2 recovery cycles. Without it, cycle 1 of the buggy
# binary alone could satisfy the bound below.
ready=0
for _ in $(seq 1 50); do
  sleep 1
  rec=$(query "SELECT coalesce(max(state_value), 0) FROM system.local_system_states WHERE name='${MV}' AND state_name='recover_times'")
  if [[ "${rec:-0}" -ge 2 ]]; then ready=1; break; fi
done

# Phase 2: every sample must hold the bound; ANY violation fails.
result=0
if [[ "$ready" == "1" ]]; then
  result=1
  for _ in $(seq 1 10); do
    out=$(query "SELECT count() = 0 OR max(peak_memory_usage) < 256*1024*1024 FROM system.processes WHERE query_id LIKE '.mv-%' AND query LIKE '%${SRC}%'")
    if [[ "$out" != "true" && "$out" != "1" ]]; then
      result=0
      break
    fi
    sleep 1
  done
fi

echo $result
