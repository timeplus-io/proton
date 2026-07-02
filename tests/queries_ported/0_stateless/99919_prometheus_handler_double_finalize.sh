#!/usr/bin/env bash
# Tags: no-parallel
# Regression test for #11972: when a Prometheus scrape is aborted mid-stream
# (client RST), the server used to call WriteBuffer::finalize() twice, which
# raised LOGICAL_ERROR "Cannot finalize buffer after cancellation" and masked
# the real NetException in the log. After the fix, the real exception is
# logged and no LOGICAL_ERROR mask appears.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! command -v python3 >/dev/null 2>&1; then
    echo "0"
    exit 0
fi

# start-local-timeplusd.sh picks the Prometheus port as DEFAULT_PROMETHEUS_PORT
# (9363) + offset-from-DEFAULT_NODE_TCP_PORT (8463) = tcp_port + 900.
PROM_PORT=$((${CLICKHOUSE_PORT_TCP:-8463} + 900))

# Sanity-check the endpoint; skip if Prometheus is disabled or on a different port.
if ! ${CLICKHOUSE_CURL} "http://${CLICKHOUSE_HOST}:${PROM_PORT}/metrics" | grep -q '^TimeplusdProfileEvents'; then
    echo "0"
    exit 0
fi

python3 - "${CLICKHOUSE_HOST}" "$PROM_PORT" <<'PY' 2>/dev/null
import socket, struct, sys
host, port = sys.argv[1], int(sys.argv[2])
for version in (b'1.1', b'1.0'):
    for _ in range(10):
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.sendall(b'GET /metrics HTTP/' + version + b'\r\nHost: x\r\n\r\n')
            s.recv(256)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            s.close()
        except OSError:
            pass
PY

# Give handler threads a moment to finish writing log entries, then force a
# synchronous flush of system.timeplusd_log before we query it - otherwise
# the asserted count can miss buffered entries on slow/loaded runners.
sleep 1
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

${CLICKHOUSE_CLIENT} --query "
    SELECT count()
      FROM table(system.timeplusd_log)
     WHERE _tp_time > now() - 30s
       AND raw LIKE '%PrometheusRequestHandler%'
       AND raw LIKE '%Cannot finalize buffer after cancellation%'"
