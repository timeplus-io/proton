-- js_telemetry_log_enrich_demo.sql
-- Telemetry‑oriented JavaScript UDF perf demo.
--
-- This uses a synthetic nginx access log stream and a CPU‑intensive JS UDF
-- to simulate log/metrics/tracing style enrichment. It is intended as a more
-- general benchmark than the HKJC QTT permutation example.
-- ---------------------------------------------------------------------------
-- Synthetic telemetry workload: nginx access logs
-- ---------------------------------------------------------------------------
-- This is similar to examples/nginx-grafana/01_nginx-access-log.sql but kept
-- self‑contained for the JS UDF perf demo.

DROP STREAM IF EXISTS nginx_access_log;
CREATE RANDOM STREAM IF NOT EXISTS nginx_access_log (
  remote_ip ipv4 default random_in_type('ipv4'),
  rfc1413_ident string default '-',
  remote_user string default '-',
  date_time datetime64 default random_in_type('datetime64', 365, y -> to_time('2023-6-6') + interval y day),
  http_method string default ['GET', 'POST', 'PUT', 'DELETE', 'HEAD'][rand()%5],
  path string default ['/rss/', '/', '/programmatic-creation-of-the-ghost-admin-user/', '/sitemap.xml', '/favicon.ico', '/Core/Skin/Login.aspx', '/Public/home/js/check.js', '/robots.txt', '/engineering-vs-technology-company/', '/switching-to-a-newer-version-of-rsync-on-macos/', '/.env'][rand()%11],
  http_version string default ['HTTP/1.0', 'HTTP/1.1', 'HTTP/2.0'][rand()%3],
  status int default [200, 301, 302, 304, 400, 404, 429, 500, 502, 503][rand()%10],
  size int default rand()%5000000,
  referer string default ['-', 'https://example.com/', 'https://search.example/', 'https://monitoring.example/'][rand()%4],
  user_agent string default [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' || to_string(rand()%30 + 100) || '.0.0.0 Safari/537.36',
    'curl/7.88.1',
    'kube-probe/1.26',
    'Prometheus/2.53.0',
    'Grafana/11.0.0',
    'SyntheticBot/1.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15',
    'load-tester/chaos-monkey',
    'Datadog Agent/7.53.0'
  ][rand()%9],
  malicious_request string default if(rand()%100 < 5, '\x16\x03\x01\x00\xEA\x01\x00\x00\xE6\x03\x03', '')
)
SETTINGS eps = 200000;

-- ---------------------------------------------------------------------------
-- JS scalar UDF: telemetry/log enrichment + heavy computation
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION nginx_log_enrich_js(
  method string,
  path string,
  status int,
  size int,
  user_agent string,
  malicious_request string
)
RETURNS string
LANGUAGE JAVASCRIPT AS $$
// Basic helpers for classification and hashing.
function classifyStatus(status) {
  if (status >= 500) return '5xx';
  if (status >= 400) return '4xx';
  if (status >= 300) return '3xx';
  if (status >= 200) return '2xx';
  return 'other';
}

function classifyUserAgent(ua) {
  const lower = String(ua || '').toLowerCase();
  if (lower.includes('kube-probe')) return 'kube-probe';
  if (lower.includes('prometheus')) return 'prometheus';
  if (lower.includes('grafana')) return 'grafana';
  if (lower.includes('datadog')) return 'datadog-agent';
  if (lower.includes('bot') || lower.includes('crawler')) return 'bot';
  if (lower.includes('curl')) return 'curl';
  if (lower.includes('mozilla')) return 'browser';
  return 'other';
}

// Normalize path by collapsing IDs / hashes into a :id segment.
function normalizePath(path) {
  if (!path) return '/';
  const segments = String(path).split('/');
  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    // Treat long hex / numeric tokens as IDs.
    if (/^[0-9a-fA-F-]{8,}$/.test(seg)) {
      segments[i] = ':id';
    }
  }
  const normalized = segments.join('/');
  return normalized || '/';
}

function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
  }
  return hash >>> 0; // unsigned
}

// CPU‑intensive scoring function to stress V8:
// combines hashing, trigonometry, and roots over multiple iterations.
function computeAnomalyScore(status, size, path, uaClass, isMalicious) {
  const baseHash = hashString(path + '|' + uaClass);
  const iter = 64;  // tune to adjust CPU cost
  let score = 0;

  const magnitude = (size >>> 0) % 1000000;

  for (let i = 1; i <= iter; i++) {
    const x = (baseHash ^ (magnitude * i)) % 1000003;
    const xf = x / 997.0;
    score += Math.sin(xf) * Math.cos(xf / 3.0) + Math.sqrt((x % 1000) + 1);
  }

  // Penalize errors and large responses.
  if (status >= 500) score *= 1.8;
  else if (status >= 400) score *= 1.3;

  if (magnitude > 500000) score *= 1.2;

  if (isMalicious) score *= 2.0;

  // Compress to a reasonable range.
  return Math.log10(Math.abs(score) + 10);
}

function nginx_log_enrich_js(method, path, status, size, user_agent, malicious_request) {
  // In Proton JS scalar UDFs, each argument is an array of column values for
  // the current micro‑batch. We must return an array of results of the declared
  // return type (string) with the same length.
  const n = method.length || 0;
  const result = new Array(n);

  for (let i = 0; i < n; i++) {
    const methodStr = String(method[i] || '');
    const pathStr = String(path[i] || '/');
    const userAgentStr = String(user_agent[i] || '');
    const statusVal = status[i] | 0;
    const sizeVal = size[i] | 0;
    const maliciousVal = malicious_request[i];

    const statusFamily = classifyStatus(statusVal);
    const uaClass = classifyUserAgent(userAgentStr);
    const normalizedPath = normalizePath(pathStr);
    const isMalicious = !!(maliciousVal && String(maliciousVal).length > 0);

    const anomalyScore = computeAnomalyScore(
      statusVal,
      sizeVal,
      normalizedPath,
      uaClass,
      isMalicious
    );

    result[i] = JSON.stringify({
      method: methodStr,
      path: normalizedPath,
      status: statusVal,
      statusFamily: statusFamily,
      size: sizeVal,
      uaClass: uaClass,
      isMalicious: isMalicious,
      anomalyScore: anomalyScore
    });
  }

  return result;
}
$$;

-- ---------------------------------------------------------------------------
-- View using the JS UDF over the random telemetry stream
-- ---------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS nginx_log_enriched_js_v
AS
SELECT
  date_time,
  remote_ip,
  http_method,
  path,
  status,
  size,
  nginx_log_enrich_js(http_method, path, status, size, user_agent, malicious_request) AS enriched_json
FROM nginx_access_log;

-- Example perf query (run separately):
SELECT *
FROM nginx_log_enriched_js_v
FORMAT Null
SETTINGS eps = 5000000;

