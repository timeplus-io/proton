/**
 * server.ts — Static file server + Proton proxy for the Query Playground
 *
 * • Serves index.html on http://localhost:8000
 * • Proxies all non-static requests to Proton on localhost:3218
 * • /api/config  — returns the Proton target URL and auth mode (used by the UI)
 * • /api/ping    — tests whether Proton is reachable
 *
 * Authentication priority (highest → lowest):
 *   1. Client-supplied Authorization header (username/password from the UI)
 *   2. PROTON_USER / PROTON_PASS env vars (server-side fallback)
 *   3. No auth
 *
 * Usage:
 *   npx ts-node server.ts
 *
 * Environment variables:
 *   PORT         — server listen port         (default: 8000)
 *   PROTON_HOST  — Proton server host         (default: localhost)
 *   PROTON_PORT  — Proton HTTP/stream port    (default: 3218)
 *   PROTON_PROTO — http | https               (default: http)
 *   PROTON_USER  — Proton username (optional)
 *   PROTON_PASS  — Proton password (optional)
 */

import * as http from "http";
import * as https from "https";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_PORT = parseInt(process.env.PORT || "8000");
const PROTON_HOST = process.env.PROTON_HOST || "localhost";
const PROTON_PORT = parseInt(process.env.PROTON_PORT || "3218");
const PROTON_PROTO = process.env.PROTON_PROTO || "http";
const PROTON_USER = process.env.PROTON_USER || "";
const PROTON_PASS = process.env.PROTON_PASS || "";
const STATIC_ROOT = path.resolve(__dirname);
const PROTON_URL = `${PROTON_PROTO}://${PROTON_HOST}:${PROTON_PORT}`;
const MAX_BODY_SIZE = 10 * 1024 * 1024; // 10MB limit for proxy buffering

// Pre-compute the server-side Basic Auth header (if env vars are set)
const SERVER_AUTH_HEADER = PROTON_USER
  ? "Basic " + Buffer.from(`${PROTON_USER}:${PROTON_PASS}`).toString("base64")
  : "";

// ── MIME types ────────────────────────────────────────────────────────────────
const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
};

const CORS: Record<string, string> = {
  "Access-Control-Allow-Origin": "http://localhost:" + SERVER_PORT,
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "*",
  "Access-Control-Expose-Headers": "*",
};

// ── Static file serving ───────────────────────────────────────────────────────
function serveStatic(urlPath: string, res: http.ServerResponse): boolean {
  // Sanitize path to prevent traversal - block anything with ".."
  if (urlPath.includes("..")) {
    res.writeHead(403); res.end("Forbidden"); return true;
  }
  const normalizedPath = urlPath === "/" ? "/index.html" : urlPath.split("?")[0];
  const filePath = path.join(STATIC_ROOT, normalizedPath);

  if (!filePath.startsWith(STATIC_ROOT)) {
    res.writeHead(403); res.end("Forbidden"); return true;
  }
  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    return false;
  }
  const ext = path.extname(filePath).toLowerCase();
  const mime = MIME[ext] || "application/octet-stream";
  try {
    const content = fs.readFileSync(filePath);
    res.writeHead(200, { "Content-Type": mime, ...CORS });
    res.end(content);
  } catch {
    res.writeHead(500); res.end("Server error");
  }
  return true;
}

// Headers that must not be forwarded between hops (RFC 2616 §13.5.1)
const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailers", "transfer-encoding", "upgrade",
]);

// ── Resolve which Authorization header to use ─────────────────────────────────
// Priority:
//   1. Client sent an Authorization header (username/password typed in the UI)
//   2. Server has PROTON_USER/PROTON_PASS env vars set → inject those
//   3. Neither → omit the header entirely
function resolveAuthHeader(clientHeaders: http.IncomingHttpHeaders): string {
  const clientAuth = clientHeaders["authorization"];
  if (clientAuth) return clientAuth as string;      // UI credentials take priority
  if (SERVER_AUTH_HEADER) return SERVER_AUTH_HEADER; // fall back to env vars
  return "";
}

// ── Proxy to Proton ───────────────────────────────────────────────────────────
// Collects the full request body before forwarding so we can set an accurate
// Content-Length — Proton rejects chunked-encoded query bodies.
function proxyToProton(clientReq: http.IncomingMessage, clientRes: http.ServerResponse): void {
  const chunks: Buffer[] = [];

  clientReq.on("data", (chunk: Buffer) => {
    chunks.push(chunk);
    const totalLength = chunks.reduce((acc, cur) => acc + cur.length, 0);
    if (totalLength > MAX_BODY_SIZE) {
      console.error("[proxy] request body too large");
      clientRes.writeHead(413, { ...CORS, "Content-Type": "application/json" });
      clientRes.end(JSON.stringify({ error: "Request body too large", limit: "10MB" }));
      clientReq.destroy();
    }
  });

  clientReq.on("end", () => {
    if (clientRes.writableEnded) return;
    const body = Buffer.concat(chunks);

    // Build clean headers — strip hop-by-hop, then set our own
    const forwardHeaders: http.OutgoingHttpHeaders = {};
    for (const [k, v] of Object.entries(clientReq.headers)) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) forwardHeaders[k] = v;
    }
    forwardHeaders["host"] = `${PROTON_HOST}:${PROTON_PORT}`;
    forwardHeaders["content-length"] = body.length;
    forwardHeaders["connection"] = "close"; // tell Proton to close after the response

    // Auth: use client-supplied creds, fall back to server env vars, or omit
    const auth = resolveAuthHeader(clientReq.headers);
    if (auth) {
      forwardHeaders["authorization"] = auth;
    } else {
      delete forwardHeaders["authorization"]; // ensure none leaks through
    }

    const authSource = clientReq.headers["authorization"] ? "client"
      : SERVER_AUTH_HEADER ? "server-env"
        : "none";
    console.log(`[proxy] ${clientReq.method} ${clientReq.url} → ${PROTON_URL}${clientReq.url} (body ${body.length}b, auth: ${authSource})`);

    const options: http.RequestOptions = {
      hostname: PROTON_HOST,
      port: PROTON_PORT,
      path: clientReq.url ?? "/",
      method: clientReq.method,
      headers: forwardHeaders,
    };

    const transport = PROTON_PROTO === "https" ? https : http;

    const protonReq = transport.request(options, (protonRes) => {
      console.log(`[proxy] Proton responded ${protonRes.statusCode}`);

      // Strip hop-by-hop from response, inject CORS
      const responseHeaders: http.OutgoingHttpHeaders = { ...CORS };
      for (const [k, v] of Object.entries(protonRes.headers)) {
        if (!HOP_BY_HOP.has(k.toLowerCase())) responseHeaders[k] = v;
      }
      delete responseHeaders["content-length"]; // streaming — length is unknown

      clientRes.writeHead(protonRes.statusCode ?? 200, responseHeaders);
      protonRes.pipe(clientRes, { end: true });

      protonRes.on("error", (err) => {
        console.error("[proxy] upstream response error:", err.message);
        if (!clientRes.writableEnded) clientRes.end();
      });
    });

    protonReq.on("error", (err) => {
      console.error(`[proxy] cannot reach Proton at ${PROTON_URL} — ${err.message}`);
      if (!clientRes.headersSent) {
        clientRes.writeHead(502, { ...CORS, "Content-Type": "application/json" });
      }
      if (!clientRes.writableEnded) {
        clientRes.end(JSON.stringify({
          error: `Cannot reach Proton at ${PROTON_URL}`,
          detail: err.message,
          hint: `Set PROTON_PORT env var to match your Proton HTTP port and restart the server.`,
        }));
      }
    });

    clientRes.on("close", () => protonReq.destroy());
    protonReq.end(body);
  });

  clientReq.on("error", (err) => {
    console.error("[proxy] client request error:", err.message);
  });
}

// ── Main server ───────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const url = req.url ?? "/";

  // Pre-flight
  if (req.method === "OPTIONS") {
    res.writeHead(204, { ...CORS, "Content-Length": "0" });
    res.end();
    return;
  }

  // /api/config — tells the UI where this server is forwarding to, and auth mode
  if (url === "/api/config") {
    res.writeHead(200, { "Content-Type": "application/json", ...CORS });
    res.end(JSON.stringify({
      protonUrl: PROTON_URL,
      serverPort: SERVER_PORT,
      // Tell the UI whether server-side credentials are pre-configured,
      // so it can show "server auth active" instead of empty username/password fields.
      serverAuth: !!SERVER_AUTH_HEADER,
    }));
    return;
  }

  // /api/ping — tests whether Proton is reachable (uses server-side auth if configured)
  if (url === "/api/ping") {
    const transport = PROTON_PROTO === "https" ? https : http;
    const pingHeaders: http.OutgoingHttpHeaders = { connection: "close" };
    if (SERVER_AUTH_HEADER) pingHeaders["authorization"] = SERVER_AUTH_HEADER;

    const probe = transport.request(
      { hostname: PROTON_HOST, port: PROTON_PORT, path: "/ping", method: "GET", headers: pingHeaders },
      (r) => {
        res.writeHead(200, { "Content-Type": "application/json", ...CORS });
        res.end(JSON.stringify({ ok: true, status: r.statusCode }));
        r.resume();
      }
    );
    probe.on("error", (err) => {
      if (!res.headersSent) {
        res.writeHead(200, { "Content-Type": "application/json", ...CORS });
        res.end(JSON.stringify({
          ok: false,
          error: `Cannot reach Proton at ${PROTON_HOST}:${PROTON_PORT} — ${err.message}`,
          hint: `Start Proton with: docker run -d -p ${PROTON_PORT}:${PROTON_PORT} ghcr.io/timeplus-io/proton:latest`,
        }));
      }
    });
    probe.setTimeout(3000, () => {
      probe.destroy();
      if (!res.headersSent) {
        res.writeHead(200, { "Content-Type": "application/json", ...CORS });
        res.end(JSON.stringify({ ok: false, error: `Timeout connecting to Proton at ${PROTON_HOST}:${PROTON_PORT}` }));
      }
    });
    probe.end();
    return;
  }

  // Static files (GET / → index.html)
  if (req.method === "GET" && serveStatic(url, res)) return;

  // Everything else → proxy to Proton
  proxyToProton(req, res);
});

server.listen(SERVER_PORT, "127.0.0.1", () => {
  const authLine = PROTON_USER
    ? `Auth     →  server-env (user: ${PROTON_USER})`
    : `Auth     →  client-supplied (or none)`;
  console.log(`
╔════════════════════════════════════════════════════════╗
║        Proton Query Playground  —  running             ║
╠════════════════════════════════════════════════════════╣
║  UI      →  http://localhost:${String(SERVER_PORT).padEnd(27)}║
║  Proton  →  ${String(PROTON_URL).padEnd(43)}║
║  ${authLine.padEnd(53)}║
╚════════════════════════════════════════════════════════╝

Open: http://localhost:${SERVER_PORT}

Auth priority: UI fields > PROTON_USER/PROTON_PASS env vars > none
Example: PROTON_USER=admin PROTON_PASS=secret npm start

Press Ctrl+C to stop.
`);
});

process.on("SIGINT", () => { console.log("\nStopping…"); server.close(); process.exit(0); });
process.on("SIGTERM", () => { server.close(); process.exit(0); });
