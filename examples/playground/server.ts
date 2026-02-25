/**
 * server.ts — Static file server + Proton proxy for the Query Playground
 *
 * • Serves index.html on http://localhost:8000
 * • Proxies all non-static requests to Proton on localhost:3218
 * • /api/config  — returns the Proton target URL (used by the UI)
 * • /api/ping    — tests whether Proton is reachable
 *
 * Usage:
 *   npx ts-node server.ts
 *
 * Environment variables:
 *   PORT         — server listen port       (default: 8000)
 *   PROTON_HOST  — Proton server host       (default: localhost)
 *   PROTON_PORT  — Proton HTTP/stream port  (default: 3218)
 *   PROTON_PROTO — http | https             (default: http)
 */

import * as http from "http";
import * as https from "https";
import * as fs from "fs";
import * as path from "path";
import { URL } from "url";

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_PORT = parseInt(process.env.PORT || "8000");
const PROTON_HOST = process.env.PROTON_HOST || "localhost";
const PROTON_PORT = parseInt(process.env.PROTON_PORT || "3218");
const PROTON_PROTO = process.env.PROTON_PROTO || "http";
const STATIC_ROOT = process.cwd();
const PROTON_URL = `${PROTON_PROTO}://${PROTON_HOST}:${PROTON_PORT}`;

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
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "*",
  "Access-Control-Expose-Headers": "*",
};

// ── Static file serving ───────────────────────────────────────────────────────
function serveStatic(urlPath: string, res: http.ServerResponse): boolean {
  const filePath = path.join(
    STATIC_ROOT,
    urlPath === "/" ? "index.html" : urlPath.split("?")[0]
  );
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

// ── Proxy to Proton ───────────────────────────────────────────────────────────
// We collect the full request body before forwarding so we can set an
// accurate Content-Length — Proton rejects chunked-encoded query bodies.
function proxyToProton(clientReq: http.IncomingMessage, clientRes: http.ServerResponse): void {
  const chunks: Buffer[] = [];

  clientReq.on("data", (chunk: Buffer) => chunks.push(chunk));

  clientReq.on("end", () => {
    const body = Buffer.concat(chunks);

    // Build clean headers — strip hop-by-hop and let us set content-length ourselves
    const forwardHeaders: http.OutgoingHttpHeaders = {};
    for (const [k, v] of Object.entries(clientReq.headers)) {
      if (!HOP_BY_HOP.has(k.toLowerCase())) forwardHeaders[k] = v;
    }
    forwardHeaders["host"] = `${PROTON_HOST}:${PROTON_PORT}`;
    forwardHeaders["content-length"] = body.length;
    forwardHeaders["connection"] = "close";   // tell Proton to close after response

    const options: http.RequestOptions = {
      hostname: PROTON_HOST,
      port: PROTON_PORT,
      path: (clientReq.url ?? "/"),
      method: clientReq.method,
      headers: forwardHeaders,
    };

    const transport = PROTON_PROTO === "https" ? https : http;

    console.log(`[proxy] ${clientReq.method} ${clientReq.url} → ${PROTON_URL}${clientReq.url} (body ${body.length}b)`);

    const protonReq = transport.request(options, (protonRes) => {
      console.log(`[proxy] Proton responded ${protonRes.statusCode}`);

      // Strip hop-by-hop from response too, inject CORS
      const responseHeaders: http.OutgoingHttpHeaders = { ...CORS };
      for (const [k, v] of Object.entries(protonRes.headers)) {
        if (!HOP_BY_HOP.has(k.toLowerCase())) responseHeaders[k] = v;
      }
      // Don't forward content-length — streaming response length is unknown
      delete responseHeaders["content-length"];

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

    // Abort the upstream request if the browser disconnects
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

  // /api/config — tells the UI where this server is forwarding to
  if (url === "/api/config") {
    res.writeHead(200, { "Content-Type": "application/json", ...CORS });
    res.end(JSON.stringify({ protonUrl: PROTON_URL, serverPort: SERVER_PORT }));
    return;
  }

  // /api/ping — tests whether Proton is reachable right now
  if (url === "/api/ping") {
    const transport = PROTON_PROTO === "https" ? https : http;
    const probe = transport.request(
      { hostname: PROTON_HOST, port: PROTON_PORT, path: "/ping", method: "GET" },
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

server.listen(SERVER_PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════╗
║        Proton Query Playground  —  running             ║
╠════════════════════════════════════════════════════════╣
║  UI      →  http://localhost:${String(SERVER_PORT).padEnd(27)}║
║  Proton  →  ${String(PROTON_URL).padEnd(43)}║
╚════════════════════════════════════════════════════════╝

Open: http://localhost:${SERVER_PORT}
Override Proton port: PROTON_PORT=8001 npm start

Press Ctrl+C to stop.
`);
});

process.on("SIGINT", () => { console.log("\nStopping…"); server.close(); process.exit(0); });
process.on("SIGTERM", () => { server.close(); process.exit(0); });