# Proton Query Playground

A browser-based SQL query playground for [Timeplus Proton](https://github.com/timeplus-io/proton), powered by:

- **[`@timeplus/proton-javascript-driver`](https://github.com/timeplus-io/proton-javascript-driver)** — streaming REST client (UMD, no build)
- **[`@timeplus/vistral`](https://github.com/timeplus-io/vistral)** — streaming data visualization (UMD, no build)
- **`server.ts`** — Node.js server that both **serves `index.html`** and **proxies** requests to Proton

## Architecture

```
Browser
  │  http://localhost:8000          ← opens the UI
  │  ProtonClient → localhost:8000  ← sends queries
  ▼
server.ts  (port 8000)
  │  GET /  →  serves index.html
  │  everything else  →  proxy
  ▼
Proton  (port 3218)
  └  streaming REST API
```

No CORS issues — the browser talks to the same origin (`localhost:8000`) for both the UI and queries.

## Project Files

```
proton-playground/
├── index.html   ← frontend UI (no build step — pure HTML + UMD scripts)
├── server.ts    ← Node.js server: serves HTML + proxies to Proton
├── package.json ← dev deps (ts-node + typescript only)
└── README.md
```

## Quick Start

### 1. Start Proton

Proton's streaming REST API is on port **3218** by default.

```bash
docker run -d -p 3218:3218 --name proton --platform linux/amd64 ghcr.io/timeplus-io/proton:latest
```

### 2. Install deps & start the server

```bash
npm install       # installs ts-node + typescript
npm start         # → http://localhost:8000
```

### 3. Open your browser

```
http://localhost:8000
```

The **Server URL** field is pre-filled as `http://localhost:8000`. Leave it as-is unless you've changed the server port.

### 4. Run a query

```sql
-- Create a random stream to simulate market data
CREATE RANDOM STREAM IF NOT EXISTS us_market_data (
        symbol string default ['AAPL', 'MSFT', 'GOOGL'][rand()%3+1],
        price float64 default rand()%1000/100 + 150
      ) SETTINGS eps=10

-- Streaming query (runs forever — hit Stop when done)
SELECT _tp_time, symbol, price FROM us_market_data

-- Batch query (finishes automatically)
SELECT now() as _tp_time, number % 5 as id, rand() % 1000 as value
FROM numbers(200)
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8000` | Port the server listens on |
| `PROTON_HOST` | `localhost` | Proton host |
| `PROTON_PORT` | `3218` | Proton streaming REST port |
| `PROTON_PROTO` | `http` | `http` or `https` |

## Production build (optional)

```bash
npm run build       # compiles server.ts → dist/server.js
npm run start:prod  # node dist/server.js
```
