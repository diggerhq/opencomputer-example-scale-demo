// Scale-demo collector: counts pings from agent sessions and streams them to
// the visualization page over SSE. Zero dependencies — `node server/index.mjs`.
//
//   GET  /          -> the point-cloud logo reveal
//   GET  /events    -> SSE stream ({created,ping,snapshot,reset})
//   POST /created   -> {cell}                    session was created
//   POST /ping      -> {cell, sessionId?}        an agent turn finished its ping
//   POST /simulate  -> {count?, rate?, delay?}   fake a run without agents
//   POST /reset     -> clear all cells
//   GET  /stats     -> JSON counters

import http from "node:http";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PORT || 8787);
const PUBLIC = join(dirname(fileURLToPath(import.meta.url)), "public");

const cells = new Map(); // cell -> "created" | "pinged"
const clients = new Set();
const simTimers = new Set();
let startedAt = null;

function stopSims() {
  for (const t of simTimers) clearInterval(t);
  simTimers.clear();
}

function broadcast(event) {
  const frame = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of clients) {
    try {
      res.write(frame);
    } catch {
      clients.delete(res);
    }
  }
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(data || "{}"));
      } catch {
        resolve({});
      }
    });
  });
}

function emitCreated(cell) {
  if (!cells.has(cell)) {
    cells.set(cell, "created");
    broadcast({ type: "created", cell });
  }
}

function emitPing(cell) {
  const was = cells.get(cell);
  if (was === "pinged") return;
  cells.set(cell, "pinged");
  broadcast({ type: "ping", cell });
}

// Fake a run: `rate` creations per second, each pinging after `delay`±50% ms.
// Emitting straight through broadcast so the page cannot tell sim from real.
function simulate({ count = 1000, rate = 100, delay = 800 }) {
  let i = 0;
  const pending = []; // [fireAt, cell] — near-sorted because fireAt grows with i
  const tickMs = 100;
  const perTick = Math.max(1, Math.ceil((rate * tickMs) / 1000));
  const timer = setInterval(() => {
    const now = Date.now();
    for (let n = 0; n < perTick && i < count; n += 1, i += 1) {
      emitCreated(i);
      pending.push([now + delay * (0.5 + Math.random()), i]);
    }
    pending.sort((a, b) => a[0] - b[0]);
    while (pending.length && pending[0][0] <= now) emitPing(pending.shift()[1]);
    if (i >= count && !pending.length) {
      clearInterval(timer);
      simTimers.delete(timer);
    }
  }, tickMs);
  simTimers.add(timer);
  return timer;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://collector");
  res.setHeader("access-control-allow-origin", "*");
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    return res.end(readFileSync(join(PUBLIC, "index.html")));
  }

  if (req.method === "GET" && url.pathname === "/events") {
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    res.write(":ok\n\n");
    clients.add(res);
    req.on("close", () => clients.delete(res));
    const snapshot = [...cells.entries()].map(([cell, state]) => [
      cell,
      state === "pinged" ? 2 : 1,
    ]);
    res.write(`data: ${JSON.stringify({ type: "snapshot", cells: snapshot, startedAt })}\n\n`);
    return;
  }

  if (req.method === "POST" && url.pathname === "/created") {
    const { cell } = await readBody(req);
    if (typeof cell === "number") emitCreated(cell);
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  if (req.method === "POST" && url.pathname === "/ping") {
    const { cell } = await readBody(req);
    if (typeof cell === "number") emitPing(cell);
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  if (req.method === "POST" && url.pathname === "/simulate") {
    const opts = await readBody(req);
    stopSims();
    cells.clear();
    startedAt = Date.now();
    broadcast({ type: "reset" });
    simulate(opts);
    res.writeHead(202, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  if (req.method === "POST" && url.pathname === "/reset") {
    stopSims();
    cells.clear();
    startedAt = null;
    broadcast({ type: "reset" });
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true }));
  }

  if (req.method === "GET" && url.pathname === "/stats") {
    let pinged = 0;
    for (const s of cells.values()) if (s === "pinged") pinged += 1;
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(
      JSON.stringify({
        created: cells.size,
        pinged,
        startedAt,
        elapsedMs: startedAt ? Date.now() - startedAt : 0,
      }),
    );
  }

  res.writeHead(404);
  res.end("not found");
});

server.listen(PORT, () => {
  console.log(`scale-demo collector on http://localhost:${PORT}`);
});
