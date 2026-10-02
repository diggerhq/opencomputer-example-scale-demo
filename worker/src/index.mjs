// Hosted scale-demo collector + fanout, one Durable Object.
//
//   GET  /events    -> SSE stream ({created,ping,snapshot,reset,fanout})
//   POST /created   -> {cell}                       session was created
//   POST /ping      -> {cell, sessionId?}           an agent's ping-back landed
//   POST /simulate  -> {count?, rate?, delay?}      fake a run without agents
//   POST /run       -> {apiKey, baseUrl?, agentId, n, concurrency?}  REAL run:
//                      fanout executes inside this DO against the managed API
//   POST /reset     -> clear all cells
//   GET  /stats     -> JSON counters
//   POST /mcp       -> MCP (Streamable HTTP, stateless): the agent's ping
//                      call lands here directly — no tool sandbox involved
//
// The apiKey is used only for the duration of /run and never persisted.

const PING_TOOL = {
  name: "ping",
  description:
    "Ignite one cell on the scale-demo board. Call exactly once with the cell index you were assigned.",
  inputSchema: {
    type: "object",
    properties: {
      cell: { type: "integer", description: "The cell index assigned to this session" },
      sessionId: { type: "string", description: "Your session id, if you know it" },
    },
    required: ["cell"],
  },
};

const MAX_N = 10_000;
const MAX_CONCURRENCY = 50;
const DEFAULT_BASE_URL = "https://app.opencomputer.dev/api/managed-agents";

async function sha256Hex(text) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export class Collector {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.cells = new Map(); // cell -> "created" | "pinged"
    this.clients = new Set();
    this.simTimers = new Set();
    this.startedAt = null;
    this.runs = new Map(); // sha256(apiKey) -> runId (one active run per key)
    this.activeRun = null; // {id, target, done, failed, lastError?}
    this.lastRun = null; // finished-run summary so late viewers see failures
    // In-memory state survives only while the DO is hot; persist so a late
    // viewer still sees the finished run after eviction.
    this.state.blockConcurrencyWhile(async () => {
      const stored = await this.state.storage.list({ prefix: "cell:" });
      for (const [key, val] of stored) this.cells.set(Number(key.slice(5)), val);
      this.startedAt = (await this.state.storage.get("startedAt")) ?? null;
      this.lastRun = (await this.state.storage.get("lastRun")) ?? null;
    });
  }

  persistCell(cell) {
    // Fire-and-forget: writes queue in order, no need to await.
    this.state.storage.put(`cell:${cell}`, this.cells.get(cell));
  }

  async clearPersisted() {
    this.state.storage.put("startedAt", this.startedAt);
    const stored = await this.state.storage.list({ prefix: "cell:" });
    await this.state.storage.delete([...stored.keys(), "lastRun"]);
  }

  stopSims() {
    for (const t of this.simTimers) clearInterval(t);
    this.simTimers.clear();
  }

  broadcast(event) {
    const frame = `data: ${JSON.stringify(event)}\n\n`;
    for (const ctrl of this.clients) {
      try {
        ctrl.enqueue(new TextEncoder().encode(frame));
      } catch {
        this.clients.delete(ctrl);
      }
    }
  }

  emitCreated(cell) {
    if (!this.cells.has(cell)) {
      this.cells.set(cell, "created");
      this.persistCell(cell);
      this.broadcast({ type: "created", cell });
    }
  }

  emitPing(cell) {
    if (this.cells.get(cell) === "pinged") return;
    this.cells.set(cell, "pinged");
    this.persistCell(cell);
    this.broadcast({ type: "ping", cell });
  }

  simulate({ count = 1000, rate = 100, delay = 800 }) {
    let i = 0;
    const pending = [];
    const tickMs = 100;
    const perTick = Math.max(1, Math.ceil((rate * tickMs) / 1000));
    let resolveDone;
    const done = new Promise((r) => (resolveDone = r));
    const timer = setInterval(() => {
      const now = Date.now();
      for (let n = 0; n < perTick && i < count; n += 1, i += 1) {
        this.emitCreated(i);
        pending.push([now + delay * (0.5 + Math.random()), i]);
      }
      pending.sort((a, b) => a[0] - b[0]);
      while (pending.length && pending[0][0] <= now) this.emitPing(pending.shift()[1]);
      if (i >= count && !pending.length) {
        clearInterval(timer);
        this.simTimers.delete(timer);
        resolveDone();
      }
    }, tickMs);
    this.simTimers.add(timer);
    this.state.waitUntil(done); // keep the DO alive until the sim drains
  }

  // The real fan-out. Runs entirely inside this DO: for each cell, create a
  // session then admit one turn carrying {cell}. The agent pings back over
  // MCP (or plain HTTP for the local flow) to this same Worker.
  async fanout({ apiKey, baseUrl, agentId, n, concurrency, origin, runId, keyHash }) {
    const run = { id: runId, target: n, done: 0, failed: 0 };
    this.activeRun = run;
    const headers = (idem) => ({
      "content-type": "application/json",
      "x-api-key": apiKey,
      "idempotency-key": idem,
    });
    const postJson = (path, body, idem) =>
      fetch(`${baseUrl}${path}`, { method: "POST", headers: headers(idem), body: JSON.stringify(body) });

    const isFatal = (status) => status === 401 || status === 403 || status === 404;
    let next = 0;
    const work = async (cell) => {
      // Bail on an aborted run (reset) or a config-level failure (bad key,
      // missing deployment) — every remaining cell would fail identically.
      if (run.fatal || this.runs.get(keyHash) !== runId) return;
      try {
        const created = await postJson("/sessions", {
          agentId,
          labels: { demo: "scale", run: runId },
        }, `${runId}/${cell}`);
        if (!created.ok) {
          run.failed += 1;
          if (!run.lastError) {
            run.lastError = `sessions ${created.status}: ${(await created.text()).slice(0, 300)}`;
            console.log(run.lastError);
          }
          if (isFatal(created.status)) run.fatal = run.lastError;
          return;
        }
        const { session } = await created.json();
        run.done += 1;
        this.emitCreated(cell);
        const turn = await postJson(`/sessions/${session.id}/turns`, {
          input: "Ping the demo server, then stop.",
          payload: { cell }, // no collector URL -> the agent uses its MCP ping
        }, `${runId}/${cell}/turn`);
        if (!turn.ok) {
          run.failed += 1;
          if (!run.lastError) {
            run.lastError = `turns ${turn.status}: ${(await turn.text()).slice(0, 300)}`;
            console.log(run.lastError);
          }
          if (isFatal(turn.status)) run.fatal = run.lastError;
        }
      } catch (err) {
        run.failed += 1;
        if (!run.lastError) {
          run.lastError = `exception: ${String(err && err.message || err).slice(0, 300)}`;
          console.log(run.lastError);
        }
      }
      if ((run.done + run.failed) % 200 === 0 || run.done + run.failed === n) {
        this.broadcast({ type: "fanout", run: { id: runId, done: run.done, failed: run.failed, target: n } });
      }
    };
    const worker = async () => {
      for (let cell; (cell = next++) < n; ) await work(cell);
    };
    await Promise.all(Array.from({ length: concurrency }, worker));
    const summary = { id: runId, done: run.done, failed: run.failed, target: n, finished: true, lastError: run.lastError };
    this.lastRun = summary;
    this.state.waitUntil(this.state.storage.put("lastRun", summary));
    this.broadcast({ type: "fanout", run: summary });
    this.activeRun = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/events") {
      let controller;
      const stream = new ReadableStream({
        start: (c) => {
          controller = c;
          c.enqueue(new TextEncoder().encode(":ok\n\n"));
        },
        cancel: () => this.clients.delete(controller),
      });
      this.clients.add(controller);
      const snapshot = [...this.cells.entries()].map(([cell, s]) => [cell, s === "pinged" ? 2 : 1]);
      controller.enqueue(new TextEncoder().encode(
        `data: ${JSON.stringify({ type: "snapshot", cells: snapshot, startedAt: this.startedAt })}\n\n`,
      ));
      return new Response(stream, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "access-control-allow-origin": "*",
        },
      });
    }

    const body = request.method === "POST" ? await request.json().catch(() => ({})) : {};

    if (request.method === "POST" && url.pathname === "/mcp") {
      const rpc = body;
      const result = (res) => Response.json({ jsonrpc: "2.0", id: rpc.id ?? null, result: res });
      const error = (code, message) =>
        Response.json({ jsonrpc: "2.0", id: rpc.id ?? null, error: { code, message } });
      if (rpc.method === "initialize") {
        return result({
          protocolVersion: rpc.params?.protocolVersion ?? "2025-03-26",
          capabilities: { tools: {} },
          serverInfo: { name: "scale-demo-collector", version: "1.0.0" },
        });
      }
      if (typeof rpc.method === "string" && rpc.method.startsWith("notifications/")) {
        return new Response(null, { status: 202 });
      }
      if (rpc.method === "ping") return result({});
      if (rpc.method === "tools/list") return result({ tools: [PING_TOOL] });
      if (rpc.method === "tools/call") {
        if (rpc.params?.name !== "ping") return error(-32601, "unknown tool");
        const cell = rpc.params?.arguments?.cell;
        if (typeof cell === "number") this.emitPing(cell);
        return result({ content: [{ type: "text", text: `cell ${cell} ignited` }] });
      }
      return error(-32601, "method not found");
    }

    if (request.method === "POST" && url.pathname === "/created") {
      if (typeof body.cell === "number") this.emitCreated(body.cell);
      return Response.json({ ok: true });
    }

    if (request.method === "POST" && url.pathname === "/ping") {
      if (typeof body.cell === "number") this.emitPing(body.cell);
      return Response.json({ ok: true });
    }

    if (request.method === "POST" && url.pathname === "/simulate") {
      this.stopSims();
      this.cells.clear();
      this.lastRun = null;
      this.startedAt = Date.now();
      this.state.waitUntil(this.clearPersisted());
      this.broadcast({ type: "reset" });
      this.simulate(body);
      return Response.json({ ok: true }, { status: 202 });
    }

    if (request.method === "POST" && url.pathname === "/reset") {
      this.stopSims();
      this.cells.clear();
      this.startedAt = null;
      this.activeRun = null;
      this.lastRun = null;
      this.runs.clear(); // drops every run lock; in-flight fanouts see the missing entry and exit
      this.state.waitUntil(this.clearPersisted());
      this.broadcast({ type: "reset" });
      return Response.json({ ok: true });
    }

    if (request.method === "POST" && url.pathname === "/run") {
      const apiKey = String(body.apiKey || "").trim();
      const agentId = String(body.agentId || "").trim();
      const baseUrl = String(body.baseUrl || DEFAULT_BASE_URL).replace(/\/+$/, "");
      const n = Math.min(Math.max(Number(body.n) || 0, 1), MAX_N);
      const concurrency = Math.min(Math.max(Number(body.concurrency) || 40, 1), MAX_CONCURRENCY);
      if (!apiKey) return Response.json({ error: "apiKey required" }, { status: 400 });
      if (!agentId) return Response.json({ error: "agentId required" }, { status: 400 });
      const keyHash = await sha256Hex(apiKey);
      if (this.runs.has(keyHash)) {
        return Response.json({ error: "a run is already active for this key" }, { status: 409 });
      }
      const runId = `run-${Date.now().toString(36)}`;
      this.runs.set(keyHash, runId);
      this.stopSims();
      this.cells.clear();
      this.lastRun = null;
      this.startedAt = Date.now();
      this.state.waitUntil(this.clearPersisted());
      this.broadcast({ type: "reset" });
      const origin = `${url.protocol}//${url.host}`;
      this.state.waitUntil(
        this.fanout({ apiKey, baseUrl, agentId, n, concurrency, origin, runId, keyHash })
          .finally(() => this.runs.delete(keyHash)),
      );
      return Response.json({ ok: true, runId, n, concurrency }, { status: 202 });
    }

    if (request.method === "GET" && url.pathname === "/stats") {
      let pinged = 0;
      for (const s of this.cells.values()) if (s === "pinged") pinged += 1;
      return Response.json({
        created: this.cells.size,
        pinged,
        startedAt: this.startedAt,
        elapsedMs: this.startedAt ? Date.now() - this.startedAt : 0,
        run: this.activeRun,
        lastError: this.activeRun?.lastError ?? this.lastRun?.lastError ?? null,
        lastRun: this.lastRun,
      });
    }

    return new Response("not found", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    const API_PATHS = new Set([
      "/events", "/created", "/ping", "/simulate", "/reset", "/run", "/stats", "/mcp",
    ]);
    if (API_PATHS.has(url.pathname)) {
      const stub = env.COLLECTOR.get(env.COLLECTOR.idFromName("cells"));
      return stub.fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};
