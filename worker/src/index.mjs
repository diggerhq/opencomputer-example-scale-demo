// Hosted scale-demo collector + horizontally sharded fan-out.
// API keys are passed only through live DO requests and are never persisted.

const PING_TOOL = { name: "ping", description: "Ignite one cell on the scale-demo board. Call exactly once with the cell index you were assigned.", inputSchema: { type: "object", properties: { cell: { type: "integer" }, sessionId: { type: "string" } }, required: ["cell"] } };
const MAX_N = 1_000_000;
const MAX_CONCURRENCY = 2_000;
const MAX_CHAINS = 256;
const SHARD_SIZE = 100;
const OUTBOUND_CONNECTIONS_PER_SHARD = 6;
const MAX_ATTEMPTS = 5;
const REQUEST_TIMEOUT_MS = 5_000;
const PROGRESS_BATCH_SIZE = 25;
const MAX_SNAPSHOT_CELLS = 20_000;
const DEFAULT_BASE_URL = "https://app.opencomputer.dev/api/managed-agents";

const jsonRequest = (body) => ({ method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

export class FanoutShard {
  constructor(state, env) { this.state = state; this.env = env; this.running = false; }

  async run(input) {
    const { apiKey, agentToken, baseUrl, agentId, mode, runId, shardIndex, shardStep, totalShards, concurrency } = input;
    const first = shardIndex * SHARD_SIZE;
    const count = Math.min(SHARD_SIZE, input.target - first);
    const result = { runId, shardIndex, attempted: 0, created: 0, createFailed: 0, turnsAdmitted: 0, turnFailed: 0 };
    let nextOffset = 0;
    let fatal = null;
    let lastError = null;
    const collector = this.env.COLLECTOR.get(this.env.COLLECTOR.idFromName("cells"));
    const pendingCreatedCells = [];
    let progressDelivery = Promise.resolve();
    const queueProgress = (force = false) => {
      if (!pendingCreatedCells.length || (!force && pendingCreatedCells.length < PROGRESS_BATCH_SIZE)) return;
      const cells = pendingCreatedCells.splice(0, pendingCreatedCells.length);
      progressDelivery = progressDelivery.then(async () => {
        const response = await collector.fetch(
          "https://collector.internal/progress",
          jsonRequest({ runId, cells }),
        );
        await response.arrayBuffer();
      }).catch((error) => {
        console.warn("progress delivery failed", String(error?.message || error));
      });
    };
    const post = (path, body, key) => {
      const init = {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(agentToken
            ? { "x-opencomputer-agent-token": agentToken }
            : { "x-api-key": apiKey }),
          "idempotency-key": key,
          "x-opencomputer-scale-admission": "create-only-v1",
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      };
      if (agentToken && path === "/sessions" && this.env.MANAGED_AGENTS) {
        return this.env.MANAGED_AGENTS.fetch(
          new Request("https://mo-oc-dev.com/v1/sessions", init),
        );
      }
      return fetch(`${baseUrl}${path}`, init);
    };
    const postWithRetry = async (path, body, key) => {
      let lastError;
      for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
        try {
          const response = await post(path, body, key);
          const retryable = response.status === 429 || [500, 502, 503, 504].includes(response.status);
          if (!retryable || attempt === MAX_ATTEMPTS - 1) return response;
          await response.arrayBuffer();
        } catch (error) {
          lastError = error;
          if (attempt === MAX_ATTEMPTS - 1) throw error;
        }
        const delay = Math.min(100 * (2 ** attempt), 1_000) + Math.floor(Math.random() * 100);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
      throw lastError ?? new Error("request retries exhausted");
    };
    const failureText = async (stage, response) => `${stage} ${response.status}: ${(await response.text()).slice(0, 300)}`;
    const work = async () => {
      for (;;) {
        const offset = nextOffset++;
        if (offset >= count || fatal) return;
        const cell = first + offset;
        result.attempted += 1;
        try {
          const created = await postWithRetry("/sessions", { agentId, labels: { demo: "scale", run: runId } }, `${runId}/${cell}`);
          if (!created.ok) {
            result.createFailed += 1;
            lastError ||= await failureText("sessions", created);
            if ([401, 403, 404].includes(created.status)) fatal = lastError;
            continue;
          }
          const body = await created.json();
          result.created += 1;
          pendingCreatedCells.push(cell);
          queueProgress();
          if (mode === "create") continue;
          const turn = await postWithRetry(`/sessions/${body.session.id}/turns`, { input: "Ping the demo server, then stop.", payload: { cell } }, `${runId}/${cell}/turn`);
          if (turn.ok) result.turnsAdmitted += 1;
          else {
            result.turnFailed += 1;
            lastError ||= await failureText("turns", turn);
            if ([401, 403, 404].includes(turn.status)) fatal = lastError;
          }
        } catch (error) {
          result.createFailed += 1;
          lastError ||= `exception: ${String(error?.message || error).slice(0, 300)}`;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, count) }, work));
    queueProgress(true);
    await progressDelivery;
    const recorded = await collector.fetch("https://collector.internal/shard", jsonRequest({ ...result, fatal, lastError }));
    const state = await recorded.json();
    const nextShard = shardIndex + shardStep;
    if (state.active && !fatal && nextShard < totalShards) {
      const next = this.env.FANOUT.get(this.env.FANOUT.idFromName(`${runId}:${nextShard}`));
      await next.fetch("https://fanout.internal/start", jsonRequest({ ...input, shardIndex: nextShard }));
    }
  }

  async fetch(request) {
    if (request.method !== "POST" || new URL(request.url).pathname !== "/start") return new Response("not found", { status: 404 });
    if (this.running) return Response.json({ ok: true, duplicate: true }, { status: 202 });
    this.running = true;
    const input = await request.json();
    this.state.waitUntil(this.run(input).finally(() => { this.running = false; }));
    return Response.json({ ok: true }, { status: 202 });
  }
}

export class Collector {
  constructor(state, env) {
    this.state = state; this.env = env; this.cells = new Map(); this.clients = new Set();
    this.startedAt = null; this.activeRun = null; this.lastRun = null; this.created = 0; this.pinged = 0;
    this.state.blockConcurrencyWhile(async () => {
      const saved = await this.state.storage.get(["startedAt", "activeRun", "lastRun", "created", "pinged"]);
      this.startedAt = saved.get("startedAt") ?? null; this.lastRun = saved.get("lastRun") ?? null;
      this.activeRun = saved.get("activeRun") ?? null;
      this.created = saved.get("created") ?? 0; this.pinged = saved.get("pinged") ?? 0;
    });
  }

  broadcast(event) {
    const encoded = new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`);
    for (const controller of this.clients) { try { controller.enqueue(encoded); } catch { this.clients.delete(controller); } }
  }
  remember(cell, status) { if (this.cells.size < MAX_SNAPSHOT_CELLS || this.cells.has(cell)) this.cells.set(cell, status); }
  emitCreated(cell) {
    if (this.cells.has(cell)) return;
    this.created += 1; this.remember(cell, "created"); this.broadcast({ type: "created", cell, created: this.created });
  }
  emitCreatedBatch(cells) {
    for (const cell of cells) this.remember(cell, "created");
    this.broadcast({ type: "created_batch", cells, created: this.activeRun?.created ?? this.created });
  }
  emitPing(cell) {
    if (this.cells.get(cell) === "pinged") return;
    this.pinged += 1; this.remember(cell, "pinged"); this.broadcast({ type: "ping", cell, pinged: this.pinged });
  }
  emitPingBatch(cells) {
    for (const cell of cells) this.remember(cell, "pinged");
    this.pinged += cells.length;
    this.broadcast({ type: "ping_batch", cells, pinged: this.pinged });
  }
  async stopSimulation() {
    await this.state.storage.delete("simulation");
    await this.state.storage.deleteAlarm();
  }
  async reset() {
    await this.stopSimulation();
    this.cells.clear(); this.startedAt = null; this.activeRun = null; this.lastRun = null; this.created = 0; this.pinged = 0;
    await this.state.storage.deleteAll(); this.broadcast({ type: "reset" });
  }

  async simulate({ count = 1_000, rate = 100, delay = 800 }) {
    const simulation = { count, rate, delay, startedAt: Date.now(), createdNext: 0, pingNext: 0 };
    await this.state.storage.put("simulation", simulation);
    await this.state.storage.setAlarm(Date.now());
  }

  async alarm() {
    const simulation = await this.state.storage.get("simulation");
    if (!simulation) return;
    const elapsed = Date.now() - simulation.startedAt;
    const createdTarget = Math.min(simulation.count, Math.floor((elapsed * simulation.rate) / 1_000));
    const pingTarget = Math.min(
      simulation.count,
      Math.floor((Math.max(0, elapsed - simulation.delay) * simulation.rate) / 1_000),
    );
    const createdEnd = Math.min(createdTarget, simulation.createdNext + 5_000);
    if (createdEnd > simulation.createdNext) {
      const cells = Array.from({ length: createdEnd - simulation.createdNext }, (_, i) => simulation.createdNext + i);
      for (const cell of cells) this.remember(cell, "created");
      simulation.createdNext = createdEnd;
      this.created = createdEnd;
      this.broadcast({ type: "created_batch", cells, created: this.created });
    }
    const pingEnd = Math.min(pingTarget, simulation.pingNext + 5_000);
    if (pingEnd > simulation.pingNext) {
      const cells = Array.from({ length: pingEnd - simulation.pingNext }, (_, i) => simulation.pingNext + i);
      simulation.pingNext = pingEnd;
      this.emitPingBatch(cells);
    }
    if (simulation.createdNext >= simulation.count && simulation.pingNext >= simulation.count) {
      await this.state.storage.delete("simulation");
      return;
    }
    await this.state.storage.put("simulation", simulation);
    const caughtUp = simulation.createdNext >= createdTarget && simulation.pingNext >= pingTarget;
    await this.state.storage.setAlarm(Date.now() + (caughtUp ? 100 : 1));
  }

  async recordShard(body) {
    const run = this.activeRun;
    if (!run || run.id !== body.runId) return { active: false };
    for (const key of ["attempted", "created", "createFailed", "turnsAdmitted", "turnFailed"]) run[key] += body[key];
    run.completedShards += 1; run.lastError ||= body.lastError || undefined;
    this.created = Math.max(this.created, run.created);
    this.broadcast({ type: "created_batch", cells: [], created: this.created });
    const finished = Boolean(body.fatal) || run.completedShards >= run.totalShards;
    const summary = { ...run, finished };
    this.broadcast({ type: "fanout", run: summary });
    if (finished) {
      this.lastRun = summary; this.activeRun = null;
      await this.state.storage.put({ lastRun: summary, startedAt: this.startedAt, created: this.created, pinged: this.pinged });
      await this.state.storage.delete("activeRun");
    } else {
      await this.state.storage.put("activeRun", run);
    }
    return { active: !finished };
  }

  recordProgress(body) {
    const run = this.activeRun;
    if (!run || run.id !== body.runId) return { active: false };
    const cells = Array.isArray(body.cells) ? body.cells : [];
    for (const cell of cells) this.remember(cell, "created");
    this.created += cells.length;
    this.broadcast({ type: "created_batch", cells, created: this.created });
    return { active: true };
  }

  async startRun(body) {
    const apiKey = String(body.apiKey || "").trim();
    const agentId = String(body.agentId || "").trim();
    const baseUrl = String(body.baseUrl || DEFAULT_BASE_URL).replace(/\/+$/, "");
    const target = Math.min(Math.max(Math.floor(Number(body.n) || 0), 1), MAX_N);
    const totalConcurrency = Math.min(Math.max(Math.floor(Number(body.concurrency) || 40), 1), MAX_CONCURRENCY);
    const mode = body.mode === "turns" ? "turns" : "create";
    if (!apiKey) return Response.json({ error: "apiKey required" }, { status: 400 });
    if (!agentId) return Response.json({ error: "agentId required" }, { status: 400 });
    if (this.activeRun) return Response.json({ error: "a run is already active" }, { status: 409 });
    let agentToken = "";
    if (mode === "create") {
      const tokenResponse = await fetch(`${baseUrl}/dev-scale-token`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": apiKey,
          "x-opencomputer-scale-admission": "create-only-v1",
        },
        body: JSON.stringify({ agentId }),
      });
      if (!tokenResponse.ok) {
        return Response.json(
          { error: `benchmark token ${tokenResponse.status}: ${(await tokenResponse.text()).slice(0, 300)}` },
          { status: 502 },
        );
      }
      const tokenBody = await tokenResponse.json();
      agentToken = typeof tokenBody.token === "string" ? tokenBody.token : "";
      if (!agentToken) return Response.json({ error: "benchmark token was missing" }, { status: 502 });
    }
    await this.stopSimulation();
    this.cells.clear(); this.created = 0; this.pinged = 0; this.lastRun = null; this.startedAt = Date.now();
    const runId = `run-${Date.now().toString(36)}`;
    const totalShards = Math.ceil(target / SHARD_SIZE);
    const chainCount = Math.min(
      totalShards,
      MAX_CHAINS,
      Math.ceil(totalConcurrency / OUTBOUND_CONNECTIONS_PER_SHARD),
    );
    const concurrency = Math.ceil(totalConcurrency / chainCount);
    this.activeRun = { id: runId, mode, target, attempted: 0, created: 0, createFailed: 0, turnsAdmitted: 0, turnFailed: 0, completedShards: 0, totalShards };
    await this.state.storage.deleteAll();
    await this.state.storage.put({ activeRun: this.activeRun, startedAt: this.startedAt, created: 0, pinged: 0 });
    this.broadcast({ type: "reset" });
    const common = {
      ...(agentToken ? { agentToken } : { apiKey }),
      baseUrl,
      agentId,
      mode,
      runId,
      target,
      shardStep: chainCount,
      totalShards,
      concurrency,
    };
    await Promise.all(Array.from({ length: chainCount }, async (_, shardIndex) => {
      const shard = this.env.FANOUT.get(this.env.FANOUT.idFromName(`${runId}:${shardIndex}`));
      await shard.fetch("https://fanout.internal/start", jsonRequest({ ...common, shardIndex }));
    }));
    return Response.json({ ok: true, runId, n: target, mode, concurrency: totalConcurrency, shards: totalShards }, { status: 202 });
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/events") {
      let controller;
      const stream = new ReadableStream({ start(c) { controller = c; c.enqueue(new TextEncoder().encode(":ok\n\n")); }, cancel: () => this.clients.delete(controller) });
      this.clients.add(controller);
      const cells = [...this.cells].map(([cell, status]) => [cell, status === "pinged" ? 2 : 1]);
      controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify({ type: "snapshot", cells, created: this.created, pinged: this.pinged, startedAt: this.startedAt, run: this.activeRun })}\n\n`));
      return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "access-control-allow-origin": "*" } });
    }
    const body = request.method === "POST" ? await request.json().catch(() => ({})) : {};
    if (request.method === "POST" && url.pathname === "/shard") return Response.json(await this.recordShard(body));
    if (request.method === "POST" && url.pathname === "/progress") return Response.json(this.recordProgress(body));
    if (request.method === "POST" && url.pathname === "/mcp") {
      const result = (value) => Response.json({ jsonrpc: "2.0", id: body.id ?? null, result: value });
      const error = (code, message) => Response.json({ jsonrpc: "2.0", id: body.id ?? null, error: { code, message } });
      if (body.method === "initialize") return result({ protocolVersion: body.params?.protocolVersion ?? "2025-03-26", capabilities: { tools: {} }, serverInfo: { name: "scale-demo-collector", version: "2.0.0" } });
      if (typeof body.method === "string" && body.method.startsWith("notifications/")) return new Response(null, { status: 202 });
      if (body.method === "ping") return result({});
      if (body.method === "tools/list") return result({ tools: [PING_TOOL] });
      if (body.method === "tools/call") {
        if (body.params?.name !== "ping") return error(-32601, "unknown tool");
        const cell = body.params?.arguments?.cell; if (typeof cell === "number") this.emitPing(cell);
        return result({ content: [{ type: "text", text: `cell ${cell} ignited` }] });
      }
      return error(-32601, "method not found");
    }
    if (request.method === "POST" && url.pathname === "/created") { if (typeof body.cell === "number") this.emitCreated(body.cell); return Response.json({ ok: true }); }
    if (request.method === "POST" && url.pathname === "/ping") { if (typeof body.cell === "number") this.emitPing(body.cell); return Response.json({ ok: true }); }
    if (request.method === "POST" && url.pathname === "/simulate") {
      if (this.activeRun) return Response.json({ error: "a real run is active" }, { status: 409 });
      await this.reset(); this.startedAt = Date.now();
      await this.simulate({ count: Math.min(Math.max(Number(body.count) || 1_000, 1), MAX_N), rate: Math.min(Math.max(Number(body.rate) || 100, 1), 100_000), delay: Number(body.delay) || 800 });
      return Response.json({ ok: true }, { status: 202 });
    }
    if (request.method === "POST" && url.pathname === "/reset") { await this.reset(); return Response.json({ ok: true }); }
    if (request.method === "POST" && url.pathname === "/run") return this.startRun(body);
    if (request.method === "GET" && url.pathname === "/stats") return Response.json({ created: this.created, pinged: this.pinged, startedAt: this.startedAt, elapsedMs: this.startedAt ? Date.now() - this.startedAt : 0, run: this.activeRun, lastError: this.activeRun?.lastError ?? this.lastRun?.lastError ?? null, lastRun: this.lastRun });
    return new Response("not found", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const paths = new Set(["/events", "/created", "/ping", "/simulate", "/reset", "/run", "/stats", "/mcp"]);
    return paths.has(new URL(request.url).pathname) ? env.COLLECTOR.get(env.COLLECTOR.idFromName("cells")).fetch(request) : env.ASSETS.fetch(request);
  },
};
