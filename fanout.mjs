// Fan-out: create N real serverless agent sessions, each told to ping the
// collector once. Each cell in the visualization is one session.
//
//   OPENCOMPUTER_API_KEY   required (or ~/.opencomputer/config.json)
//   OPENCOMPUTER_BASE_URL  default https://app.opencomputer.dev/api/managed-agents
//   COLLECTOR_URL          default http://localhost:8787
//   N                      sessions to create (default 100)
//   CONCURRENCY            parallel create+send workers (default 32)
//   RATE                   max session creations per second (default 0 = off)
//   AGENT_ID               default scale-demo@development (your project slug)
//   RESET                  "1" POSTs /reset to the collector first (default 1)
//   RUN_ID                 idempotency prefix (default: timestamp)
//
//   node fanout.mjs

import { OpenComputer } from "@opencomputer/sdk/agents";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const N = Number(process.env.N || 100);
const CONCURRENCY = Number(process.env.CONCURRENCY || 32);
const RATE = Number(process.env.RATE || 0);
const AGENT_ID = process.env.AGENT_ID || "scale-demo@development";
const COLLECTOR = (process.env.COLLECTOR_URL || "http://localhost:8787").replace(/\/+$/, "");
const RUN_ID = process.env.RUN_ID || `run-${Date.now().toString(36)}`;

function apiKey() {
  if (process.env.OPENCOMPUTER_API_KEY) return process.env.OPENCOMPUTER_API_KEY;
  try {
    const cfg = JSON.parse(readFileSync(join(homedir(), ".opencomputer/config.json"), "utf8"));
    return cfg.apiKey || cfg.api_key || cfg.token;
  } catch {
    return null;
  }
}

const key = apiKey();
if (!key) {
  console.error("no API key: set OPENCOMPUTER_API_KEY or run `opencomputer login`");
  process.exit(1);
}
const oc = new OpenComputer({ apiKey: key, baseUrl: process.env.OPENCOMPUTER_BASE_URL });

const post = (path, body) =>
  fetch(`${COLLECTOR}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  }).catch(() => null);

// simple token bucket for RATE creations/s
let last = 0;
async function throttle() {
  if (!RATE) return;
  const wait = Math.max(0, last + 1000 / RATE - Date.now());
  last = Math.max(last + 1000 / RATE, Date.now());
  if (wait) await new Promise((r) => setTimeout(r, wait));
}

let created = 0;
let failed = 0;
const t0 = Date.now();

async function work(cell) {
  try {
    const { session } = await oc.sessions.create(
      { agentId: AGENT_ID, labels: { demo: "scale", run: RUN_ID } },
      { idempotencyKey: `${RUN_ID}/${cell}` },
    );
    created += 1;
    post("/created", { cell });
    await oc.sessions.turns.send(session.id, {
      input: "Ping the demo server, then stop.",
      payload: { collector: COLLECTOR, cell },
      idempotencyKey: `${RUN_ID}/${cell}/turn`,
    });
  } catch (e) {
    failed += 1;
    if (failed <= 10 || failed % 100 === 0) console.error(`cell ${cell}: ${e.message}`);
  }
}

if (process.env.RESET !== "0") await post("/reset", {});
console.log(`${RUN_ID}: fanning out ${N} sessions (${AGENT_ID}), concurrency ${CONCURRENCY}`);

let next = 0;
async function worker() {
  for (let cell; (cell = next++) < N; ) {
    await throttle();
    await work(cell);
    if ((created + failed) % 500 === 0) {
      const s = (Date.now() - t0) / 1000;
      console.log(`${created + failed}/${N} dispatched (${created} ok, ${failed} failed, ${((created + failed) / s).toFixed(1)}/s)`);
    }
  }
}
await Promise.all(Array.from({ length: CONCURRENCY }, worker));

console.log(`${RUN_ID}: dispatched ${created}/${N} sessions in ${((Date.now() - t0) / 1000).toFixed(1)}s (${failed} failed)`);
console.log("pings land as each session's turn completes — watch the visual fill.");
