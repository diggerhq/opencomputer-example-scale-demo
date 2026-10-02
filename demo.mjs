// One-command real run: collector server + public tunnel + fanout.
//
//   node demo.mjs            → N=1000 real sessions
//   N=250 node demo.mjs
//   COLLECTOR_URL=https://…  → skip the tunnel, reuse a running collector
//   NO_FANOUT=1              → just server + tunnel (drive the visual yourself)
//
// The pinger agent must already be deployed (`opencomputer deploy`, or the
// one-click template button in the README).

import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { Tunnel } from "cloudflared";

const LOCAL_PORT = 8787;
const N = Number(process.env.N || "1000");
const CONCURRENCY = Number(process.env.CONCURRENCY || "40");

const die = (msg) => { console.error(msg); process.exit(1); };

// ── 1. collector server ────────────────────────────────────────────────────
const localStats = `http://localhost:${LOCAL_PORT}/stats`;
const serverUp = () => fetch(localStats).then((r) => r.ok).catch(() => false);

let server;
if (await serverUp()) {
  console.log("collector already running on :8787");
} else {
  server = spawn(process.execPath, ["server/index.mjs"], {
    stdio: ["ignore", "inherit", "inherit"],
  });
  for (let i = 0; i < 50 && !(await serverUp()); i += 1) await sleep(200);
  if (!(await serverUp())) die("collector failed to start on :8787");
}

// ── 2. public URL for the collector (agents must be able to reach it) ──────
let collectorUrl = process.env.COLLECTOR_URL;
let tunnel;
if (!collectorUrl) {
  console.log("opening a cloudflared quick tunnel…");
  tunnel = Tunnel.quick(`http://localhost:${LOCAL_PORT}`);
  collectorUrl = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("tunnel timed out")), 30_000);
    tunnel.once("url", (u) => { clearTimeout(timer); resolve(u); });
    tunnel.once("error", reject);
  }).catch((e) => die(`cloudflared tunnel failed: ${e.message}
Set COLLECTOR_URL yourself (e.g. \`cloudflared tunnel --url http://localhost:8787\`) and retry.`));
}
// Quick tunnels take a few seconds to become routable — verify the public
// URL actually reaches the collector before fanning out, or early posts vanish.
for (let i = 0; i < 60; i += 1) {
  if (await fetch(`${collectorUrl}/stats`).then((r) => r.ok).catch(() => false)) break;
  if (i === 59) die(`collector unreachable at ${collectorUrl} — the tunnel did not come up; retry or set COLLECTOR_URL manually`);
  await sleep(1000);
}
console.log(`collector:  ${collectorUrl}`);
console.log(`visual:     http://localhost:${LOCAL_PORT}/?n=${N}`);

// ── 3. fanout ──────────────────────────────────────────────────────────────
if (process.env.NO_FANOUT === "1") {
  console.log("\nNO_FANOUT=1 — open the visual and hit Simulate, or run:");
  console.log(`  COLLECTOR_URL=${collectorUrl} N=${N} node fanout.mjs`);
} else {
  const fanout = spawn(
    process.execPath,
    ["fanout.mjs"],
    {
      stdio: "inherit",
      env: { ...process.env, COLLECTOR_URL: collectorUrl, N: String(N), CONCURRENCY: String(CONCURRENCY) },
    },
  );
  fanout.on("exit", (code) => {
    console.log(code === 0
      ? `\nrun complete — the visual at http://localhost:${LOCAL_PORT}/?n=${N} shows the result.`
      : `\nfanout exited with code ${code}`);
    process.exit(code ?? 1);
  });
}

// Keep the server + tunnel alive while this process runs; Ctrl-C kills all.
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => { tunnel?.stop(); server?.kill(); process.exit(130); });
}
