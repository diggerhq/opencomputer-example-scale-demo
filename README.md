# OpenComputer Scale Demo

Spin up thousands of real serverless agent sessions — each one runs a model
turn and pings a shared server, and a live 3D visualization paints the
`opencomputer` wordmark as the pings land.

No sandboxes, no servers: every session is a durable agent session with a
single `ping` tool. Because it never touches a VM, sessions spawn in
milliseconds and each one costs nothing but a model call.

[Deploy with OpenComputer →](https://app.opencomputer.dev/new?repository-url=https%3A%2F%2Fgithub.com%2Fdiggerhq%2Fopencomputer-example-scale-demo)

## One click, zero setup

The `worker/` directory is the whole demo as one deployable unit — collector,
fan-out, and viz inside a Cloudflare Worker + Durable Object. `cd worker &&
npx wrangler deploy`, then open the page, paste your API key + agent id, and
hit **Run real agents**. The worker also serves `/mcp` — agents ping it as a
remote MCP server, so nothing extra has to run. Nothing runs locally, and
your key is only held for the duration of the run and is never persisted. The
hosted fan-out shards work across multiple Durable Objects (cap: 1,000,000
sessions, 2,000 in-flight requests, one active run). Up to 256 shard chains
run concurrently, with small 100-session shards keeping work flowing between
waves. Retryable create failures use the same idempotency key with bounded
backoff. Million-session runs use
the default **create sessions** mode; choose **create + model turn** only when
you intentionally want provider traffic and cost.

Point `demo-collector`'s URL in `opencomputer/agents/pinger/agent.ts` at your
deployed worker's `/mcp` path to light up your own board.

## Or run it locally (3 commands)

```bash
npm install
npx --package @opencomputer/cli opencomputer login
npm run demo
```

`npm run demo` starts the collector, opens a public tunnel to it, and fans
out **1,000 real sessions** against your deployed agent — watch the wordmark
light up at `http://localhost:8787/?n=1000`.

If you used the Deploy button above, your project is already linked and the
agent deployed. Otherwise link and deploy once first:

```bash
npx --package @opencomputer/cli opencomputer link --create-project scale-demo
npm run deploy
```

No agents handy? The visual works standalone — `npm run server` then hit
**Simulate run**, or open `http://localhost:8787/?auto=1`.

## What's inside

- `opencomputer/agents/pinger/` — the agent (~40 lines): declares `haiku-4.5`
  and pings the collector as an MCP server (`defineMcpServer` +
  `useMcpServer`), or via a plain `ping` tool when the turn payload carries a
  collector URL.
- `server/` — a zero-dependency Node collector + visualization server on
  `:8787`: serves the WebGL point-cloud page, accepts `/created` and `/ping`
  posts, streams updates over SSE.
- `fanout.mjs` — the orchestrator (~80 lines): creates N sessions through the
  TypeScript SDK and sends each a turn carrying its cell index + collector
  URL, under a concurrency cap.
- `demo.mjs` — glues it together: server + cloudflared tunnel + fanout in one
  command.
- `worker/` — the hosted version: the same collector + fan-out + viz as a
  Cloudflare Worker with a Durable Object (`npx wrangler deploy`).

## How the ping-back works

1. `fanout.mjs` calls `sessions.create` and immediately POSTs `/created` to
   the collector — a dim particle flies into place.
2. The session runs on OpenComputer's serverless runtime (a Durable Object,
   no VM) and the model makes an MCP `ping` call straight to the collector
   (`POST /mcp` on the worker) — that's the real ping-back, and the moment
   the dot ignites. In the local flow the turn payload carries a collector
   URL instead, so the agent calls its plain `ping` tool, which `fetch`es
   `${COLLECTOR_URL}/ping` from inside the agent. No VM is allocated on
   either path.

## Tuning

| Variable | Default | Purpose |
|---|---|---|
| `N` | `1000` | Number of sessions (`demo.mjs`; `fanout.mjs` default 100) |
| `CONCURRENCY` | `40` | Max in-flight session creates |
| `COLLECTOR_URL` | tunnel / `localhost:8787` | Collector base URL agents ping |
| `AGENT_ID` | `scale-demo@development` | Agent to address |
| `OPENCOMPUTER_API_KEY` | `~/.opencomputer/config.json` | API key |
| `OPENCOMPUTER_BASE_URL` | production API | Managed-agents API base URL |
| `RATE` | uncapped | Optional creations/sec cap |
| `RESET` | `1` | Reset the collector before the run |
| `NO_FANOUT` | unset | Server + tunnel only, no fanout |

Set `AGENT_ID` to your project slug (e.g. `scale-demo@development`).

## Numbers

~1,000 real sessions dispatched in ~2 minutes at 40-way concurrency; every
ping landed. Throughput scales with concurrency and provider rate limits —
there is nothing to warm up or pre-provision.
