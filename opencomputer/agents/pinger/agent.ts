import {
  defineMcpServer,
  useInput,
  useMcpServer,
  useModel,
  useTool,
} from "@opencomputer/agent";
import { ping } from "./tools/ping.js";

// The scale-demo collector's MCP endpoint (must be a string literal — the
// deploy manifest is built from static analysis). The default is the shared
// hosted board; point this at your own deployed `worker/` to light up yours.
const demoCollector = defineMcpServer({
  id: "demo-collector",
  url: "https://scale-demo-mo-dev.ujn.workers.dev/mcp",
});

export default function Agent() {
  const input = useInput();
  const payload = (input.payload ?? {}) as {
    collector?: string;
    cell?: number;
  };

  useModel("anthropic/claude-haiku-4.5");

  // Two ping paths for the demo: the MCP call goes straight from the agent's
  // runtime to the demo server, while the `ping` tool exists so a local
  // collector (its URL arrives per-turn in the payload) works too.
  if (!payload.collector) {
    useMcpServer(demoCollector);
    return [
      "You are one cell in a scale demo. Call the demo-collector MCP server's",
      `ping tool exactly once with cell=${payload.cell ?? -1},`,
      "then reply with the tool's status and stop.",
    ].join(" ");
  }

  useTool(ping);
  return [
    "You are one cell in a scale demo. Call the ping tool exactly once,",
    `with collector=${JSON.stringify(payload.collector)} and cell=${payload.cell ?? -1},`,
    "then reply with the tool's status and stop.",
  ].join(" ");
}
