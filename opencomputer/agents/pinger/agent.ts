import { useInput, useModel, useTool } from "@opencomputer/agent";
import { ping } from "./tools/ping.js";

export default function Agent() {
  const input = useInput();
  const payload = (input.payload ?? {}) as {
    collector?: string;
    cell?: number;
  };

  useModel("anthropic/claude-haiku-4.5");
  useTool(ping);

  return [
    "You are one cell in a scale demo. Call the ping tool exactly once,",
    `with collector=${JSON.stringify(payload.collector ?? "")} and cell=${payload.cell ?? -1},`,
    "then reply with the tool's status and stop.",
  ].join(" ");
}
