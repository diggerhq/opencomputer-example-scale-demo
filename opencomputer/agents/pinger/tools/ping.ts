import { defineTool } from "@opencomputer/agent";

export const ping = defineTool({
  name: "ping",
  description:
    "Send one ping to the scale-demo visualization server. Call exactly once.",
  input: {
    type: "object",
    properties: {
      collector: {
        type: "string",
        description: "Base URL of the demo collector",
      },
      cell: {
        type: "number",
        description: "Cell index this agent lights up",
      },
    },
    required: ["collector", "cell"],
    additionalProperties: false,
  },
  async run({ input, sessionId }) {
    const res = await fetch(`${String(input.collector)}/ping`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ cell: input.cell, sessionId }),
    });
    return { ok: res.ok, status: res.status };
  },
});
