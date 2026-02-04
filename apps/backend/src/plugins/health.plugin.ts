import type { Plugin } from "../types";

const healthPlugin: Plugin = {
  name: "health",
  manifest: {
    name: "health",
    kind: "utility",
    title: "Health"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/health",
      handler: () => {
        return new Response(
          JSON.stringify({ ok: true, service: "backend", ts: Date.now() }),
          { headers: { "content-type": "application/json" } }
        );
      }
    });
  }
};

export default healthPlugin;
