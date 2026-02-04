import { Router } from "./router";
import { loadPlugins } from "./plugins";

const PORT = Number(Bun.env.PORT ?? "3001");

const router = new Router();

const plugins = await loadPlugins({
  registerRoute: (route) => router.register(route)
});

router.register({
  method: "GET",
  path: "/api/plugins",
  handler: () => {
    const manifests = plugins.map((p) => {
      return (
        p.manifest ?? {
          name: p.name,
          kind: "utility",
          title: p.name
        }
      );
    });

    return new Response(JSON.stringify({ plugins: manifests }), {
      headers: { "content-type": "application/json" }
    });
  }
});

Bun.serve({
  port: PORT,
  fetch: (req: Request) => {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(req)
      });
    }

    if (url.pathname.startsWith("/api/")) {
      return withCors(req, router.handle(req));
    }

    return new Response(JSON.stringify({
      service: "backend",
      plugins: plugins.map((p) => p.name)
    }), {
      headers: {
        "content-type": "application/json",
        ...corsHeaders(req)
      }
    });
  }
});

function corsHeaders(req: Request): Record<string, string> {
  const origin = req.headers.get("origin") ?? "*";

  return {
    "access-control-allow-origin": origin,
    "access-control-allow-methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
    "access-control-allow-headers": "content-type,authorization",
    "access-control-allow-credentials": "true",
    "vary": "origin"
  };
}

async function withCors(req: Request, responsePromise: Promise<Response>): Promise<Response> {
  const res = await responsePromise;
  const headers = new Headers(res.headers);
  const cors = corsHeaders(req);

  for (const [k, v] of Object.entries(cors)) {
    headers.set(k, v);
  }

  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}
