import type { HttpMethod, RouteDef, RouteHandler } from "./types";

type Key = `${HttpMethod} ${string}`;

export class Router {
  private routes = new Map<Key, RouteHandler>();

  register(route: RouteDef) {
    const key = this.key(route.method, route.path);
    this.routes.set(key, route.handler);
  }

  async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const key = this.key(req.method as HttpMethod, url.pathname);
    const handler = this.routes.get(key);

    if (!handler) {
      return new Response(JSON.stringify({ error: "Not Found" }), {
        status: 404,
        headers: { "content-type": "application/json" }
      });
    }

    try {
      return await handler(req);
    } catch (e) {
      return new Response(
        JSON.stringify({
          error: e instanceof Error ? e.message : String(e)
        }),
        {
          status: 500,
          headers: { "content-type": "application/json" }
        }
      );
    }
  }

  private key(method: HttpMethod, path: string): Key {
    return `${method} ${path}` as Key;
  }
}
