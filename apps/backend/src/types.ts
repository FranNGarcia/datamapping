export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

export type RouteHandler = (req: Request) => Response | Promise<Response>;

export type RouteDef = {
  method: HttpMethod;
  path: string;
  handler: RouteHandler;
};

export type PluginContext = {
  registerRoute: (route: RouteDef) => void;
};

export type PluginKind = "datasource" | "utility";

export type PluginManifest = {
  name: string;
  kind: PluginKind;
  title: string;
  description?: string;
};

export type Plugin = {
  name: string;
  manifest?: PluginManifest;
  register: (ctx: PluginContext) => void | Promise<void>;
};
