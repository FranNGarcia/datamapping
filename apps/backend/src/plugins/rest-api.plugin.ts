import { Buffer } from "buffer";
import type { Plugin } from "../types";
import type { Product, ProductType } from "../domain";

type AuthType = "none" | "bearer" | "basic";

type RestApiConfig = {
  openapiUrl: string;
  authType?: AuthType;
  bearerToken?: string;
  username?: string;
  password?: string;
};

type OpenApiCandidate = {
  category: "accounts" | "creditCards" | "insurances" | "credits" | "investments";
  method: "GET";
  path: string;
  operationId?: string;
  score: number;
  reasons: string[];
  identityParam?: {
    name: string;
    in: "path" | "query" | "header" | "cookie";
  };
};

const configs = new Map<string, RestApiConfig>();

const restApiPlugin: Plugin = {
  name: "rest-api",
  manifest: {
    name: "rest-api",
    kind: "datasource",
    title: "Banking Core API (OpenAPI)",
    description: "Inspect an OpenAPI/Swagger document to find customer product endpoints"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/rest-api/config",
      handler: (req) => {
        const id = requireIdFromUrl(req.url);
        const currentConfig = configs.get(id) ?? null;

        return json({
          id,
          config: currentConfig
            ? {
                ...currentConfig,
                bearerToken: currentConfig.bearerToken ? "********" : "",
                password: currentConfig.password ? "********" : ""
              }
            : null
        });
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/rest-api/config",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const next = parseConfig(config);
          configs.set(id, next);
          return json({ ok: true, id });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/rest-api/test",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          if (!String(cfg.openapiUrl ?? "").trim()) {
            throw new Error("openapiUrl is required");
          }

          const doc = await fetchOpenApiJson(cfg);
          const paths = getPathsObject(doc);
          const pathCount = Object.keys(paths).length;
          const getCount = Object.entries(paths).reduce((acc, [, item]) => {
            if (item && typeof item === "object" && "get" in item && (item as any).get) return acc + 1;
            return acc;
          }, 0);

          return json({ ok: true, id, paths: pathCount, getOperations: getCount });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/rest-api/inspect-openapi",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          if (!String(cfg.openapiUrl ?? "").trim()) {
            throw new Error("openapiUrl is required");
          }

          const doc = await fetchOpenApiJson(cfg);
          const candidates = inspectOpenApi(doc);

          return json({
            ok: true,
            id,
            candidates
          });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/rest-api/deep-inspect-endpoint",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const endpoint = (body as any)?.endpoint;
          if (!endpoint || typeof endpoint !== "object") {
            throw new Error("endpoint is required");
          }

          const method = String((endpoint as any).method ?? "GET").toUpperCase();
          const path = String((endpoint as any).path ?? "").trim();
          if (!path) {
            throw new Error("endpoint.path is required");
          }

          const identityParam = (endpoint as any).identityParam as
            | { name?: string; in?: string }
            | undefined;
          const identityValue = String((endpoint as any).identityValue ?? "").trim();

          const url = buildEndpointUrlFromConfig(cfg, path, identityParam, identityValue);

          const headers = buildAuthHeaders(cfg);
          headers.set("accept", "application/json, */*;q=0.8");

          const res = await fetch(url, { method, headers });
          const status = res.status;
          const statusText = res.statusText;

          let bodyPreview: unknown = null;
          let rawText: string | null = null;
          try {
            bodyPreview = await res.clone().json();
          } catch {
            rawText = await res.text();
            const text = rawText ?? "";
            bodyPreview = text.length > 4000 ? `${text.slice(0, 4000)}...` : text;
          }

          const limitedHeaders: Record<string, string> = {};
          for (const [k, v] of res.headers.entries()) {
            if (limitedHeaders[k] == null && Object.keys(limitedHeaders).length < 20) {
              limitedHeaders[k] = v;
            }
          }

          const products = extractProductsFromBody(bodyPreview, identityValue || null);

          return json({
            ok: true,
            id,
            endpoint: {
              method,
              path,
              identityParam: identityParam
                ? { name: String(identityParam.name ?? ""), in: String(identityParam.in ?? "") }
                : null,
              identityValue: identityValue || null
            },
            status,
            statusText,
            headers: limitedHeaders,
            body: bodyPreview,
            debug: {
              requestedUrl: url,
              authorization: headers.get("authorization") ?? null
            },
            products
          });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });
  }
};

async function fetchOpenApiJson(cfg: RestApiConfig): Promise<unknown> {
  const url = String(cfg.openapiUrl ?? "").trim();
  if (!url) throw new Error("openapiUrl is required");

  const headers = buildAuthHeaders(cfg);
  const res = await fetch(url, { method: "GET", headers });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`OpenAPI HTTP ${res.status}`);
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("OpenAPI document must be JSON");
  }
}

function buildAuthHeaders(cfg: RestApiConfig): Headers {
  const headers = new Headers();
  headers.set("accept", "application/json");

  const authType: AuthType = (cfg.authType ?? "none") as AuthType;
  if (authType === "bearer") {
    const token = String(cfg.bearerToken ?? "").trim();
    if (token) headers.set("authorization", `Bearer ${token}`);
  }

  if (authType === "basic") {
    const username = String(cfg.username ?? "");
    const password = String(cfg.password ?? "");
    const raw = `${username}:${password}`;
    const encoded = Buffer.from(raw, "utf8").toString("base64");
    headers.set("authorization", `Basic ${encoded}`);
  }

  return headers;
}

function buildEndpointUrlFromConfig(
  cfg: RestApiConfig,
  path: string,
  identityParam: { name?: string; in?: string } | undefined,
  identityValue: string
): string {
  const trimmedPath = path.trim();
  if (/^https?:\/\//i.test(trimmedPath)) {
    // Absolute URL already.
    return applyIdentityToUrl(trimmedPath, identityParam, identityValue);
  }

  const openapiUrl = String(cfg.openapiUrl ?? "").trim();
  if (!openapiUrl) {
    throw new Error("openapiUrl is required to build endpoint URL");
  }

  const base = new URL(openapiUrl);
  const url = new URL(trimmedPath, base.origin);
  return applyIdentityToUrl(url.toString(), identityParam, identityValue);
}

function applyIdentityToUrl(
  rawUrl: string,
  identityParam: { name?: string; in?: string } | undefined,
  identityValue: string
): string {
  if (!identityParam || !identityParam.name || !identityValue) {
    return rawUrl;
  }

  const url = new URL(rawUrl);
  const name = String(identityParam.name);
  const where = String(identityParam.in ?? "query").toLowerCase();

  if (where === "path") {
    const placeholder = `{${name}}`;
    const encodedPlaceholder = encodeURIComponent(placeholder); // e.g. %7BcustomerId%7D
    let p = url.pathname;

    if (p.includes(placeholder)) {
      p = p.replace(placeholder, encodeURIComponent(identityValue));
    } else if (p.includes(encodedPlaceholder)) {
      p = p.replace(encodedPlaceholder, encodeURIComponent(identityValue));
    }

    if (p !== url.pathname) {
      url.pathname = p;
      return url.toString();
    }
  }

  if (where === "query") {
    url.searchParams.set(name, identityValue);
    return url.toString();
  }

  // For header/cookie we don't currently inject identity (requires more context).
  // Fallback to leaving URL unchanged.
  return url.toString();
}

function extractProductsFromBody(body: unknown, customerId: string | null): Product[] {
  const out: Product[] = [];
  if (!body || typeof body !== "object") return out;

  const pushIfValid = (raw: any) => {
    if (!raw || typeof raw !== "object") return;

    const amount = Number((raw as any).amount ?? 0);
    const currency = String((raw as any).currency ?? "").trim() || "XXX";
    const expiration = String((raw as any).expiration ?? "").trim();
    const productId = String((raw as any).productId ?? "").trim();
    const name = String((raw as any).name ?? "").trim();
    const type = normalizeProductType(String((raw as any).type ?? ""));
    const status = String((raw as any).status ?? "").trim();

    if (!productId) return;

    const product: Product = {
      customerId: (customerId ?? "").trim(),
      amount: Number.isFinite(amount) ? amount : 0,
      currency,
      expiration,
      productId,
      name: name || productId,
      type,
      status
    };
    out.push(product);
  };

  if (Array.isArray(body)) {
    for (const item of body) pushIfValid(item);
    return out;
  }

  const obj = body as Record<string, unknown>;

  // Common containers: products, accounts, cards, etc.
  for (const [k, v] of Object.entries(obj)) {
    if (!Array.isArray(v)) continue;
    for (const item of v) pushIfValid(item);
  }

  return out;
}

function normalizeProductType(raw: string): ProductType {
  const t = raw.toUpperCase();
  if (t.includes("ACCOUNT") || t.includes("CUENTA")) return "ACCOUNT";
  if (t.includes("CARD") || t.includes("TARJETA")) return "CARD";
  if (t.includes("INVEST")) return "INVESTMENT";
  if (t.includes("CREDIT") || t.includes("LOAN") || t.includes("PRESTAM")) return "CREDIT";
  if (t.includes("INSUR")) return "INSURANCE";
  return "OTHER";
}

function inspectOpenApi(doc: unknown): OpenApiCandidate[] {
  const paths = getPathsObject(doc);
  const out: OpenApiCandidate[] = [];

  for (const [path, pathItem] of Object.entries(paths)) {
    if (!pathItem || typeof pathItem !== "object") continue;
    const getOp = (pathItem as any).get;
    if (!getOp || typeof getOp !== "object") continue;

    const mergedParams = mergeParameters((pathItem as any).parameters, getOp.parameters);
    const identity = findBestIdentityParam(mergedParams, path);
    if (!identity) continue;

    const text = `${path} ${(safeString(getOp.operationId))} ${(safeString(getOp.summary))} ${(safeString(getOp.description))}`
      .toLowerCase()
      .trim();

    const categories = detectCategories(text);
    if (categories.length === 0) continue;

    for (const category of categories) {
      const reasons: string[] = [];
      let score = 0;

      score += identity.score;
      reasons.push(...identity.reasons);

      const categoryScore = scoreCategory(category, text);
      score += categoryScore.score;
      reasons.push(...categoryScore.reasons);

      const extraParamPenalty = penaltyForExtraParams(mergedParams, identity.param);
      score += extraParamPenalty.score;
      reasons.push(...extraParamPenalty.reasons);

      const responseBonus = scoreResponseShape(getOp.responses);
      score += responseBonus.score;
      reasons.push(...responseBonus.reasons);

      out.push({
        category,
        method: "GET",
        path,
        operationId: safeString(getOp.operationId) || undefined,
        score: clampScore(score),
        reasons: reasons.filter(Boolean).slice(0, 6),
        identityParam: { name: identity.param.name, in: identity.param.in }
      });
    }
  }

  out.sort((a, b) => b.score - a.score);
  return out.slice(0, 30);
}

function getPathsObject(doc: unknown): Record<string, unknown> {
  if (!doc || typeof doc !== "object") {
    throw new Error("Invalid OpenAPI document");
  }
  const obj = doc as Record<string, unknown>;
  const paths = obj.paths;
  if (!paths || typeof paths !== "object") {
    throw new Error("OpenAPI document missing 'paths'");
  }
  return paths as Record<string, unknown>;
}

function mergeParameters(a: unknown, b: unknown): Array<{ name: string; in: any; required?: boolean }> {
  const out: Array<{ name: string; in: any; required?: boolean }> = [];
  const pushFrom = (x: unknown) => {
    if (!Array.isArray(x)) return;
    for (const p of x) {
      if (!p || typeof p !== "object") continue;
      const po = p as Record<string, unknown>;
      const name = String(po.name ?? "").trim();
      const pin = String(po.in ?? "").trim();
      if (!name || !pin) continue;
      out.push({ name, in: pin, required: Boolean(po.required) });
    }
  };
  pushFrom(a);
  pushFrom(b);

  const seen = new Set<string>();
  return out.filter((p) => {
    const k = `${p.in}:${p.name}`.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function findBestIdentityParam(
  params: Array<{ name: string; in: any; required?: boolean }>,
  path: string
): { param: { name: string; in: "path" | "query" | "header" | "cookie" }; score: number; reasons: string[] } | null {
  const reasons: string[] = [];
  const identityNames = [
    "identitynumber",
    "identity_number",
    "idnumber",
    "documentnumber",
    "document_number",
    "dni",
    "cedula",
    "cpf",
    "ssn"
  ];

  const normalized = (s: string) => s.toLowerCase().replace(/[^a-z0-9_]/g, "");
  let best: { name: string; in: any; required?: boolean } | null = null;
  let bestScore = -Infinity;
  let bestReason: string | null = null;

  for (const p of params) {
    const n = normalized(p.name);
    let s = 0;
    let r: string | null = null;

    if (n === "identitynumber" || n === "identity_number") {
      s += 55;
      r = `param '${p.name}' matches identityNumber`;
    } else if (identityNames.some((x) => n.includes(normalized(x)))) {
      s += 35;
      r = `param '${p.name}' looks like identity`;
    } else if (n === "customerid" || n === "customer_id") {
      // Treat customerId-style parameters as lower-confidence identity params so that
      // endpoints like /customers/{customerId}/products/headers are not discarded.
      s += 25;
      r = `param '${p.name}' looks like customer identifier`;
    }

    if (s === 0) continue;

    const pin = String(p.in ?? "").toLowerCase();
    if (pin === "path") {
      s += 30;
      r = `${r} (in path)`;
    } else if (pin === "query") {
      s += 20;
      r = `${r} (in query)`;
    } else if (pin === "header") {
      s += 10;
      r = `${r} (in header)`;
    } else if (pin === "cookie") {
      s += 5;
      r = `${r} (in cookie)`;
    }

    if (pin === "path" && path.includes(`{${p.name}}`)) {
      s += 10;
    }

    if (s > bestScore) {
      best = p;
      bestScore = s;
      bestReason = r;
    }
  }

  if (!best || bestScore < 1) return null;
  if (bestReason) reasons.push(bestReason);

  const pin = String(best.in ?? "query").toLowerCase() as any;
  const identityParam = {
    name: best.name,
    in: (pin === "path" || pin === "query" || pin === "header" || pin === "cookie" ? pin : "query") as
      | "path"
      | "query"
      | "header"
      | "cookie"
  };
  return { param: identityParam, score: bestScore, reasons };
}

function detectCategories(text: string): Array<OpenApiCandidate["category"]> {
  const cats: Array<OpenApiCandidate["category"]> = [];

  if (hasAny(text, ["account", "accounts", "checking", "savings", "deposit"])) cats.push("accounts");
  if (hasAny(text, ["credit card", "creditcard", "credit-card", "card", "cards"])) cats.push("creditCards");
  if (hasAny(text, ["insurance", "insurances", "policy", "policies"])) cats.push("insurances");
  if (hasAny(text, ["credit", "credits", "loan", "loans", "lending", "mortgage"])) cats.push("credits");
  if (hasAny(text, ["investment", "investments", "portfolio", "fund", "funds", "securities"])) cats.push("investments");

  // Fallback: treat generic product/product headers endpoints as low-confidence product candidates.
  // This helps surface URLs like /customers/{id}/products/headers even if they don't contain
  // strong account/card/loan/investment keywords.
  if (cats.length === 0 && hasAny(text, ["product", "products"])) {
    cats.push("accounts");
  }

  return [...new Set(cats)];
}

function scoreCategory(category: OpenApiCandidate["category"], text: string): { score: number; reasons: string[] } {
  const reasons: string[] = [];
  const strong: Record<OpenApiCandidate["category"], string[]> = {
    accounts: ["accounts", "account"],
    creditCards: ["credit card", "creditcard", "credit-card"],
    insurances: ["insurance", "policy"],
    credits: ["loan", "credit"],
    investments: ["investment", "portfolio", "fund"]
  };

  const hits = strong[category].filter((k) => text.includes(k));
  if (hits.length > 0) {
    reasons.push(`matched ${category} keywords: ${hits.slice(0, 2).join(", ")}`);
    return { score: 30 + Math.min(10, hits.length * 5), reasons };
  }

  reasons.push(`matched ${category} category`);
  return { score: 20, reasons };
}

function penaltyForExtraParams(
  params: Array<{ name: string; in: any; required?: boolean }>,
  identity: { name: string; in: string }
): { score: number; reasons: string[] } {
  const reasons: string[] = [];

  const relevant = params.filter((p) => {
    const pin = String(p.in ?? "").toLowerCase();
    return pin === "path" || pin === "query";
  });
  const extra = relevant.filter((p) => {
    const pin = String(p.in ?? "").toLowerCase();
    return !(pin === identity.in && String(p.name) === identity.name);
  });

  if (extra.length === 0) return { score: 0, reasons };

  const requiredExtra = extra.filter((p) => Boolean(p.required));
  const penalty = -5 * extra.length + -5 * requiredExtra.length;
  reasons.push(`extra params: ${extra.length}${requiredExtra.length ? ` (${requiredExtra.length} required)` : ""}`);
  return { score: penalty, reasons };
}

function scoreResponseShape(responses: unknown): { score: number; reasons: string[] } {
  const reasons: string[] = [];
  if (!responses || typeof responses !== "object") return { score: 0, reasons };

  const obj = responses as Record<string, unknown>;
  const r200 = (obj["200"] ?? obj["201"] ?? obj["206"]) as any;
  if (!r200 || typeof r200 !== "object") return { score: 0, reasons };

  const content = (r200 as any).content;
  if (!content || typeof content !== "object") return { score: 0, reasons };

  const appJson = (content as any)["application/json"];
  const schema = appJson?.schema;
  const schemaType = schema?.type;
  if (schemaType === "array") {
    reasons.push("response is array");
    return { score: 10, reasons };
  }

  const items = schema?.items;
  if (items && typeof items === "object") {
    reasons.push("response looks like list");
    return { score: 5, reasons };
  }

  return { score: 0, reasons };
}

function safeString(v: unknown): string {
  if (v == null) return "";
  return typeof v === "string" ? v : String(v);
}

function hasAny(text: string, needles: string[]): boolean {
  return needles.some((n) => text.includes(n));
}

function clampScore(score: number): number {
  if (!Number.isFinite(score)) return 0;
  return Math.max(0, Math.min(100, Math.round(score)));
}

function requireConfig(cfg: RestApiConfig | null): RestApiConfig {
  if (!cfg) {
    throw new Error("REST API datasource is not configured");
  }
  return cfg;
}

function parseBodyWithId(body: unknown): { id: string; config?: unknown } {
  if (!body || typeof body !== "object") {
    throw new Error("Invalid body");
  }

  const obj = body as Record<string, unknown>;
  const id = String(obj.id ?? "").trim();
  if (!id) {
    throw new Error("id is required");
  }

  const config = obj.config;
  return { id, config };
}

function requireIdFromUrl(url: string): string {
  const u = new URL(url);
  const id = u.searchParams.get("id") ?? "";
  if (!id) {
    throw new Error("id is required");
  }
  return id;
}

function parseConfig(input: unknown): RestApiConfig {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;
  const openapiUrl = String(obj.openapiUrl ?? "").trim();

  const authType = (String(obj.authType ?? "none").trim() || "none") as AuthType;
  if (authType !== "none" && authType !== "bearer" && authType !== "basic") {
    throw new Error("authType is invalid");
  }

  const bearerToken = obj.bearerToken == null ? undefined : String(obj.bearerToken);
  const username = obj.username == null ? undefined : String(obj.username);
  const password = obj.password == null ? undefined : String(obj.password);

  return { openapiUrl, authType, bearerToken, username, password };
}

async function readJson(req: Request): Promise<unknown> {
  const text = await req.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON");
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" }
  });
}

export default restApiPlugin;
