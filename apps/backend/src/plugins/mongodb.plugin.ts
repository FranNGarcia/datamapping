import { MongoClient } from "mongodb";
import type { Plugin } from "../types";
import {
  suggestCustomerMappingsDeepInspectWithGemini,
  suggestCustomerMappingsWithGemini
} from "../infrastructure/gemini";
import { getCustomerAggregateFieldPaths } from "../domain/customerFields";
import type { DatasourceQueryRequest, DatasourceQueryResponse } from "../domain/query";

type MongoDbConfig = {
  uri: string;
  database: string;
  collection: string;
  sampleSize?: number;
};

type DbSchemaColumn = {
  name: string;
  dataType: string;
  isNullable: boolean;
  ordinalPosition: number;
};

type DbSchemaTable = {
  schema: string;
  name: string;
  columns: DbSchemaColumn[];
};

type MappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

const configs = new Map<string, MongoDbConfig>();

const mongodbPlugin: Plugin = {
  name: "mongodb",
  manifest: {
    name: "mongodb",
    kind: "datasource",
    title: "MongoDB",
    description: "Connect to a MongoDB database and map collections to Customer"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/mongodb/config",
      handler: (req) => {
        const id = requireIdFromUrl(req.url);
        const currentConfig = configs.get(id) ?? null;

        return json({
          id,
          config: currentConfig
            ? {
                ...currentConfig,
                uri: currentConfig.uri ? maskMongoUri(currentConfig.uri) : ""
              }
            : null
        });
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mongodb/deep-inspect",
      handler: async (req) => {
        let client: MongoClient | null = null;
        try {
          const body = (await readJson(req)) as any;
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const tables = (body?.tables ?? null) as DbSchemaTable[] | null;
          if (!Array.isArray(tables) || tables.length === 0) {
            throw new Error("Deep inspect requires schema tables. Run schema inspect first.");
          }

          const candidates = (body?.candidates ?? []) as MappingCandidate[];

          client = await new MongoClient(toInspect.uri).connect();
          const db = client.db(toInspect.database);
          const collection = db.collection(toInspect.collection);

          const rows = (await collection.find({}).limit(5).toArray()) as unknown[];
          const tableKey = `${toInspect.database}.${toInspect.collection}`;
          const samples = [{ table: tableKey, rows }];

          const customerFields = getCustomerAggregateFieldPaths();
          const ai = await suggestCustomerMappingsDeepInspectWithGemini({
            schema: { tables },
            customerModel: { aggregateName: "Customer", fields: customerFields },
            samples,
            existingCandidates: candidates
          });

          return json({ ok: true, id, tables, candidates: ai.candidates, samples });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        } finally {
          if (client) {
            try {
              await client.close();
            } catch {
              // ignore
            }
          }
        }
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/mongodb/config",
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
      path: "/api/datasources/mongodb/test",
      handler: async (req) => {
        let client: MongoClient | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toTest = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          client = await new MongoClient(toTest.uri).connect();
          const db = client.db(toTest.database);

          const collections = await db.listCollections({}, { nameOnly: true }).toArray();
          const names = (collections ?? [])
            .map((c: any) => String(c?.name ?? "").trim())
            .filter(Boolean)
            .slice(0, 50);

          return json({ ok: true, id, database: toTest.database, collections: names });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        } finally {
          if (client) {
            try {
              await client.close();
            } catch {
              // ignore
            }
          }
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mongodb/schema",
      handler: async (req) => {
        let client: MongoClient | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          client = await new MongoClient(toInspect.uri).connect();
          const tables = await buildSchema(client, toInspect);

          return json({ ok: true, id, tables });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        } finally {
          if (client) {
            try {
              await client.close();
            } catch {
              // ignore
            }
          }
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mongodb/suggest-mappings",
      handler: async (req) => {
        let client: MongoClient | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          client = await new MongoClient(toInspect.uri).connect();
          const tables = await buildSchema(client, toInspect);

          const customerFields = getCustomerAggregateFieldPaths();
          const ai = await suggestCustomerMappingsWithGemini({
            schema: { tables },
            customerModel: {
              aggregateName: "Customer",
              fields: customerFields
            }
          });

          return json({ ok: true, id, tables, candidates: ai.candidates });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        } finally {
          if (client) {
            try {
              await client.close();
            } catch {
              // ignore
            }
          }
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mongodb/query",
      handler: async (req) => {
        let client: MongoClient | null = null;
        try {
          const body = (await readJson(req)) as DatasourceQueryRequest | null;
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const q = String((body as any)?.query ?? "find").trim().toLowerCase();
          if (q !== "find") {
            throw new Error('MongoDB query supports only query="find" for now');
          }

          const limitNum = Number((body as any)?.limit ?? 100);
          const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.min(limitNum, 1000) : 100;

          const params = (body as any)?.params;
          const filter = params && typeof params === "object" ? (params as any).filter : undefined;
          const projection = params && typeof params === "object" ? (params as any).projection : undefined;

          client = await new MongoClient(cfg.uri).connect();
          const db = client.db(cfg.database);
          const collection = db.collection(cfg.collection);

          const rows = (await collection
            .find(filter ?? {}, { projection: projection ?? { _id: 0 } })
            .limit(limit)
            .toArray()) as unknown[];

          const columns = rows.length > 0 && rows[0] && typeof rows[0] === "object"
            ? Object.keys(rows[0] as Record<string, unknown>)
            : [];

          const resp: DatasourceQueryResponse = { ok: true, id, columns, rows };
          return json(resp);
        } catch (e) {
          return json(
            {
              ok: false,
              id: (e && typeof e === "object" ? String((e as any).id ?? "") : "") || "",
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        } finally {
          if (client) {
            try {
              await client.close();
            } catch {
              // ignore
            }
          }
        }
      }
    });
  }
};

async function buildSchema(client: MongoClient, cfg: MongoDbConfig): Promise<DbSchemaTable[]> {
  const db = client.db(cfg.database);
  const collection = db.collection(cfg.collection);

  const sampleSize = Number(cfg.sampleSize ?? 50);
  const n = Number.isFinite(sampleSize) && sampleSize > 0 ? Math.min(sampleSize, 500) : 50;

  const docs = await collection.find({}).limit(n).toArray();

  const stats = new Map<string, { present: number; types: Set<string> }>();
  for (const doc of docs as any[]) {
    const flat = flattenMongoDoc(doc);
    for (const [k, v] of Object.entries(flat)) {
      let st = stats.get(k);
      if (!st) {
        st = { present: 0, types: new Set<string>() };
        stats.set(k, st);
      }
      st.present += 1;
      st.types.add(inferMongoType(v));
    }
  }

  // Always include _id so AI candidate suggestion can map it.
  if (!stats.has("_id")) {
    const st = { present: docs.length, types: new Set<string>() };
    // If we sampled docs but didn't see _id for some reason, consider it non-null.
    st.types.add(docs.length > 0 ? "objectId" : "objectId | null");
    stats.set("_id", st);
  }

  const total = docs.length;
  const columns: DbSchemaColumn[] = Array.from(stats.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, st], idx) => {
      const dataType = Array.from(st.types.values()).sort().join(" | ") || "unknown";
      const isNullable = total === 0 ? true : st.present < total;
      return {
        name,
        dataType,
        isNullable,
        ordinalPosition: idx + 1
      };
    });

  return [
    {
      schema: cfg.database,
      name: cfg.collection,
      columns
    }
  ];
}

function flattenMongoDoc(input: unknown, prefix = ""): Record<string, unknown> {
  const out: Record<string, unknown> = {};

  if (!input || typeof input !== "object") {
    return out;
  }

  const obj = input as Record<string, unknown>;
  for (const [key, value] of Object.entries(obj)) {
    const k = prefix ? `${prefix}.${key}` : key;

    if (value && typeof value === "object") {
      if (Array.isArray(value)) {
        out[k] = value;
      } else {
        const nested = flattenMongoDoc(value, k);
        for (const [nk, nv] of Object.entries(nested)) {
          out[nk] = nv;
        }
      }
    } else {
      out[k] = value;
    }
  }

  return out;
}

function inferMongoType(v: unknown): string {
  if (v === null) return "null";
  if (v === undefined) return "undefined";
  if (Array.isArray(v)) {
    const inner = new Set<string>();
    for (const x of v.slice(0, 20)) {
      inner.add(inferMongoType(x));
    }
    const t = Array.from(inner.values()).sort().join(" | ") || "unknown";
    return `array<${t}>`;
  }
  const t = typeof v;
  if (t === "string") return "string";
  if (t === "number") return "number";
  if (t === "boolean") return "boolean";
  if (t === "bigint") return "bigint";
  if (t === "object") return "object";
  return t;
}

function maskMongoUri(uri: string): string {
  const u = String(uri ?? "");
  if (!u) return "";

  // Mask credentials in: mongodb://user:pass@host/... or mongodb+srv://user:pass@host/...
  // Keep the host+path intact.
  return u.replace(/\/\/([^@/]+)@/i, "//********:********@");
}

function requireConfig(cfg: MongoDbConfig | null): MongoDbConfig {
  if (!cfg) {
    throw new Error("MongoDB datasource is not configured");
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

function parseConfig(input: unknown): MongoDbConfig {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;
  const uri = String(obj.uri ?? "").trim();
  const database = String(obj.database ?? "").trim();
  const collection = String(obj.collection ?? "").trim();
  const sampleSize = obj.sampleSize == null ? undefined : Number(obj.sampleSize);

  if (!uri) throw new Error("uri is required");
  if (!database) throw new Error("database is required");
  if (!collection) throw new Error("collection is required");
  if (sampleSize != null && (!Number.isFinite(sampleSize) || sampleSize <= 0)) {
    throw new Error("sampleSize is invalid");
  }

  return { uri, database, collection, sampleSize };
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

export default mongodbPlugin;
