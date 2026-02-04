import postgres from "postgres";
import type { Plugin } from "../types";
import {
  suggestCustomerMappingsDeepInspectWithGemini,
  suggestCustomerMappingsWithGemini
} from "../infrastructure/gemini";
import { getCustomerAggregateFieldPaths } from "../domain/customerFields";
import type { DatasourceQueryRequest, DatasourceQueryResponse } from "../domain/query";

type PostgresConfig = {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
  ssl?: boolean;
};

type PgSchemaColumn = {
  name: string;
  dataType: string;
  isNullable: boolean;
  ordinalPosition: number;
};

type PgSchemaTable = {
  schema: string;
  name: string;
  columns: PgSchemaColumn[];
};

type MappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

const configs = new Map<string, PostgresConfig>();
const clients = new Map<string, { sig: string; sql: postgres.Sql }>();

function logConnectionEndedIfNeeded(e: unknown, cfg: PostgresConfig) {
  const msg = e instanceof Error ? e.message : String(e ?? "");
  // Supabase pooler may surface either CONNECTION_ENDED or pool-size/session limit errors.
  if (
    msg.includes("CONNECTION_ENDED") ||
    msg.includes("MaxClientsInSessionMode") ||
    msg.toLowerCase().includes("max clients reached")
  ) {
    console.warn(`CONNECTION_ENDED ${cfg.host}:${cfg.port}`);
  }
}

const postgresPlugin: Plugin = {
  name: "postgres",
  manifest: {
    name: "postgres",
    kind: "datasource",
    title: "PostgreSQL",
    description: "Connect to a PostgreSQL database"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/postgres/config",
      handler: (req) => {
        const id = requireIdFromUrl(req.url);
        const currentConfig = configs.get(id) ?? null;

        return json({
          id,
          config: currentConfig
            ? {
                ...currentConfig,
                password: currentConfig.password ? "********" : undefined
              }
            : null
        });
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/postgres/deep-inspect",
      handler: async (req) => {
        let toInspect: PostgresConfig | null = null;
        try {
          const body = (await readJson(req)) as any;
          const { id, config } = parseBodyWithId(body);
          toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const tables = (body?.tables ?? null) as PgSchemaTable[] | null;
          if (!Array.isArray(tables) || tables.length === 0) {
            throw new Error("Deep inspect requires schema tables. Run schema inspect first.");
          }

          const candidates = (body?.candidates ?? []) as MappingCandidate[];
          const sql = getClient(id, toInspect);

          const byTable = new Map<string, MappingCandidate[]>();
          for (const c of candidates) {
            const t = String((c as any)?.table ?? "").trim();
            if (!t) continue;
            if (!byTable.has(t)) byTable.set(t, []);
            byTable.get(t)!.push(c);
          }

          const samples: Array<{ table: string; rows: unknown[] }> = [];
          for (const [tableKey] of byTable.entries()) {
            const [schema, table] = tableKey.split(".");
            if (!schema || !table) continue;
            const q = `select * from ${pgIdent(schema)}.${pgIdent(table)} limit 5`;
            let rows: unknown[] = [];
            try {
              rows = (await (sql as any).unsafe(q, [])) as unknown[];
            } catch {
              rows = [];
            }
            samples.push({ table: tableKey, rows: Array.isArray(rows) ? rows.slice(0, 5) : [] });
          }

          const customerFields = getCustomerAggregateFieldPaths();
          const ai = await suggestCustomerMappingsDeepInspectWithGemini({
            schema: { tables },
            customerModel: { aggregateName: "Customer", fields: customerFields },
            samples,
            existingCandidates: candidates
          });

          return json({ ok: true, id, tables, candidates: ai.candidates, samples });
        } catch (e) {
          if (toInspect) logConnectionEndedIfNeeded(e, toInspect);
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        }
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/postgres/config",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const next = parseConfig(config);
          configs.set(id, next);
          resetClient(id);
          return json({ ok: true, id });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/postgres/test",
      handler: async (req) => {
        let toTest: PostgresConfig | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          toTest = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const sql = getClient(id, toTest);
          await sql`select 1 as ok`;

          return json({ ok: true });
        } catch (e) {
          if (toTest) logConnectionEndedIfNeeded(e, toTest);
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/postgres/schema",
      handler: async (req) => {
        let toInspect: PostgresConfig | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const sql = getClient(id, toInspect);

          const rows = (await sql`
            select
              table_schema,
              table_name,
              column_name,
              data_type,
              is_nullable,
              ordinal_position
            from information_schema.columns
            where table_schema not in ('pg_catalog', 'information_schema')
            order by table_schema, table_name, ordinal_position
          `) as Array<{
            table_schema: string;
            table_name: string;
            column_name: string;
            data_type: string;
            is_nullable: "YES" | "NO";
            ordinal_position: number;
          }>;

          const tables = buildSchema(rows);

          return json({ ok: true, id, tables });
        } catch (e) {
          if (toInspect) logConnectionEndedIfNeeded(e, toInspect);
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/postgres/suggest-mappings",
      handler: async (req) => {
        let toInspect: PostgresConfig | null = null;
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const sql = getClient(id, toInspect);

          const rows = (await sql`
            select
              table_schema,
              table_name,
              column_name,
              data_type,
              is_nullable,
              ordinal_position
            from information_schema.columns
            where table_schema not in ('pg_catalog', 'information_schema')
            order by table_schema, table_name, ordinal_position
          `) as Array<{
            table_schema: string;
            table_name: string;
            column_name: string;
            data_type: string;
            is_nullable: "YES" | "NO";
            ordinal_position: number;
          }>;

          const tables = buildSchema(rows);

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
          if (toInspect) logConnectionEndedIfNeeded(e, toInspect);
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/postgres/query",
      handler: async (req) => {
        let cfg: PostgresConfig | null = null;
        try {
          const body = (await readJson(req)) as DatasourceQueryRequest | null;
          const { id, config } = parseBodyWithId(body);
          cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const q = String((body as any)?.query ?? "").trim();
          if (!q) {
            throw new Error("query is required");
          }

          const limitNum = Number((body as any)?.limit ?? 100);
          const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.min(limitNum, 1000) : 100;

          const params = (body as any)?.params;
          const args = Array.isArray(params) ? params : [];

          const sql = getClient(id, cfg);

          const rows = (await (sql as any).unsafe(q, args)) as unknown[];

          const limited = Array.isArray(rows) ? rows.slice(0, limit) : [];
          const columns = limited.length > 0 && limited[0] && typeof limited[0] === "object"
            ? Object.keys(limited[0] as Record<string, unknown>)
            : [];

          const resp: DatasourceQueryResponse = { ok: true, id, columns, rows: limited };
          return json(resp);
        } catch (e) {
          if (cfg) logConnectionEndedIfNeeded(e, cfg);
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        }
      }
    });
  }
};

function buildSchema(
  rows: Array<{
    table_schema: string;
    table_name: string;
    column_name: string;
    data_type: string;
    is_nullable: "YES" | "NO";
    ordinal_position: number;
  }>
): PgSchemaTable[] {
  const map = new Map<string, PgSchemaTable>();

  for (const r of rows) {
    const key = `${r.table_schema}.${r.table_name}`;
    let t = map.get(key);
    if (!t) {
      t = { schema: r.table_schema, name: r.table_name, columns: [] };
      map.set(key, t);
    }

    t.columns.push({
      name: r.column_name,
      dataType: r.data_type,
      isNullable: r.is_nullable === "YES",
      ordinalPosition: Number(r.ordinal_position)
    });
  }

  return Array.from(map.values());
}

function requireConfig(cfg: PostgresConfig | null): PostgresConfig {
  if (!cfg) {
    throw new Error("PostgreSQL datasource is not configured");
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

function parseConfig(input: unknown): PostgresConfig {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;

  const host = String(obj.host ?? "").trim();
  const database = String(obj.database ?? "").trim();
  const user = String(obj.user ?? "").trim();
  const password = obj.password == null ? undefined : String(obj.password);
  const portNum = Number(obj.port ?? 5432);
  const ssl = obj.ssl == null ? host.includes("supabase.co") : Boolean(obj.ssl);

  if (!host) throw new Error("host is required");
  if (!database) throw new Error("database is required");
  if (!user) throw new Error("user is required");
  if (!Number.isFinite(portNum) || portNum <= 0) throw new Error("port is invalid");

  return {
    host,
    port: portNum,
    database,
    user,
    password,
    ssl
  };
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

export default postgresPlugin;

function pgIdent(name: string): string {
  const s = String(name ?? "");
  return `"${s.replace(/"/g, '""')}"`;
}

function signature(cfg: PostgresConfig): string {
  return JSON.stringify({
    host: cfg.host,
    port: cfg.port,
    database: cfg.database,
    user: cfg.user,
    password: cfg.password ?? "",
    ssl: Boolean(cfg.ssl)
  });
}

function resetClient(id: string) {
  const existing = clients.get(id);
  if (!existing) return;
  clients.delete(id);
  try {
    existing.sql.end({ timeout: 2 });
  } catch {
    // ignore
  }
}

function getClient(id: string, cfg: PostgresConfig): postgres.Sql {
  const sig = signature(cfg);
  const existing = clients.get(id);
  if (existing && existing.sig === sig) {
    return existing.sql;
  }

  if (existing) {
    resetClient(id);
  }

  // NOTE: When using Supabase pooler (especially Session mode), opening a new TCP connection per request can
  // quickly exhaust pool_size. Reuse a single client per datasource instance and keep pool size tiny.
  const sql = postgres({
    host: cfg.host,
    port: cfg.port,
    database: cfg.database,
    username: cfg.user,
    password: cfg.password,
    ssl: cfg.ssl ? "require" : undefined,
    max: 1,
    idle_timeout: 30,
    connect_timeout: 10,
    prepare: false
  });

  clients.set(id, { sig, sql });
  return sql;
}
