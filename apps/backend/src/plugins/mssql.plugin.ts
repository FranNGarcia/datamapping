import * as sql from "mssql";
import type { Plugin } from "../types";
import {
  suggestCustomerMappingsDeepInspectWithGemini,
  suggestCustomerMappingsWithGemini
} from "../infrastructure/gemini";
import { getCustomerAggregateFieldPaths } from "../domain/customerFields";
import type { DatasourceQueryRequest, DatasourceQueryResponse } from "../domain/query";

type MsSqlConfig = {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
  encrypt?: boolean;
  trustServerCertificate?: boolean;
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

const configs = new Map<string, MsSqlConfig>();

const mssqlPlugin: Plugin = {
  name: "mssql",
  manifest: {
    name: "mssql",
    kind: "datasource",
    title: "SQL Server",
    description: "Connect to a Microsoft SQL Server database"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/mssql/config",
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
      path: "/api/datasources/mssql/deep-inspect",
      handler: async (req) => {
        try {
          const body = (await readJson(req)) as any;
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const tables = (body?.tables ?? null) as DbSchemaTable[] | null;
          if (!Array.isArray(tables) || tables.length === 0) {
            throw new Error("Deep inspect requires schema tables. Run schema inspect first.");
          }

          const candidates = (body?.candidates ?? []) as MappingCandidate[];

          const pool = await (sql as any).connect({
            server: toInspect.host,
            port: toInspect.port,
            database: toInspect.database,
            user: toInspect.user,
            password: toInspect.password,
            options: {
              encrypt: Boolean(toInspect.encrypt ?? false),
              trustServerCertificate: Boolean(toInspect.trustServerCertificate ?? false)
            }
          });

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
            const q = `select top (5) * from ${msIdent(schema)}.${msIdent(table)}`;
            let rows: unknown[] = [];
            try {
              const r = await pool.request().query(q);
              rows = (r?.recordset ?? []) as unknown[];
            } catch {
              rows = [];
            }
            samples.push({ table: tableKey, rows: Array.isArray(rows) ? rows.slice(0, 5) : [] });
          }

          await pool.close();

          const customerFields = getCustomerAggregateFieldPaths();
          const ai = await suggestCustomerMappingsDeepInspectWithGemini({
            schema: { tables },
            customerModel: { aggregateName: "Customer", fields: customerFields },
            samples,
            existingCandidates: candidates
          });

          return json({ ok: true, id, tables, candidates: ai.candidates, samples });
        } catch (e) {
          try {
            await (sql as any).close();
          } catch {
            // ignore
          }
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/mssql/config",
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
      path: "/api/datasources/mssql/test",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toTest = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const pool = await (sql as any).connect({
            server: toTest.host,
            port: toTest.port,
            database: toTest.database,
            user: toTest.user,
            password: toTest.password,
            options: {
              encrypt: Boolean(toTest.encrypt ?? false),
              trustServerCertificate: Boolean(toTest.trustServerCertificate ?? false)
            }
          });

          await pool.request().query("select 1 as ok");
          await pool.close();

          return json({ ok: true });
        } catch (e) {
          try {
            await (sql as any).close();
          } catch {
            // ignore
          }
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mssql/schema",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const pool = await (sql as any).connect({
            server: toInspect.host,
            port: toInspect.port,
            database: toInspect.database,
            user: toInspect.user,
            password: toInspect.password,
            options: {
              encrypt: Boolean(toInspect.encrypt ?? false),
              trustServerCertificate: Boolean(toInspect.trustServerCertificate ?? false)
            }
          });

          const result = await pool
            .request()
            .query(`
              select
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                ordinal_position
              from information_schema.columns
              where table_schema not in ('INFORMATION_SCHEMA', 'sys')
              order by table_schema, table_name, ordinal_position
            `);

          const rows = (result.recordset ?? []) as Array<{
            table_schema: string;
            table_name: string;
            column_name: string;
            data_type: string;
            is_nullable: "YES" | "NO";
            ordinal_position: number;
          }>;

          const tables = buildSchema(rows);
          await pool.close();

          return json({ ok: true, id, tables });
        } catch (e) {
          try {
            await (sql as any).close();
          } catch {
            // ignore
          }
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mssql/suggest-mappings",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toInspect = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const pool = await (sql as any).connect({
            server: toInspect.host,
            port: toInspect.port,
            database: toInspect.database,
            user: toInspect.user,
            password: toInspect.password,
            options: {
              encrypt: Boolean(toInspect.encrypt ?? false),
              trustServerCertificate: Boolean(toInspect.trustServerCertificate ?? false)
            }
          });

          const result = await pool
            .request()
            .query(`
              select
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                ordinal_position
              from information_schema.columns
              where table_schema not in ('INFORMATION_SCHEMA', 'sys')
              order by table_schema, table_name, ordinal_position
            `);

          const rows = (result.recordset ?? []) as Array<{
            table_schema: string;
            table_name: string;
            column_name: string;
            data_type: string;
            is_nullable: "YES" | "NO";
            ordinal_position: number;
          }>;

          const tables = buildSchema(rows);
          await pool.close();

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
          try {
            await (sql as any).close();
          } catch {
            // ignore
          }
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/mssql/query",
      handler: async (req) => {
        let pool: any | null = null;
        try {
          const body = (await readJson(req)) as DatasourceQueryRequest | null;
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const q = String((body as any)?.query ?? "").trim();
          if (!q) {
            throw new Error("query is required");
          }

          const params = (body as any)?.params;
          const args = Array.isArray(params) ? params : [];

          const limitNum = Number((body as any)?.limit ?? 100);
          const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.min(limitNum, 1000) : 100;

          pool = await (sql as any).connect({
            server: cfg.host,
            port: cfg.port,
            database: cfg.database,
            user: cfg.user,
            password: cfg.password,
            options: {
              encrypt: Boolean(cfg.encrypt ?? false),
              trustServerCertificate: Boolean(cfg.trustServerCertificate ?? false)
            }
          });

          const request = pool.request();
          for (let i = 0; i < args.length; i++) {
            request.input(`p${i}`, args[i]);
          }

          const result = await request.query(q);
          await pool.close();
          pool = null;

          const rows = (result.recordset ?? []) as unknown[];
          const limited = rows.slice(0, limit);
          const columns = limited.length > 0 && limited[0] && typeof limited[0] === "object"
            ? Object.keys(limited[0] as Record<string, unknown>)
            : [];

          const resp: DatasourceQueryResponse = { ok: true, id, columns, rows: limited };
          return json(resp);
        } catch (e) {
          if (pool) {
            try {
              await pool.close();
            } catch {
              // ignore
            }
          }
          try {
            await (sql as any).close();
          } catch {
            // ignore
          }
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
): DbSchemaTable[] {
  const map = new Map<string, DbSchemaTable>();

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

function requireConfig(cfg: MsSqlConfig | null): MsSqlConfig {
  if (!cfg) {
    throw new Error("SQL Server datasource is not configured");
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

function parseConfig(input: unknown): MsSqlConfig {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;

  const host = String(obj.host ?? "").trim();
  const database = String(obj.database ?? "").trim();
  const user = String(obj.user ?? "").trim();
  const password = obj.password == null ? undefined : String(obj.password);
  const portNum = Number(obj.port ?? 1433);
  const encrypt = Boolean(obj.encrypt ?? false);
  const trustServerCertificate = Boolean(obj.trustServerCertificate ?? false);

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
    encrypt,
    trustServerCertificate
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

export default mssqlPlugin;

function msIdent(name: string): string {
  const s = String(name ?? "");
  return `[${s.replace(/\]/g, "]]" )}]`;
}
