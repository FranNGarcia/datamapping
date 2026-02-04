import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import type { Plugin } from "../types";
import { getCustomerAggregateFieldPaths } from "../domain/customerFields";
import {
  suggestCustomerMappingsDeepInspectWithGemini,
  suggestCustomerMappingsWithGemini
} from "../infrastructure/gemini";
import type { DatasourceQueryRequest, DatasourceQueryResponse } from "../domain/query";

import parquet from "parquetjs-lite";

type S3Config = {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  bucket: string;
  prefix?: string;
};

type MappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

const configs = new Map<string, S3Config>();

const s3Plugin: Plugin = {
  name: "s3",
  manifest: {
    name: "s3",
    kind: "datasource",
    title: "AWS S3",
    description: "Browse S3 and map Parquet schemas to Customer"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/s3/config",
      handler: (req) => {
        const id = requireIdFromUrl(req.url);
        const currentConfig = configs.get(id) ?? null;

        return json({
          id,
          config: currentConfig
            ? {
                ...currentConfig,
                secretAccessKey: currentConfig.secretAccessKey ? "********" : "",
                sessionToken: currentConfig.sessionToken ? "********" : ""
              }
            : null
        });
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3/deep-inspect",
      handler: async (req) => {
        try {
          const body = (await readJson(req)) as any;
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const tables = (body?.tables ?? null) as any[] | null;
          if (!Array.isArray(tables) || tables.length === 0) {
            throw new Error("Deep inspect requires schema tables. Run schema inspect first.");
          }

          const candidates = (body?.candidates ?? []) as MappingCandidate[];

          const byTable = new Map<string, MappingCandidate[]>();
          for (const c of candidates) {
            const t = String((c as any)?.table ?? "").trim();
            if (!t) continue;
            if (!byTable.has(t)) byTable.set(t, []);
            byTable.get(t)!.push(c);
          }

          const client = createClient(cfg);
          const samples: Array<{ table: string; rows: unknown[] }> = [];

          for (const [tableKey] of byTable.entries()) {
            // tableKey for S3 is like: s3.s3://bucket/key
            const parts = tableKey.split(".");
            const name = parts.slice(1).join(".");
            const key = name.startsWith("s3://") ? name.slice("s3://".length) : name;
            const firstSlash = key.indexOf("/");
            const bucket = firstSlash >= 0 ? key.slice(0, firstSlash) : cfg.bucket;
            const objKey = firstSlash >= 0 ? key.slice(firstSlash + 1) : key;
            if (!objKey) {
              samples.push({ table: tableKey, rows: [] });
              continue;
            }

            let rows: unknown[] = [];
            try {
              const obj = await client.send(new GetObjectCommand({ Bucket: bucket, Key: objKey }));
              const buf = await readBodyToBuffer(obj.Body);

              const reader = await (parquet as any).ParquetReader.openBuffer(buf);
              const cursor = reader.getCursor();
              for (let i = 0; i < 5; i++) {
                const r = await cursor.next();
                if (!r) break;
                rows.push(r);
              }
              await reader.close();
            } catch {
              rows = [];
            }

            samples.push({ table: tableKey, rows });
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
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/s3/config",
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
      path: "/api/datasources/s3/test",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toTest = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const client = createClient(toTest);
          const prefix = normalizePrefix(toTest.prefix ?? "");

          const res = await client.send(
            new ListObjectsV2Command({
              Bucket: toTest.bucket,
              Prefix: prefix,
              MaxKeys: 5
            })
          );

          return json({
            ok: true,
            id,
            bucket: toTest.bucket,
            prefix,
            keys: (res.Contents ?? [])
              .map((o: { Key?: string }) => o.Key)
              .filter((k: string | undefined): k is string => Boolean(k))
              .map(String)
          });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3/list-files",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const client = createClient(cfg);
          const prefix = normalizePrefix(cfg.prefix ?? "");

          const res = await client.send(
            new ListObjectsV2Command({
              Bucket: cfg.bucket,
              Prefix: prefix,
              Delimiter: "/",
              MaxKeys: 500
            })
          );

          const dirs = (res.CommonPrefixes ?? [])
            .map((p: { Prefix?: string }) => p.Prefix)
            .filter((k: string | undefined): k is string => Boolean(k))
            .map(String);

          const files = (res.Contents ?? [])
            .map((o: { Key?: string }) => o.Key)
            .filter((k: string | undefined): k is string => Boolean(k))
            .map(String)
            .filter((k: string) => k.toLowerCase().endsWith(".parquet"));

          return json({ ok: true, id, bucket: cfg.bucket, prefix, dirs, files });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3/inspect-schema",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const files = (body && typeof body === "object" ? (body as any).files : null) as unknown;
          const selectedFiles = Array.isArray(files)
            ? (files as unknown[]).map((f) => String(f)).filter(Boolean)
            : [];
          if (selectedFiles.length === 0) {
            throw new Error("files is required (select at least one .parquet file)");
          }

          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);
          const client = createClient(cfg);

          const tables = [] as Array<{
            schema: string;
            name: string;
            columns: Array<{ name: string; dataType: string; isNullable: boolean; ordinalPosition: number }>;
          }>;

          for (const key of selectedFiles.slice(0, 10)) {
            if (!key.toLowerCase().endsWith(".parquet")) continue;

            const obj = await client.send(new GetObjectCommand({ Bucket: cfg.bucket, Key: key }));
            const buf = await readBodyToBuffer(obj.Body);

            const reader = await (parquet as any).ParquetReader.openBuffer(buf);
            const fields = (reader.schema?.fields ?? {}) as Record<
              string,
              { primitiveType?: string; originalType?: string; repetitionType?: string }
            >;

            const cols = Object.keys(fields).map((name, idx) => {
              const f = fields[name] ?? {};
              const dt = String(f.originalType ?? f.primitiveType ?? "unknown");
              const rep = String(f.repetitionType ?? "OPTIONAL").toUpperCase();
              return {
                name,
                dataType: dt,
                isNullable: rep !== "REQUIRED",
                ordinalPosition: idx + 1
              };
            });

            await reader.close();

            tables.push({
              schema: "s3",
              name: `s3://${cfg.bucket}/${key}`,
              columns: cols
            });
          }

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
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3/query",
      handler: async (req) => {
        try {
          const body = (await readJson(req)) as DatasourceQueryRequest | null;
          const { id } = parseBodyWithId(body);

          const resp: DatasourceQueryResponse = {
            ok: false,
            id,
            error: "S3 query is not implemented yet"
          };
          return json(resp, 501);
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });
  }
};

function normalizePrefix(prefix: string): string {
  const p = String(prefix ?? "").trim();
  if (!p) return "";
  return p.endsWith("/") ? p : `${p}/`;
}

async function readBodyToBuffer(body: any): Promise<Buffer> {
  if (!body) return Buffer.from([]);

  // AWS SDK v3 Body can be a ReadableStream in browsers/bun.
  if (typeof body === "string") return Buffer.from(body);
  if (body instanceof Uint8Array) return Buffer.from(body);
  if (body instanceof ArrayBuffer) return Buffer.from(new Uint8Array(body));

  // Works for ReadableStream and many stream-like bodies.
  const ab = await new Response(body).arrayBuffer();
  return Buffer.from(new Uint8Array(ab));
}

function createClient(cfg: S3Config): S3Client {
  return new S3Client({
    region: cfg.region,
    credentials: {
      accessKeyId: cfg.accessKeyId,
      secretAccessKey: cfg.secretAccessKey,
      sessionToken: cfg.sessionToken
    }
  });
}

function requireConfig(cfg: S3Config | null): S3Config {
  if (!cfg) {
    throw new Error("S3 datasource is not configured");
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

function parseConfig(input: unknown): S3Config {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;
  const region = String(obj.region ?? "").trim();
  const accessKeyId = String(obj.accessKeyId ?? "").trim();
  const secretAccessKey = String(obj.secretAccessKey ?? "").trim();
  const sessionToken = obj.sessionToken == null ? undefined : String(obj.sessionToken).trim();
  const bucket = String(obj.bucket ?? "").trim();
  const prefix = obj.prefix == null ? undefined : String(obj.prefix);

  if (!region) throw new Error("region is required");
  if (!bucket) throw new Error("bucket is required");
  if (!accessKeyId) throw new Error("accessKeyId is required");
  if (!secretAccessKey) throw new Error("secretAccessKey is required");

  return {
    region,
    accessKeyId,
    secretAccessKey,
    sessionToken,
    bucket,
    prefix
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

export default s3Plugin;
