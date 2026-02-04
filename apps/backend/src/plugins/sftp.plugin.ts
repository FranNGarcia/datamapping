import SftpClient from "ssh2-sftp-client";
import type { Plugin } from "../types";
import {
  suggestCustomerMappingsDeepInspectWithGemini,
  suggestCustomerMappingsWithGemini
} from "../infrastructure/gemini";
import { getCustomerAggregateFieldPaths } from "../domain/customerFields";
import type { DatasourceQueryRequest, DatasourceQueryResponse } from "../domain/query";

type SftpConfig = {
  host: string;
  port: number;
  username: string;
  password?: string;
  privateKey?: string;
  passphrase?: string;
  basePath?: string;
};

type MappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

const configs = new Map<string, SftpConfig>();

const sftpPlugin: Plugin = {
  name: "sftp",
  manifest: {
    name: "sftp",
    kind: "datasource",
    title: "SFTP",
    description: "Connect to an SFTP server"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/sftp/config",
      handler: (req) => {
        const id = requireIdFromUrl(req.url);
        const currentConfig = configs.get(id) ?? null;

        return json({
          id,
          config: currentConfig
            ? {
                ...currentConfig,
                password: currentConfig.password ? "********" : undefined,
                privateKey: currentConfig.privateKey ? "********" : undefined,
                passphrase: currentConfig.passphrase ? "********" : undefined
              }
            : null
        });
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/sftp/deep-inspect",
      handler: async (req) => {
        let client: SftpClient | null = null;
        try {
          const body = (await readJson(req)) as any;
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const tables = (body?.tables ?? null) as Array<{
            schema: string;
            name: string;
            columns: Array<{ name: string; dataType: string; isNullable: boolean; ordinalPosition: number }>;
          }> | null;
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

          client = new SftpClient();
          await client.connect({
            host: cfg.host,
            port: cfg.port,
            username: cfg.username,
            password: cfg.password,
            privateKey: cfg.privateKey,
            passphrase: cfg.passphrase
          });

          const samples: Array<{ table: string; rows: unknown[] }> = [];
          for (const [tableKey] of byTable.entries()) {
            // tableKey is like: sftp./path/to/file.csv
            const filePath = tableKey.startsWith("sftp.") ? tableKey.slice("sftp.".length) : tableKey;
            let rows: unknown[] = [];
            try {
              const buf = (await client.get(filePath)) as any;
              const text = decodeSftpFileContent(buf);
              const df = buildCsvDataFrame(text);
              rows = (df.rows ?? []).slice(0, 5);
            } catch {
              rows = [];
            }
            samples.push({ table: tableKey, rows });
          }

          await client.end();
          client = null;

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
              await client.end();
            } catch {
              // ignore
            }
          }
        }
      }
    });

    ctx.registerRoute({
      method: "PUT",
      path: "/api/datasources/sftp/config",
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
      path: "/api/datasources/sftp/test",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const toTest = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const client = new SftpClient();
          await client.connect({
            host: toTest.host,
            port: toTest.port,
            username: toTest.username,
            password: toTest.password,
            privateKey: toTest.privateKey,
            passphrase: toTest.passphrase
          });

          const path = (toTest.basePath ?? "/").trim() || "/";
          const list = await client.list(path);
          await client.end();

          return json({ ok: true, id, path, entries: list.slice(0, 50) });
        } catch (e) {
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
      path: "/api/datasources/sftp/list-files",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const client = new SftpClient();
          await client.connect({
            host: cfg.host,
            port: cfg.port,
            username: cfg.username,
            password: cfg.password,
            privateKey: cfg.privateKey,
            passphrase: cfg.passphrase
          });

          const basePath = normalizeRemotePath(cfg.basePath ?? "/");
          const list = await client.list(basePath);
          await client.end();

          const dirs = list
            .filter((e) => e && typeof e.name === "string")
            .filter((e) => String((e as any).type ?? "") === "d")
            .map((e) => String(e.name))
            .filter((n) => n !== "." && n !== "..")
            .map((n) => joinRemotePath(basePath, n));

          const files = list
            .filter((e) => e && typeof e.name === "string")
            .filter((e) => String((e as any).type ?? "") !== "d")
            .map((e) => String(e.name))
            .filter((n) => n.toLowerCase().endsWith(".csv"))
            .map((n) => joinRemotePath(basePath, n));

          return json({ ok: true, id, basePath, dirs, files });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/sftp/inspect-schema",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const files = (body && typeof body === "object" ? (body as any).files : null) as unknown;
          const selectedFiles = Array.isArray(files)
            ? (files as unknown[]).map((f) => String(f)).filter(Boolean)
            : [];
          if (selectedFiles.length === 0) {
            throw new Error("files is required (select at least one .csv file)");
          }

          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const client = new SftpClient();
          await client.connect({
            host: cfg.host,
            port: cfg.port,
            username: cfg.username,
            password: cfg.password,
            privateKey: cfg.privateKey,
            passphrase: cfg.passphrase
          });

          const tables = [] as Array<{
            schema: string;
            name: string;
            columns: Array<{ name: string; dataType: string; isNullable: boolean; ordinalPosition: number }>;
          }>;

          for (const filePath of selectedFiles.slice(0, 10)) {
            if (!filePath.toLowerCase().endsWith(".csv")) {
              continue;
            }

            const buf = (await client.get(filePath)) as any;
            const text = decodeSftpFileContent(buf);
            const parsed = parseCsvSchema(text);
            if (!parsed) {
              continue;
            }

            tables.push({
              schema: "sftp",
              name: filePath,
              columns: parsed.columns.map((c, i) => ({
                name: c.name,
                dataType: c.dataType,
                isNullable: c.isNullable,
                ordinalPosition: i + 1
              }))
            });
          }

          await client.end();

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
      path: "/api/datasources/sftp/query",
      handler: async (req) => {
        let client: SftpClient | null = null;
        try {
          const body = (await readJson(req)) as DatasourceQueryRequest | null;
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const params = (body && typeof body === "object" ? (body as any).params : null) as any;
          const filePath = String(params?.filePath ?? params?.file ?? "").trim();
          if (!filePath) {
            throw new Error("params.filePath is required");
          }

          const select = Array.isArray(params?.select)
            ? (params.select as unknown[]).map((x) => String(x)).filter(Boolean)
            : null;
          const filterExpr = params?.filter == null ? "" : String(params.filter);

          const limitNum = Number((body as any)?.limit ?? params?.limit ?? 100);
          const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.min(limitNum, 2000) : 100;

          client = new SftpClient();
          await client.connect({
            host: cfg.host,
            port: cfg.port,
            username: cfg.username,
            password: cfg.password,
            privateKey: cfg.privateKey,
            passphrase: cfg.passphrase
          });

          const buf = (await client.get(filePath)) as any;
          const text = decodeSftpFileContent(buf);

          const df = buildCsvDataFrame(text);
          const filtered = filterExpr ? dfFilter(df, filterExpr) : df;
          const projected = select && select.length > 0 ? dfSelect(filtered, select) : filtered;
          const limitedRows = projected.rows.slice(0, limit);

          const resp: DatasourceQueryResponse = {
            ok: true,
            id,
            columns: projected.columns,
            rows: limitedRows
          };
          return json(resp);
        } catch (e) {
          return json(
            {
              ok: false,
              error: e instanceof Error ? e.message : String(e)
            },
            400
          );
        } finally {
          if (client) {
            try {
              await client.end();
            } catch {
              // ignore
            }
          }
        }
      }
    });
  }
};

type CsvDataFrame = {
  columns: string[];
  rows: Array<Record<string, unknown>>;
};

function buildCsvDataFrame(text: string): CsvDataFrame {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (lines.length === 0) {
    return { columns: [], rows: [] };
  }

  const header = parseCsvLine(lines[0]);
  const columns = header.map((h) => String(h ?? "").trim()).filter(Boolean);
  if (columns.length === 0) {
    return { columns: [], rows: [] };
  }

  const data = lines.slice(1);
  const rawRows = data.map(parseCsvLine);

  const typeByCol = inferCsvTypes(columns, rawRows);

  const rows: Array<Record<string, unknown>> = rawRows.map((r) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i];
      const raw = i < r.length ? String(r[i] ?? "") : "";
      obj[col] = parseCsvValue(raw, typeByCol.get(col) ?? "text");
    }
    return obj;
  });

  return { columns, rows };
}

function inferCsvTypes(columns: string[], rawRows: string[][]): Map<string, string> {
  const out = new Map<string, string>();
  const sampleRows = rawRows.slice(0, 200);
  for (let i = 0; i < columns.length; i++) {
    const values = sampleRows.map((r) => (i < r.length ? String(r[i] ?? "").trim() : ""));
    const nonEmpty = values.filter((v) => v !== "");
    out.set(columns[i], inferCsvType(nonEmpty));
  }
  return out;
}

function parseCsvValue(raw: string, dataType: string): unknown {
  const v = String(raw ?? "").trim();
  if (v === "") return null;
  const dt = String(dataType ?? "text").toLowerCase();

  if (dt === "boolean") {
    const x = v.toLowerCase();
    if (x === "true" || x === "1") return true;
    if (x === "false" || x === "0") return false;
    return null;
  }

  if (dt === "integer") {
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : null;
  }

  if (dt === "float") {
    const n = Number.parseFloat(v);
    return Number.isFinite(n) ? n : null;
  }

  if (dt === "date") {
    const ms = Date.parse(v);
    return Number.isFinite(ms) ? new Date(ms).toISOString() : v;
  }

  return v;
}

function dfSelect(df: CsvDataFrame, cols: string[]): CsvDataFrame {
  const wanted = cols.map((c) => String(c ?? "").trim()).filter(Boolean);
  const set = new Set(wanted);
  const columns = df.columns.filter((c) => set.has(c));
  const rows = df.rows.map((r) => {
    const o: Record<string, unknown> = {};
    for (const c of columns) {
      o[c] = r[c];
    }
    return o;
  });
  return { columns, rows };
}

function dfFilter(df: CsvDataFrame, expr: string): CsvDataFrame {
  const ast = parseFilterExpr(expr);
  const rows = df.rows.filter((r) => Boolean(evalFilterAst(ast, r)));
  return { columns: df.columns, rows };
}

type FilterToken =
  | { t: "ident"; v: string }
  | { t: "number"; v: number }
  | { t: "string"; v: string }
  | { t: "op"; v: string }
  | { t: "paren"; v: "(" | ")" }
  | { t: "eof" };

function tokenizeFilterExpr(input: string): FilterToken[] {
  const s = String(input ?? "");
  const out: FilterToken[] = [];
  let i = 0;
  const isWs = (c: string) => c === " " || c === "\t" || c === "\n" || c === "\r";
  const isIdStart = (c: string) => /[A-Za-z_]/.test(c);
  const isId = (c: string) => /[A-Za-z0-9_\.]/.test(c);

  while (i < s.length) {
    const ch = s[i];
    if (isWs(ch)) {
      i++;
      continue;
    }

    if (ch === "(" || ch === ")") {
      out.push({ t: "paren", v: ch });
      i++;
      continue;
    }

    const two = s.slice(i, i + 2);
    if (two === "==" || two === "!=" || two === ">=" || two === "<=" || two === "&&" || two === "||") {
      out.push({ t: "op", v: two });
      i += 2;
      continue;
    }

    if (ch === ">" || ch === "<") {
      out.push({ t: "op", v: ch });
      i++;
      continue;
    }

    if (ch === "'" || ch === '"') {
      const quote = ch;
      i++;
      let cur = "";
      while (i < s.length) {
        const c = s[i];
        if (c === "\\" && i + 1 < s.length) {
          cur += s[i + 1];
          i += 2;
          continue;
        }
        if (c === quote) {
          i++;
          break;
        }
        cur += c;
        i++;
      }
      out.push({ t: "string", v: cur });
      continue;
    }

    if (/[0-9]/.test(ch) || (ch === "-" && /[0-9]/.test(s[i + 1] ?? ""))) {
      let j = i + 1;
      while (j < s.length && /[0-9\.]/.test(s[j])) j++;
      const raw = s.slice(i, j);
      const n = Number(raw);
      if (!Number.isFinite(n)) {
        throw new Error(`Invalid number: ${raw}`);
      }
      out.push({ t: "number", v: n });
      i = j;
      continue;
    }

    if (isIdStart(ch)) {
      let j = i + 1;
      while (j < s.length && isId(s[j])) j++;
      const id = s.slice(i, j);
      const low = id.toLowerCase();
      if (low === "and" || low === "or" || low === "not") {
        out.push({ t: "op", v: low });
      } else {
        out.push({ t: "ident", v: id });
      }
      i = j;
      continue;
    }

    throw new Error(`Unexpected character in filter: ${ch}`);
  }

  out.push({ t: "eof" });
  return out;
}

type FilterAst =
  | { k: "lit"; v: unknown }
  | { k: "col"; name: string }
  | { k: "not"; x: FilterAst }
  | { k: "and"; a: FilterAst; b: FilterAst }
  | { k: "or"; a: FilterAst; b: FilterAst }
  | { k: "cmp"; op: "==" | "!=" | ">" | ">=" | "<" | "<="; a: FilterAst; b: FilterAst };

function parseFilterExpr(input: string): FilterAst {
  const tokens = tokenizeFilterExpr(input);
  let idx = 0;
  const peek = () => tokens[idx];
  const take = () => tokens[idx++];
  const expectOp = (v: string) => {
    const t = take();
    if (t.t !== "op" || t.v !== v) throw new Error(`Expected '${v}'`);
  };

  const parsePrimary = (): FilterAst => {
    const t = peek();
    if (t.t === "paren" && t.v === "(") {
      take();
      const x = parseOr();
      const r = take();
      if (r.t !== "paren" || r.v !== ")") throw new Error("Expected ')'");
      return x;
    }
    if (t.t === "ident") {
      take();
      return { k: "col", name: t.v };
    }
    if (t.t === "number") {
      take();
      return { k: "lit", v: t.v };
    }
    if (t.t === "string") {
      take();
      return { k: "lit", v: t.v };
    }
    throw new Error("Expected identifier, string, number, or '(' in filter");
  };

  const parseNot = (): FilterAst => {
    const t = peek();
    if (t.t === "op" && (t.v === "not")) {
      take();
      return { k: "not", x: parseNot() };
    }
    return parseCmp();
  };

  const parseCmp = (): FilterAst => {
    let a = parsePrimary();
    const t = peek();
    if (t.t === "op" && (t.v === "==" || t.v === "!=" || t.v === ">" || t.v === ">=" || t.v === "<" || t.v === "<=")) {
      take();
      const b = parsePrimary();
      return { k: "cmp", op: t.v as any, a, b };
    }
    return a;
  };

  const parseAnd = (): FilterAst => {
    let x = parseNot();
    while (true) {
      const t = peek();
      if (t.t === "op" && (t.v === "and" || t.v === "&&")) {
        take();
        x = { k: "and", a: x, b: parseNot() };
        continue;
      }
      return x;
    }
  };

  const parseOr = (): FilterAst => {
    let x = parseAnd();
    while (true) {
      const t = peek();
      if (t.t === "op" && (t.v === "or" || t.v === "||")) {
        take();
        x = { k: "or", a: x, b: parseAnd() };
        continue;
      }
      return x;
    }
  };

  const expr = parseOr();
  const end = peek();
  if (end.t !== "eof") {
    throw new Error("Unexpected token at end of filter");
  }
  return expr;
}

function evalFilterAst(ast: FilterAst, row: Record<string, unknown>): unknown {
  switch (ast.k) {
    case "lit":
      return ast.v;
    case "col":
      return row[ast.name];
    case "not":
      return !Boolean(evalFilterAst(ast.x, row));
    case "and":
      return Boolean(evalFilterAst(ast.a, row)) && Boolean(evalFilterAst(ast.b, row));
    case "or":
      return Boolean(evalFilterAst(ast.a, row)) || Boolean(evalFilterAst(ast.b, row));
    case "cmp": {
      const av = evalFilterAst(ast.a, row);
      const bv = evalFilterAst(ast.b, row);
      const aNum = typeof av === "number" ? av : Number(av);
      const bNum = typeof bv === "number" ? bv : Number(bv);
      const bothNum = Number.isFinite(aNum) && Number.isFinite(bNum);
      const a = bothNum ? aNum : String(av ?? "");
      const b = bothNum ? bNum : String(bv ?? "");

      if (ast.op === "==") return a === b;
      if (ast.op === "!=") return a !== b;
      if (ast.op === ">") return (a as any) > (b as any);
      if (ast.op === ">=") return (a as any) >= (b as any);
      if (ast.op === "<") return (a as any) < (b as any);
      if (ast.op === "<=") return (a as any) <= (b as any);
      return false;
    }
  }
}

function normalizeRemotePath(path: string): string {
  const p = String(path ?? "").trim();
  if (!p) return "/";
  // Important: many SFTP servers use chroot. In that case, absolute paths like
  // "/home/foo/..." may not exist; the effective root ("/") is already the user's home.
  // So we allow both absolute and relative basePath values.
  return p;
}

function joinRemotePath(base: string, name: string): string {
  const b = base === "." ? "." : base.endsWith("/") ? base.slice(0, -1) : base;
  const n = name.startsWith("/") ? name.slice(1) : name;
  return b === "." ? `./${n}` : `${b}/${n}`;
}

function parseCsvSchema(text: string):
  | {
      columns: Array<{ name: string; dataType: string; isNullable: boolean }>;
    }
  | null {
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (lines.length === 0) return null;

  const header = parseCsvLine(lines[0]);
  const colNames = header.map((h) => String(h ?? "").trim()).filter(Boolean);
  if (colNames.length === 0) return null;

  const sampleRows = lines.slice(1, 51).map(parseCsvLine);

  const columns = colNames.map((name, idx) => {
    const values = sampleRows.map((r) => (idx < r.length ? r[idx] : ""));
    const nonEmpty = values.map((v) => String(v ?? "").trim()).filter((v) => v !== "");
    const isNullable = nonEmpty.length !== values.length;
    const dataType = inferCsvType(nonEmpty);
    return { name, dataType, isNullable };
  });

  return { columns };
}

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function inferCsvType(values: string[]): string {
  if (values.length === 0) return "text";

  const isBool = values.every((v) => {
    const x = v.toLowerCase();
    return x === "true" || x === "false" || x === "0" || x === "1";
  });
  if (isBool) return "boolean";

  const isInt = values.every((v) => /^-?\d+$/.test(v));
  if (isInt) return "integer";

  const isFloat = values.every((v) => /^-?\d+(\.\d+)?$/.test(v));
  if (isFloat) return "float";

  const isDate = values.every((v) => !Number.isNaN(Date.parse(v)));
  if (isDate) return "date";

  return "text";
}

function decodeSftpFileContent(buf: unknown): string {
  if (typeof buf === "string") return buf;
  if (buf && typeof buf === "object") {
    // ssh2-sftp-client typically returns a Buffer, but in Bun we avoid Buffer type checks.
    if (buf instanceof Uint8Array) {
      return new TextDecoder().decode(buf);
    }
    if ("toString" in (buf as any) && typeof (buf as any).toString === "function") {
      try {
        return String((buf as any).toString("utf8"));
      } catch {
        return String((buf as any).toString());
      }
    }
  }
  return String(buf ?? "");
}

function requireConfig(cfg: SftpConfig | null): SftpConfig {
  if (!cfg) {
    throw new Error("SFTP datasource is not configured");
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

function parseConfig(input: unknown): SftpConfig {
  if (!input || typeof input !== "object") {
    throw new Error("Invalid config");
  }

  const obj = input as Record<string, unknown>;

  const host = String(obj.host ?? "").trim();
  const portNum = Number(obj.port ?? 22);
  const username = String(obj.username ?? "").trim();

  const password = obj.password == null ? undefined : String(obj.password);
  const privateKey = obj.privateKey == null ? undefined : String(obj.privateKey);
  const passphrase = obj.passphrase == null ? undefined : String(obj.passphrase);
  const basePath = obj.basePath == null ? undefined : String(obj.basePath);

  if (!host) throw new Error("host is required");
  if (!username) throw new Error("username is required");
  if (!Number.isFinite(portNum) || portNum <= 0) throw new Error("port is invalid");

  if (!password && !privateKey) {
    throw new Error("Either password or privateKey is required");
  }

  return {
    host,
    port: portNum,
    username,
    password,
    privateKey,
    passphrase,
    basePath
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

export default sftpPlugin;
