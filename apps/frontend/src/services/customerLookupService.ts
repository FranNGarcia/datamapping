import type { CustomerProfile } from "./customerProfileService";

export type MappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

export type DatasourcePluginName = "postgres" | "mssql" | "mongodb" | "sftp" | "s3" | "rest-api";

export type DatasourceInstance = {
  id: string;
  pluginName: DatasourcePluginName;
};

export type DatasourceContext = {
  instances: DatasourceInstance[];

  pgConfigsById: Record<string, unknown>;
  mssqlConfigsById: Record<string, unknown>;
  mongoConfigsById: Record<string, unknown>;
  sftpConfigsById: Record<string, unknown>;
  s3ConfigsById: Record<string, unknown>;
  restApiConfigsById: Record<string, unknown>;

  pgCandidatesById: Record<string, MappingCandidate[]>;
  mssqlCandidatesById: Record<string, MappingCandidate[]>;
  mongoCandidatesById: Record<string, MappingCandidate[]>;
  sftpCandidatesById: Record<string, MappingCandidate[]>;
  s3CandidatesById: Record<string, MappingCandidate[]>;
  restApiCandidatesById: Record<string, MappingCandidate[]>;

  sftpSelectedFileById: Record<string, string | null>;
};

type QueryResponse = {
  ok: boolean;
  id: string;
  columns?: string[];
  rows?: unknown[];
  error?: string;
};

type ScoredValue = { value: unknown; confidence: number; source: { plugin: DatasourcePluginName; id: string } };

type Aggregated = {
  fields: Record<string, ScoredValue>;
};

type DetailItem = { label: string; value: string; statusColor?: "green" | "yellow" | "red" | "gray" };
type DetailSection = { title: string; items: DetailItem[] };

function parseDateLoose(input: unknown): Date | null {
  if (input == null) return null;
  if (input instanceof Date && Number.isFinite(input.getTime())) return input;

  if (typeof input === "number" && Number.isFinite(input)) {
    const d = new Date(input);
    return Number.isFinite(d.getTime()) ? d : null;
  }

  const s = String(input).trim();
  if (!s) return null;

  const direct = new Date(s);
  if (Number.isFinite(direct.getTime())) return direct;

  // Support YYYYMMDD
  const m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m) {
    const y = Number(m[1]);
    const mo = Number(m[2]);
    const d = Number(m[3]);
    if (Number.isFinite(y) && Number.isFinite(mo) && Number.isFinite(d)) {
      const dt = new Date(Date.UTC(y, mo - 1, d));
      return Number.isFinite(dt.getTime()) ? dt : null;
    }
  }

  return null;
}

function extractJoinKeys(row: Record<string, unknown> | null): Record<string, string> {
  if (!row) return {};

  const out: Record<string, string> = {};
  const entries = Object.entries(row);

  for (const [k, v] of entries) {
    const key = String(k ?? "");
    const val = v == null ? "" : String(v).trim();
    if (!val) continue;

    const lk = key.toLowerCase();
    if (lk === "person_id" || lk === "personid") out.person_id = val;
    if (lk === "customer_id" || lk === "customerid") out.customer_id = val;
    if (lk === "user_id" || lk === "userid") out.user_id = val;
    if (lk === "id") out.id = val;
    if (lk === "identification_number" || lk === "identity_number" || lk === "document_number") {
      out.identity_number = val;
    }
  }

  return out;
}

async function queryOneRowByHeuristics(args: {
  plugin: "postgres" | "mssql";
  instanceId: string;
  config: unknown;
  table: string;
  identityNumber: string;
  keys: Record<string, string>;
}): Promise<Record<string, unknown> | null> {
  const { plugin, instanceId, config, table, identityNumber, keys } = args;
  const t = normalizeSqlIdent(table);

  // Try joining with keys from base row first.
  const joinAttempts: Array<{ col: string; value: string }> = [];
  if (keys.person_id) joinAttempts.push({ col: "person_id", value: keys.person_id });
  if (keys.customer_id) joinAttempts.push({ col: "customer_id", value: keys.customer_id });
  if (keys.user_id) joinAttempts.push({ col: "user_id", value: keys.user_id });
  if (keys.id) joinAttempts.push({ col: "id", value: keys.id });
  if (keys.identity_number) joinAttempts.push({ col: "identification_number", value: keys.identity_number });

  // Fallbacks using provided identityNumber.
  joinAttempts.push({ col: "identification_number", value: identityNumber });
  joinAttempts.push({ col: "identity_number", value: identityNumber });
  joinAttempts.push({ col: "document_number", value: identityNumber });

  for (const attempt of joinAttempts) {
    const col = normalizeSqlIdent(attempt.col);
    const value = attempt.value;
    try {
      const q =
        plugin === "postgres"
          ? `select * from ${t} where ${col} = $1 limit 1`
          : `select top (1) * from ${t} where ${col} = @p0`;

      const resp = await callDatasourceQuery(plugin, {
        id: instanceId,
        config,
        query: q,
        params: [value],
        limit: 1
      });

      const row = Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
      if (row && typeof row === "object") {
        return row as Record<string, unknown>;
      }
    } catch {
      // Ignore: the table might not have this column.
    }
  }

  return null;
}

function yearsFromNow(from: Date): number {
  const ms = Date.now() - from.getTime();
  if (!Number.isFinite(ms) || ms < 0) return 0;
  const MS_PER_YEAR = 1000 * 60 * 60 * 24 * 365.25;
  return Math.floor(ms / MS_PER_YEAR);
}

function getCandidatesFor(ctx: DatasourceContext, plugin: DatasourcePluginName, id: string): MappingCandidate[] {
  if (plugin === "postgres") return ctx.pgCandidatesById[id] ?? [];
  if (plugin === "mssql") return ctx.mssqlCandidatesById[id] ?? [];
  if (plugin === "mongodb") return ctx.mongoCandidatesById[id] ?? [];
  if (plugin === "sftp") return ctx.sftpCandidatesById[id] ?? [];
  if (plugin === "s3") return ctx.s3CandidatesById[id] ?? [];
  if (plugin === "rest-api") return ctx.restApiCandidatesById[id] ?? [];
  return [];
}

function getConfigFor(ctx: DatasourceContext, plugin: DatasourcePluginName, id: string): unknown {
  if (plugin === "postgres") return ctx.pgConfigsById[id] ?? null;
  if (plugin === "mssql") return ctx.mssqlConfigsById[id] ?? null;
  if (plugin === "mongodb") return ctx.mongoConfigsById[id] ?? null;
  if (plugin === "sftp") return ctx.sftpConfigsById[id] ?? null;
  if (plugin === "s3") return ctx.s3ConfigsById[id] ?? null;
  if (plugin === "rest-api") return ctx.restApiConfigsById[id] ?? null;
  return null;
}

function pickBestCandidate(cands: MappingCandidate[], customerPath: string): MappingCandidate | null {
  const matches = cands.filter((c) => c.customerPath === customerPath);
  if (matches.length === 0) return null;
  matches.sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0));
  return matches[0] ?? null;
}

function upsertField(out: Aggregated, path: string, value: unknown, score: number, source: ScoredValue["source"]) {
  if (value == null) return;
  const existing = out.fields[path];
  if (!existing || score > existing.confidence) {
    out.fields[path] = { value, confidence: score, source };
  }
}

function normalizeSqlIdent(id: string): string {
  return id;
}

async function callDatasourceQuery(
  plugin: DatasourcePluginName,
  body: unknown
): Promise<QueryResponse> {
  const res = await fetch(`/api/datasources/${plugin}/query`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });
  const data = (await res.json()) as QueryResponse;
  if (!res.ok || !data.ok) {
    throw new Error(data?.error ?? `Query failed (${plugin})`);
  }
  return data;
}

async function resolveDefaultIdentityNumber(ctx: DatasourceContext): Promise<string | null> {
  const instances = ctx.instances;
  for (const inst of instances) {
    const config = getConfigFor(ctx, inst.pluginName, inst.id);
    if (!config) {
      continue;
    }

    const candidates = getCandidatesFor(ctx, inst.pluginName, inst.id);
    const identityCandidate = candidates.find((c) => c.customerPath === "identityNumber");
    if (!identityCandidate) {
      continue;
    }

    if (inst.pluginName === "s3") {
      continue;
    }

    let resp: QueryResponse | null = null;
    if (inst.pluginName === "postgres") {
      const table = normalizeSqlIdent(identityCandidate.table);
      const col = normalizeSqlIdent(identityCandidate.column);
      resp = await callDatasourceQuery(inst.pluginName, {
        id: inst.id,
        config,
        query: `select ${col} from ${table} limit 1`,
        params: [],
        limit: 1
      });
    } else if (inst.pluginName === "mssql") {
      const table = normalizeSqlIdent(identityCandidate.table);
      const col = normalizeSqlIdent(identityCandidate.column);
      resp = await callDatasourceQuery(inst.pluginName, {
        id: inst.id,
        config,
        query: `select top (1) ${col} from ${table}`,
        params: [],
        limit: 1
      });
    } else if (inst.pluginName === "mongodb") {
      const field = identityCandidate.column;
      resp = await callDatasourceQuery(inst.pluginName, {
        id: inst.id,
        config,
        query: "find",
        params: { filter: {}, projection: { _id: 0, [field]: 1 } },
        limit: 1
      });
    } else if (inst.pluginName === "sftp") {
      const filePath = ctx.sftpSelectedFileById[inst.id];
      if (!filePath) {
        continue;
      }
      const col = identityCandidate.column;
      resp = await callDatasourceQuery(inst.pluginName, {
        id: inst.id,
        config,
        query: "csv",
        params: { filePath, select: [col] },
        limit: 1
      });
    }

    const row = resp && Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
    const v = row && typeof row === "object" ? (row as any)[identityCandidate.column] : null;
    const s = v == null ? "" : String(v).trim();
    if (s) {
      return s;
    }
  }

  return null;
}

async function queryOneRowByIdentity(
  plugin: DatasourcePluginName,
  instanceId: string,
  config: unknown,
  identityCandidate: MappingCandidate,
  identityNumber: string,
  ctx: DatasourceContext
): Promise<Record<string, unknown> | null> {
  if (plugin === "postgres") {
    const table = normalizeSqlIdent(identityCandidate.table);
    const col = normalizeSqlIdent(identityCandidate.column);
    const q = `select * from ${table} where ${col} = $1 limit 1`;
    const resp = await callDatasourceQuery(plugin, { id: instanceId, config, query: q, params: [identityNumber], limit: 1 });
    const row = Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
    return row && typeof row === "object" ? (row as Record<string, unknown>) : null;
  }

  if (plugin === "mssql") {
    const table = normalizeSqlIdent(identityCandidate.table);
    const col = normalizeSqlIdent(identityCandidate.column);
    const q = `select top (1) * from ${table} where ${col} = @p0`;
    const resp = await callDatasourceQuery(plugin, { id: instanceId, config, query: q, params: [identityNumber], limit: 1 });
    const row = Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
    return row && typeof row === "object" ? (row as Record<string, unknown>) : null;
  }

  if (plugin === "mongodb") {
    const field = identityCandidate.column;
    const resp = await callDatasourceQuery(plugin, {
      id: instanceId,
      config,
      query: "find",
      params: { filter: { [field]: identityNumber }, projection: { _id: 0 } },
      limit: 1
    });
    const row = Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
    return row && typeof row === "object" ? (row as Record<string, unknown>) : null;
  }

  if (plugin === "sftp") {
    const filePath = ctx.sftpSelectedFileById[instanceId];
    if (!filePath) {
      throw new Error("No CSV file selected for SFTP instance");
    }
    const col = identityCandidate.column;
    const filter = `${col} == "${String(identityNumber).replace(/"/g, "\\\"")}"`;
    const resp = await callDatasourceQuery(plugin, {
      id: instanceId,
      config,
      query: "csv",
      params: { filePath, filter },
      limit: 1
    });
    const row = Array.isArray(resp.rows) && resp.rows.length > 0 ? (resp.rows[0] as any) : null;
    return row && typeof row === "object" ? (row as Record<string, unknown>) : null;
  }

  return null;
}

function toCustomerProfile(agg: Aggregated, identityNumber: string): CustomerProfile & { avatarUrl: string } {
  const get = (path: string) => agg.fields[path]?.value;

  const usedPaths = new Set<string>();
  const take = (path: string) => {
    usedPaths.add(path);
    return get(path);
  };

  const toText = (v: unknown) => {
    if (v == null) return "";
    if (typeof v === "string") return v.trim();
    if (typeof v === "number" || typeof v === "boolean" || typeof v === "bigint") return String(v);
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  };

  const toMultilineText = (v: unknown) => {
    if (v == null) return "";
    if (Array.isArray(v)) {
      return v
        .map((x) => (x == null ? "" : String(x).trim()))
        .filter(Boolean)
        .join("\n");
    }
    const s = toText(v);
    if (!s) return "";

    // Postgres array text format: {a,b,"c d"}
    if (s.startsWith("{") && s.endsWith("}")) {
      const inner = s.slice(1, -1);
      const out: string[] = [];
      let cur = "";
      let inQuotes = false;
      let escape = false;
      for (let i = 0; i < inner.length; i += 1) {
        const ch = inner[i] ?? "";
        if (escape) {
          cur += ch;
          escape = false;
          continue;
        }
        if (ch === "\\") {
          escape = true;
          continue;
        }
        if (ch === '"') {
          inQuotes = !inQuotes;
          continue;
        }
        if (!inQuotes && ch === ",") {
          const part = cur.trim();
          if (part) out.push(part);
          cur = "";
          continue;
        }
        cur += ch;
      }
      const last = cur.trim();
      if (last) out.push(last);
      const cleaned = out
        .map((x) => x.replace(/^"|"$/g, "").trim())
        .filter(Boolean);
      if (cleaned.length > 0) {
        return cleaned.join("\n");
      }
    }

    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        const arr = JSON.parse(s);
        if (Array.isArray(arr)) {
          return arr
            .map((x) => (x == null ? "" : String(x).trim()))
            .filter(Boolean)
            .join("\n");
        }
      } catch {
        // ignore
      }
    }
    // Heuristic: split on commas/semicolons if it looks like a list
    if (s.includes(",") || s.includes(";")) {
      const parts = s
        .split(/[;,]/g)
        .map((p) => p.trim())
        .filter(Boolean);
      if (parts.length > 1) {
        return parts.join("\n");
      }
    }
    return s;
  };

  const pickFirstRaw = (paths: string[]) => {
    for (const p of paths) {
      const v = take(p);
      if (v == null) continue;
      const t = toText(v);
      if (t) return { path: p, value: v };
    }
    return null;
  };

  const pickFirst = (paths: string[]) => {
    for (const p of paths) {
      const v = take(p);
      const s = toText(v);
      if (s) return { path: p, value: s };
    }
    return null;
  };

  const pushItemIfValue = (items: DetailItem[], label: string, value: string, statusColor?: DetailItem["statusColor"]) => {
    const v = value.trim();
    if (!v) return;
    items.push({ label, value: v, statusColor });
  };

  const pushItem = (items: DetailItem[], label: string, value: string, statusColor?: DetailItem["statusColor"]) => {
    const v = value.trim();
    items.push({ label, value: v || "-", statusColor: v ? statusColor : undefined });
  };

  const inferStatusColor = (label: string, value: string): DetailItem["statusColor"] | undefined => {
    const v = value.trim().toLowerCase();
    const l = label.trim().toLowerCase();
    if (l.includes("estado") && (v === "activo" || v === "completed")) return "green";
    if (l.includes("estado") && (v === "inactivo" || v === "inactive" || v === "no")) return "red";
    if (l.includes("riesgo") && (v === "medio" || v === "medium")) return "yellow";
    if (l.includes("riesgo") && (v === "alto" || v === "high")) return "red";
    if (l.includes("riesgo") && (v === "bajo" || v === "low")) return "green";
    if (l === "pep" && (v === "si" || v === "sí" || v === "yes")) return "red";
    if (l === "pep" && (v === "no")) return "green";
    if (v === "en mora" || v === "moroso") return "red";
    return undefined;
  };

  const firstName = String(take("naturalData.firstName") ?? take("firstName") ?? "").trim();
  const lastName = String(take("naturalData.lastName") ?? take("lastName") ?? "").trim();
  const fullName = `${firstName} ${lastName}`.trim() || identityNumber;

  const customerTypeText =
    pickFirst([
      "customerType",
      "type",
      "legalData.customerType",
      "naturalData.personType",
      "personType"
    ])?.value ?? "";
  const customerTypeNorm = customerTypeText.trim().toLowerCase();
  const personType: "natural" | "legal" =
    customerTypeNorm.includes("jur") ||
    customerTypeNorm.includes("legal") ||
    customerTypeNorm.includes("empresa") ||
    customerTypeNorm.includes("sociedad")
      ? "legal"
      : "natural";

  const birthDate = parseDateLoose(
    take("naturalData.birthDate") ?? take("naturalData.dateOfBirth") ?? take("birthDate")
  );
  const constitutionDate = parseDateLoose(
    take("constitutionDate") ??
      take("legalData.constitutionDate") ??
      take("incorporationDate") ??
      take("legalData.incorporationDate")
  );

  const computedAge =
    personType === "legal"
      ? constitutionDate
        ? yearsFromNow(constitutionDate)
        : 0
      : birthDate
        ? yearsFromNow(birthDate)
        : 0;

  const rawProfilePicture = take("profilePicture");
  const profilePicture = rawProfilePicture == null ? "" : String(rawProfilePicture).trim();

  let avatarUrl = "";
  if (profilePicture) {
    if (profilePicture.startsWith("data:")) {
      avatarUrl = profilePicture;
    } else {
      const mime = profilePicture.startsWith("/9j/")
        ? "image/jpeg"
        : profilePicture.startsWith("iVBOR")
          ? "image/png"
          : "image/*";
      avatarUrl = `data:${mime};base64,${profilePicture}`;
    }
  }

  const email = String(take("naturalData.emailList") ?? take("email") ?? "");
  const phone = String(take("naturalData.mobilePhoneNumberList") ?? take("phone") ?? "");

  const country = String(take("addresses.country") ?? take("country") ?? "").trim();
  const city = String(take("addresses.city") ?? take("city") ?? "").trim();
  const location = [city, country].filter(Boolean).join(", ");

  const infoItems: DetailItem[] = [];
  pushItem(infoItems, "Tipo de documento", pickFirst(["identityType", "documentType", "document.type"])?.value ?? "");
  pushItem(infoItems, "Número de documento", identityNumber);
  pushItem(
    infoItems,
    "Nacionalidad",
    pickFirst([
      "nationalityCode",
      "naturalData.nationalityCode",
      "countryOfNationalityCode",
      "paisNacionalidadCode",
      "nationality",
      "naturalData.nationality",
      "countryOfNationality",
      "paisNacionalidad"
    ])?.value ?? ""
  );
  pushItem(
    infoItems,
    "Estado civil",
    pickFirst(["maritalStatus", "civilStatus", "naturalData.maritalStatus", "estadoCivil"])?.value ?? ""
  );
  pushItem(infoItems, "Lugar de Constitución", pickFirst(["constitutionPlace", "legalData.constitutionPlace", "placeOfIncorporation", "incorporationPlace"])?.value ?? "");
  pushItem(infoItems, "Rubro, actividad o industria", pickFirst(["industry", "activity", "legalData.industry", "segment.industry"])?.value ?? "");
  pushItem(infoItems, "Tipo de cliente", pickFirst(["customerType", "type", "legalData.customerType"])?.value ?? "");

  const segItems: DetailItem[] = [];
  const customerStatus = pickFirst(["status", "customerStatus", "kyc.status", "segment.status"]);
  pushItem(segItems, "Estado del cliente", customerStatus?.value ?? "", customerStatus ? inferStatusColor("Estado del cliente", customerStatus.value) : undefined);
  const customerBrand = pickFirst(["brand", "customerBrand", "segment.brand"]);
  pushItem(segItems, "Marca del cliente", customerBrand?.value ?? "");
  const bank = pickFirst(["banking", "bank", "segment.bank", "banking.bank"]);
  pushItem(segItems, "Banca", bank?.value ?? "");
  const segment = pickFirst(["segment", "segment.name", "segmento"]);
  pushItem(segItems, "Segmento", segment?.value ?? "");
  const risk = pickFirst(["riskLevel", "risk.level", "segment.risk", "nivelRiesgo"]);
  pushItem(segItems, "Nivel de riesgo", risk?.value ?? "", risk ? inferStatusColor("Nivel de riesgo", risk.value) : undefined);
  const delinquency = pickFirst(["delinquency", "mora", "arrears", "segment.delinquency"]);
  pushItem(segItems, "Mora", delinquency?.value ?? "", delinquency ? inferStatusColor("Mora", delinquency.value) : undefined);
  const billing = pickFirst(["billing", "billingAmount", "facturacion", "segment.billing"]);
  pushItem(segItems, "Facturación", billing?.value ?? "");
  const income = pickFirst(["income", "ingresos", "segment.income", "financial.income"]);
  pushItem(segItems, "Ingresos", income?.value ?? "");
  const profitability = pickFirst(["profitability", "rentabilidad", "segment.profitability"]);
  pushItem(segItems, "Rentabilidad", profitability?.value ?? "");
  const homeBanking = pickFirst(["homeBanking", "usesHomeBanking", "usoHomeBanking", "segment.homeBanking"]);
  pushItem(segItems, "Uso de home banking", homeBanking?.value ?? "", homeBanking ? inferStatusColor("Uso de home banking", homeBanking.value) : undefined);
  const mobileBanking = pickFirst(["mobileBanking", "mobileBankingUsage", "segment.mobileBanking"]);
  pushItem(segItems, "Uso de mobile banking", mobileBanking?.value ?? "", mobileBanking ? inferStatusColor("Uso de mobile banking", mobileBanking.value) : undefined);
  const preferredChannel = pickFirst(["preferredChannel", "preferredUsageChannel", "canalPreferencia", "segment.preferredChannel"]);
  pushItem(segItems, "Canal de uso de preferencia", preferredChannel?.value ?? "");
  const kyc = pickFirst(["kyc.state", "kycStatus", "kyc.status", "segment.kyc"]);
  pushItem(segItems, "Estado KYC", kyc?.value ?? "", kyc ? inferStatusColor("Estado KYC", kyc.value) : undefined);
  const pep = pickFirst(["kyc.isPep", "pep", "isPep", "segment.pep"]);
  pushItem(segItems, "PEP", pep?.value ?? "", pep ? inferStatusColor("PEP", pep.value) : undefined);

  const contactItems: DetailItem[] = [];
  const mobilePhones = pickFirstRaw([
    "naturalData.mobilePhoneNumberList",
    "phones.mobile",
    "mobilePhones",
    "mobilePhone"
  ]);
  pushItem(contactItems, "Celular:", toMultilineText(mobilePhones?.value ?? ""));

  const homePhones = pickFirstRaw([
    "naturalData.homePhoneNumberList",
    "phones.home",
    "homePhones",
    "homePhone"
  ]);
  pushItem(contactItems, "Particular:", toMultilineText(homePhones?.value ?? ""));

  const workPhones = pickFirstRaw([
    "naturalData.workPhoneNumberList",
    "phones.work",
    "workPhones",
    "workPhone"
  ]);
  pushItem(contactItems, "Laboral:", toMultilineText(workPhones?.value ?? ""));

  const emails = pickFirstRaw([
    "naturalData.emailList",
    "emails",
    "emailList",
    "email"
  ]);
  pushItem(contactItems, "Correo electrónico:", toMultilineText(emails?.value ?? ""));

  const homeAddress = pickFirstRaw([
    "addresses.street",
    "addresses.address",
    "address",
    "contact.address"
  ]);

  const homeCity = pickFirst(["addresses.city", "contact.city", "city"]);
  const homeCountry = pickFirst(["addresses.country", "contact.country", "country"]);
  const homeStreetText = toMultilineText(homeAddress?.value ?? "");
  const homeAddressText = [homeStreetText, homeCity?.value ?? "", homeCountry?.value ?? ""]
    .map((x) => String(x ?? "").trim())
    .filter(Boolean)
    .join(", ");

  pushItem(contactItems, "Domicilio Particular:", homeAddressText);

  const workStreet = pickFirstRaw([
    "addresses.workStreet",
    "addresses.work.address",
    "addresses.work.street",
    "workAddress",
    "address.work"
  ]);
  const workCity = pickFirst(["addresses.workCity", "addresses.work.city"]);
  const workCountry = pickFirst(["addresses.workCountry", "addresses.work.country"]);
  const workStreetText = toMultilineText(workStreet?.value ?? "");
  const workAddressText = [workStreetText, workCity?.value ?? "", workCountry?.value ?? ""]
    .map((x) => String(x ?? "").trim())
    .filter(Boolean)
    .join(", ");
  pushItem(contactItems, "Domicilio Laboral:", workAddressText);

  const carteraItems: DetailItem[] = [];
  const branch = pickFirst(["branch", "sucursal", "portfolio.branch", "carterizacion.branch"]);
  pushItem(carteraItems, "Sucursal", branch?.value ?? "");
  const officer = pickFirst(["officer", "oficial", "portfolio.officer", "carterizacion.officer"]);
  pushItem(carteraItems, "Oficial", officer?.value ?? "");
  const portfolio = pickFirst(["portfolio", "cartera", "portfolio.name", "carterizacion.portfolio"]);
  pushItem(carteraItems, "Cartera", portfolio?.value ?? "");

  const otherItems: DetailItem[] = [];
  for (const [path, sv] of Object.entries(agg.fields)) {
    if (usedPaths.has(path)) continue;
    if (path === "identityNumber") continue;
    const val = toText(sv.value);
    if (!val) continue;
    const label = path.split(".").slice(-2).join(".");
    otherItems.push({ label, value: val });
  }
  otherItems.sort((a, b) => a.label.localeCompare(b.label));

  const sections: DetailSection[] = [];
  sections.push({ title: "Información del cliente", items: infoItems });
  sections.push({ title: "Segmentos y Marcas", items: segItems });
  sections.push({ title: "Datos de contactabilidad", items: contactItems });
  sections.push({ title: "Datos de carterización", items: carteraItems });
  if (otherItems.length > 0) sections.push({ title: "Otros Datos", items: otherItems });

  return {
    id: identityNumber,
    fullName,
    personType,
    age: computedAge,
    phone,
    email,
    location,
    avatarUrl,
    details: { sections },
    products: [],
    priorities: [],
    history: []
  };
}

export async function lookupCustomerProfileFromDatasources(
  ctx: DatasourceContext,
  identityNumber: string,
  minConfidence = 0.5
): Promise<CustomerProfile & { avatarUrl: string }> {
  const agg: Aggregated = { fields: {} };

  const instances = ctx.instances;
  for (const inst of instances) {
    const config = getConfigFor(ctx, inst.pluginName, inst.id);
    if (!config) {
      continue;
    }

    const candidates = getCandidatesFor(ctx, inst.pluginName, inst.id).filter(
      (c) => (c.confidence ?? 0) >= minConfidence
    );

    const identityCandidate = pickBestCandidate(candidates, "identityNumber");
    if (!identityCandidate) {
      continue;
    }

    const baseRow = await queryOneRowByIdentity(inst.pluginName, inst.id, config, identityCandidate, identityNumber, ctx);
    if (!baseRow) {
      continue;
    }

    // Group candidates by table to support mappings spanning multiple tables.
    const byTable = new Map<string, MappingCandidate[]>();
    for (const c of candidates) {
      const t = String(c.table ?? "").trim();
      if (!t) continue;
      const arr = byTable.get(t) ?? [];
      arr.push(c);
      byTable.set(t, arr);
    }

    const keys = extractJoinKeys(baseRow);
    const identityTable = String(identityCandidate.table ?? "").trim();

    for (const [table, tableCandidates] of byTable.entries()) {
      let row: Record<string, unknown> | null = null;

      if (table === identityTable) {
        row = baseRow;
      } else if (inst.pluginName === "postgres" || inst.pluginName === "mssql") {
        row = await queryOneRowByHeuristics({
          plugin: inst.pluginName,
          instanceId: inst.id,
          config,
          table,
          identityNumber,
          keys
        });
      }

      if (!row) {
        continue;
      }

      for (const c of tableCandidates) {
        const v = (row as any)[c.column];
        upsertField(agg, c.customerPath, v, c.confidence ?? 0, { plugin: inst.pluginName, id: inst.id });
      }
    }
  }

  return toCustomerProfile(agg, identityNumber);
}

export async function lookupCustomerProfileFromDatasourcesWithDefaultIdentity(
  ctx: DatasourceContext,
  identityNumber: string | null | undefined,
  minConfidence = 0.5
): Promise<{ profile: CustomerProfile & { avatarUrl: string }; identityNumber: string }> {
  let resolved = String(identityNumber ?? "").trim();
  if (!resolved) {
    resolved = (await resolveDefaultIdentityNumber(ctx)) ?? "";
  }
  if (!resolved) {
    throw new Error("Customer id is required");
  }

  const profile = await lookupCustomerProfileFromDatasources(ctx, resolved, minConfidence);
  return { profile, identityNumber: resolved };
}
