import { z } from "zod";

export type GeminiMappingCandidate = {
  table: string;
  column: string;
  customerPath: string;
  confidence: number;
  reason: string;
};

export type GeminiSuggestMappingsInput = {
  schema: {
    tables: Array<{
      schema: string;
      name: string;
      columns: Array<{
        name: string;
        dataType: string;
        isNullable: boolean;
      }>;
    }>;
  };
  customerModel: {
    aggregateName: string;
    fields: string[];
  };
};

export type GeminiDeepInspectSample = {
  table: string;
  rows: unknown[];
};

export type GeminiDeepInspectInput = GeminiSuggestMappingsInput & {
  existingCandidates?: GeminiMappingCandidate[];
  samples: GeminiDeepInspectSample[];
};

export type GeminiSuggestMappingsOutput = {
  candidates: GeminiMappingCandidate[];
};

export type GeminiDeepInspectOutput = GeminiSuggestMappingsOutput;

export async function suggestCustomerMappingsWithGemini(
  input: GeminiSuggestMappingsInput
): Promise<GeminiSuggestMappingsOutput> {
  const apiKey = (Bun.env.GEMINI_API_KEY ?? "").trim();
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is not set");
  }

  const model = normalizeModelName(
    (Bun.env.GEMINI_MODEL ?? "gemini-1.5-flash-latest").trim() || "gemini-1.5-flash-latest"
  );

  const system =
    "You are a backend assistant that returns STRICT JSON only (no markdown, no prose).";

  const prompt = {
    task:
      "Infer which database columns are good candidates to map into the Customer aggregate.",
    rules: [
      "Return ONLY valid JSON.",
      "Return at most 30 candidates.",
      "confidence is a number 0..1.",
      "customerPath must be one of the provided fields (exact match).",
      "Prefer exact semantic matches (e.g. first_name -> naturalData.firstName).",
      "If unsure, omit the candidate.",
      "table must be 'schema.table' and column must be the column name."
    ],
    outputShape: {
      candidates: [
        {
          table: "schema.table",
          column: "column_name",
          customerPath: "naturalData.firstName",
          confidence: 0.9,
          reason: "brief reason"
        }
      ]
    },
    customerModel: input.customerModel,
    schema: input.schema
  };

  const controller = new AbortController();
  const timeoutMs = Number(Bun.env.GEMINI_TIMEOUT_MS ?? "60000");
  const effectiveTimeoutMs = Number.isFinite(timeoutMs) ? timeoutMs : 60000;
  const t = setTimeout(() => controller.abort(), effectiveTimeoutMs);

  try {
    let data: any;
    try {
      ({ data } = await generateContentWithFallback({
        apiKey,
        model,
        system,
        prompt,
        signal: controller.signal
      }));
    } catch (e) {
      if (isAbortError(e)) {
        throw new Error(
          `Gemini request timed out after ${effectiveTimeoutMs}ms. Increase GEMINI_TIMEOUT_MS or reduce schema size.`
        );
      }
      throw e;
    }

    const text =
      data?.candidates?.[0]?.content?.parts
        ?.map((p: any) => p?.text)
        .filter(Boolean)
        .join("") ??
      "";

    const jsonText = extractJsonObject(text);

    const CandidateSchema = z.object({
      table: z.string().min(1),
      column: z.string().min(1),
      customerPath: z.string().min(1),
      confidence: z.number(),
      reason: z.string().min(1)
    });

    const OutputSchema = z.object({
      candidates: z.array(CandidateSchema)
    });

    let parsedJson: unknown;
    try {
      parsedJson = JSON.parse(jsonText);
    } catch (e) {
      const raw = String(text ?? "");
      const snippet = raw.length > 800 ? `${raw.slice(0, 800)}...` : raw;
      throw new Error(
        `JSON Parse error: Unable to parse JSON string. ${e instanceof Error ? e.message : String(e)}. Raw: ${snippet}`
      );
    }

    const parsed = OutputSchema.parse(parsedJson);

    const parsedCandidates = parsed.candidates as GeminiMappingCandidate[];

    const allowed = new Set(input.customerModel.fields);
    return {
      candidates: parsedCandidates
        .filter((c: GeminiMappingCandidate) => allowed.has(c.customerPath))
        .map((c: GeminiMappingCandidate) => ({
          ...c,
          confidence: clamp01(c.confidence)
        }))
        .slice(0, 30)
    };
  } finally {
    clearTimeout(t);
  }
}

export async function suggestCustomerMappingsDeepInspectWithGemini(
  input: GeminiDeepInspectInput
): Promise<GeminiDeepInspectOutput> {
  const apiKey = (Bun.env.GEMINI_API_KEY ?? "").trim();
  if (!apiKey) {
    throw new Error("GEMINI_API_KEY is not set");
  }

  const model = normalizeModelName(
    (Bun.env.GEMINI_MODEL ?? "gemini-1.5-flash-latest").trim() || "gemini-1.5-flash-latest"
  );

  const system =
    "You are a backend assistant that returns STRICT JSON only (no markdown, no prose).";

  const prompt = {
    task:
      "Refine and expand mapping candidates using the interpreted schema PLUS real sample rows per table.",
    rules: [
      "Return ONLY valid JSON.",
      "Return at most 30 candidates.",
      "confidence is a number 0..1.",
      "customerPath must be one of the provided fields (exact match).",
      "Prefer columns whose sample values match expected data types/semantics.",
      "If unsure, omit the candidate.",
      "table must be 'schema.table' and column must be the column name.",
      "If existingCandidates is present, you may keep good ones, remove bad ones, and add new ones."
    ],
    outputShape: {
      candidates: [
        {
          table: "schema.table",
          column: "column_name",
          customerPath: "naturalData.firstName",
          confidence: 0.9,
          reason: "brief reason"
        }
      ]
    },
    customerModel: input.customerModel,
    schema: input.schema,
    samples: input.samples,
    existingCandidates: input.existingCandidates ?? []
  };

  const controller = new AbortController();
  const timeoutMs = Number(Bun.env.GEMINI_TIMEOUT_MS ?? "60000");
  const effectiveTimeoutMs = Number.isFinite(timeoutMs) ? timeoutMs : 60000;
  const t = setTimeout(() => controller.abort(), effectiveTimeoutMs);

  try {
    let data: any;
    try {
      ({ data } = await generateContentWithFallback({
        apiKey,
        model,
        system,
        prompt,
        signal: controller.signal
      }));
    } catch (e) {
      if (isAbortError(e)) {
        throw new Error(
          `Gemini request timed out after ${effectiveTimeoutMs}ms. Increase GEMINI_TIMEOUT_MS or reduce schema/sample size.`
        );
      }
      throw e;
    }

    const text =
      data?.candidates?.[0]?.content?.parts
        ?.map((p: any) => p?.text)
        .filter(Boolean)
        .join("") ??
      "";

    const jsonText = extractJsonObject(text);

    const CandidateSchema = z.object({
      table: z.string().min(1),
      column: z.string().min(1),
      customerPath: z.string().min(1),
      confidence: z.number(),
      reason: z.string().min(1)
    });

    const OutputSchema = z.object({
      candidates: z.array(CandidateSchema)
    });

    let parsedJson: unknown;
    try {
      parsedJson = JSON.parse(jsonText);
    } catch (e) {
      const raw = String(text ?? "");
      const snippet = raw.length > 800 ? `${raw.slice(0, 800)}...` : raw;
      throw new Error(
        `JSON Parse error: Unable to parse JSON string. ${e instanceof Error ? e.message : String(e)}. Raw: ${snippet}`
      );
    }

    const parsed = OutputSchema.parse(parsedJson);

    const parsedCandidates = parsed.candidates as GeminiMappingCandidate[];

    const allowed = new Set(input.customerModel.fields);
    return {
      candidates: parsedCandidates
        .filter((c: GeminiMappingCandidate) => allowed.has(c.customerPath))
        .map((c: GeminiMappingCandidate) => ({
          ...c,
          confidence: clamp01(c.confidence)
        }))
        .slice(0, 30)
    };
  } finally {
    clearTimeout(t);
  }
}

function isAbortError(e: unknown): boolean {
  return (
    typeof e === "object" &&
    e !== null &&
    "name" in e &&
    // DOMException in browsers, AbortError in undici/node/bun
    String((e as any).name) === "AbortError"
  );
}

async function generateContentWithFallback(args: {
  apiKey: string;
  model: string;
  system: string;
  prompt: unknown;
  signal: AbortSignal;
}): Promise<{ data: any; usedModel: string }> {
  const first = await generateContent({ ...args, model: args.model });
  if (first.ok) {
    return { data: first.data, usedModel: args.model };
  }

  const msg = errorMessageFromGeminiResponse(first.data, first.status);
  const isModelNotFound =
    msg.toLowerCase().includes("not found") && msg.toLowerCase().includes("models/");
  if (!isModelNotFound) {
    throw new Error(msg);
  }

  const fallback = await pickFallbackModel(args.apiKey);
  const second = await generateContent({ ...args, model: fallback });
  if (!second.ok) {
    throw new Error(errorMessageFromGeminiResponse(second.data, second.status));
  }
  return { data: second.data, usedModel: fallback };
}

async function generateContent(args: {
  apiKey: string;
  model: string;
  system: string;
  prompt: unknown;
  signal: AbortSignal;
}): Promise<{ ok: boolean; status: number; data: any }> {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    normalizeModelName(args.model)
  )}:generateContent?key=${encodeURIComponent(args.apiKey)}`;

  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: args.system }] },
      contents: [{ role: "user", parts: [{ text: JSON.stringify(args.prompt) }] }]
    }),
    signal: args.signal
  });

  const data = (await res.json().catch(() => null)) as any;
  return { ok: res.ok, status: res.status, data };
}

function errorMessageFromGeminiResponse(data: any, status: number): string {
  return (
    data?.error?.message ??
    data?.error ??
    (typeof data === "string" ? data : null) ??
    `HTTP ${status}`
  );
}

async function pickFallbackModel(apiKey: string): Promise<string> {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(
    apiKey
  )}`;

  const res = await fetch(endpoint);
  const data = (await res.json().catch(() => null)) as any;
  if (!res.ok) {
    throw new Error(errorMessageFromGeminiResponse(data, res.status));
  }

  const models = (data?.models ?? []) as Array<{
    name?: string;
    supportedGenerationMethods?: string[];
  }>;

  const supported = models
    .filter((m) => (m.supportedGenerationMethods ?? []).includes("generateContent"))
    .map((m) => normalizeModelName(m.name ?? ""))
    .filter(Boolean);

  const preferred = supported.find((m) => m.includes("gemini-1.5-flash"));
  if (preferred) return preferred;

  const anyFlash = supported.find((m) => m.includes("flash"));
  if (anyFlash) return anyFlash;

  const anyPro = supported.find((m) => m.includes("pro"));
  if (anyPro) return anyPro;

  if (supported.length === 0) {
    throw new Error("No Gemini models available for generateContent");
  }
  return supported[0];
}

function normalizeModelName(model: string): string {
  const trimmed = model.trim();
  if (!trimmed) return "";
  return trimmed.startsWith("models/") ? trimmed.slice("models/".length) : trimmed;
}

function clamp01(n: number): number {
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function extractJsonObject(text: string): string {
  const trimmed = text.trim();
  if (!trimmed) {
    throw new Error("Gemini returned empty response");
  }

  const noFences = trimmed
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();

  // Accept either a JSON object or array, and locate the first balanced JSON value.
  const firstObj = noFences.indexOf("{");
  const firstArr = noFences.indexOf("[");
  const start =
    firstObj < 0
      ? firstArr
      : firstArr < 0
        ? firstObj
        : Math.min(firstObj, firstArr);
  if (start < 0) {
    throw new Error("Gemini did not return a JSON object");
  }

  const open = noFences[start];
  const close = open === "[" ? "]" : "}";
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < noFences.length; i += 1) {
    const ch = noFences[i] ?? "";
    if (escape) {
      escape = false;
      continue;
    }
    if (ch === "\\") {
      if (inString) escape = true;
      continue;
    }
    if (ch === '"') {
      inString = !inString;
      continue;
    }
    if (inString) continue;

    if (ch === open) depth += 1;
    if (ch === close) {
      depth -= 1;
      if (depth === 0) {
        return noFences.slice(start, i + 1);
      }
    }
  }

  throw new Error("Gemini did not return a JSON object");
}
