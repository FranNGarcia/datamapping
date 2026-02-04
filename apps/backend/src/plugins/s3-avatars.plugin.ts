import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import type { Plugin } from "../types";

type S3Config = {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  bucket: string;
  prefix?: string;
};

type AvatarFolderCandidate = {
  prefix: string;
  score: number;
  imageCount: number;
  identityImageCount: number;
  confidence: number;
  sampleKeys: string[];
  reasons: string[];
};

const configs = new Map<string, S3Config>();

const s3AvatarsPlugin: Plugin = {
  name: "s3-avatars",
  manifest: {
    name: "s3-avatars",
    kind: "datasource",
    title: "AWS S3 (Avatars)",
    description: "Scan S3 folders to find likely customer profile picture locations"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/datasources/s3-avatars/config",
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
      method: "PUT",
      path: "/api/datasources/s3-avatars/config",
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
      path: "/api/datasources/s3-avatars/test",
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
              MaxKeys: 10
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
      path: "/api/datasources/s3-avatars/inspect-avatar-folders",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const maxKeys = clampInt((body as any)?.maxKeys, 1, 10000, 2000);
          const maxCandidates = clampInt((body as any)?.maxCandidates, 1, 100, 30);

          const client = createClient(cfg);
          const prefix = normalizePrefix(cfg.prefix ?? "");

          const keys = await listKeys(client, cfg.bucket, prefix, maxKeys);

          const images = keys.filter(isImageKey);

          const totalImagesByFolder = new Map<string, number>();
          const identityImagesByFolder = new Map<string, { count: number; sample: string[] }>();

          for (const key of images) {
            const folder = folderPrefix(key);
            totalImagesByFolder.set(folder, (totalImagesByFolder.get(folder) ?? 0) + 1);

            if (!isIdentityNumberImageKey(key)) continue;
            if (!identityImagesByFolder.has(folder)) identityImagesByFolder.set(folder, { count: 0, sample: [] });
            const entry = identityImagesByFolder.get(folder)!;
            entry.count += 1;
            if (entry.sample.length < 5) entry.sample.push(key);
          }

          const scored: AvatarFolderCandidate[] = [];
          for (const [folder, info] of identityImagesByFolder.entries()) {
            const totalImages = totalImagesByFolder.get(folder) ?? info.count;
            const { score, reasons } = scoreFolder(folder, info.count);
            const confidence = computeAvatarFolderConfidence({
              prefix: folder,
              identityImageCount: info.count,
              totalImageCount: totalImages,
              score
            });
            scored.push({
              prefix: folder,
              score,
              imageCount: totalImages,
              identityImageCount: info.count,
              confidence,
              sampleKeys: info.sample,
              reasons
            });
          }

          scored.sort((a, b) => b.score - a.score || b.imageCount - a.imageCount || a.prefix.localeCompare(b.prefix));

          return json({
            ok: true,
            id,
            bucket: cfg.bucket,
            prefix,
            scannedKeys: keys.length,
            imageKeys: images.length,
            candidates: scored.slice(0, maxCandidates)
          });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3-avatars/find-avatar",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const identityNumber = String((body as any)?.identityNumber ?? "").trim();
          if (!identityNumber) {
            throw new Error("identityNumber is required");
          }

          const folderPrefixInput = String((body as any)?.folderPrefix ?? "");
          const folderPrefixNorm = normalizeFolderPrefix(folderPrefixInput);

          const client = createClient(cfg);
          const wantedPrefix = `${folderPrefixNorm}${identityNumber}`;

          const res = await client.send(
            new ListObjectsV2Command({
              Bucket: cfg.bucket,
              Prefix: wantedPrefix,
              MaxKeys: 50
            })
          );

          const keys = (res.Contents ?? [])
            .map((o: { Key?: string }) => o.Key)
            .filter((k: string | undefined): k is string => Boolean(k))
            .map(String);

          const match =
            keys.find((k: string) => isIdentityNumberImageKey(k) && getKeyBasenameNoExt(k) === identityNumber) ?? null;

          return json({ ok: true, id, bucket: cfg.bucket, key: match });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });

    ctx.registerRoute({
      method: "POST",
      path: "/api/datasources/s3-avatars/presign-get",
      handler: async (req) => {
        try {
          const body = await readJson(req);
          const { id, config } = parseBodyWithId(body);
          const cfg = config ? parseConfig(config) : requireConfig(configs.get(id) ?? null);

          const key = String((body as any)?.key ?? "").trim();
          if (!key) {
            throw new Error("key is required");
          }

          const expiresInSeconds = clampInt((body as any)?.expiresInSeconds, 10, 3600, 300);

          const client = createClient(cfg);
          const url = await getSignedUrl(client, new GetObjectCommand({ Bucket: cfg.bucket, Key: key }), {
            expiresIn: expiresInSeconds
          });

          return json({ ok: true, id, bucket: cfg.bucket, key, url, expiresInSeconds });
        } catch (e) {
          return json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 400);
        }
      }
    });
  }
};

function clampInt(v: unknown, min: number, max: number, fallback: number): number {
  const n = typeof v === "number" ? v : Number(String(v ?? ""));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function isImageKey(key: string): boolean {
  const k = key.toLowerCase();
  return (
    k.endsWith(".jpg") ||
    k.endsWith(".jpeg") ||
    k.endsWith(".png") ||
    k.endsWith(".webp") ||
    k.endsWith(".gif")
  );
}

function getKeyBasenameNoExt(key: string): string {
  const idx = key.lastIndexOf("/");
  const name = idx >= 0 ? key.slice(idx + 1) : key;
  const dot = name.lastIndexOf(".");
  return dot >= 0 ? name.slice(0, dot) : name;
}

function looksLikeIdentityNumber(input: string): boolean {
  const s = String(input ?? "").trim();
  return /^\d{6,20}$/.test(s);
}

function isIdentityNumberImageKey(key: string): boolean {
  if (!isImageKey(key)) return false;
  const base = getKeyBasenameNoExt(key);
  return looksLikeIdentityNumber(base);
}

function folderPrefix(key: string): string {
  const idx = key.lastIndexOf("/");
  if (idx < 0) return "";
  return key.slice(0, idx + 1);
}

function scoreFolder(prefix: string, imageCount: number): { score: number; reasons: string[] } {
  const reasons: string[] = [];
  const p = String(prefix ?? "").toLowerCase();

  let score = 0;

  const tokens = [
    "avatar",
    "avatars",
    "profile",
    "profiles",
    "picture",
    "pictures",
    "photo",
    "photos",
    "image",
    "images",
    "img",
    "perfil",
    "foto",
    "fotos"
  ];

  for (const t of tokens) {
    if (p.includes(t)) {
      score += 30;
      reasons.push(`folder contains '${t}'`);
      break;
    }
  }

  if (p.includes("customer") || p.includes("customers") || p.includes("client") || p.includes("clients")) {
    score += 10;
    reasons.push("folder mentions customer/client");
  }

  // Prefer folders with some density, but avoid putting huge weight on raw count.
  if (imageCount >= 1) score += 5;
  if (imageCount >= 5) score += 10;
  if (imageCount >= 20) score += 10;
  if (imageCount >= 100) score += 5;

  if (imageCount >= 5) reasons.push(`contains ${imageCount} image files`);

  // Shallow folders tend to be more "assets"-like; not always correct but helpful.
  const depth = p.split("/").filter(Boolean).length;
  if (depth <= 2) {
    score += 5;
    reasons.push("shallow folder");
  }

  if (reasons.length === 0) {
    reasons.push("contains image files");
  }

  return { score, reasons };
}

function computeAvatarFolderConfidence(args: {
  prefix: string;
  identityImageCount: number;
  totalImageCount: number;
  score: number;
}): number {
  const { prefix, identityImageCount, totalImageCount, score } = args;
  const ratio = totalImageCount > 0 ? identityImageCount / totalImageCount : 0;
  const countScore = Math.min(1, identityImageCount / 10);

  // Score is typically in ~[0..80]; map into [0..1] softly.
  const scoreScore = Math.max(0, Math.min(1, score / 80));

  const p = String(prefix ?? "").toLowerCase();
  const keywordBoost =
    p.includes("avatar") ||
    p.includes("profile") ||
    p.includes("picture") ||
    p.includes("photo") ||
    p.includes("foto") ||
    p.includes("perfil")
      ? 0.1
      : 0;

  const confidence = 0.45 * ratio + 0.35 * countScore + 0.2 * scoreScore + keywordBoost;
  return Math.max(0, Math.min(1, confidence));
}

async function listKeys(client: S3Client, bucket: string, prefix: string, maxKeys: number): Promise<string[]> {
  const out: string[] = [];
  let token: string | undefined;

  while (out.length < maxKeys) {
    const res = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        MaxKeys: Math.min(1000, maxKeys - out.length),
        ContinuationToken: token
      })
    );

    const keys = (res.Contents ?? [])
      .map((o: { Key?: string }) => o.Key)
      .filter((k: string | undefined): k is string => Boolean(k))
      .map(String);

    out.push(...keys);

    if (!res.IsTruncated || !res.NextContinuationToken) {
      break;
    }
    token = res.NextContinuationToken;
  }

  return out;
}

function normalizePrefix(prefix: string): string {
  const p = String(prefix ?? "").trim();
  if (!p) return "";
  return p.endsWith("/") ? p : `${p}/`;
}

function normalizeFolderPrefix(prefix: string): string {
  const p = String(prefix ?? "").trim();
  if (!p) return "";
  return p.endsWith("/") ? p : `${p}/`;
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
    throw new Error("S3 avatars datasource is not configured");
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

export default s3AvatarsPlugin;
