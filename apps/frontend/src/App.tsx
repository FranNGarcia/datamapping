import { useEffect, useRef, useState, type DragEvent, type PointerEvent as ReactPointerEvent } from "react";
import CustomerProfilePanel from "./components/CustomerProfilePanel";

type Health = {
  ok: boolean;
  service: string;
  ts: number;
};

 const DEFAULT_CUSTOMER_FIELD_PATHS: string[] = [
   "identityType",
   "identityNumber",
   "profilePicture",
   "isClient",
   "isProspect",
   "nationalityCode",
   "customerStatus",
   "customerBrand",
   "banking",
   "segment",
   "riskLevel",
   "delinquency",
   "billing",
   "income",
   "profitability",
   "homeBankingUsage",
   "mobileBankingUsage",
   "preferredUsageChannel",
   "boolFreeFields",
   "integerFreeFields",
   "floatFreeFields",
   "stringFreeFields",
   "naturalData.birthDate",
   "naturalData.firstName",
   "naturalData.secondName",
   "naturalData.lastName",
   "naturalData.secondLastName",
   "naturalData.homePhoneNumberList",
   "naturalData.workPhoneNumberList",
   "naturalData.mobilePhoneNumberList",
   "naturalData.emailList",
   "naturalData.overrideLists",
   "naturalData.gender",
   "naturalData.maritalStatus",
   "legalData.businessName",
   "legalData.companyDate",
   "legalData.homePhoneNumberList",
   "legalData.workPhoneNumberList",
   "legalData.mobilePhoneNumberList",
   "legalData.emailList",
   "addresses.customerIdentityNumber",
   "addresses.country",
   "addresses.city",
   "addresses.street",
   "addresses.streetNumber",
   "addresses.floor",
   "addresses.flat",
   "addresses.zipCode",
   "addresses.state",
   "addresses.neighborhood",
   "addresses.type",
   "addresses.region",
   "kyc.customerIdentityNumber",
   "kyc.state",
   "kyc.declarationDate",
   "kyc.effectiveDate",
   "kyc.obligatedSubject",
   "kyc.isPep",
   "kyc.isFATCA",
   "kyc.activity",
   "kyc.salaryRange",
   "kyc.taxIdentificationKey",
   "kyc.identificationKey",
   "personalRelationships.boolFreeFields",
   "personalRelationships.integerFreeFields",
   "personalRelationships.floatFreeFields",
   "personalRelationships.stringFreeFields",
   "businessRelationships.boolFreeFields",
   "businessRelationships.integerFreeFields",
   "businessRelationships.floatFreeFields",
   "businessRelationships.stringFreeFields"
 ];
 
 const DEFAULT_CUSTOMER_PRIMARY_KEYS: string[] = ["identityNumber"];

type PluginManifest = {
  name: string;
  kind: "datasource" | "utility";
  title: string;
  description?: string;
};

type PluginsResponse = {
  plugins: PluginManifest[];
};

type DatasourceInstance = {
  id: string;
  pluginName: string;
  title: string;
  x: number;
  y: number;
};

type PointerState = {
  id: string;
  pluginName: string;
  startClientX: number;
  startClientY: number;
  offsetX: number;
  offsetY: number;
  dragging: boolean;
};

 type N5OSnapshotV1 = {
  version: 1;
  exportedAt: string;
  mergeMode: boolean;
  instances: DatasourceInstance[];
  configs: {
    postgres: Record<string, PostgresConfig>;
    mssql: Record<string, MsSqlConfig>;
    mongodb: Record<string, MongoDbConfig>;
    sftp: Record<string, SftpConfig>;
    s3: Record<string, S3Config>;
    s3Avatars: Record<string, S3Config>;
    restApi: Record<string, RestApiConfig>;
  };
  schemas: {
    postgres: Record<string, PgSchemaTable[]>;
    mssql: Record<string, PgSchemaTable[]>;
    mongodb: Record<string, PgSchemaTable[]>;
    sftp: Record<string, PgSchemaTable[]>;
    s3: Record<string, PgSchemaTable[]>;
  };
  candidates: {
    postgres: Record<string, MappingCandidate[]>;
    mssql: Record<string, MappingCandidate[]>;
    mongodb: Record<string, MappingCandidate[]>;
    sftp: Record<string, MappingCandidate[]>;
    s3: Record<string, MappingCandidate[]>;
    restApi: Record<string, MappingCandidate[]>;
  };
  files: {
    sftpFilesById: Record<string, SftpFileEntry[]>;
    s3FilesById: Record<string, S3FileEntry[]>;
  };
  avatarFolders?: {
    s3AvatarsById: Record<string, S3AvatarFolderCandidate[]>;
    s3AvatarsSelectedFolderPrefixById?: Record<string, string>;
  };
  restApiIdentityValueById?: Record<string, string>;
  restApiIdentifierFieldById?: Record<string, string>;
  restApiSelectedPathById?: Record<string, string | null>;
};

function serializeN5OSnapshot(snapshot: N5OSnapshotV1): string {
  return JSON.stringify(snapshot, null, 2);
}

function parseN5OSnapshot(text: string): N5OSnapshotV1 {
  const data = JSON.parse(text) as Partial<N5OSnapshotV1>;
  if (!data || data.version !== 1 || !Array.isArray(data.instances)) {
    throw new Error("Invalid .n5o file");
  }
  return data as N5OSnapshotV1;
}

function downloadN5OFile(snapshot: N5OSnapshotV1, fileBaseName = "datasource-canvas"): void {
  const safeTs = snapshot.exportedAt.replace(/[:.]/g, "-");
  const blob = new Blob([serializeN5OSnapshot(snapshot)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${fileBaseName}-${safeTs}.n5o`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

type PostgresConfig = {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
  ssl?: boolean;
};

type MsSqlConfig = {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
  encrypt?: boolean;
  trustServerCertificate?: boolean;
};

type SftpConfig = {
  host: string;
  port: number;
  username: string;
  password?: string;
  privateKey?: string;
  passphrase?: string;
  basePath?: string;
};

type S3Config = {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  bucket: string;
  prefix?: string;
};

type MongoDbConfig = {
  uri: string;
  database: string;
  collection: string;
  sampleSize?: number;
};

type RestApiConfig = {
  openapiUrl: string;
  authType?: "none" | "bearer" | "basic";
  bearerToken?: string;
  username?: string;
  password?: string;
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

type SftpFileEntry = {
  path: string;
  selected: boolean;
};

type SftpDirEntry = {
  path: string;
};

type S3FileEntry = {
  key: string;
  selected: boolean;
};

type S3DirEntry = {
  prefix: string;
};

type S3AvatarFolderCandidate = {
  prefix: string;
  score: number;
  imageCount: number;
  identityImageCount: number;
  confidence: number;
  sampleKeys: string[];
  reasons: string[];
};

type DatasourceUiStatus = "not_configured" | "configured" | "schema_analyzed" | "error";

type CustomerFieldsResponse = {
  ok: boolean;
  aggregate: string;
  fields: string[];
  primaryKeys?: string[];
  error?: string;
};

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [plugins, setPlugins] = useState<PluginManifest[]>([]);
  const [instances, setInstances] = useState<DatasourceInstance[]>([]);
  const [selectedInstanceId, setSelectedInstanceId] = useState<string | null>(null);
  const [drawerInstanceId, setDrawerInstanceId] = useState<string | null>(null);
  const [mergeMode, setMergeMode] = useState(false);
  const [rightPanelCollapsed, setRightPanelCollapsed] = useState(false);
  const [splitAnimating, setSplitAnimating] = useState(false);

  const MIN_LEFT_WIDTH = 520;
  const MIN_RIGHT_WIDTH = 360;
  const SPLIT_DIVIDER_WIDTH = 8;
  const REOPEN_RIGHT_WIDTH = 400;

  const [splitLeftWidth, setSplitLeftWidth] = useState<number>(() =>
    Math.max(MIN_LEFT_WIDTH, document.documentElement.clientWidth - MIN_RIGHT_WIDTH - SPLIT_DIVIDER_WIDTH - 80)
  );
  const splitLeftWidthRef = useRef<number>(splitLeftWidth);
  const rightPanelCollapsedRef = useRef<boolean>(rightPanelCollapsed);
  const resizingSplitRef = useRef(false);
  const splitRootRef = useRef<HTMLDivElement | null>(null);

  const n5oImportInputRef = useRef<HTMLInputElement | null>(null);

  const canvasRef = useRef<HTMLDivElement | null>(null);
  const pointerStateRef = useRef<PointerState | null>(null);
  const [pointerActive, setPointerActive] = useState(false);

  const [restApiIdentityValueById, setRestApiIdentityValueById] = useState<Record<string, string>>({});
  const [restApiIdentifierFieldById, setRestApiIdentifierFieldById] = useState<Record<string, string>>({});
  const [restApiSelectedPathById, setRestApiSelectedPathById] = useState<Record<string, string | null>>({});
  const [restApiDeepInspectResultById, setRestApiDeepInspectResultById] = useState<
    Record<string, any | null>
  >({});

  function buildSnapshot(): N5OSnapshotV1 {
    return {
      version: 1,
      exportedAt: new Date().toISOString(),
      mergeMode,
      instances,
      configs: {
        postgres: pgConfigsById,
        mssql: mssqlConfigsById,
        mongodb: mongoConfigsById,
        sftp: sftpConfigsById,
        s3: s3ConfigsById,
        s3Avatars: s3AvatarsConfigsById,
        restApi: restApiConfigsById
      },
      schemas: {
        postgres: pgSchemaById,
        mssql: mssqlSchemaById,
        mongodb: mongoSchemaById,
        sftp: sftpSchemaById,
        s3: s3SchemaById
      },
      candidates: {
        postgres: pgCandidatesById,
        mssql: mssqlCandidatesById,
        mongodb: mongoCandidatesById,
        sftp: sftpCandidatesById,
        s3: s3CandidatesById,
        restApi: restApiCandidatesById
      },
      files: {
        sftpFilesById,
        s3FilesById
      },
      avatarFolders: {
        s3AvatarsById: s3AvatarsFoldersById
        ,
        s3AvatarsSelectedFolderPrefixById: s3AvatarsSelectedFolderPrefixById as Record<string, string>
      },
      restApiIdentityValueById,
      restApiIdentifierFieldById,
      restApiSelectedPathById
    };
  }

  function applySnapshot(snapshot: N5OSnapshotV1): void {
    setMergeMode(Boolean(snapshot.mergeMode));
    setInstances(snapshot.instances ?? []);
    setSelectedInstanceId(null);
    setDrawerInstanceId(null);

    setPgConfigsById(snapshot.configs?.postgres ?? {});
    setMssqlConfigsById(snapshot.configs?.mssql ?? {});
    setMongoConfigsById(snapshot.configs?.mongodb ?? {});
    setSftpConfigsById(snapshot.configs?.sftp ?? {});
    setS3ConfigsById(snapshot.configs?.s3 ?? {});
    setS3AvatarsConfigsById(snapshot.configs?.s3Avatars ?? {});
    setRestApiConfigsById(snapshot.configs?.restApi ?? {});

    setPgSchemaById(snapshot.schemas?.postgres ?? {});
    setMssqlSchemaById(snapshot.schemas?.mssql ?? {});
    setMongoSchemaById(snapshot.schemas?.mongodb ?? {});
    setSftpSchemaById(snapshot.schemas?.sftp ?? {});
    setS3SchemaById(snapshot.schemas?.s3 ?? {});

    setPgCandidatesById(snapshot.candidates?.postgres ?? {});
    setMssqlCandidatesById(snapshot.candidates?.mssql ?? {});
    setMongoCandidatesById(snapshot.candidates?.mongodb ?? {});
    setSftpCandidatesById(snapshot.candidates?.sftp ?? {});
    setS3CandidatesById(snapshot.candidates?.s3 ?? {});
    setRestApiCandidatesById(snapshot.candidates?.restApi ?? {});

    setSftpFilesById(snapshot.files?.sftpFilesById ?? {});
    setS3FilesById(snapshot.files?.s3FilesById ?? {});

    setS3AvatarsFoldersById(snapshot.avatarFolders?.s3AvatarsById ?? {});
    setS3AvatarsSelectedFolderPrefixById(snapshot.avatarFolders?.s3AvatarsSelectedFolderPrefixById ?? {});

    setRestApiIdentityValueById(snapshot.restApiIdentityValueById ?? {});
    setRestApiIdentifierFieldById(snapshot.restApiIdentifierFieldById ?? {});
    setRestApiSelectedPathById(snapshot.restApiSelectedPathById ?? {});
  }

  function onExportN5O(): void {
    try {
      setError(null);
      downloadN5OFile(buildSnapshot());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function onRequestImportN5O(): void {
    n5oImportInputRef.current?.click();
  }

  async function onImportN5OFileSelected(file: File | null): Promise<void> {
    if (!file) return;
    const text = await file.text();
    const snapshot = parseN5OSnapshot(text);
    applySnapshot(snapshot);
  }

  useEffect(() => {
    splitLeftWidthRef.current = splitLeftWidth;
  }, [splitLeftWidth]);

  useEffect(() => {
    rightPanelCollapsedRef.current = rightPanelCollapsed;
  }, [rightPanelCollapsed]);


  const [pgConfigsById, setPgConfigsById] = useState<Record<string, PostgresConfig>>({});
  const [pgStatus, setPgStatus] = useState<string | null>(null);
  const [pgSchemaById, setPgSchemaById] = useState<Record<string, PgSchemaTable[]>>({});
  const [pgSchemaStatusById, setPgSchemaStatusById] = useState<Record<string, string | null>>({});
  const [pgCandidatesById, setPgCandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [pgCandidatesStatusById, setPgCandidatesStatusById] = useState<Record<string, string | null>>({});

  const [mssqlConfigsById, setMssqlConfigsById] = useState<Record<string, MsSqlConfig>>({});
  const [mssqlStatusById, setMssqlStatusById] = useState<Record<string, string | null>>({});
  const [mssqlSchemaById, setMssqlSchemaById] = useState<Record<string, PgSchemaTable[]>>({});
  const [mssqlSchemaStatusById, setMssqlSchemaStatusById] = useState<Record<string, string | null>>({});
  const [mssqlCandidatesById, setMssqlCandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [mssqlCandidatesStatusById, setMssqlCandidatesStatusById] = useState<Record<string, string | null>>({});

  const [sftpConfigsById, setSftpConfigsById] = useState<Record<string, SftpConfig>>({});
  const [sftpStatusById, setSftpStatusById] = useState<Record<string, string | null>>({});

  const [sftpFilesById, setSftpFilesById] = useState<Record<string, SftpFileEntry[]>>({});
  const [sftpFilesStatusById, setSftpFilesStatusById] = useState<Record<string, string | null>>({});

  const [sftpDirsById, setSftpDirsById] = useState<Record<string, SftpDirEntry[]>>({});

  const [sftpSchemaById, setSftpSchemaById] = useState<Record<string, PgSchemaTable[]>>({});
  const [sftpSchemaStatusById, setSftpSchemaStatusById] = useState<Record<string, string | null>>({});
  const [sftpCandidatesById, setSftpCandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [sftpCandidatesStatusById, setSftpCandidatesStatusById] = useState<Record<string, string | null>>({});

  const [s3ConfigsById, setS3ConfigsById] = useState<Record<string, S3Config>>({});
  const [s3StatusById, setS3StatusById] = useState<Record<string, string | null>>({});
  const [s3FilesById, setS3FilesById] = useState<Record<string, S3FileEntry[]>>({});
  const [s3FilesStatusById, setS3FilesStatusById] = useState<Record<string, string | null>>({});
  const [s3DirsById, setS3DirsById] = useState<Record<string, S3DirEntry[]>>({});
  const [s3SchemaById, setS3SchemaById] = useState<Record<string, PgSchemaTable[]>>({});
  const [s3SchemaStatusById, setS3SchemaStatusById] = useState<Record<string, string | null>>({});
  const [s3CandidatesById, setS3CandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [s3CandidatesStatusById, setS3CandidatesStatusById] = useState<Record<string, string | null>>({});

  const [s3AvatarsConfigsById, setS3AvatarsConfigsById] = useState<Record<string, S3Config>>({});
  const [s3AvatarsStatusById, setS3AvatarsStatusById] = useState<Record<string, string | null>>({});
  const [s3AvatarsFoldersById, setS3AvatarsFoldersById] = useState<Record<string, S3AvatarFolderCandidate[]>>({});
  const [s3AvatarsFoldersStatusById, setS3AvatarsFoldersStatusById] = useState<Record<string, string | null>>({});
  const [s3AvatarsSelectedFolderPrefixById, setS3AvatarsSelectedFolderPrefixById] = useState<
    Record<string, string>
  >({});

  const [restApiConfigsById, setRestApiConfigsById] = useState<Record<string, RestApiConfig>>({});
  const [restApiStatusById, setRestApiStatusById] = useState<Record<string, string | null>>({});
  const [restApiCandidatesById, setRestApiCandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [restApiCandidatesStatusById, setRestApiCandidatesStatusById] = useState<Record<string, string | null>>({});

  const [mongoConfigsById, setMongoConfigsById] = useState<Record<string, MongoDbConfig>>({});
  const [mongoStatusById, setMongoStatusById] = useState<Record<string, string | null>>({});
  const [mongoSchemaById, setMongoSchemaById] = useState<Record<string, PgSchemaTable[]>>({});
  const [mongoSchemaStatusById, setMongoSchemaStatusById] = useState<Record<string, string | null>>({});
  const [mongoCandidatesById, setMongoCandidatesById] = useState<Record<string, MappingCandidate[]>>({});
  const [mongoCandidatesStatusById, setMongoCandidatesStatusById] = useState<Record<string, string | null>>({});

  const [identityNumberAlertById, setIdentityNumberAlertById] = useState<Record<string, string | null>>({});

  const [customerFieldsAggregate, setCustomerFieldsAggregate] = useState<string>("customer");
  const [customerFieldsStatus, setCustomerFieldsStatus] = useState<string | null>(null);
  const [customerFields, setCustomerFields] = useState<string[]>([]);
  const [customerFieldPaths, setCustomerFieldPaths] = useState<string[]>([]);
  const [customerPrimaryKeys, setCustomerPrimaryKeys] = useState<Set<string>>(
    () => new Set(DEFAULT_CUSTOMER_PRIMARY_KEYS)
  );
  const [resolvedProfilePictureConfidence, setResolvedProfilePictureConfidence] = useState<number | null>(null);

  useEffect(() => {
    const clampLeftWidth = (vw: number, desired: number) => {
      const maxLeft = Math.max(0, vw - SPLIT_DIVIDER_WIDTH);
      const minLeft = Math.min(MIN_LEFT_WIDTH, maxLeft);
      return Math.min(maxLeft, Math.max(minLeft, desired));
    };

    const collapseNow = () => {
      setRightPanelCollapsed(true);
      rightPanelCollapsedRef.current = true;
    };

    const animateSnapClosed = (vw: number) => {
      const maxLeft = Math.max(0, vw - SPLIT_DIVIDER_WIDTH);
      setSplitAnimating(true);
      splitLeftWidthRef.current = maxLeft;
      setSplitLeftWidth(maxLeft);
      window.setTimeout(() => setSplitAnimating(false), 180);
    };

    const onMove = (e: PointerEvent) => {
      if (!resizingSplitRef.current) return;

      const vw = document.documentElement.clientWidth;
      const rootLeft = splitRootRef.current?.getBoundingClientRect().left ?? 0;
      const desired = e.clientX - rootLeft;
      const clamped = clampLeftWidth(vw, desired);
      const rightWidth = vw - clamped - SPLIT_DIVIDER_WIDTH;

      if (!rightPanelCollapsedRef.current && rightWidth < MIN_RIGHT_WIDTH) {
        setSplitAnimating(false);
        splitLeftWidthRef.current = clamped;
        setSplitLeftWidth(clamped);
        collapseNow();
        return;
      }

      if (rightPanelCollapsedRef.current && rightWidth >= REOPEN_RIGHT_WIDTH) {
        setRightPanelCollapsed(false);
      }

      splitLeftWidthRef.current = clamped;
      setSplitLeftWidth(clamped);
    };

    const onUp = () => {
      if (!resizingSplitRef.current) return;
      resizingSplitRef.current = false;

      const vw = document.documentElement.clientWidth;
      const left = splitLeftWidthRef.current;
      const rightWidth = vw - left - SPLIT_DIVIDER_WIDTH;

      if (!rightPanelCollapsedRef.current && rightWidth < MIN_RIGHT_WIDTH) {
        collapseNow();
      }

      if (rightPanelCollapsedRef.current) {
        animateSnapClosed(vw);
      }
    };

    const onResize = () => {
      const vw = document.documentElement.clientWidth;
      const left = clampLeftWidth(vw, splitLeftWidthRef.current);
      if (left !== splitLeftWidthRef.current) {
        setSplitLeftWidth(left);
      }

      const rightWidth = vw - left - SPLIT_DIVIDER_WIDTH;
      if (!rightPanelCollapsedRef.current && rightWidth < MIN_RIGHT_WIDTH) {
        collapseNow();
      }
    };

    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("resize", onResize);

    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("resize", onResize);
    };
  }, [MIN_LEFT_WIDTH, MIN_RIGHT_WIDTH, REOPEN_RIGHT_WIDTH, SPLIT_DIVIDER_WIDTH]);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const [healthRes, pluginsRes] = await Promise.all([
          fetch("/api/health"),
          fetch("/api/plugins")
        ]);

        if (!healthRes.ok) {
          throw new Error(`Health HTTP ${healthRes.status}`);
        }

        if (!pluginsRes.ok) {
          throw new Error(`Plugins HTTP ${pluginsRes.status}`);
        }

        const healthData = (await healthRes.json()) as Health;
        const pluginsData = (await pluginsRes.json()) as PluginsResponse;

        if (!cancelled) {
          setHealth(healthData);
          setPlugins(pluginsData.plugins);
        }
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  async function saveS3Config() {
    if (!drawerInstanceId) {
      return;
    }
    setS3StatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = s3ConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/s3/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setS3StatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setS3StatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectSftpInstance(instanceId: string) {
    setSftpCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Deep inspecting..." }));
    try {
      const cfg = sftpConfigsById[instanceId];
      const tables = sftpSchemaById[instanceId] ?? [];
      if (!tables || tables.length === 0) {
        throw new Error("Run schema inspect first");
      }
      const candidates = sftpCandidatesById[instanceId] ?? [];

      const res = await fetch("/api/datasources/sftp/deep-inspect", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, tables, candidates })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const next = (data?.candidates ?? []) as MappingCandidate[];
      setSftpCandidatesById((prev) => ({ ...prev, [instanceId]: next }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: next.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setSftpCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setSftpCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectRestApiEndpoint(instanceId: string, candidate: MappingCandidate) {
    try {
      const cfg = restApiConfigsById[instanceId];
      const identityValue = restApiIdentityValueById[instanceId] ?? "";

      const res = await fetch("/api/datasources/rest-api/deep-inspect-endpoint", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: instanceId,
          config: cfg,
          endpoint: {
            method: "GET",
            path: candidate.column,
            identityParam: { name: "customerId", in: "path" },
            identityValue
          }
        })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setRestApiDeepInspectResultById((prev) => ({ ...prev, [instanceId]: data }));
      setRestApiCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: `Deep inspect OK. HTTP ${data?.status ?? "?"} ${data?.statusText ?? ""}`
      }));
    } catch (e) {
      setRestApiCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectS3Instance(instanceId: string) {
    setS3CandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Deep inspecting..." }));
    try {
      const cfg = s3ConfigsById[instanceId];
      const tables = s3SchemaById[instanceId] ?? [];
      if (!tables || tables.length === 0) {
        throw new Error("Run schema inspect first");
      }
      const candidates = s3CandidatesById[instanceId] ?? [];

      const res = await fetch("/api/datasources/s3/deep-inspect", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, tables, candidates })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const next = (data?.candidates ?? []) as MappingCandidate[];
      setS3CandidatesById((prev) => ({ ...prev, [instanceId]: next }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: next.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setS3CandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setS3CandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectMssqlInstance(instanceId: string) {
    setMssqlCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Deep inspecting..." }));
    try {
      const cfg = mssqlConfigsById[instanceId];
      const tables = mssqlSchemaById[instanceId] ?? [];
      if (!tables || tables.length === 0) {
        throw new Error("Run schema inspect first");
      }
      const candidates = mssqlCandidatesById[instanceId] ?? [];

      const res = await fetch("/api/datasources/mssql/deep-inspect", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, tables, candidates })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const next = (data?.candidates ?? []) as MappingCandidate[];
      setMssqlCandidatesById((prev) => ({ ...prev, [instanceId]: next }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: next.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setMssqlCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setMssqlCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectMongoInstance(instanceId: string) {
    setMongoCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Deep inspecting..." }));
    try {
      const cfg = mongoConfigsById[instanceId];
      const tables = mongoSchemaById[instanceId] ?? [];
      if (!tables || tables.length === 0) {
        throw new Error("Run schema inspect first");
      }
      const candidates = mongoCandidatesById[instanceId] ?? [];

      const res = await fetch("/api/datasources/mongodb/deep-inspect", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, tables, candidates })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const next = (data?.candidates ?? []) as MappingCandidate[];
      setMongoCandidatesById((prev) => ({ ...prev, [instanceId]: next }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: next.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setMongoCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setMongoCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadMongoConfig(instanceId: string) {
    setMongoStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/mongodb/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setMongoConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            uri: data.config.uri ?? "",
            database: data.config.database ?? "",
            collection: data.config.collection ?? "",
            sampleSize: data.config.sampleSize == null ? undefined : Number(data.config.sampleSize)
          }
        }));
      }
    } catch (e) {
      setMongoStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function testMongoConnection(instanceId: string) {
    setMongoStatusById((prev) => ({ ...prev, [instanceId]: "Testing connection..." }));
    try {
      const cfg = mongoConfigsById[instanceId];
      const res = await fetch("/api/datasources/mongodb/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      const count = Array.isArray(data?.collections) ? data.collections.length : 0;
      setMongoStatusById((prev) => ({
        ...prev,
        [instanceId]: `Connection OK. Listed ${count} collections from ${data?.database ?? ""}`
      }));
    } catch (e) {
      setMongoStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectMongoSchema(instanceId: string) {
    setMongoSchemaStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting schema..." }));
    try {
      const cfg = mongoConfigsById[instanceId];
      const res = await fetch("/api/datasources/mongodb/schema", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setMongoSchemaById((prev) => ({
        ...prev,
        [instanceId]: (data?.tables ?? []) as PgSchemaTable[]
      }));
      setMongoSchemaStatusById((prev) => ({ ...prev, [instanceId]: null }));

      void suggestMappingsForMongoInstance(instanceId);
    } catch (e) {
      setMongoSchemaStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function suggestMappingsForMongoInstance(instanceId: string) {
    setMongoCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Analyzing..." }));
    try {
      const cfg = mongoConfigsById[instanceId];
      const res = await fetch("/api/datasources/mongodb/suggest-mappings", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const candidates = (data?.candidates ?? []) as MappingCandidate[];
      setMongoCandidatesById((prev) => ({ ...prev, [instanceId]: candidates }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: candidates.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setMongoCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setMongoCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function saveMongoConfig() {
    if (!drawerInstanceId) {
      return;
    }
    setMongoStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = mongoConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/mongodb/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }

      setMongoStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setMongoStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function saveMssqlConfig() {
    if (!drawerInstanceId) {
      return;
    }
    setMssqlStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = mssqlConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/mssql/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }

      setMssqlStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setMssqlStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function suggestMappingsForMssqlInstance(instanceId: string) {
    setMssqlCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Analyzing..." }));
    try {
      const cfg = mssqlConfigsById[instanceId];
      const res = await fetch("/api/datasources/mssql/suggest-mappings", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const candidates = (data?.candidates ?? []) as MappingCandidate[];
      setMssqlCandidatesById((prev) => ({ ...prev, [instanceId]: candidates }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: candidates.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setMssqlCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setMssqlCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectMssqlSchema(instanceId: string) {
    setMssqlSchemaStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting schema..." }));
    try {
      const cfg = mssqlConfigsById[instanceId];
      const res = await fetch("/api/datasources/mssql/schema", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setMssqlSchemaById((prev) => ({
        ...prev,
        [instanceId]: (data?.tables ?? []) as PgSchemaTable[]
      }));
      setMssqlSchemaStatusById((prev) => ({ ...prev, [instanceId]: null }));

      void suggestMappingsForMssqlInstance(instanceId);
    } catch (e) {
      setMssqlSchemaStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadMssqlConfig(instanceId: string) {
    setMssqlStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/mssql/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setMssqlConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            host: data.config.host ?? "localhost",
            port: Number(data.config.port ?? 1433),
            database: data.config.database ?? "",
            user: data.config.user ?? "",
            password: "",
            encrypt: Boolean(data.config.encrypt ?? false),
            trustServerCertificate: Boolean(data.config.trustServerCertificate ?? false)
          }
        }));
      }
    } catch (e) {
      setMssqlStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadS3Config(instanceId: string) {
    setS3StatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/s3/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setS3ConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            region: data.config.region ?? "",
            accessKeyId: data.config.accessKeyId ?? "",
            secretAccessKey: "",
            sessionToken: "",
            bucket: data.config.bucket ?? "",
            prefix: data.config.prefix ?? ""
          }
        }));
      }
    } catch (e) {
      setS3StatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadS3AvatarsConfig(instanceId: string) {
    setS3AvatarsStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/s3-avatars/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setS3AvatarsConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            region: data.config.region ?? "",
            accessKeyId: data.config.accessKeyId ?? "",
            secretAccessKey: "",
            sessionToken: "",
            bucket: data.config.bucket ?? "",
            prefix: data.config.prefix ?? ""
          }
        }));
      }
    } catch (e) {
      setS3AvatarsStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadRestApiConfig(instanceId: string) {
    setRestApiStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/rest-api/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setRestApiConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            openapiUrl: data.config.openapiUrl ?? "",
            authType: data.config.authType ?? "none",
            bearerToken: data.config.bearerToken ?? "",
            username: data.config.username ?? "",
            password: data.config.password ?? ""
          }
        }));
      }
    } catch (e) {
      setRestApiStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function saveRestApiConfig() {
    if (!drawerInstanceId) {
      return;
    }
    setRestApiStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = restApiConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }

      const res = await fetch("/api/datasources/rest-api/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setRestApiStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setRestApiStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function testRestApiConnection(instanceId: string) {
    setRestApiStatusById((prev) => ({ ...prev, [instanceId]: "Testing connection..." }));
    try {
      const cfg = restApiConfigsById[instanceId];
      const res = await fetch("/api/datasources/rest-api/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setRestApiStatusById((prev) => ({
        ...prev,
        [instanceId]: `OpenAPI OK. Found ${Number(data?.paths ?? 0)} paths (${Number(data?.getOperations ?? 0)} GET).`
      }));
    } catch (e) {
      setRestApiStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectRestApiOpenApi(instanceId: string) {
    setRestApiCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting OpenAPI..." }));
    try {
      const cfg = restApiConfigsById[instanceId];
      const res = await fetch("/api/datasources/rest-api/inspect-openapi", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const candidates = Array.isArray(data?.candidates) ? (data.candidates as any[]) : [];
      const mapped: MappingCandidate[] = candidates.map((c: any) => ({
        table: String(c?.category ?? "unknown"),
        column: String(c?.path ?? ""),
        customerPath: "identityNumber",
        confidence: Math.max(0, Math.min(1, Number(c?.score ?? 0) / 100)),
        reason: String((c?.reasons ?? []).slice(0, 2).join(" · "))
      }));

      setRestApiCandidatesById((prev) => ({ ...prev, [instanceId]: mapped }));
      setRestApiDeepInspectResultById((prev) => ({ ...prev, [instanceId]: null }));
      const total = mapped.length;
      setRestApiCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]:
          total === 0
            ? "Analysis completed. No suitable product endpoints were detected in this OpenAPI document."
            : `Analysis completed. Found ${total} candidate product endpoints (grouped by category and path).`
      }));
    } catch (e) {
      setRestApiCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function testS3Connection(instanceId: string) {
    setS3StatusById((prev) => ({ ...prev, [instanceId]: "Testing connection..." }));
    try {
      const cfg = s3ConfigsById[instanceId];
      const res = await fetch("/api/datasources/s3/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      setS3StatusById((prev) => ({
        ...prev,
        [instanceId]: `Connection OK. Listed ${Array.isArray(data?.keys) ? data.keys.length : 0} keys from s3://${data?.bucket ?? ""}/${data?.prefix ?? ""}`
      }));
    } catch (e) {
      setS3StatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function saveS3AvatarsConfig() {
    if (!drawerInstanceId) {
      return;
    }
    setS3AvatarsStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = s3AvatarsConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/s3-avatars/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setS3AvatarsStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setS3AvatarsStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function testS3AvatarsConnection(instanceId: string) {
    setS3AvatarsStatusById((prev) => ({ ...prev, [instanceId]: "Testing connection..." }));
    try {
      const cfg = s3AvatarsConfigsById[instanceId];
      const res = await fetch("/api/datasources/s3-avatars/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      setS3AvatarsStatusById((prev) => ({
        ...prev,
        [instanceId]: `Connection OK. Listed ${Array.isArray(data?.keys) ? data.keys.length : 0} keys from s3://${data?.bucket ?? ""}/${data?.prefix ?? ""}`
      }));
    } catch (e) {
      setS3AvatarsStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectS3AvatarFolders(instanceId: string) {
    setS3AvatarsFoldersStatusById((prev) => ({ ...prev, [instanceId]: "Scanning folders for images..." }));
    try {
      const cfg = s3AvatarsConfigsById[instanceId];
      const res = await fetch("/api/datasources/s3-avatars/inspect-avatar-folders", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, maxKeys: 2000, maxCandidates: 30 })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const candidates = (data?.candidates ?? []) as S3AvatarFolderCandidate[];
      setS3AvatarsFoldersById((prev) => ({ ...prev, [instanceId]: candidates }));

      if (candidates.length > 0) {
        setS3AvatarsSelectedFolderPrefixById((prev) =>
          prev[instanceId]
            ? prev
            : {
                ...prev,
                [instanceId]: String(candidates[0]?.prefix ?? "")
              }
        );
      }
      setS3AvatarsFoldersStatusById((prev) => ({
        ...prev,
        [instanceId]: `Scanned ${Number(data?.scannedKeys ?? 0)} keys. Found ${Number(data?.imageKeys ?? 0)} images.`
      }));
    } catch (e) {
      setS3AvatarsFoldersStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  function goUpOnePrefix(prefix: string): string {
    const p = String(prefix ?? "").trim();
    const normalized = p.endsWith("/") ? p.slice(0, -1) : p;
    if (!normalized) return "";
    const idx = normalized.lastIndexOf("/");
    if (idx < 0) return "";
    return normalized.slice(0, idx + 1);
  }

  async function listS3ParquetFiles(instanceId: string) {
    setS3FilesStatusById((prev) => ({ ...prev, [instanceId]: "Listing .parquet files..." }));
    try {
      const cfg = s3ConfigsById[instanceId];
      const res = await fetch("/api/datasources/s3/list-files", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const prevSelection = new Set(
        (s3FilesById[instanceId] ?? []).filter((f) => f.selected).map((f) => f.key)
      );

      const dirs = Array.isArray(data?.dirs) ? (data.dirs as unknown[]).map((d) => String(d)) : [];
      setS3DirsById((prev) => ({
        ...prev,
        [instanceId]: dirs.map((p) => ({ prefix: p }))
      }));

      const files = Array.isArray(data?.files) ? (data.files as unknown[]).map((f) => String(f)) : [];
      setS3FilesById((prev) => ({
        ...prev,
        [instanceId]: files.map((k) => ({ key: k, selected: prevSelection.has(k) }))
      }));

      setS3FilesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setS3FilesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectS3ParquetSchema(instanceId: string) {
    setS3SchemaStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting Parquet schema..." }));
    setS3CandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const cfg = s3ConfigsById[instanceId];
      const selectedFiles = (s3FilesById[instanceId] ?? [])
        .filter((f) => f.selected)
        .map((f) => f.key);
      const res = await fetch("/api/datasources/s3/inspect-schema", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, files: selectedFiles })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setS3SchemaById((prev) => ({
        ...prev,
        [instanceId]: (data?.tables ?? []) as PgSchemaTable[]
      }));
      const candidates = (data?.candidates ?? []) as MappingCandidate[];
      setS3CandidatesById((prev) => ({ ...prev, [instanceId]: candidates }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: candidates.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setS3SchemaStatusById((prev) => ({ ...prev, [instanceId]: null }));
      setS3CandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setS3SchemaStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  useEffect(() => {
    let cancelled = false;

    (async () => {
      setCustomerFieldsStatus(null);
      try {
        const res = await fetch("/api/domain/customer/fields");
        const data = (await res.json().catch(() => null)) as CustomerFieldsResponse | null;
        if (!res.ok) {
          throw new Error((data && data.error) || `HTTP ${res.status}`);
        }

        if (!data || !data.ok || !Array.isArray(data.fields)) {
          throw new Error("Invalid response from /api/domain/customer/fields");
        }

        if (!cancelled) {
          const fields = data.fields.map((f) => String(f)).filter(Boolean);
          const primaryKeys = (data.primaryKeys ?? []).map((k) => String(k)).filter(Boolean);
          setCustomerFieldPaths(fields.length > 0 ? fields : DEFAULT_CUSTOMER_FIELD_PATHS);
          setCustomerPrimaryKeys(
            new Set((primaryKeys.length > 0 ? primaryKeys : DEFAULT_CUSTOMER_PRIMARY_KEYS).filter(Boolean))
          );
        }
      } catch (e) {
        if (!cancelled) {
          setCustomerFieldPaths(DEFAULT_CUSTOMER_FIELD_PATHS);
          setCustomerPrimaryKeys(new Set(DEFAULT_CUSTOMER_PRIMARY_KEYS));
          setCustomerFieldsStatus(e instanceof Error ? e.message : String(e));
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  async function loadPostgresConfig(instanceId: string) {
    setPgStatus(null);
    try {
      const res = await fetch(`/api/datasources/postgres/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setPgConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            host: data.config.host ?? "localhost",
            port: Number(data.config.port ?? 5432),
            database: data.config.database ?? "",
            user: data.config.user ?? "",
            password: "",
            ssl: Boolean(data.config.ssl ?? false)
          }
        }));
      }
    } catch (e) {
      setPgStatus(e instanceof Error ? e.message : String(e));
    }
  }

  function goUpOneFolder(path: string): string {
    const p = String(path ?? "").trim();
    if (!p || p === "/" || p === ".") {
      return "/";
    }
    const normalized = p.endsWith("/") ? p.slice(0, -1) : p;
    const idx = normalized.lastIndexOf("/");
    if (idx <= 0) return "/";
    return normalized.slice(0, idx);
  }

  async function saveSftpConfig() {
    if (!drawerInstanceId) {
      return;
    }
    setSftpStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = sftpConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/sftp/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setSftpStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Saved" }));
    } catch (e) {
      setSftpStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function loadSftpConfig(instanceId: string) {
    setSftpStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const res = await fetch(`/api/datasources/sftp/config?id=${encodeURIComponent(instanceId)}`);
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      if (data?.config) {
        setSftpConfigsById((prev) => ({
          ...prev,
          [instanceId]: {
            host: data.config.host ?? "",
            port: Number(data.config.port ?? 22),
            username: data.config.username ?? "",
            password: "",
            privateKey: "",
            passphrase: "",
            basePath: data.config.basePath ?? "/"
          }
        }));
      }
    } catch (e) {
      setSftpStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function listSftpCsvFiles(instanceId: string) {
    setSftpFilesStatusById((prev) => ({ ...prev, [instanceId]: "Listing .csv files..." }));
    try {
      const cfg = sftpConfigsById[instanceId];
      const res = await fetch("/api/datasources/sftp/list-files", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const prevSelection = new Set(
        (sftpFilesById[instanceId] ?? []).filter((f) => f.selected).map((f) => f.path)
      );

      const dirs = Array.isArray(data?.dirs) ? (data.dirs as unknown[]).map((d) => String(d)) : [];
      setSftpDirsById((prev) => ({
        ...prev,
        [instanceId]: dirs.map((p) => ({ path: p }))
      }));

      const files = Array.isArray(data?.files) ? (data.files as unknown[]).map((f) => String(f)) : [];
      setSftpFilesById((prev) => ({
        ...prev,
        [instanceId]: files.map((p) => ({
          path: p,
          selected: prevSelection.has(p)
        }))
      }));
      setSftpFilesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setSftpFilesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectSftpCsvSchema(instanceId: string) {
    setSftpSchemaStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting CSV schema..." }));
    setSftpCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    try {
      const cfg = sftpConfigsById[instanceId];
      const selectedFiles = (sftpFilesById[instanceId] ?? []).filter((f) => f.selected).map((f) => f.path);
      const res = await fetch("/api/datasources/sftp/inspect-schema", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, files: selectedFiles })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setSftpSchemaById((prev) => ({
        ...prev,
        [instanceId]: (data?.tables ?? []) as PgSchemaTable[]
      }));
      const candidates = (data?.candidates ?? []) as MappingCandidate[];
      setSftpCandidatesById((prev) => ({ ...prev, [instanceId]: candidates }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: candidates.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setSftpSchemaStatusById((prev) => ({ ...prev, [instanceId]: null }));
      setSftpCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setSftpSchemaStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function inspectPostgresSchema(instanceId: string) {
    setPgSchemaStatusById((prev) => ({ ...prev, [instanceId]: "Inspecting schema..." }));
    try {
      const cfg = pgConfigsById[instanceId];
      const res = await fetch("/api/datasources/postgres/schema", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setPgSchemaById((prev) => ({
        ...prev,
        [instanceId]: (data?.tables ?? []) as PgSchemaTable[]
      }));
      setPgSchemaStatusById((prev) => ({ ...prev, [instanceId]: null }));

      void suggestMappingsForInstance(instanceId);
    } catch (e) {
      setPgSchemaStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function suggestMappingsForInstance(instanceId: string) {
    setPgCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Analyzing..." }));
    try {
      const cfg = pgConfigsById[instanceId];
      const res = await fetch("/api/datasources/postgres/suggest-mappings", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const candidates = (data?.candidates ?? []) as MappingCandidate[];
      setPgCandidatesById((prev) => ({ ...prev, [instanceId]: candidates }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: candidates.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setPgCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setPgCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function deepInspectPostgresInstance(instanceId: string) {
    setPgCandidatesStatusById((prev) => ({ ...prev, [instanceId]: "Deep inspecting..." }));
    try {
      const cfg = pgConfigsById[instanceId];
      const tables = pgSchemaById[instanceId] ?? [];
      if (!tables || tables.length === 0) {
        throw new Error("Run schema inspect first");
      }
      const candidates = pgCandidatesById[instanceId] ?? [];

      const res = await fetch("/api/datasources/postgres/deep-inspect", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg, tables, candidates })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      const next = (data?.candidates ?? []) as MappingCandidate[];
      setPgCandidatesById((prev) => ({ ...prev, [instanceId]: next }));
      setIdentityNumberAlertById((prev) => ({
        ...prev,
        [instanceId]: next.some((c) => c.customerPath === "identityNumber")
          ? null
          : "No candidate found for identityNumber"
      }));
      setPgCandidatesStatusById((prev) => ({ ...prev, [instanceId]: null }));
    } catch (e) {
      setPgCandidatesStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  async function savePostgresConfig() {
    setPgStatus(null);
    try {
      if (!drawerInstanceId) {
        throw new Error("Select a datasource card first");
      }

      const cfg = pgConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/postgres/config", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }

      setPgStatus("Saved");
    } catch (e) {
      setPgStatus(e instanceof Error ? e.message : String(e));
    }
  }

  function onDragStartPlugin(e: DragEvent<HTMLButtonElement>, pluginName: string) {
    e.dataTransfer.setData("text/plugin", pluginName);
    e.dataTransfer.effectAllowed = "copy";
  }

  function onDragOverCanvas(e: DragEvent) {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }

  function onDropCanvas(e: DragEvent) {
    e.preventDefault();
    const pluginName = e.dataTransfer.getData("text/plugin");
    if (!pluginName) {
      return;
    }

    const rect = canvasRef.current?.getBoundingClientRect();
    const rawX = rect ? e.clientX - rect.left : 0;
    const rawY = rect ? e.clientY - rect.top : 0;
    const pos = snapToGrid({ x: rawX, y: rawY });

    const plugin = plugins.find((p) => p.name === pluginName);
    const id =
      typeof crypto !== "undefined" && "randomUUID" in crypto
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(16).slice(2)}`;

    const next: DatasourceInstance = {
      id,
      pluginName,
      title: plugin?.title ?? pluginName,
      x: pos.x,
      y: pos.y
    };

    setInstances((prev) => [...prev, next]);
    setSelectedInstanceId(id);

    if (pluginName === "postgres") {
      setPgConfigsById((prev) => ({
        ...prev,
        [id]: {
          host: "localhost",
          port: 5432,
          database: "",
          user: "",
          password: "",
          ssl: false
        }
      }));
      setPgStatus(null);
    }

    if (pluginName === "mssql") {
      setMssqlConfigsById((prev) => ({
        ...prev,
        [id]: {
          host: "localhost",
          port: 1433,
          database: "",
          user: "",
          password: "",
          encrypt: false,
          trustServerCertificate: false
        }
      }));
      setMssqlStatusById((prev) => ({ ...prev, [id]: null }));
    }

    if (pluginName === "mongodb") {
      setMongoConfigsById((prev) => ({
        ...prev,
        [id]: {
          uri: "",
          database: "",
          collection: "",
          sampleSize: 50
        }
      }));
      setMongoStatusById((prev) => ({ ...prev, [id]: null }));
    }

    if (pluginName === "sftp") {
      setSftpConfigsById((prev) => ({
        ...prev,
        [id]: {
          host: "",
          port: 22,
          username: "",
          password: "",
          privateKey: "",
          passphrase: "",
          basePath: "/"
        }
      }));
      setSftpStatusById((prev) => ({ ...prev, [id]: null }));
    }

    if (pluginName === "s3") {
      setS3ConfigsById((prev) => ({
        ...prev,
        [id]: {
          region: "",
          accessKeyId: "",
          secretAccessKey: "",
          sessionToken: "",
          bucket: "",
          prefix: ""
        }
      }));
      setS3StatusById((prev) => ({ ...prev, [id]: null }));
    }

    if (pluginName === "s3-avatars") {
      setS3AvatarsConfigsById((prev) => ({
        ...prev,
        [id]: {
          region: "",
          accessKeyId: "",
          secretAccessKey: "",
          sessionToken: "",
          bucket: "",
          prefix: ""
        }
      }));
      setS3AvatarsStatusById((prev) => ({ ...prev, [id]: null }));
      setS3AvatarsFoldersStatusById((prev) => ({ ...prev, [id]: null }));
    }

    if (pluginName === "rest-api") {
      setRestApiConfigsById((prev) => ({
        ...prev,
        [id]: {
          openapiUrl: "",
          authType: "none",
          bearerToken: "",
          username: "",
          password: ""
        }
      }));
      setRestApiStatusById((prev) => ({ ...prev, [id]: null }));
      setRestApiCandidatesStatusById((prev) => ({ ...prev, [id]: null }));
    }
  }

  const selectedInstance = selectedInstanceId
    ? instances.find((i) => i.id === selectedInstanceId) ?? null
    : null;

  const drawerInstance = drawerInstanceId
    ? instances.find((i) => i.id === drawerInstanceId) ?? null
    : null;

  const selectedPgConfig =
    drawerInstanceId && drawerInstance?.pluginName === "postgres"
      ? pgConfigsById[drawerInstanceId] ?? {
          host: "localhost",
          port: 5432,
          database: "",
          user: "",
          password: "",
          ssl: false
        }
      : null;

  const selectedMssqlConfig =
    drawerInstanceId && drawerInstance?.pluginName === "mssql"
      ? mssqlConfigsById[drawerInstanceId] ?? {
          host: "localhost",
          port: 1433,
          database: "",
          user: "",
          password: "",
          encrypt: false,
          trustServerCertificate: false
        }
      : null;

  const selectedMongoConfig =
    drawerInstanceId && drawerInstance?.pluginName === "mongodb"
      ? mongoConfigsById[drawerInstanceId] ?? {
          uri: "",
          database: "",
          collection: "",
          sampleSize: 50
        }
      : null;

  const selectedS3Config =
    drawerInstanceId && drawerInstance?.pluginName === "s3"
      ? s3ConfigsById[drawerInstanceId] ?? {
          region: "",
          accessKeyId: "",
          secretAccessKey: "",
          sessionToken: "",
          bucket: "",
          prefix: ""
        }
      : null;

  const selectedS3AvatarsConfig =
    drawerInstanceId && drawerInstance?.pluginName === "s3-avatars"
      ? s3AvatarsConfigsById[drawerInstanceId] ?? {
          region: "",
          accessKeyId: "",
          secretAccessKey: "",
          sessionToken: "",
          bucket: "",
          prefix: ""
        }
      : null;

  const selectedRestApiConfig =
    drawerInstanceId && drawerInstance?.pluginName === "rest-api"
      ? restApiConfigsById[drawerInstanceId] ?? {
          openapiUrl: "",
          authType: "none",
          bearerToken: "",
          username: "",
          password: ""
        }
      : null;

  useEffect(() => {
    if (!pointerActive) {
      return;
    }

    const onMove = (e: PointerEvent) => {
      const state = pointerStateRef.current;
      if (!state) {
        return;
      }

      const rect = canvasRef.current?.getBoundingClientRect();
      if (!rect) {
        return;
      }

      const dx = e.clientX - state.startClientX;
      const dy = e.clientY - state.startClientY;
      const dist = Math.hypot(dx, dy);

      if (!state.dragging && dist > 6) {
        state.dragging = true;
        setSelectedInstanceId(state.id);
        setDrawerInstanceId(null);
      }

      if (!state.dragging) {
        return;
      }

      const rawX = e.clientX - rect.left - state.offsetX;
      const rawY = e.clientY - rect.top - state.offsetY;
      const pos = snapToGrid({ x: rawX, y: rawY });

      setInstances((prev) =>
        prev.map((inst) =>
          inst.id === state.id ? { ...inst, x: pos.x, y: pos.y } : inst
        )
      );
    };

    const onUp = () => {
      const state = pointerStateRef.current;
      pointerStateRef.current = null;
      setPointerActive(false);

      if (state && !state.dragging) {
        setSelectedInstanceId(state.id);
      }
    };

    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp, { once: true });

    return () => {
      window.removeEventListener("pointermove", onMove);
    };
  }, [pointerActive]);

  function onPointerDownCard(e: ReactPointerEvent<HTMLElement>, inst: DatasourceInstance) {
    e.preventDefault();

    const canvas = canvasRef.current;
    if (!canvas) return;
    const cardRect = e.currentTarget.getBoundingClientRect();

    pointerStateRef.current = {
      id: inst.id,
      pluginName: inst.pluginName,
      startClientX: e.clientX,
      startClientY: e.clientY,
      offsetX: e.clientX - cardRect.left,
      offsetY: e.clientY - cardRect.top,
      dragging: false
    };
    setSelectedInstanceId(inst.id);
    setPointerActive(true);
  }

  function openDrawerForInstance(inst: DatasourceInstance) {
    setSelectedInstanceId(inst.id);
    setDrawerInstanceId(inst.id);
    if (inst.pluginName === "postgres") {
      void loadPostgresConfig(inst.id);
    }
    if (inst.pluginName === "mssql") {
      void loadMssqlConfig(inst.id);
    }
    if (inst.pluginName === "mongodb") {
      void loadMongoConfig(inst.id);
    }
    if (inst.pluginName === "sftp") {
      void loadSftpConfig(inst.id);
    }
    if (inst.pluginName === "s3") {
      void loadS3Config(inst.id);
    }
    if (inst.pluginName === "s3-avatars") {
      void loadS3AvatarsConfig(inst.id);
    }
    if (inst.pluginName === "rest-api") {
      void loadRestApiConfig(inst.id);
    }
  }

  function removeInstance(instanceId: string) {
    setInstances((prev) => prev.filter((i) => i.id !== instanceId));

    setSelectedInstanceId((prev) => (prev === instanceId ? null : prev));
    setDrawerInstanceId((prev) => (prev === instanceId ? null : prev));

    setPgConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMssqlConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3ConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setS3AvatarsConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setRestApiConfigsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setMssqlStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3StatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setS3AvatarsStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setRestApiStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setPgCandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setPgSchemaStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoCandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoSchemaStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMssqlCandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMssqlSchemaStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpCandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpSchemaStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpFilesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3CandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3SchemaStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setRestApiCandidatesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setS3AvatarsFoldersStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3FilesStatusById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setPgCandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setPgSchemaById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoCandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMongoSchemaById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMssqlCandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setMssqlSchemaById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpCandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpSchemaById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpDirsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setSftpFilesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3CandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3SchemaById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3DirsById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
    setS3FilesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setS3AvatarsFoldersById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setS3AvatarsSelectedFolderPrefixById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });

    setRestApiCandidatesById((prev) => {
      const { [instanceId]: _, ...rest } = prev;
      return rest;
    });
  }

  function onInspectSchemaFromCard(e: React.MouseEvent, inst: DatasourceInstance) {
    e.preventDefault();
    e.stopPropagation();

    setSelectedInstanceId(inst.id);
    setDrawerInstanceId(inst.id);

    if (inst.pluginName === "postgres") {
      void loadPostgresConfig(inst.id);
      void inspectPostgresSchema(inst.id);
    }

    if (inst.pluginName === "mssql") {
      void loadMssqlConfig(inst.id);
      void inspectMssqlSchema(inst.id);
    }

    if (inst.pluginName === "mongodb") {
      void loadMongoConfig(inst.id);
      void inspectMongoSchema(inst.id);
    }

    if (inst.pluginName === "sftp") {
      void loadSftpConfig(inst.id);
      void inspectSftpCsvSchema(inst.id);
    }

    if (inst.pluginName === "s3") {
      void loadS3Config(inst.id);
      void inspectS3ParquetSchema(inst.id);
    }
  }

  async function testSftpConnection(instanceId: string) {
    setSftpStatusById((prev) => ({ ...prev, [instanceId]: "Testing connection..." }));
    try {
      const cfg = sftpConfigsById[instanceId];
      const res = await fetch("/api/datasources/sftp/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: instanceId, config: cfg })
      });
      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      setSftpStatusById((prev) => ({
        ...prev,
        [instanceId]: `Connection OK. Listed ${Array.isArray(data?.entries) ? data.entries.length : 0} entries from ${data?.path ?? ""}`
      }));
    } catch (e) {
      setSftpStatusById((prev) => ({
        ...prev,
        [instanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  function buildCandidateIndex(instanceId: string): Record<string, MappingCandidate[]> {
    const list = pgCandidatesById[instanceId] ?? [];
    const idx: Record<string, MappingCandidate[]> = {};
    for (const c of list) {
      const key = `${c.table}:${c.column}`;
      (idx[key] ??= []).push(c);
    }
    return idx;
  }

  function buildMssqlCandidateIndex(instanceId: string): Record<string, MappingCandidate[]> {
    const list = mssqlCandidatesById[instanceId] ?? [];
    const idx: Record<string, MappingCandidate[]> = {};
    for (const c of list) {
      const key = `${c.table}:${c.column}`;
      (idx[key] ??= []).push(c);
    }
    return idx;
  }

  function buildMongoCandidateIndex(instanceId: string): Record<string, MappingCandidate[]> {
    const list = mongoCandidatesById[instanceId] ?? [];
    const idx: Record<string, MappingCandidate[]> = {};
    for (const c of list) {
      const key = `${c.table}:${c.column}`;
      (idx[key] ??= []).push(c);
    }
    return idx;
  }

  function buildSftpCandidateIndex(instanceId: string): Record<string, MappingCandidate[]> {
    const list = sftpCandidatesById[instanceId] ?? [];
    const idx: Record<string, MappingCandidate[]> = {};
    for (const c of list) {
      const key = `${c.table}:${c.column}`;
      (idx[key] ??= []).push(c);
    }
    return idx;
  }

  function buildS3CandidateIndex(instanceId: string): Record<string, MappingCandidate[]> {
    const list = s3CandidatesById[instanceId] ?? [];
    const idx: Record<string, MappingCandidate[]> = {};
    for (const c of list) {
      const key = `${c.table}:${c.column}`;
      (idx[key] ??= []).push(c);
    }
    return idx;
  }

  function isPostgresConfigured(instanceId: string): boolean {
    const cfg = pgConfigsById[instanceId];
    if (!cfg) return false;
    return Boolean(String(cfg.host ?? "").trim() && String(cfg.database ?? "").trim() && String(cfg.user ?? "").trim());
  }

  function isMssqlConfigured(instanceId: string): boolean {
    const cfg = mssqlConfigsById[instanceId];
    if (!cfg) return false;
    return Boolean(String(cfg.host ?? "").trim() && String(cfg.database ?? "").trim() && String(cfg.user ?? "").trim());
  }

  function isMongoConfigured(instanceId: string): boolean {
    const cfg = mongoConfigsById[instanceId];
    if (!cfg) return false;
    return Boolean(
      String(cfg.uri ?? "").trim() && String(cfg.database ?? "").trim() && String(cfg.collection ?? "").trim()
    );
  }

  function isSftpConfigured(instanceId: string): boolean {
    const cfg = sftpConfigsById[instanceId];
    if (!cfg) return false;
    const hasAuth = Boolean(String(cfg.password ?? "").trim() || String(cfg.privateKey ?? "").trim());
    return Boolean(String(cfg.host ?? "").trim() && String(cfg.username ?? "").trim() && hasAuth);
  }

  function isS3Configured(instanceId: string): boolean {
    const cfg = s3ConfigsById[instanceId];
    if (!cfg) return false;
    return Boolean(
      String(cfg.region ?? "").trim() &&
        String(cfg.accessKeyId ?? "").trim() &&
        String(cfg.secretAccessKey ?? "").trim() &&
        String(cfg.bucket ?? "").trim()
    );
  }

  function isS3AvatarsConfigured(instanceId: string): boolean {
    const cfg = s3AvatarsConfigsById[instanceId];
    if (!cfg) return false;
    return Boolean(
      String(cfg.region ?? "").trim() &&
        String(cfg.accessKeyId ?? "").trim() &&
        String(cfg.secretAccessKey ?? "").trim() &&
        String(cfg.bucket ?? "").trim()
    );
  }

  function getDatasourceUiStatus(inst: DatasourceInstance): DatasourceUiStatus {
    const id = inst.id;

    const schemaAnalyzed =
      (inst.pluginName === "postgres" && (pgSchemaById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "mssql" && (mssqlSchemaById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "mongodb" && (mongoSchemaById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "sftp" && (sftpSchemaById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "s3" && (s3SchemaById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "rest-api" && (restApiCandidatesById[id]?.length ?? 0) > 0) ||
      (inst.pluginName === "s3-avatars" &&
        ((s3AvatarsFoldersById[id]?.length ?? 0) > 0 ||
          (s3AvatarsFoldersStatusById[id] != null &&
            (s3AvatarsFoldersStatusById[id] ?? "").startsWith("Scanned "))));

    if (schemaAnalyzed) return "schema_analyzed";

    const isError =
      (inst.pluginName === "postgres" &&
        ((pgSchemaStatusById[id] != null && pgSchemaStatusById[id] !== "Inspecting schema...") ||
          (pgCandidatesStatusById[id] != null && pgCandidatesStatusById[id] !== "Analyzing...") ||
          (pgStatus != null && pgStatus !== "Saved" && pgStatus !== "Connection OK"))) ||
      (inst.pluginName === "mssql" &&
        ((mssqlSchemaStatusById[id] != null && mssqlSchemaStatusById[id] !== "Inspecting schema...") ||
          (mssqlCandidatesStatusById[id] != null && mssqlCandidatesStatusById[id] !== "Analyzing...") ||
          (mssqlStatusById[id] != null && mssqlStatusById[id] !== "Saved" && mssqlStatusById[id] !== "Connection OK"))) ||
      (inst.pluginName === "mongodb" &&
        ((mongoSchemaStatusById[id] != null && mongoSchemaStatusById[id] !== "Inspecting schema...") ||
          (mongoCandidatesStatusById[id] != null && mongoCandidatesStatusById[id] !== "Analyzing...") ||
          (mongoStatusById[id] != null && mongoStatusById[id] !== "Saved" && mongoStatusById[id] !== "Connection OK"))) ||
      (inst.pluginName === "sftp" &&
        ((sftpStatusById[id] != null &&
          sftpStatusById[id] !== "Saved" &&
          sftpStatusById[id] !== "Testing connection..." &&
          !sftpStatusById[id].startsWith("Connection OK.")) ||
          (sftpFilesStatusById[id] != null && sftpFilesStatusById[id] !== "Listing .csv files...") ||
          (sftpSchemaStatusById[id] != null && sftpSchemaStatusById[id] !== "Inspecting CSV schema..."))) ||
      (inst.pluginName === "s3" &&
        ((s3StatusById[id] != null &&
          s3StatusById[id] !== "Saved" &&
          s3StatusById[id] !== "Testing connection..." &&
          !s3StatusById[id].startsWith("Connection OK.")) ||
          (s3FilesStatusById[id] != null && s3FilesStatusById[id] !== "Listing .parquet files...") ||
          (s3SchemaStatusById[id] != null && s3SchemaStatusById[id] !== "Inspecting Parquet schema..."))) ||
      (inst.pluginName === "s3-avatars" &&
        ((s3AvatarsStatusById[id] != null &&
          s3AvatarsStatusById[id] !== "Saved" &&
          s3AvatarsStatusById[id] !== "Testing connection..." &&
          !s3AvatarsStatusById[id].startsWith("Connection OK.")) ||
          (s3AvatarsFoldersStatusById[id] != null &&
            s3AvatarsFoldersStatusById[id] !== "Scanning folders for images..." &&
            !(s3AvatarsFoldersStatusById[id] ?? "").startsWith("Scanned "))));

    const restApiStatus = restApiStatusById[id];
    const restApiCandidatesStatus = restApiCandidatesStatusById[id];
    const restApiError =
      inst.pluginName === "rest-api" &&
      ((restApiStatus != null &&
        restApiStatus !== "Saved" &&
        restApiStatus !== "Testing connection..." &&
        !restApiStatus.startsWith("OpenAPI OK.")) ||
        (restApiCandidatesStatus != null && restApiCandidatesStatus !== "Inspecting OpenAPI..."));

    if (isError || restApiError) return "error";

    const configured =
      (inst.pluginName === "postgres" && isPostgresConfigured(id)) ||
      (inst.pluginName === "mssql" && isMssqlConfigured(id)) ||
      (inst.pluginName === "mongodb" && isMongoConfigured(id)) ||
      (inst.pluginName === "sftp" && isSftpConfigured(id)) ||
      (inst.pluginName === "s3" && isS3Configured(id)) ||
      (inst.pluginName === "rest-api" && Boolean(String(restApiConfigsById[id]?.openapiUrl ?? "").trim())) ||
      (inst.pluginName === "s3-avatars" && isS3AvatarsConfigured(id));

    if (configured) return "configured";
    return "not_configured";
  }

  function getActiveDatasourceForCustomerPanel(): DatasourceInstance | null {
    if (drawerInstanceId) {
      return instances.find((i) => i.id === drawerInstanceId) ?? null;
    }
    if (selectedInstanceId) {
      return instances.find((i) => i.id === selectedInstanceId) ?? null;
    }
    return null;
  }

  function getCandidatesForInstance(inst: DatasourceInstance | null): MappingCandidate[] {
    if (!inst) return [];
    if (inst.pluginName === "postgres") return pgCandidatesById[inst.id] ?? [];
    if (inst.pluginName === "mssql") return mssqlCandidatesById[inst.id] ?? [];
    if (inst.pluginName === "mongodb") return mongoCandidatesById[inst.id] ?? [];
    if (inst.pluginName === "sftp") return sftpCandidatesById[inst.id] ?? [];
    if (inst.pluginName === "s3") return s3CandidatesById[inst.id] ?? [];
    return [];
  }

  function getBestCandidateByCustomerPath(candidates: MappingCandidate[]): Record<string, MappingCandidate> {
    const best: Record<string, MappingCandidate> = {};
    for (const c of candidates) {
      const prev = best[c.customerPath];
      if (!prev || c.confidence > prev.confidence) {
        best[c.customerPath] = c;
      }
    }
    return best;
  }

  function getMergedBestCandidates(): {
    best: Record<string, MappingCandidate>;
    sourceByCustomerPath: Record<string, string>;
  } {
    const best: Record<string, MappingCandidate> = {};
    const sourceByCustomerPath: Record<string, string> = {};

    for (const inst of instances) {
      const candidates = getCandidatesForInstance(inst);
      for (const c of candidates) {
        const prev = best[c.customerPath];
        if (!prev || c.confidence > prev.confidence) {
          best[c.customerPath] = c;
          sourceByCustomerPath[c.customerPath] = inst.title;
        }
      }
    }

    return { best, sourceByCustomerPath };
  }

  function snapToGrid(pos: { x: number; y: number }) {
    const step = 24;
    const x = Math.max(0, Math.round(pos.x / step) * step);
    const y = Math.max(0, Math.round(pos.y / step) * step);
    return { x, y };
  }

  async function testPostgresConnection() {
    setPgStatus(null);
    try {
      if (!drawerInstanceId) {
        throw new Error("Select a datasource card first");
      }

      const cfg = pgConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/postgres/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setPgStatus("Connection OK");
    } catch (e) {
      setPgStatus(e instanceof Error ? e.message : String(e));
    }
  }

  async function testMssqlConnection() {
    if (!drawerInstanceId) {
      return;
    }
    setMssqlStatusById((prev) => ({ ...prev, [drawerInstanceId]: null }));
    try {
      const cfg = mssqlConfigsById[drawerInstanceId];
      if (!cfg) {
        throw new Error("No configuration found for selected datasource");
      }
      const res = await fetch("/api/datasources/mssql/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ id: drawerInstanceId, config: cfg })
      });

      const data = (await res.json().catch(() => null)) as any;
      if (!res.ok) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }

      setMssqlStatusById((prev) => ({ ...prev, [drawerInstanceId]: "Connection OK" }));
    } catch (e) {
      setMssqlStatusById((prev) => ({
        ...prev,
        [drawerInstanceId]: e instanceof Error ? e.message : String(e)
      }));
    }
  }

  const customerPanelInstance = mergeMode ? null : getActiveDatasourceForCustomerPanel();
  const customerPanelStatus = customerPanelInstance ? getDatasourceUiStatus(customerPanelInstance) : null;
  const customerPanelCandidates = mergeMode ? [] : getCandidatesForInstance(customerPanelInstance);
  const customerPanelBest = mergeMode
    ? getMergedBestCandidates().best
    : getBestCandidateByCustomerPath(customerPanelCandidates);
  const customerPanelSourceByPath = mergeMode ? getMergedBestCandidates().sourceByCustomerPath : {};

  const identityAlertMissingInstances = mergeMode
    ? instances.filter((i) => identityNumberAlertById[i.id])
    : [];

  const identityAlertText = mergeMode
    ? identityAlertMissingInstances.length > 0
      ? `No candidate found for identityNumber in: ${identityAlertMissingInstances.map((i) => i.title).join(", ")}`
      : null
    : customerPanelInstance
      ? identityNumberAlertById[customerPanelInstance.id] ?? null
      : null;

  return (
    <div className="splitRoot" ref={splitRootRef}>
      <div className={splitAnimating ? "splitLeft splitLeftAnimating" : "splitLeft"} style={{ width: splitLeftWidth }}>
        <div className="layout">
          <aside className="sidebar">
            <div className="sidebarHeader">
              <div className="sidebarHeaderTop">
                <img className="sidebarLogo" src="/n5.png" alt="N5" />
                <div>
                  <div className="appTitle">ouija</div>
                  <div className="appSubtitle">Datasource plugins</div>
                </div>
              </div>
            </div>

            <div className="sidebarBody">
              {error && <pre className="error">{error}</pre>}
              {!error && plugins.length === 0 && <p>Loading...</p>}

              {plugins
                .filter((p) => p.kind === "datasource")
                .map((p) => (
                  <button
                    key={p.name}
                    className="pluginRow"
                    draggable
                    onDragStart={(e) => onDragStartPlugin(e, p.name)}
                    type="button"
                  >
                    <div className="pluginRowTitle">{p.title}</div>
                    {p.description && <div className="pluginRowDesc">{p.description}</div>}
                  </button>
                ))}
            </div>

            <div className="sidebarFooter">
              <div className="metaRow">
                <div className="metaLabel">Version</div>
                <div className="metaValue">v{__APP_VERSION__}</div>
              </div>

              <div className="metaRow">
                <div className="metaLabel">Backend</div>
                <div className="metaValue">{health ? (health.ok ? "OK" : "Down") : "..."}</div>
              </div>
            </div>
          </aside>

          <main className="main">
            <div className="mainCard">
              <div
                ref={canvasRef}
                className="canvas"
                onDragOver={onDragOverCanvas}
                onDrop={onDropCanvas}
              >
                <div className="canvasTopBar">
                  <div className="canvasHeader">Datasource canvas</div>
                  <div className="canvasTopBarActions">
                    <input
                      ref={n5oImportInputRef}
                      type="file"
                      accept=".n5o,application/json"
                      style={{ display: "none" }}
                      onChange={async (e) => {
                        try {
                          await onImportN5OFileSelected(e.target.files?.[0] ?? null);
                        } catch (err) {
                          setError(err instanceof Error ? err.message : String(err));
                        } finally {
                          e.target.value = "";
                        }
                      }}
                    />
                    <button className="mergeBtn" type="button" onClick={onExportN5O}>
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path
                          fill="currentColor"
                          d="M12 3a1 1 0 0 1 1 1v8.59l2.3-2.3a1 1 0 1 1 1.4 1.42l-4.01 4.01a1 1 0 0 1-1.38 0l-4.01-4.01a1 1 0 1 1 1.4-1.42L11 12.59V4a1 1 0 0 1 1-1ZM5 19a1 1 0 0 1 1-1h12a1 1 0 1 1 0 2H6a1 1 0 0 1-1-1Z"
                        />
                      </svg>
                      Export
                    </button>
                    <button className="mergeBtn" type="button" onClick={onRequestImportN5O}>
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path
                          fill="currentColor"
                          d="M12 21a1 1 0 0 1-1-1v-9.59l-2.3 2.3a1 1 0 1 1-1.4-1.42l4.01-4.01a1 1 0 0 1 1.38 0l4.01 4.01a1 1 0 1 1-1.4 1.42L13 10.41V20a1 1 0 0 1-1 1ZM5 15a1 1 0 0 1 1 1v2h12v-2a1 1 0 1 1 2 0v3a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1v-3a1 1 0 0 1 1-1Z"
                        />
                      </svg>
                      Import
                    </button>
                    <button className="mergeBtn" type="button" onClick={() => setMergeMode((v) => !v)}>
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path
                          fill="currentColor"
                          d="M7 7a1 1 0 0 1 1-1h8a1 1 0 1 1 0 2H10.41l2.3 2.3a1 1 0 0 1-1.42 1.4l-4.01-4.01A1 1 0 0 1 7 7Zm10 10a1 1 0 0 1-1 1H8a1 1 0 1 1 0-2h5.59l-2.3-2.3a1 1 0 0 1 1.42-1.4l4.01 4.01A1 1 0 0 1 17 17Z"
                        />
                      </svg>
                      {mergeMode ? "Unmerge" : "Merge"}
                    </button>
                  </div>
                </div>
                <div className="canvasHint">Drag a datasource from the left catalog and drop it here.</div>

                {instances.map((inst) => (
                  <div
                    key={inst.id}
                    className={selectedInstanceId === inst.id ? "dsCard active" : "dsCard"}
                    style={{ left: inst.x, top: inst.y }}
                    onPointerDown={(e) => onPointerDownCard(e, inst)}
                  >
                    <div
                      className={
                        getDatasourceUiStatus(inst) === "not_configured"
                          ? "dsStatus grey"
                          : getDatasourceUiStatus(inst) === "configured"
                            ? "dsStatus blue"
                            : getDatasourceUiStatus(inst) === "schema_analyzed"
                              ? "dsStatus green"
                              : "dsStatus red"
                      }
                    />
                    <button
                      className="dsRemoveBtn"
                      type="button"
                      aria-label="Remove datasource"
                      title="Remove"
                      onPointerDown={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                      }}
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        removeInstance(inst.id);
                      }}
                    >
                      <svg viewBox="0 0 24 24" aria-hidden="true">
                        <path
                          fill="currentColor"
                          d="M9 3a1 1 0 0 0-1 1v1H5a1 1 0 1 0 0 2h1v13a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V7h1a1 1 0 1 0 0-2h-3V4a1 1 0 0 0-1-1H9Zm1 2h4v0H10V5Zm-2 4a1 1 0 0 1 1 1v9a1 1 0 1 1-2 0v-9a1 1 0 0 1 1-1Zm8 0a1 1 0 0 1 1 1v9a1 1 0 1 1-2 0v-9a1 1 0 0 1 1-1Zm-4 0a1 1 0 0 1 1 1v9a1 1 0 1 1-2 0v-9a1 1 0 0 1 1-1Z"
                        />
                      </svg>
                    </button>
                    <button
                      className="dsConfigBtn"
                      type="button"
                      aria-label="Configure datasource"
                      title="Configure"
                      onPointerDown={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                      }}
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        openDrawerForInstance(inst);
                      }}
                    >
                      ⚙
                    </button>
                    <div className="dsTitle">{inst.title}</div>
                    <div className="dsMeta">{inst.pluginName}</div>
                    {(inst.pluginName === "postgres" ||
                      inst.pluginName === "mssql" ||
                      inst.pluginName === "mongodb" ||
                      inst.pluginName === "sftp" ||
                      inst.pluginName === "s3") && (
                      <div className="dsActions">
                        <button
                          type="button"
                          onPointerDown={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                          }}
                          onClick={(e) => onInspectSchemaFromCard(e, inst)}
                        >
                          Schema inspect
                        </button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </main>

          <aside className="customerPanel">
            <div className="customerPanelHeader">
              <div className="customerPanelTitle">Customer aggregate</div>
              <div className="customerPanelSubtitle">
                {mergeMode
                  ? `Merged candidates across ${instances.length} datasources`
                  : customerPanelInstance
                    ? `Active datasource: ${customerPanelInstance.title}`
                    : "Select a datasource card"}
              </div>
            </div>

            <div className="customerPanelBody">
              {customerFieldsStatus && <div className="customerFieldsStatus">{customerFieldsStatus}</div>}
              {identityAlertText && <div className="customerFieldsAlert">{identityAlertText}</div>}
              <div className="customerLegend">
                <div className="customerLegendItem">
                  <span className="customerDot grey" />
                  <span className="customerLegendText">No candidate</span>
                </div>
                <div className="customerLegendItem">
                  <span className="customerDot blue" />
                  <span className="customerLegendText">Candidate</span>
                </div>
                <div className="customerLegendItem">
                  <span className="customerDot green" />
                  <span className="customerLegendText">High confidence</span>
                </div>
                <div className="customerLegendItem">
                  <span className="customerDot red" />
                  <span className="customerLegendText">Datasource error</span>
                </div>
              </div>

              <div className="customerFields">
                {(customerFieldPaths ?? []).map((path) => {
                  const best = customerPanelBest[path];
                  const isPk = customerPrimaryKeys.has(path);

                  const selectedS3AvatarFolderConfidence = (() => {
                    if (path !== "profilePicture") return null;

                    const instancesToCheck = mergeMode
                      ? instances.filter((i) => i.pluginName === "s3-avatars")
                      : customerPanelInstance && customerPanelInstance.pluginName === "s3-avatars"
                        ? [customerPanelInstance]
                        : [];

                    let bestConfidence: number | null = null;
                    for (const inst of instancesToCheck) {
                      const selectedPrefix = s3AvatarsSelectedFolderPrefixById[inst.id];
                      if (!selectedPrefix) continue;
                      const candidates = s3AvatarsFoldersById[inst.id] ?? [];
                      const match = candidates.find((c) => String(c.prefix ?? "") === String(selectedPrefix));
                      const conf = match?.confidence;
                      if (typeof conf === "number" && !Number.isNaN(conf)) {
                        if (bestConfidence == null || conf > bestConfidence) bestConfidence = conf;
                      }
                    }

                    return bestConfidence;
                  })();

                  const resolvedConfidence =
                    path === "profilePicture" && resolvedProfilePictureConfidence != null
                      ? resolvedProfilePictureConfidence
                      : null;

                  const effectiveConfidence =
                    resolvedConfidence != null
                      ? resolvedConfidence
                      : selectedS3AvatarFolderConfidence != null
                        ? selectedS3AvatarFolderConfidence
                        : best?.confidence;

                  const cls =
                    customerPanelStatus === "error"
                      ? "customerField red"
                      : effectiveConfidence != null
                        ? effectiveConfidence >= 0.85
                          ? "customerField green"
                          : "customerField blue"
                        : "customerField grey";

                  return (
                    <div key={path} className={cls}>
                      <div className="customerFieldLeft">
                        <div className="customerFieldPath">
                          <span className="customerFieldPathText">{path}</span>
                          {isPk && <span className="customerFieldPk">PK</span>}
                        </div>
                        {best && (
                          <div className="customerFieldMeta">
                            {mergeMode && customerPanelSourceByPath[path]
                              ? `${customerPanelSourceByPath[path]} · ${best.table}.${best.column}`
                              : `${best.table}.${best.column}`}
                          </div>
                        )}
                      </div>
                      <div className="customerFieldRight">
                        {effectiveConfidence != null ? `${Math.round(effectiveConfidence * 100)}%` : ""}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </aside>
        </div>
      </div>

      <div
        className="splitDivider"
        onPointerDown={(e) => {
          setSplitAnimating(false);
          resizingSplitRef.current = true;
        }}
      >
        <button
          className="splitDividerHandle"
          type="button"
          aria-label="Resize panels"
          onPointerDown={(e) => {
            setSplitAnimating(false);
            resizingSplitRef.current = true;
          }}
        />
      </div>

      <div className={rightPanelCollapsed ? "splitRight splitRightCollapsed" : "splitRight"}>
        {!rightPanelCollapsed && (
          <CustomerProfilePanel
            theme="light"
            datasourceContext={{
              pgConfigsById,
              mssqlConfigsById,
              mongoConfigsById: mongoConfigsById,
              sftpConfigsById,
              s3ConfigsById,
              restApiConfigsById: restApiConfigsById as any,
              pgCandidatesById,
              mssqlCandidatesById,
              mongoCandidatesById,
              sftpCandidatesById,
              s3CandidatesById,
              restApiCandidatesById: restApiCandidatesById as any,
              sftpSelectedFileById: Object.fromEntries(
                Object.entries(sftpFilesById).map(([id, files]) => [
                  id,
                  (files ?? []).find((f) => f.selected)?.path ?? null
                ])
              )
            } as any}
            instances={instances
              .filter((i) =>
                i.pluginName === "postgres" ||
                i.pluginName === "mssql" ||
                i.pluginName === "mongodb" ||
                i.pluginName === "sftp" ||
                i.pluginName === "s3"
              )
              .map((i) => ({ id: i.id, pluginName: i.pluginName as any }))}
            mergeMode={mergeMode}
            activeInstance={getActiveDatasourceForCustomerPanel() as any}
            candidatesById={{
              postgres: pgCandidatesById,
              mssql: mssqlCandidatesById,
              mongodb: mongoCandidatesById,
              sftp: sftpCandidatesById,
              s3: s3CandidatesById
            }}
            selectedSftpCsvById={Object.fromEntries(
              Object.entries(sftpFilesById).map(([id, files]) => [
                id,
                (files ?? []).find((f) => f.selected)?.path ?? null
              ])
            )}
            s3Avatars={{
              instances: instances.filter((i) => i.pluginName === "s3-avatars").map((i) => ({ id: i.id })),
              configsById: s3AvatarsConfigsById as any,
              selectedFolderPrefixById: s3AvatarsSelectedFolderPrefixById,
              foldersById: s3AvatarsFoldersById as any
            }}
            restApiIdentifierField={(() => {
              if (!mergeMode) return undefined;
              const restApiInst = instances.find((i) => i.pluginName === "rest-api");
              if (!restApiInst) return undefined;
              return restApiIdentifierFieldById[restApiInst.id];
            })()}
            restApiInstanceId={(() => {
              if (!mergeMode) return null;
              const restApiInst = instances.find((i) => i.pluginName === "rest-api");
              return restApiInst ? restApiInst.id : null;
            })()}
            restApiSelectedPath={(() => {
              if (!mergeMode) return null;
              const restApiInst = instances.find((i) => i.pluginName === "rest-api");
              if (!restApiInst) return null;
              return restApiSelectedPathById[restApiInst.id] ?? null;
            })()}
            onProfilePictureConfidence={setResolvedProfilePictureConfidence}
          />
        )}
      </div>

      <div
        className={drawerInstance ? "drawerOverlay open" : "drawerOverlay"}
        onClick={() => setDrawerInstanceId(null)}
      />

      <div className={drawerInstance ? "drawer open" : "drawer"}>
        <div className="drawerHeader">
          <div className="drawerTitle">{drawerInstance?.title ?? ""}</div>
          <button className="drawerClose" type="button" onClick={() => setDrawerInstanceId(null)}>
            Close
          </button>
        </div>

        <div className="drawerBody">
          {!drawerInstance && <p>Select a datasource card to configure it.</p>}

          {drawerInstance && (
            <label className="field">
              <div className="label">Name</div>
              <input
                value={drawerInstance.title}
                onChange={(e) => {
                  const next = e.target.value;
                  setInstances((prev) =>
                    prev.map((inst) => (inst.id === drawerInstance.id ? { ...inst, title: next } : inst))
                  );
                }}
              />
            </label>
          )}

          {drawerInstance?.pluginName === "postgres" && (
            <>
              <h2>PostgreSQL</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">Host</div>
                  <input
                    value={selectedPgConfig?.host ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          host: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Port</div>
                  <input
                    type="number"
                    value={selectedPgConfig?.port ?? 5432}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          port: Number(e.target.value)
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Database</div>
                  <input
                    value={selectedPgConfig?.database ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          database: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">User</div>
                  <input
                    value={selectedPgConfig?.user ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          user: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Password</div>
                  <input
                    type="password"
                    value={selectedPgConfig?.password ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          password: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field checkbox">
                  <input
                    type="checkbox"
                    checked={Boolean(selectedPgConfig?.ssl)}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setPgConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedPgConfig!),
                          ssl: e.target.checked
                        }
                      }))
                    }
                  />
                  <div className="label">SSL</div>
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={savePostgresConfig}>
                  Save config
                </button>
                <button type="button" onClick={testPostgresConnection}>
                  Test connection
                </button>
                <button
                  type="button"
                  onClick={() => drawerInstanceId && inspectPostgresSchema(drawerInstanceId)}
                >
                  Schema inspect
                </button>
                {drawerInstanceId && (pgSchemaById[drawerInstanceId]?.length ?? 0) > 0 && (
                  <button
                    type="button"
                    onClick={() => drawerInstanceId && deepInspectPostgresInstance(drawerInstanceId)}
                  >
                    Deep inspect
                  </button>
                )}
              </div>

              {pgStatus && <pre className="json">{pgStatus}</pre>}

              {drawerInstanceId && pgSchemaStatusById[drawerInstanceId] && (
                <pre className="json">{pgSchemaStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && pgSchemaById[drawerInstanceId] && pgSchemaById[drawerInstanceId].length > 0 && (
                <div className="schema">
                  {drawerInstanceId && (
                    <>
                      {pgCandidatesStatusById[drawerInstanceId] && (
                        <pre className="json">{pgCandidatesStatusById[drawerInstanceId]}</pre>
                      )}

                      {pgCandidatesById[drawerInstanceId] && pgCandidatesById[drawerInstanceId].length > 0 && (
                        <div className="candidates">
                          <div className="candidatesTitle">Mapping candidates</div>
                          {pgCandidatesById[drawerInstanceId]
                            .slice()
                            .sort((a, b) => b.confidence - a.confidence)
                            .slice(0, 20)
                            .map((c, i) => (
                              <div key={`${c.table}:${c.column}:${c.customerPath}:${i}`} className="candidateRow">
                                <div className="candidateLeft">
                                  <span className="candidateDb">{c.table}.{c.column}</span>
                                  <span className="candidateArrow">→</span>
                                  <span className="candidatePath">{c.customerPath}</span>
                                </div>
                                <div className="candidateRight">{Math.round(c.confidence * 100)}%</div>
                              </div>
                            ))}
                        </div>
                      )}
                    </>
                  )}

                  {pgSchemaById[drawerInstanceId].map((t) => (
                    <div key={`${t.schema}.${t.name}`} className="schemaTable">
                      <div className="schemaTableTitle">
                        {t.schema}.{t.name}
                      </div>
                      <div className="schemaColumns">
                        {t.columns
                          .slice()
                          .sort((a, b) => a.ordinalPosition - b.ordinalPosition)
                          .map((c) => (
                            <div
                              key={c.name}
                              className={
                                drawerInstanceId &&
                                (buildCandidateIndex(drawerInstanceId)[`${t.schema}.${t.name}:${c.name}`]?.length ?? 0) > 0
                                  ? "schemaColumn candidate"
                                  : "schemaColumn"
                              }
                            >
                              <div className="schemaColumnName">{c.name}</div>
                              <div className="schemaColumnMeta">
                                {c.dataType}
                                {c.isNullable ? "" : " NOT NULL"}
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "s3-avatars" && (
            <>
              <h2>AWS S3 (Avatars)</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">Region</div>
                  <input
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.region ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          region: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Access key ID</div>
                  <input
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.accessKeyId ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          accessKeyId: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Secret access key</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.secretAccessKey ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          secretAccessKey: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Session token (optional)</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.sessionToken ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          sessionToken: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Bucket</div>
                  <input
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.bucket ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          bucket: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Prefix</div>
                  <input
                    value={drawerInstanceId ? s3AvatarsConfigsById[drawerInstanceId]?.prefix ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3AvatarsConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          prefix: e.target.value
                        }
                      }))
                    }
                  />
                </label>

              </div>

              <div className="actions">
                <button type="button" onClick={saveS3AvatarsConfig}>
                  Save config
                </button>
                <button type="button" onClick={() => drawerInstanceId && testS3AvatarsConnection(drawerInstanceId)}>
                  Test connection
                </button>
                <button type="button" onClick={() => drawerInstanceId && inspectS3AvatarFolders(drawerInstanceId)}>
                  Inspect avatar folders
                </button>
              </div>

              {drawerInstanceId && s3AvatarsStatusById[drawerInstanceId] && (
                <pre className="json">{s3AvatarsStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && s3AvatarsFoldersStatusById[drawerInstanceId] && (
                <pre className="json">{s3AvatarsFoldersStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && (s3AvatarsFoldersById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle">Avatar folder candidates</div>
                    <div className="schemaColumns">
                      {(s3AvatarsFoldersById[drawerInstanceId] ?? []).map((c) => (
                        <div key={c.prefix} className="schemaColumn candidate">
                          <div className="schemaColumnName">{c.prefix}</div>
                          <div className="schemaColumnMeta">
                            confidence {Math.round((Number(c.confidence ?? 0) * 1000)) / 10}% · {c.identityImageCount} id-images · {c.imageCount} images · score {c.score}
                          </div>
                          {(c.reasons?.length ?? 0) > 0 && (
                            <div className="schemaColumnMeta">{(c.reasons ?? []).slice(0, 3).join(" · ")}</div>
                          )}

                          {drawerInstanceId && (
                            <div className="actions" style={{ marginTop: 8 }}>
                              <button
                                type="button"
                                onClick={() =>
                                  setS3AvatarsSelectedFolderPrefixById((prev) => ({
                                    ...prev,
                                    [drawerInstanceId]: c.prefix
                                  }))
                                }
                              >
                                {s3AvatarsSelectedFolderPrefixById[drawerInstanceId] === c.prefix
                                  ? "Selected"
                                  : "Use this folder"}
                              </button>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "rest-api" && (
            <>
              <h2>Banking Core API (OpenAPI)</h2>
              <div className="grid">
                <label className="field" style={{ gridColumn: "1 / -1" }}>
                  <div className="label">OpenAPI / Swagger URL (JSON)</div>
                  <input
                    value={selectedRestApiConfig?.openapiUrl ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedRestApiConfig!),
                          openapiUrl: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Auth type</div>
                  <select
                    value={selectedRestApiConfig?.authType ?? "none"}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedRestApiConfig!),
                          authType: e.target.value as any
                        }
                      }))
                    }
                  >
                    <option value="none">none</option>
                    <option value="bearer">bearer</option>
                    <option value="basic">basic</option>
                  </select>
                </label>

                <label className="field">
                  <div className="label">Bearer token</div>
                  <input
                    type="password"
                    value={selectedRestApiConfig?.bearerToken ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedRestApiConfig!),
                          bearerToken: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Username</div>
                  <input
                    value={selectedRestApiConfig?.username ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedRestApiConfig!),
                          username: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Password</div>
                  <input
                    type="password"
                    value={selectedRestApiConfig?.password ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedRestApiConfig!),
                          password: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field" style={{ gridColumn: "1 / -1" }}>
                  <div className="label">Customer identifier field</div>
                  <select
                    value={drawerInstanceId ? restApiIdentifierFieldById[drawerInstanceId] ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiIdentifierFieldById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: e.target.value
                      }))
                    }
                  >
                    <option value="">(select field)</option>
                    {customerFieldPaths.map((f) => (
                      <option key={f} value={f}>
                        {f}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="field" style={{ gridColumn: "1 / -1" }}>
                  <div className="label">Sample identity value (e.g. customerId)</div>
                  <input
                    value={drawerInstanceId ? restApiIdentityValueById[drawerInstanceId] ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setRestApiIdentityValueById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: e.target.value
                      }))
                    }
                    placeholder="Type a sample identity value to use when deep-inspecting endpoints"
                  />
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={saveRestApiConfig}>
                  Save config
                </button>
                <button type="button" onClick={() => drawerInstanceId && testRestApiConnection(drawerInstanceId)}>
                  Test connection
                </button>
                <button type="button" onClick={() => drawerInstanceId && inspectRestApiOpenApi(drawerInstanceId)}>
                  Inspect OpenAPI
                </button>
              </div>

              {drawerInstanceId && restApiStatusById[drawerInstanceId] && (
                <pre className="json">{restApiStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && restApiCandidatesStatusById[drawerInstanceId] && (
                <pre className="json">{restApiCandidatesStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && (restApiCandidatesById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span>Candidate endpoints (top)</span>
                      <button
                        type="button"
                        onClick={() => {
                          const list = restApiCandidatesById[drawerInstanceId] ?? [];
                          if (list.length > 0) {
                            deepInspectRestApiEndpoint(drawerInstanceId, list[0]);
                          }
                        }}
                      >
                        Deep inspect top candidate
                      </button>
                    </div>
                    <div className="schemaColumns">
                      {restApiCandidatesById[drawerInstanceId]
                        .slice()
                        .sort((a, b) => b.confidence - a.confidence)
                        .slice(0, 12)
                        .map((c, i) => (
                          <div
                            key={`${c.table}:${c.column}:${i}`}
                            className="schemaColumn candidate"
                            style={{ height: "auto" }}
                          >
                            <div
                              className="schemaColumnName"
                              style={{ whiteSpace: "normal", wordBreak: "break-all" }}
                            >
                              {c.table} · {c.column}
                            </div>
                            <div
                              className="schemaColumnMeta"
                              style={{ whiteSpace: "normal", wordBreak: "break-all" }}
                            >
                              confidence {Math.round(c.confidence * 100)}%{c.reason ? ` · ${c.reason}` : ""}
                            </div>
                            {drawerInstanceId && (
                              <div className="schemaColumnMeta" style={{ marginTop: 4 }}>
                                <button
                                  type="button"
                                  onClick={() => deepInspectRestApiEndpoint(drawerInstanceId, c)}
                                >
                                  Deep inspect
                                </button>
                              </div>
                            )}
                            {drawerInstanceId && (
                              <div className="schemaColumnMeta" style={{ marginTop: 4 }}>
                                <button
                                  type="button"
                                  onClick={() =>
                                    setRestApiSelectedPathById((prev) => ({
                                      ...prev,
                                      [drawerInstanceId]: c.column
                                    }))
                                  }
                                >
                                  {restApiSelectedPathById[drawerInstanceId] === c.column
                                    ? "Selected for customer profile"
                                    : "Use for customer profile"}
                                </button>
                              </div>
                            )}
                          </div>
                        ))}
                    </div>
                  </div>
                </div>
              )}

              {drawerInstanceId && restApiDeepInspectResultById[drawerInstanceId] && (
                <div className="schema" style={{ marginTop: 12 }}>
                  <div className="schemaTable">
                    <div className="schemaTableTitle">Deep inspect response (sample)</div>
                    <pre className="json">
                      {JSON.stringify(restApiDeepInspectResultById[drawerInstanceId], null, 2)}
                    </pre>
                  </div>
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "mongodb" && (
            <>
              <h2>MongoDB</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">URI</div>
                  <input
                    value={selectedMongoConfig?.uri ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMongoConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMongoConfig!),
                          uri: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Database</div>
                  <input
                    value={selectedMongoConfig?.database ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMongoConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMongoConfig!),
                          database: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Collection</div>
                  <input
                    value={selectedMongoConfig?.collection ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMongoConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMongoConfig!),
                          collection: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Sample size</div>
                  <input
                    type="number"
                    value={selectedMongoConfig?.sampleSize ?? 50}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMongoConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMongoConfig!),
                          sampleSize: Number(e.target.value)
                        }
                      }))
                    }
                  />
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={saveMongoConfig}>
                  Save config
                </button>
                <button type="button" onClick={() => drawerInstanceId && testMongoConnection(drawerInstanceId)}>
                  Test connection
                </button>
                <button type="button" onClick={() => drawerInstanceId && inspectMongoSchema(drawerInstanceId)}>
                  Schema inspect
                </button>
                {drawerInstanceId && (mongoSchemaById[drawerInstanceId]?.length ?? 0) > 0 && (
                  <button
                    type="button"
                    onClick={() => drawerInstanceId && deepInspectMongoInstance(drawerInstanceId)}
                  >
                    Deep inspect
                  </button>
                )}
              </div>

              {drawerInstanceId && mongoStatusById[drawerInstanceId] && (
                <pre className="json">{mongoStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && mongoSchemaStatusById[drawerInstanceId] && (
                <pre className="json">{mongoSchemaStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && mongoSchemaById[drawerInstanceId] && mongoSchemaById[drawerInstanceId].length > 0 && (
                <div className="schema">
                  {drawerInstanceId && (
                    <>
                      {mongoCandidatesStatusById[drawerInstanceId] && (
                        <pre className="json">{mongoCandidatesStatusById[drawerInstanceId]}</pre>
                      )}

                      {mongoCandidatesById[drawerInstanceId] && mongoCandidatesById[drawerInstanceId].length > 0 && (
                        <div className="candidates">
                          <div className="candidatesTitle">Mapping candidates</div>
                          {mongoCandidatesById[drawerInstanceId]
                            .slice()
                            .sort((a, b) => b.confidence - a.confidence)
                            .slice(0, 20)
                            .map((c, i) => (
                              <div key={`${c.table}:${c.column}:${c.customerPath}:${i}`} className="candidateRow">
                                <div className="candidateLeft">
                                  <span className="candidateDb">{c.table}.{c.column}</span>
                                  <span className="candidateArrow">→</span>
                                  <span className="candidatePath">{c.customerPath}</span>
                                </div>
                                <div className="candidateRight">{Math.round(c.confidence * 100)}%</div>
                              </div>
                            ))}
                        </div>
                      )}
                    </>
                  )}

                  {mongoSchemaById[drawerInstanceId].map((t) => (
                    <div key={`${t.schema}.${t.name}`} className="schemaTable">
                      <div className="schemaTableTitle">
                        {t.schema}.{t.name}
                      </div>
                      <div className="schemaColumns">
                        {t.columns
                          .slice()
                          .sort((a, b) => a.ordinalPosition - b.ordinalPosition)
                          .map((c) => (
                            <div
                              key={c.name}
                              className={
                                drawerInstanceId &&
                                (buildMongoCandidateIndex(drawerInstanceId)[`${t.schema}.${t.name}:${c.name}`]?.length ?? 0) > 0
                                  ? "schemaColumn candidate"
                                  : "schemaColumn"
                              }
                            >
                              <div className="schemaColumnName">{c.name}</div>
                              <div className="schemaColumnMeta">
                                {c.dataType}
                                {c.isNullable ? "" : " NOT NULL"}
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "mssql" && (
            <>
              <h2>SQL Server</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">Host</div>
                  <input
                    value={selectedMssqlConfig?.host ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          host: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Port</div>
                  <input
                    type="number"
                    value={selectedMssqlConfig?.port ?? 1433}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          port: Number(e.target.value)
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Database</div>
                  <input
                    value={selectedMssqlConfig?.database ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          database: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">User</div>
                  <input
                    value={selectedMssqlConfig?.user ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          user: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Password</div>
                  <input
                    type="password"
                    value={selectedMssqlConfig?.password ?? ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          password: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field checkbox">
                  <input
                    type="checkbox"
                    checked={Boolean(selectedMssqlConfig?.encrypt)}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          encrypt: e.target.checked
                        }
                      }))
                    }
                  />
                  <div className="label">Encrypt</div>
                </label>

                <label className="field checkbox">
                  <input
                    type="checkbox"
                    checked={Boolean(selectedMssqlConfig?.trustServerCertificate)}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setMssqlConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? selectedMssqlConfig!),
                          trustServerCertificate: e.target.checked
                        }
                      }))
                    }
                  />
                  <div className="label">Trust server certificate</div>
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={saveMssqlConfig}>
                  Save config
                </button>
                <button type="button" onClick={testMssqlConnection}>
                  Test connection
                </button>
                <button type="button" onClick={() => drawerInstanceId && inspectMssqlSchema(drawerInstanceId)}>
                  Schema inspect
                </button>
                {drawerInstanceId && (mssqlSchemaById[drawerInstanceId]?.length ?? 0) > 0 && (
                  <button
                    type="button"
                    onClick={() => drawerInstanceId && deepInspectMssqlInstance(drawerInstanceId)}
                  >
                    Deep inspect
                  </button>
                )}
              </div>

              {drawerInstanceId && mssqlStatusById[drawerInstanceId] && (
                <pre className="json">{mssqlStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && mssqlSchemaStatusById[drawerInstanceId] && (
                <pre className="json">{mssqlSchemaStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && mssqlSchemaById[drawerInstanceId] && mssqlSchemaById[drawerInstanceId].length > 0 && (
                <div className="schema">
                  {drawerInstanceId && (
                    <>
                      {mssqlCandidatesStatusById[drawerInstanceId] && (
                        <pre className="json">{mssqlCandidatesStatusById[drawerInstanceId]}</pre>
                      )}

                      {mssqlCandidatesById[drawerInstanceId] && mssqlCandidatesById[drawerInstanceId].length > 0 && (
                        <div className="candidates">
                          <div className="candidatesTitle">Mapping candidates</div>
                          {mssqlCandidatesById[drawerInstanceId]
                            .slice()
                            .sort((a, b) => b.confidence - a.confidence)
                            .slice(0, 20)
                            .map((c, i) => (
                              <div key={`${c.table}:${c.column}:${c.customerPath}:${i}`} className="candidateRow">
                                <div className="candidateLeft">
                                  <span className="candidateDb">{c.table}.{c.column}</span>
                                  <span className="candidateArrow">→</span>
                                  <span className="candidatePath">{c.customerPath}</span>
                                </div>
                                <div className="candidateRight">{Math.round(c.confidence * 100)}%</div>
                              </div>
                            ))}
                        </div>
                      )}
                    </>
                  )}

                  {mssqlSchemaById[drawerInstanceId].map((t) => (
                    <div key={`${t.schema}.${t.name}`} className="schemaTable">
                      <div className="schemaTableTitle">
                        {t.schema}.{t.name}
                      </div>
                      <div className="schemaColumns">
                        {t.columns
                          .slice()
                          .sort((a, b) => a.ordinalPosition - b.ordinalPosition)
                          .map((c) => (
                            <div
                              key={c.name}
                              className={
                                drawerInstanceId &&
                                (buildMssqlCandidateIndex(drawerInstanceId)[`${t.schema}.${t.name}:${c.name}`]?.length ?? 0) > 0
                                  ? "schemaColumn candidate"
                                  : "schemaColumn"
                              }
                            >
                              <div className="schemaColumnName">{c.name}</div>
                              <div className="schemaColumnMeta">
                                {c.dataType}
                                {c.isNullable ? "" : " NOT NULL"}
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "sftp" && (
            <>
              <h2>SFTP</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">Host</div>
                  <input
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.host ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          host: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Port</div>
                  <input
                    type="number"
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.port ?? 22 : 22}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          port: Number(e.target.value)
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Username</div>
                  <input
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.username ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          username: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Password</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.password ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          password: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Private key</div>
                  <input
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.privateKey ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          privateKey: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Passphrase</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.passphrase ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          passphrase: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Base path</div>
                  <input
                    value={drawerInstanceId ? sftpConfigsById[drawerInstanceId]?.basePath ?? "/" : "/"}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setSftpConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            host: "",
                            port: 22,
                            username: "",
                            password: "",
                            privateKey: "",
                            passphrase: "",
                            basePath: "/"
                          }),
                          basePath: e.target.value
                        }
                      }))
                    }
                  />
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={saveSftpConfig}>
                  Save config
                </button>
                <button
                  type="button"
                  onClick={() => drawerInstanceId && testSftpConnection(drawerInstanceId)}
                >
                  Test connection
                </button>
                <button
                  type="button"
                  onClick={() => drawerInstanceId && listSftpCsvFiles(drawerInstanceId)}
                >
                  List .csv files
                </button>
                <button
                  type="button"
                  onClick={() =>
                    drawerInstanceId &&
                    setSftpConfigsById((prev) => ({
                      ...prev,
                      [drawerInstanceId]: {
                        ...(prev[drawerInstanceId] ?? {
                          host: "",
                          port: 22,
                          username: "",
                          password: "",
                          privateKey: "",
                          passphrase: "",
                          basePath: "/"
                        }),
                        basePath: goUpOneFolder(prev[drawerInstanceId]?.basePath ?? "/")
                      }
                    }))
                  }
                >
                  Up
                </button>
                <button
                  type="button"
                  onClick={() => drawerInstanceId && inspectSftpCsvSchema(drawerInstanceId)}
                >
                  Schema inspect
                </button>
                {drawerInstanceId && (sftpSchemaById[drawerInstanceId]?.length ?? 0) > 0 && (
                  <button
                    type="button"
                    onClick={() => drawerInstanceId && deepInspectSftpInstance(drawerInstanceId)}
                  >
                    Deep inspect
                  </button>
                )}
              </div>

              {drawerInstanceId && sftpStatusById[drawerInstanceId] && (
                <pre className="json">{sftpStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && sftpFilesStatusById[drawerInstanceId] && (
                <pre className="json">{sftpFilesStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && (sftpDirsById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle">Folders</div>
                    <div className="schemaColumns">
                      {(sftpDirsById[drawerInstanceId] ?? []).map((d) => (
                        <button
                          key={d.path}
                          type="button"
                          className="pluginRow"
                          onClick={() => {
                            setSftpConfigsById((prev) => ({
                              ...prev,
                              [drawerInstanceId]: {
                                ...(prev[drawerInstanceId] ?? {
                                  host: "",
                                  port: 22,
                                  username: "",
                                  password: "",
                                  privateKey: "",
                                  passphrase: "",
                                  basePath: "/"
                                }),
                                basePath: d.path
                              }
                            }));
                            void listSftpCsvFiles(drawerInstanceId);
                          }}
                        >
                          <div className="pluginRowTitle">{d.path}</div>
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {drawerInstanceId && (sftpFilesById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle">CSV files</div>
                    <div className="schemaColumns">
                      {(sftpFilesById[drawerInstanceId] ?? []).map((f) => (
                        <label key={f.path} className="field checkbox">
                          <input
                            type="checkbox"
                            checked={f.selected}
                            onChange={(e) =>
                              setSftpFilesById((prev) => ({
                                ...prev,
                                [drawerInstanceId]: (prev[drawerInstanceId] ?? []).map((x) =>
                                  x.path === f.path ? { ...x, selected: e.target.checked } : x
                                )
                              }))
                            }
                          />
                          <div className="label">{f.path}</div>
                        </label>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {drawerInstanceId && sftpSchemaStatusById[drawerInstanceId] && (
                <pre className="json">{sftpSchemaStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && sftpCandidatesStatusById[drawerInstanceId] && (
                <pre className="json">{sftpCandidatesStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && sftpSchemaById[drawerInstanceId] && sftpSchemaById[drawerInstanceId].length > 0 && (
                <div className="schema">
                  {drawerInstanceId && sftpCandidatesById[drawerInstanceId] && sftpCandidatesById[drawerInstanceId].length > 0 && (
                    <div className="candidates">
                      <div className="candidatesTitle">Mapping candidates</div>
                      {sftpCandidatesById[drawerInstanceId]
                        .slice()
                        .sort((a, b) => b.confidence - a.confidence)
                        .slice(0, 20)
                        .map((c, i) => (
                          <div key={`${c.table}:${c.column}:${c.customerPath}:${i}`} className="candidateRow">
                            <div className="candidateLeft">
                              <span className="candidateDb">{c.table}.{c.column}</span>
                              <span className="candidateArrow">→</span>
                              <span className="candidatePath">{c.customerPath}</span>
                            </div>
                            <div className="candidateRight">{Math.round(c.confidence * 100)}%</div>
                          </div>
                        ))}
                    </div>
                  )}

                  {sftpSchemaById[drawerInstanceId].map((t) => (
                    <div key={`${t.schema}.${t.name}`} className="schemaTable">
                      <div className="schemaTableTitle">
                        {t.schema}.{t.name}
                      </div>
                      <div className="schemaColumns">
                        {t.columns
                          .slice()
                          .sort((a, b) => a.ordinalPosition - b.ordinalPosition)
                          .map((c) => (
                            <div
                              key={c.name}
                              className={
                                drawerInstanceId &&
                                (buildSftpCandidateIndex(drawerInstanceId)[`${t.schema}.${t.name}:${c.name}`]?.length ?? 0) > 0
                                  ? "schemaColumn candidate"
                                  : "schemaColumn"
                              }
                            >
                              <div className="schemaColumnName">{c.name}</div>
                              <div className="schemaColumnMeta">
                                {c.dataType}
                                {c.isNullable ? "" : " NOT NULL"}
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {drawerInstance?.pluginName === "s3" && (
            <>
              <h2>AWS S3</h2>
              <div className="grid">
                <label className="field">
                  <div className="label">Region</div>
                  <input
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.region ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          region: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Access key ID</div>
                  <input
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.accessKeyId ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          accessKeyId: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Secret access key</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.secretAccessKey ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          secretAccessKey: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Session token (optional)</div>
                  <input
                    type="password"
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.sessionToken ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          sessionToken: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Bucket</div>
                  <input
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.bucket ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          bucket: e.target.value
                        }
                      }))
                    }
                  />
                </label>

                <label className="field">
                  <div className="label">Prefix</div>
                  <input
                    value={drawerInstanceId ? s3ConfigsById[drawerInstanceId]?.prefix ?? "" : ""}
                    onChange={(e) =>
                      drawerInstanceId &&
                      setS3ConfigsById((prev) => ({
                        ...prev,
                        [drawerInstanceId]: {
                          ...(prev[drawerInstanceId] ?? {
                            region: "",
                            accessKeyId: "",
                            secretAccessKey: "",
                            sessionToken: "",
                            bucket: "",
                            prefix: ""
                          }),
                          prefix: e.target.value
                        }
                      }))
                    }
                  />
                </label>
              </div>

              <div className="actions">
                <button type="button" onClick={saveS3Config}>
                  Save config
                </button>
                <button type="button" onClick={() => drawerInstanceId && testS3Connection(drawerInstanceId)}>
                  Test connection
                </button>
                <button type="button" onClick={() => drawerInstanceId && listS3ParquetFiles(drawerInstanceId)}>
                  List .parquet files
                </button>
                <button
                  type="button"
                  onClick={() =>
                    drawerInstanceId &&
                    setS3ConfigsById((prev) => ({
                      ...prev,
                      [drawerInstanceId]: {
                        ...(prev[drawerInstanceId] ?? {
                          region: "",
                          accessKeyId: "",
                          secretAccessKey: "",
                          sessionToken: "",
                          bucket: "",
                          prefix: ""
                        }),
                        prefix: goUpOnePrefix(prev[drawerInstanceId]?.prefix ?? "")
                      }
                    }))
                  }
                >
                  Up
                </button>
                <button type="button" onClick={() => drawerInstanceId && inspectS3ParquetSchema(drawerInstanceId)}>
                  Schema inspect
                </button>
                {drawerInstanceId && (s3SchemaById[drawerInstanceId]?.length ?? 0) > 0 && (
                  <button type="button" onClick={() => drawerInstanceId && deepInspectS3Instance(drawerInstanceId)}>
                    Deep inspect
                  </button>
                )}
              </div>

              {drawerInstanceId && s3StatusById[drawerInstanceId] && <pre className="json">{s3StatusById[drawerInstanceId]}</pre>}

              {drawerInstanceId && s3FilesStatusById[drawerInstanceId] && (
                <pre className="json">{s3FilesStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && (s3DirsById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle">Prefixes</div>
                    <div className="schemaColumns">
                      {(s3DirsById[drawerInstanceId] ?? []).map((d) => (
                        <button
                          key={d.prefix}
                          type="button"
                          className="pluginRow"
                          onClick={() => {
                            setS3ConfigsById((prev) => ({
                              ...prev,
                              [drawerInstanceId]: {
                                ...(prev[drawerInstanceId] ?? {
                                  region: "",
                                  accessKeyId: "",
                                  secretAccessKey: "",
                                  sessionToken: "",
                                  bucket: "",
                                  prefix: ""
                                }),
                                prefix: d.prefix
                              }
                            }));
                            void listS3ParquetFiles(drawerInstanceId);
                          }}
                        >
                          <div className="pluginRowTitle">{d.prefix}</div>
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {drawerInstanceId && (s3FilesById[drawerInstanceId]?.length ?? 0) > 0 && (
                <div className="schema">
                  <div className="schemaTable">
                    <div className="schemaTableTitle">Parquet files</div>
                    <div className="schemaColumns">
                      {(s3FilesById[drawerInstanceId] ?? []).map((f) => (
                        <label key={f.key} className="field checkbox">
                          <input
                            type="checkbox"
                            checked={f.selected}
                            onChange={(e) =>
                              setS3FilesById((prev) => ({
                                ...prev,
                                [drawerInstanceId]: (prev[drawerInstanceId] ?? []).map((x) =>
                                  x.key === f.key ? { ...x, selected: e.target.checked } : x
                                )
                              }))
                            }
                          />
                          <div className="label">{f.key}</div>
                        </label>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {drawerInstanceId && s3SchemaStatusById[drawerInstanceId] && (
                <pre className="json">{s3SchemaStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && s3CandidatesStatusById[drawerInstanceId] && (
                <pre className="json">{s3CandidatesStatusById[drawerInstanceId]}</pre>
              )}

              {drawerInstanceId && s3SchemaById[drawerInstanceId] && s3SchemaById[drawerInstanceId].length > 0 && (
                <div className="schema">
                  {drawerInstanceId && s3CandidatesById[drawerInstanceId] && s3CandidatesById[drawerInstanceId].length > 0 && (
                    <div className="candidates">
                      <div className="candidatesTitle">Mapping candidates</div>
                      {s3CandidatesById[drawerInstanceId]
                        .slice()
                        .sort((a, b) => b.confidence - a.confidence)
                        .slice(0, 20)
                        .map((c, i) => (
                          <div key={`${c.table}:${c.column}:${c.customerPath}:${i}`} className="candidateRow">
                            <div className="candidateLeft">
                              <span className="candidateDb">{c.table}.{c.column}</span>
                              <span className="candidateArrow">→</span>
                              <span className="candidatePath">{c.customerPath}</span>
                            </div>
                            <div className="candidateRight">{Math.round(c.confidence * 100)}%</div>
                          </div>
                        ))}
                    </div>
                  )}

                  {s3SchemaById[drawerInstanceId].map((t) => (
                    <div key={`${t.schema}.${t.name}`} className="schemaTable">
                      <div className="schemaTableTitle">
                        {t.schema}.{t.name}
                      </div>
                      <div className="schemaColumns">
                        {t.columns
                          .slice()
                          .sort((a, b) => a.ordinalPosition - b.ordinalPosition)
                          .map((c) => (
                            <div
                              key={c.name}
                              className={
                                drawerInstanceId &&
                                (buildS3CandidateIndex(drawerInstanceId)[`${t.schema}.${t.name}:${c.name}`]?.length ?? 0) >
                                  0
                                  ? "schemaColumn candidate"
                                  : "schemaColumn"
                              }
                            >
                              <div className="schemaColumnName">{c.name}</div>
                              <div className="schemaColumnMeta">
                                {c.dataType}
                                {c.isNullable ? "" : " NOT NULL"}
                              </div>
                            </div>
                          ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
