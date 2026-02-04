import { useEffect, useMemo, useState } from "react";
import { getCustomerProfile, type CustomerProfile } from "../services/customerProfileService";
import {
  lookupCustomerProfileFromDatasourcesWithDefaultIdentity,
  type DatasourceContext,
  type DatasourceInstance,
  type MappingCandidate
} from "../services/customerLookupService";

type TabKey = "products" | "priorities" | "history";

type CustomerProfileWithAvatar = CustomerProfile & { avatarUrl: string; avatarConfidence?: number };

function PhoneIcon() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M6.62 10.79a15.05 15.05 0 0 0 6.59 6.59l2.2-2.2a1 1 0 0 1 1.02-.24c1.12.37 2.33.57 3.57.57a1 1 0 0 1 1 1V20a1 1 0 0 1-1 1C10.07 21 3 13.93 3 5a1 1 0 0 1 1-1h3.5a1 1 0 0 1 1 1c0 1.24.2 2.45.57 3.57a1 1 0 0 1-.24 1.02l-2.21 2.2Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function ContactPhoneIcon() {
  return (
    <svg width="34" height="34" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M6.62 10.79a15.05 15.05 0 0 0 6.59 6.59l2.2-2.2a1 1 0 0 1 1.02-.24c1.12.37 2.33.57 3.57.57a1 1 0 0 1 1 1V20a1 1 0 0 1-1 1C10.07 21 3 13.93 3 5a1 1 0 0 1 1-1h3.5a1 1 0 0 1 1 1c0 1.24.2 2.45.57 3.57a1 1 0 0 1-.24 1.02l-2.21 2.2Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function ContactMailIcon() {
  return (
    <svg width="34" height="34" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M4 6h16a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2Z"
        stroke="currentColor"
        strokeWidth="1.8"
      />
      <path d="M4 8l8 6 8-6" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      <path
        d="M10.5 11.6h3"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
    </svg>
  );
}

function ContactPinIcon() {
  return (
    <svg width="34" height="34" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M12 22s7-5.2 7-12a7 7 0 1 0-14 0c0 6.8 7 12 7 12Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
      <path d="M12 13a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" stroke="currentColor" strokeWidth="1.8" />
    </svg>
  );
}

function MailIcon() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M4 6h16a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2Z"
        stroke="currentColor"
        strokeWidth="1.8"
      />
      <path d="M4 8l8 6 8-6" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
    </svg>
  );
}

function PinIcon() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path
        d="M12 22s7-5.2 7-12a7 7 0 1 0-14 0c0 6.8 7 12 7 12Z"
        stroke="currentColor"
        strokeWidth="1.8"
        strokeLinejoin="round"
      />
      <path d="M12 13a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" stroke="currentColor" strokeWidth="1.8" />
    </svg>
  );
}

function ChevronDownIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M6 9l6 6 6-6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function CardChevronIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M6 9l6 6 6-6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function SearchIcon() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="11" cy="11" r="6" stroke="currentColor" strokeWidth="2" />
      <path d="M20 20l-3.2-3.2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

export default function CustomerProfilePanel(props: {
  theme: "light" | "dark";
  datasourceContext: DatasourceContext;
  instances: DatasourceInstance[];
  mergeMode: boolean;
  activeInstance: DatasourceInstance | null;
  candidatesById: {
    postgres: Record<string, MappingCandidate[]>;
    mssql: Record<string, MappingCandidate[]>;
    mongodb: Record<string, MappingCandidate[]>;
    sftp: Record<string, MappingCandidate[]>;
    s3: Record<string, MappingCandidate[]>;
  };
  selectedSftpCsvById: Record<string, string | null>;

  s3Avatars?: {
    instances: Array<{ id: string }>;
    configsById: Record<string, unknown>;
    selectedFolderPrefixById: Record<string, string | undefined>;
    foldersById: Record<string, Array<{ prefix: string; confidence: number }>>;
  };

  onProfilePictureConfidence?: (confidence: number | null) => void;
  restApiIdentifierField?: string;
  restApiInstanceId?: string | null;
  restApiSelectedPath?: string | null;
}) {
  const [profile, setProfile] = useState<CustomerProfileWithAvatar | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [tab, setTab] = useState<TabKey>("products");
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [coreApiStatus, setCoreApiStatus] = useState<string | null>(null);
  const [coreApiResponse, setCoreApiResponse] = useState<any | null>(null);
  const [coreProducts, setCoreProducts] = useState<any[]>([]);

  const autoLoadMock = false;

  useEffect(() => {
    if (!autoLoadMock) {
      return;
    }
    let cancelled = false;
    (async () => {
      const p = await getCustomerProfile();
      if (!cancelled) {
        setProfile(p);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [autoLoadMock]);

  const headerNameLines = useMemo(() => {
    const name = profile?.fullName ?? "";
    const parts = name.split(/\s+/).filter(Boolean);
    if (parts.length <= 2) {
      return [name];
    }
    const mid = Math.ceil(parts.length / 2);
    return [parts.slice(0, mid).join(" "), parts.slice(mid).join(" ")];
  }, [profile?.fullName]);

  const initials = useMemo(() => {
    const name = profile?.fullName ?? "";
    const parts = name.split(/\s+/).filter(Boolean);
    const a = parts[0]?.[0] ?? "";
    const b = parts.length > 1 ? parts[parts.length - 1]?.[0] ?? "" : "";
    return `${a}${b}`.toUpperCase();
  }, [profile?.fullName]);

  async function onTest() {
    setLoading(true);
    setStatus(props.mergeMode ? "Querying datasources..." : "Querying datasource...");
    props.onProfilePictureConfidence?.(null);
    try {
      const instances = props.mergeMode
        ? props.instances
        : props.activeInstance
          ? [props.activeInstance]
          : [];

      const ctx: DatasourceContext = {
        instances,

        pgConfigsById: props.datasourceContext.pgConfigsById,
        mssqlConfigsById: props.datasourceContext.mssqlConfigsById,
        mongoConfigsById: props.datasourceContext.mongoConfigsById,
        sftpConfigsById: props.datasourceContext.sftpConfigsById,
        s3ConfigsById: props.datasourceContext.s3ConfigsById,
        restApiConfigsById: props.datasourceContext.restApiConfigsById,

        pgCandidatesById: props.candidatesById.postgres,
        mssqlCandidatesById: props.candidatesById.mssql,
        mongoCandidatesById: props.candidatesById.mongodb,
        sftpCandidatesById: props.candidatesById.sftp,
        s3CandidatesById: props.candidatesById.s3,
        restApiCandidatesById: props.datasourceContext.restApiCandidatesById,

        sftpSelectedFileById: props.selectedSftpCsvById
      };

      const r = await lookupCustomerProfileFromDatasourcesWithDefaultIdentity(ctx, query);
      setQuery(r.identityNumber);
      setProfile(r.profile);

      // After aggregating the customer using identityNumber, optionally call the configured
      // Banking Core API endpoint using the chosen identifier field as a foreign key.
      setCoreApiStatus(null);
      setCoreApiResponse(null);
      setCoreProducts([]);

      if (props.mergeMode && props.restApiInstanceId && props.restApiSelectedPath && props.restApiIdentifierField) {
        try {
          const fkField = props.restApiIdentifierField;
          let fkValue: string | null = null;

          if (fkField === "identityNumber") {
            fkValue = r.identityNumber;
          } else if (r.profile.details?.sections) {
            for (const section of r.profile.details.sections) {
              for (const item of section.items) {
                const label = String(item.label ?? "").trim();
                if (!label) continue;
                if (label === fkField || label.endsWith("." + fkField)) {
                  const v = String(item.value ?? "").trim();
                  if (v) {
                    fkValue = v;
                    break;
                  }
                }
              }
              if (fkValue) break;
            }
          }

          if (fkValue) {
            setCoreApiStatus("Querying Banking Core API...");
            const restCfg: any = (props.datasourceContext as any).restApiConfigsById?.[
              props.restApiInstanceId
            ];

            const res = await fetch("/api/datasources/rest-api/deep-inspect-endpoint", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                id: props.restApiInstanceId,
                config: restCfg,
                endpoint: {
                  method: "GET",
                  path: props.restApiSelectedPath,
                  identityParam: { name: "customerId", in: "path" },
                  identityValue: fkValue
                }
              })
            });

            const data = await res.json().catch(() => null as any);
            if (!res.ok || !data?.ok) {
              throw new Error(data?.error ?? `Banking Core HTTP ${res.status}`);
            }

            setCoreApiResponse(data);
            setCoreApiStatus(`Banking Core API OK. HTTP ${data.status ?? "?"} ${data.statusText ?? ""}`);

            const rawProducts: any[] = Array.isArray((data as any).products)
              ? ((data as any).products as any[])
              : Array.isArray((data as any).body)
                ? ((data as any).body as any[])
                : [];

            if (rawProducts.length > 0) {
              setCoreProducts(rawProducts);
              const uiProducts = rawProducts.map((p, idx) => {
                const amount = Number((p as any).amount ?? 0);
                const currency = String((p as any).currency ?? "").trim() || "";
                const status = String((p as any).status ?? "").trim();
                const title = String((p as any).name ?? (p as any).type ?? (p as any).productId ?? "Producto");
                const id = String((p as any).productId ?? `${title}-${idx}`);

                const formattedAmount = Number.isFinite(amount)
                  ? amount.toLocaleString("es-AR", { minimumFractionDigits: 2 })
                  : String(amount);

                return {
                  id,
                  title,
                  primaryAmount: currency ? `${currency} ${formattedAmount}` : formattedAmount,
                  secondaryAmount: status || undefined
                };
              });

              setProfile((prev) =>
                prev
                  ? {
                      ...prev,
                      products: uiProducts
                    }
                  : prev
              );
            }
          }
        } catch (e) {
          setCoreApiStatus(e instanceof Error ? e.message : String(e));
        }
      }

      // If s3-avatars is configured and included in the current query scope,
      // try to resolve a profile picture by identityNumber filename.
      const s3Avatars = props.s3Avatars;
      if (s3Avatars && props.mergeMode) {
        const s3AvatarInstances = s3Avatars.instances;

        for (const inst of s3AvatarInstances) {
          const cfg = s3Avatars.configsById[inst.id];
          const folderPrefix = s3Avatars.selectedFolderPrefixById[inst.id];
          if (!cfg || !folderPrefix) continue;

          const folderConfidence =
            (s3Avatars.foldersById[inst.id] ?? []).find((c) => c.prefix === folderPrefix)?.confidence ?? 0;

          try {
            const findRes = await fetch("/api/datasources/s3-avatars/find-avatar", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ id: inst.id, config: cfg, identityNumber: r.identityNumber, folderPrefix })
            });
            const findData = (await findRes.json().catch(() => null)) as any;
            if (!findRes.ok || !findData?.ok) {
              continue;
            }

            const key = String(findData?.key ?? "").trim();
            if (!key) {
              continue;
            }

            const signRes = await fetch("/api/datasources/s3-avatars/presign-get", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ id: inst.id, config: cfg, key, expiresInSeconds: 300 })
            });
            const signData = (await signRes.json().catch(() => null)) as any;
            if (!signRes.ok || !signData?.ok) {
              continue;
            }

            const url = String(signData?.url ?? "").trim();
            if (!url) {
              continue;
            }

            setProfile((prev) => (prev ? { ...prev, avatarUrl: url, avatarConfidence: folderConfidence } : prev));
            props.onProfilePictureConfidence?.(folderConfidence);
            break;
          } catch {
            // Ignore avatar failures; keep profile visible.
          }
        }
      }

      setStatus(null);
    } catch (e) {
      setStatus(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  if (!profile) {
    return (
      <div className="cpRoot cpThemeLight">
        <div className="cpAppBar">
          <img className="cpAppLogo" src="/n5blue.png" alt="N5" />

          <input
            className="cpAppCustomerId"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Customer id"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                onTest();
              }
            }}
          />

          <div className="cpAppBarRight">
            <button
              className="cpAppSearchBtn"
              type="button"
              aria-label="Search"
              onClick={onTest}
            >
              <SearchIcon />
            </button>
            <div className="cpAppAvatar skel" />
          </div>
        </div>

        {status && <div className="customerFieldsAlert">{status}</div>}
        {coreApiStatus && <div className="customerFieldsAlert">{coreApiStatus}</div>}

        <div className="cpTop">
          <div className={loading ? "cpAvatarCircle skel skelAnimating" : "cpAvatarCircle skel"} />

          <div className="cpName">
            <div className={loading ? "cpNameLine skel skelLine skelAnimating" : "cpNameLine skel skelLine"} />
            <div className={loading ? "cpNameLine skel skelLine skelAnimating" : "cpNameLine skel skelLine"} />
          </div>

          <div className={loading ? "cpAge skel skelSmall skelAnimating" : "cpAge skel skelSmall"} />

          <div className="cpActions">
            <div className={loading ? "cpAction skel skelAnimating" : "cpAction skel"} />
            <div className={loading ? "cpAction skel skelAnimating" : "cpAction skel"} />
            <div className={loading ? "cpAction skel skelAnimating" : "cpAction skel"} />
          </div>

          <div className={loading ? "cpMore skel skelMore skelAnimating" : "cpMore skel skelMore"} />
        </div>

        <div className="cpTabs">
          <button
            className={tab === "products" ? "cpTab active" : "cpTab"}
            type="button"
            onClick={() => setTab("products")}
          >
            Productos
          </button>
          <button
            className={tab === "priorities" ? "cpTab active" : "cpTab"}
            type="button"
            onClick={() => setTab("priorities")}
          >
            Prioridades
          </button>
          <button
            className={tab === "history" ? "cpTab active" : "cpTab"}
            type="button"
            onClick={() => setTab("history")}
          >
            Historial
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="cpRoot cpThemeLight">
      <div className="cpAppBar">
        <img className="cpAppLogo" src="/n5blue.png" alt="N5" />

        <input
          className="cpAppCustomerId"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Customer id"
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              onTest();
            }
          }}
        />

        <div className="cpAppBarRight">
          <button
            className="cpAppSearchBtn"
            type="button"
            aria-label="Search"
            onClick={onTest}
          >
            <SearchIcon />
          </button>
          <div className="cpAppAvatarPlaceholder" aria-label="PV">
            PV
          </div>
        </div>
      </div>

      {status && <div className="customerFieldsAlert">{status}</div>}

      <div className="cpTop">
        <div className="cpAvatarCircle" aria-label={profile.fullName}>
          {profile.avatarUrl ? (
            <img className="cpAvatarImg" src={profile.avatarUrl} alt={profile.fullName} />
          ) : (
            initials
          )}
        </div>

        <div className="cpName">
          {headerNameLines.map((l, idx) => (
            <div key={idx} className="cpNameLine">
              {l}
            </div>
          ))}
        </div>

        <div className="cpAge">
          {profile.age} {profile.age === 1 ? "año" : "años"}
          {profile.personType === "natural"
            ? " de edad"
            : profile.personType === "legal"
              ? " de constitución"
              : ""}
        </div>

        <div className="cpActions">
          <button className="cpAction" type="button" aria-label="Call">
            <PhoneIcon />
          </button>
          <button className="cpAction" type="button" aria-label="Email">
            <MailIcon />
          </button>
          <button className="cpAction" type="button" aria-label="Location">
            <PinIcon />
          </button>
        </div>

        <button className="cpMore" type="button" onClick={() => setExpanded((v) => !v)}>
          <ChevronDownIcon />
          <span>Más datos</span>
        </button>

        {expanded && (
          <div className="cpMorePanel">
            {(profile.details?.sections?.length ? profile.details.sections : [])
              .filter((s) => s && Array.isArray(s.items) && s.items.length > 0)
              .map((section) => (
                <div key={section.title} className="cpDetailSection">
                  <div className="cpDetailTitle">{section.title}</div>

                  {section.title === "Datos de contactabilidad" ? (
                    <div className="cpContactGrid">
                      {(() => {
                        const hasValue = (v: unknown) => {
                          const s = String(v ?? "").trim();
                          return Boolean(s && s !== "-");
                        };

                        const phoneItems = section.items.filter((it) =>
                          ["Celular:", "Particular:", "Laboral:"].includes(it.label)
                        );
                        const emailItems = section.items.filter((it) => it.label === "Correo electrónico:");
                        const addrItems = section.items
                          .filter((it) => it.label === "Domicilio Particular:" || it.label === "Domicilio Laboral:")
                          .slice(0, 2);

                        const hasPhones = phoneItems.some((it) => hasValue(it.value));
                        const hasEmail = emailItems.some((it) => hasValue(it.value));
                        const hasAddr = addrItems.some((it) => hasValue(it.value));

                        return (
                          <>
                      <div className="cpContactRow">
                        <div className={hasPhones ? "cpContactIcon" : "cpContactIcon cpContactIconDisabled"}>
                          <ContactPhoneIcon />
                        </div>
                        <div className="cpContactBody">
                          {phoneItems
                            .map((it, idx) => (
                              <div key={`phones-${idx}`} className="cpContactBlock">
                                <div className="cpContactLabel">{it.label}</div>
                                <div className="cpContactValue">{it.value}</div>
                              </div>
                            ))}
                        </div>
                      </div>

                      <div className="cpContactRow">
                        <div className={hasEmail ? "cpContactIcon" : "cpContactIcon cpContactIconDisabled"}>
                          <ContactMailIcon />
                        </div>
                        <div className="cpContactBody">
                          {emailItems
                            .map((it, idx) => (
                              <div key={`email-${idx}`} className="cpContactBlock">
                                <div className="cpContactLabel">{it.label}</div>
                                <div className="cpContactValue">{it.value}</div>
                              </div>
                            ))}
                        </div>
                      </div>

                      <div className="cpContactRow">
                        <div className={hasAddr ? "cpContactIcon" : "cpContactIcon cpContactIconDisabled"}>
                          <ContactPinIcon />
                        </div>
                        <div className="cpContactBody">
                          {addrItems
                            .map((it, idx) => (
                              <div key={`addr-${idx}`} className="cpContactBlock">
                                <div className="cpContactLabel">{it.label}</div>
                                <div className="cpContactValue">{it.value}</div>
                              </div>
                            ))}
                        </div>
                      </div>
                          </>
                        );
                      })()}
                    </div>
                  ) : (
                    <div className="cpDetailGrid">
                      {section.items.map((it, idx) => (
                        <div key={`${section.title}-${idx}`} className="cpDetailItem">
                          <div className="cpDetailLabel">
                            {it.label}
                            {it.statusColor && <span className={`cpDetailDot ${it.statusColor}`} />}
                          </div>
                          <div
                            className={
                              it.statusColor
                                ? `cpDetailValue cpDetailValue_${it.statusColor}`
                                : "cpDetailValue"
                            }
                          >
                            {it.value}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}

            {!profile.details?.sections?.length && (
              <div className="cpDetailSection">
                <div className="cpDetailTitle">Información del cliente</div>
                <div className="cpDetailGrid">
                  <div className="cpDetailItem">
                    <div className="cpDetailLabel">Teléfono</div>
                    <div className="cpDetailValue">{profile.phone}</div>
                  </div>
                  <div className="cpDetailItem">
                    <div className="cpDetailLabel">Email</div>
                    <div className="cpDetailValue">{profile.email}</div>
                  </div>
                  <div className="cpDetailItem">
                    <div className="cpDetailLabel">Ubicación</div>
                    <div className="cpDetailValue">{profile.location}</div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      <div className="cpTabs">
        <button
          type="button"
          className={tab === "products" ? "cpTab active" : "cpTab"}
          onClick={() => setTab("products")}
        >
          Productos
        </button>
        <button
          type="button"
          className={tab === "priorities" ? "cpTab active" : "cpTab"}
          onClick={() => setTab("priorities")}
        >
          Prioridades
        </button>
        <button
          type="button"
          className={tab === "history" ? "cpTab active" : "cpTab"}
          onClick={() => setTab("history")}
        >
          Historial
        </button>
      </div>

      <div className="cpBody">
        {tab === "products" && (
          <div className="cpCards">
            {[
              { key: "ACCOUNT", id: "accounts", title: "Cuentas" },
              { key: "CARD", id: "cards", title: "Tarjetas" },
              { key: "CREDIT", id: "credits", title: "Préstamos" },
              { key: "INVESTMENT", id: "investments", title: "Inversiones" },
              { key: "INSURANCE", id: "insurances", title: "Seguros" },
              { key: "OTHER", id: "others", title: "Otros" }
            ].map((cat) => {
              const items = coreProducts.filter((p) => {
                const t = String((p as any).type ?? "").toUpperCase();
                return t === cat.key;
              });

              const hasItems = items.length > 0;

              let primaryAmount: string | null = null;
              let secondaryAmount: string | null = null;
              if (hasItems) {
                const byCurrency = new Map<string, number>();
                for (const p of items) {
                  const currency = String((p as any).currency ?? "").trim() || "";
                  const amount = Number((p as any).amount ?? 0);
                  if (!Number.isFinite(amount)) continue;
                  const prev = byCurrency.get(currency) ?? 0;
                  byCurrency.set(currency, prev + amount);
                }

                const entries = Array.from(byCurrency.entries());
                if (entries.length > 0) {
                  const [cur1, amt1] = entries[0];
                  const formatted1 = amt1.toLocaleString("es-AR", { minimumFractionDigits: 2 });
                  primaryAmount = cur1 ? `${cur1} ${formatted1}` : formatted1;
                }
                if (entries.length > 1) {
                  const [cur2, amt2] = entries[1];
                  const formatted2 = amt2.toLocaleString("es-AR", { minimumFractionDigits: 2 });
                  secondaryAmount = cur2 ? `${cur2} ${formatted2}` : formatted2;
                }
              }

              const detailLines = hasItems
                ? items.map((p, idx) => {
                    const name = String((p as any).name ?? (p as any).productId ?? "Producto");
                    return `1 ${name}`;
                  })
                : [];

              return (
                <div
                  key={cat.id}
                  className={hasItems ? "cpCard cpCardProduct" : "cpCard cpCardProduct cpCardProductEmpty"}
                >
                  <div className="cpCardHeaderRow">
                    <div className="cpCardHeaderTitle">{cat.title}</div>
                    <div className="cpCardHeaderIcon">
                      <CardChevronIcon />
                    </div>
                  </div>

                  {hasItems && (
                    <>
                      {primaryAmount && <div className="cpCardPrimary cpCardPrimaryEmphasis">{primaryAmount}</div>}
                      {secondaryAmount && <div className="cpCardSecondary cpCardSecondaryMuted">{secondaryAmount}</div>}

                      {detailLines.length > 0 && (
                        <div className="cpProductList">
                          {detailLines.map((line, idx) => (
                            <div key={idx} className="cpProductListItem">
                              {line}
                            </div>
                          ))}
                        </div>
                      )}
                    </>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {tab === "priorities" && (
          <div className="cpCards">
            {profile.priorities.map((p) => (
              <div key={p.id} className="cpCard">
                <div className="cpCardLeft">
                  <div className="cpCardTitle">{p.title}</div>
                  <div className="cpCardSecondary">{p.description}</div>
                </div>
                <div className="cpCardRight">
                  <CardChevronIcon />
                </div>
              </div>
            ))}
          </div>
        )}

        {tab === "history" && (
          <div className="cpCards">
            {profile.history.map((h) => (
              <div key={h.id} className="cpCard">
                <div className="cpCardLeft">
                  <div className="cpCardTitle">{h.title}</div>
                  <div className="cpCardSecondary">{h.description}</div>
                  <div className="cpCardTertiary">{h.ts}</div>
                </div>
                <div className="cpCardRight">
                  <CardChevronIcon />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
