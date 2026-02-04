export type CustomerProfile = {
  id: string;
  fullName: string;
  personType?: "natural" | "legal";
  age: number;
  phone: string;
  email: string;
  location: string;
  details?: {
    sections: Array<{
      title: string;
      items: Array<{ label: string; value: string; statusColor?: "green" | "yellow" | "red" | "gray" }>;
    }>;
  };
  products: Array<{ id: string; title: string; primaryAmount: string; secondaryAmount?: string }>;
  priorities: Array<{ id: string; title: string; description: string }>;
  history: Array<{ id: string; title: string; description: string; ts: string }>;
};

export function makeAvatarDataUrl(fullName: string): string {
  const initials = fullName
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((p) => p[0]?.toUpperCase())
    .join("");

  const svg = `<?xml version="1.0" encoding="UTF-8"?>\n<svg xmlns="http://www.w3.org/2000/svg" width="160" height="160" viewBox="0 0 160 160">\n  <defs>\n    <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">\n      <stop offset="0" stop-color="#2c5cff"/>\n      <stop offset="1" stop-color="#00c2ff"/>\n    </linearGradient>\n  </defs>\n  <rect x="0" y="0" width="160" height="160" rx="80" fill="url(#g)"/>\n  <text x="80" y="92" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial" font-size="56" font-weight="800" fill="#ffffff">${initials}</text>\n</svg>`;

  return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
}

export async function getCustomerProfile(): Promise<CustomerProfile & { avatarUrl: string }> {
  const fullName = "Alejandro Germán\nCabrera Medina".replace(/\n/g, " ");

  return {
    id: "cust-001",
    fullName,
    personType: "natural",
    age: 19,
    phone: "+55 11 99999-9999",
    email: "alejandro.cabrera@example.com",
    location: "São Paulo, BR",
    avatarUrl: makeAvatarDataUrl(fullName),
    products: [
      {
        id: "accounts",
        title: "Cuentas",
        primaryAmount: "BRL 54.400,00",
        secondaryAmount: "USD 500,00"
      },
      {
        id: "cards",
        title: "Tarjetas",
        primaryAmount: "USD 75.485,58"
      }
    ],
    priorities: [
      {
        id: "p1",
        title: "Actualizar datos de contacto",
        description: "Validar teléfono y correo para comunicaciones."
      },
      {
        id: "p2",
        title: "Ofertas personalizadas",
        description: "Revisar elegibilidad para productos premium."
      }
    ],
    history: [
      {
        id: "h1",
        title: "Login en app",
        description: "Acceso exitoso desde dispositivo móvil.",
        ts: "2026-01-21 19:12"
      },
      {
        id: "h2",
        title: "Actualización de dirección",
        description: "Cambió ciudad y estado.",
        ts: "2026-01-18 09:04"
      }
    ]
  };
}
