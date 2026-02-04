export type DatasourceQueryRequest = {
  id: string;
  config?: unknown;
  query: string;
  params?: unknown;
  limit?: number;
};

export type DatasourceQueryResponse = {
  ok: boolean;
  id: string;
  columns?: string[];
  rows?: unknown[];
  error?: string;
};
