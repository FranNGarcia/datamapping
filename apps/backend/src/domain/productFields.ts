export const PRODUCT_AGGREGATE_FIELD_PATHS = [
  "customerId",
  "amount",
  "currency",
  "expiration",
  "productId",
  "name",
  "type",
  "status"
] as const;

export const PRODUCT_AGGREGATE_PRIMARY_KEY_FIELD_PATHS = ["productId"] as const;

export type ProductAggregateFieldPath = (typeof PRODUCT_AGGREGATE_FIELD_PATHS)[number];

export type ProductAggregatePrimaryKeyFieldPath =
  (typeof PRODUCT_AGGREGATE_PRIMARY_KEY_FIELD_PATHS)[number];

export function getProductAggregateFieldPaths(): string[] {
  return [...PRODUCT_AGGREGATE_FIELD_PATHS];
}

export function getProductAggregatePrimaryKeyFieldPaths(): string[] {
  return [...PRODUCT_AGGREGATE_PRIMARY_KEY_FIELD_PATHS];
}
