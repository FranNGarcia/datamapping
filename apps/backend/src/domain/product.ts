export type ProductType =
  | "ACCOUNT"
  | "CARD"
  | "INVESTMENT"
  | "CREDIT"
  | "INSURANCE"
  | "OTHER";

export type Product = {
  customerId: string;
  amount: number;
  currency: string;
  expiration: string;
  productId: string;
  name: string;
  type: ProductType;
  status: string;
};
