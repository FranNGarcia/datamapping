export const CUSTOMER_AGGREGATE_FIELD_PATHS = [
  "identityType",
  "identityNumber",
  "coreId",
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
] as const;

export const CUSTOMER_AGGREGATE_PRIMARY_KEY_FIELD_PATHS = ["identityNumber"] as const;

export type CustomerAggregateFieldPath = (typeof CUSTOMER_AGGREGATE_FIELD_PATHS)[number];

export type CustomerAggregatePrimaryKeyFieldPath =
  (typeof CUSTOMER_AGGREGATE_PRIMARY_KEY_FIELD_PATHS)[number];

export function getCustomerAggregateFieldPaths(): string[] {
  return [...CUSTOMER_AGGREGATE_FIELD_PATHS];
}

export function getCustomerAggregatePrimaryKeyFieldPaths(): string[] {
  return [...CUSTOMER_AGGREGATE_PRIMARY_KEY_FIELD_PATHS];
}
