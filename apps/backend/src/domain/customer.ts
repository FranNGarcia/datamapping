export type FreeFields = {
  boolFreeFields?: Record<string, boolean>;
  integerFreeFields?: Record<string, number>;
  floatFreeFields?: Record<string, number>;
  stringFreeFields?: Record<string, string>;
};

export type CustomerCore = {
  identityType: string;
  identityNumber: string;
  coreId: string;
  isClient: boolean;
  isProspect: boolean;
  nationalityCode: string;
} & FreeFields;

export type NaturalCustomerData = {
  birthDate: string;
  firstName: string;
  secondName?: string;
  lastName: string;
  secondLastName?: string;
  homePhoneNumberList?: string[];
  workPhoneNumberList?: string[];
  mobilePhoneNumberList?: string[];
  emailList?: string[];
  overrideLists?: boolean;
  gender?: string;
  maritalStatus?: string;
};

export type LegalCustomerData = {
  businessName: string;
  companyDate: string;
  homePhoneNumberList?: string[];
  workPhoneNumberList?: string[];
  mobilePhoneNumberList?: string[];
  emailList?: string[];
};

export type NaturalCustomer = CustomerCore & {
  customerType: "natural";
  naturalData: NaturalCustomerData;
};

export type LegalCustomer = CustomerCore & {
  customerType: "legal";
  legalData: LegalCustomerData;
};

export type Address = {
  customerIdentityNumber: string;
  country: string;
  city: string;
  street: string;
  streetNumber?: string;
  floor?: string;
  flat?: string;
  zipCode?: string;
  state?: string;
  neighborhood?: string;
  type?: string;
  region?: string;
};

export type Kyc = {
  customerIdentityNumber: string;
  state: string;
  declarationDate?: string;
  effectiveDate?: string;
  obligatedSubject?: string;
  isPep?: boolean;
  isFATCA?: boolean;
  activity?: string;
  salaryRange?: string;
  taxIdentificationKey?: string;
  identificationKey?: string;
};

export type PersonalRelationships = FreeFields;

export type BusinessRelationships = FreeFields;

export type Customer = (NaturalCustomer | LegalCustomer) & {
  addresses?: Address[];
  kyc?: Kyc;
  personalRelationships?: PersonalRelationships;
  businessRelationships?: BusinessRelationships;
};
