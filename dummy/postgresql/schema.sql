-- Tipo de persona
CREATE TYPE person_type AS ENUM ('INDIVIDUAL', 'COMPANY');

-- Estado del registro
CREATE TYPE record_status AS ENUM ('ACTIVE', 'INACTIVE', 'BLOCKED');

-- Tipo de cliente
CREATE TYPE customer_type AS ENUM ('CLIENT', 'PROSPECT');

CREATE TABLE person (
    person_id          BIGSERIAL PRIMARY KEY,
    person_type        person_type NOT NULL,
    identification_type VARCHAR(10) NOT NULL, -- DNI, CUIT, PASSPORT
    identification_number VARCHAR(30) NOT NULL,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    company_name       VARCHAR(200),
    birth_date         DATE,
    nationality        VARCHAR(50),
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (identification_type, identification_number)
);

CREATE TABLE customer (
    customer_id    BIGSERIAL PRIMARY KEY,
    person_id      BIGINT NOT NULL REFERENCES person(person_id),
    customer_type  customer_type NOT NULL,
    status         record_status NOT NULL DEFAULT 'ACTIVE',
    onboarding_date DATE,
    risk_score     NUMERIC(5,2),
    segment        VARCHAR(50), -- Retail, Wealth, Corporate
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE contact_info (
    contact_id  BIGSERIAL PRIMARY KEY,
    person_id   BIGINT NOT NULL REFERENCES person(person_id),
    email       VARCHAR(150),
    phone       VARCHAR(30),
    address     TEXT,
    city        VARCHAR(100),
    country     VARCHAR(50),
    is_primary  BOOLEAN DEFAULT FALSE
);

CREATE TABLE financial_product (
    product_id   BIGSERIAL PRIMARY KEY,
    product_code VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    product_type VARCHAR(50), -- Account, Credit, Investment
    is_active    BOOLEAN DEFAULT TRUE
);

CREATE TABLE customer_product (
    customer_product_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    product_id  BIGINT NOT NULL REFERENCES financial_product(product_id),
    start_date  DATE NOT NULL,
    end_date    DATE,
    balance     NUMERIC(18,2),
    status      record_status DEFAULT 'ACTIVE'
);

CREATE TABLE interaction (
    interaction_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customer(customer_id),
    interaction_type VARCHAR(50), -- Call, Email, Meeting
    channel VARCHAR(50), -- Branch, Web, Phone
    notes TEXT,
    interaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE campaign (
    campaign_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100),
    start_date DATE,
    end_date DATE
);

CREATE TABLE campaign_customer (
    campaign_id BIGINT REFERENCES campaign(campaign_id),
    customer_id BIGINT REFERENCES customer(customer_id),
    status VARCHAR(30), -- Contacted, Interested, Rejected
    PRIMARY KEY (campaign_id, customer_id)
);

CREATE INDEX idx_person_identification 
ON person (identification_type, identification_number);

CREATE INDEX idx_customer_type 
ON customer (customer_type);

CREATE INDEX idx_customer_status 
ON customer (status);


-- What to enter in your UI
-- Host: aws-1-us-east-2.pooler.supabase.com
-- Port: 5432
-- Database: postgres
-- User: postgres.pykjkwebujltcxiigmpk
-- Password: your DB password
-- SSL: ON