-- ============================================================
-- AdventureWorks OLTP Schema - Traducido a PostgreSQL
-- Solo tablas necesarias para las 4 preguntas de negocio
-- ============================================================

-- Limpiar esquemas si existen
DROP SCHEMA IF EXISTS sales CASCADE;
DROP SCHEMA IF EXISTS production CASCADE;
DROP SCHEMA IF EXISTS person CASCADE;

-- Crear esquemas
CREATE SCHEMA person;
CREATE SCHEMA production;
CREATE SCHEMA sales;

-- ============================================================
-- ESQUEMA: person
-- ============================================================

CREATE TABLE person.address_type (
    address_type_id SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL,
    modified_date   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE person.country_region (
    country_region_code VARCHAR(3) PRIMARY KEY,
    name                VARCHAR(50) NOT NULL,
    modified_date       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE person.state_province (
    state_province_id   SERIAL PRIMARY KEY,
    state_province_code CHAR(3) NOT NULL,
    country_region_code VARCHAR(3) NOT NULL REFERENCES person.country_region(country_region_code),
    is_only_state_province_flag BOOLEAN NOT NULL DEFAULT FALSE,
    name                VARCHAR(50) NOT NULL,
    territory_id        INT NOT NULL,
    modified_date       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE person.address (
    address_id       SERIAL PRIMARY KEY,
    address_line1    VARCHAR(60) NOT NULL,
    address_line2    VARCHAR(60),
    city             VARCHAR(30) NOT NULL,
    state_province_id INT NOT NULL REFERENCES person.state_province(state_province_id),
    postal_code      VARCHAR(15) NOT NULL,
    modified_date    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE person.person (
    business_entity_id SERIAL PRIMARY KEY,
    person_type        CHAR(2) NOT NULL,
    first_name         VARCHAR(50) NOT NULL,
    middle_name        VARCHAR(50),
    last_name          VARCHAR(50) NOT NULL,
    email_promotion    INT NOT NULL DEFAULT 0,
    modified_date      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE person.business_entity_address (
    business_entity_id INT NOT NULL REFERENCES person.person(business_entity_id),
    address_id         INT NOT NULL REFERENCES person.address(address_id),
    address_type_id    INT NOT NULL REFERENCES person.address_type(address_type_id),
    modified_date      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_entity_id, address_id, address_type_id)
);

-- ============================================================
-- ESQUEMA: production
-- ============================================================

CREATE TABLE production.product_category (
    product_category_id SERIAL PRIMARY KEY,
    name                VARCHAR(50) NOT NULL,
    modified_date       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE production.product_subcategory (
    product_subcategory_id SERIAL PRIMARY KEY,
    product_category_id    INT NOT NULL REFERENCES production.product_category(product_category_id),
    name                   VARCHAR(50) NOT NULL,
    modified_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE production.product (
    product_id             SERIAL PRIMARY KEY,
    name                   VARCHAR(50) NOT NULL,
    product_number         VARCHAR(25) NOT NULL UNIQUE,
    make_flag              BOOLEAN NOT NULL DEFAULT TRUE,
    finished_goods_flag    BOOLEAN NOT NULL DEFAULT TRUE,
    color                  VARCHAR(15),
    safety_stock_level     SMALLINT NOT NULL,
    reorder_point          SMALLINT NOT NULL,
    standard_cost          NUMERIC(19,4) NOT NULL,
    list_price             NUMERIC(19,4) NOT NULL,
    size                   VARCHAR(5),
    size_unit_measure_code CHAR(3),
    weight                 NUMERIC(8,2),
    days_to_manufacture    INT NOT NULL,
    product_subcategory_id INT REFERENCES production.product_subcategory(product_subcategory_id),
    sell_start_date        TIMESTAMP NOT NULL,
    sell_end_date          TIMESTAMP,
    discontinued_date      TIMESTAMP,
    modified_date          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE production.product_cost_history (
    product_id     INT NOT NULL REFERENCES production.product(product_id),
    start_date     TIMESTAMP NOT NULL,
    end_date       TIMESTAMP,
    standard_cost  NUMERIC(19,4) NOT NULL,
    modified_date  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, start_date)
);

CREATE TABLE production.product_list_price_history (
    product_id    INT NOT NULL REFERENCES production.product(product_id),
    start_date    TIMESTAMP NOT NULL,
    end_date      TIMESTAMP,
    list_price    NUMERIC(19,4) NOT NULL,
    modified_date TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, start_date)
);

-- ============================================================
-- ESQUEMA: sales
-- ============================================================

CREATE TABLE sales.sales_territory (
    territory_id  SERIAL PRIMARY KEY,
    name          VARCHAR(50) NOT NULL,
    country_region_code VARCHAR(3) NOT NULL REFERENCES person.country_region(country_region_code),
    "group"       VARCHAR(50) NOT NULL,
    sales_ytd     NUMERIC(19,4) NOT NULL DEFAULT 0,
    sales_last_year NUMERIC(19,4) NOT NULL DEFAULT 0,
    modified_date TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE sales.customer (
    customer_id        SERIAL PRIMARY KEY,
    person_id          INT REFERENCES person.person(business_entity_id),
    store_id           INT,
    territory_id       INT REFERENCES sales.sales_territory(territory_id),
    account_number     VARCHAR(10) NOT NULL,
    modified_date      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE sales.sales_order_header (
    sales_order_id           SERIAL PRIMARY KEY,
    revision_number          SMALLINT NOT NULL DEFAULT 0,
    order_date               TIMESTAMP NOT NULL,
    due_date                 TIMESTAMP NOT NULL,
    ship_date                TIMESTAMP,
    status                   SMALLINT NOT NULL DEFAULT 1,
    online_order_flag        BOOLEAN NOT NULL DEFAULT TRUE,
    purchase_order_number    VARCHAR(25),
    account_number           VARCHAR(15),
    customer_id              INT NOT NULL REFERENCES sales.customer(customer_id),
    sales_person_id          INT,
    territory_id             INT REFERENCES sales.sales_territory(territory_id),
    bill_to_address_id       INT REFERENCES person.address(address_id),
    ship_to_address_id       INT REFERENCES person.address(address_id),
    sub_total                NUMERIC(19,4) NOT NULL DEFAULT 0,
    tax_amt                  NUMERIC(19,4) NOT NULL DEFAULT 0,
    freight                  NUMERIC(19,4) NOT NULL DEFAULT 0,
    total_due                NUMERIC(19,4) NOT NULL DEFAULT 0,
    comment                  VARCHAR(128),
    modified_date            TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE sales.sales_order_detail (
    sales_order_id          INT NOT NULL REFERENCES sales.sales_order_header(sales_order_id),
    sales_order_detail_id   SERIAL,
    carrier_tracking_number VARCHAR(25),
    order_qty               SMALLINT NOT NULL,
    product_id              INT NOT NULL REFERENCES production.product(product_id),
    special_offer_id        INT NOT NULL DEFAULT 1,
    unit_price              NUMERIC(19,4) NOT NULL,
    unit_price_discount     NUMERIC(19,4) NOT NULL DEFAULT 0,
    line_total              NUMERIC(38,6) GENERATED ALWAYS AS (
                                order_qty * unit_price * (1 - unit_price_discount)
                            ) STORED,
    modified_date           TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sales_order_id, sales_order_detail_id)
);

-- ============================================================
-- ÍNDICES para mejorar performance del ETL
-- ============================================================

CREATE INDEX idx_soh_customer_id    ON sales.sales_order_header(customer_id);
CREATE INDEX idx_soh_order_date     ON sales.sales_order_header(order_date);
CREATE INDEX idx_soh_territory_id   ON sales.sales_order_header(territory_id);
CREATE INDEX idx_sod_product_id     ON sales.sales_order_detail(product_id);
CREATE INDEX idx_sod_sales_order_id ON sales.sales_order_detail(sales_order_id);
CREATE INDEX idx_product_subcat     ON production.product(product_subcategory_id);
CREATE INDEX idx_customer_person    ON sales.customer(person_id);
CREATE INDEX idx_customer_territory ON sales.customer(territory_id);
