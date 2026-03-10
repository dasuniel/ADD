-- ============================================================
-- AdventureWorks OLAP Schema - Star Schema
-- Data Warehouse para responder las 4 preguntas de negocio
-- ============================================================

DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

-- ============================================================
-- DIMENSIONES
-- ============================================================

-- Dimensión Tiempo (granularidad diaria)
CREATE TABLE dw.dim_date (
    date_key        INT PRIMARY KEY,          -- YYYYMMDD
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,        -- 1-4
    month           SMALLINT NOT NULL,        -- 1-12
    month_name      VARCHAR(15) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_month    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,        -- 1=Sunday
    day_name        VARCHAR(15) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    fiscal_year     SMALLINT NOT NULL,
    fiscal_quarter  SMALLINT NOT NULL
);

-- Dimensión Cliente (SCD Type 1)
CREATE TABLE dw.dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL UNIQUE,
    account_number  VARCHAR(10) NOT NULL,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    full_name       VARCHAR(101),
    territory_id    INT,
    territory_name  VARCHAR(50),
    region_group    VARCHAR(50),
    country_code    VARCHAR(3),
    -- Para análisis de cohortes
    first_order_date DATE,
    cohort_year     SMALLINT,
    cohort_month    SMALLINT,
    cohort_key      CHAR(7),                  -- 'YYYY-MM'
    -- SCD
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Dimensión Producto
CREATE TABLE dw.dim_product (
    product_key            SERIAL PRIMARY KEY,
    product_id             INT NOT NULL UNIQUE,
    product_number         VARCHAR(25) NOT NULL,
    product_name           VARCHAR(50) NOT NULL,
    color                  VARCHAR(15),
    size                   VARCHAR(5),
    list_price             NUMERIC(19,4),
    standard_cost          NUMERIC(19,4),
    -- Precio relativo (para categorizar)
    price_range            VARCHAR(20),       -- 'Low', 'Mid', 'High'
    subcategory_id         INT,
    subcategory_name       VARCHAR(50),
    category_id            INT,
    category_name          VARCHAR(50),
    -- Para análisis
    is_bike                BOOLEAN NOT NULL DEFAULT FALSE,
    is_accessory           BOOLEAN NOT NULL DEFAULT FALSE,
    -- SCD
    created_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Dimensión Territorio
CREATE TABLE dw.dim_territory (
    territory_key       SERIAL PRIMARY KEY,
    territory_id        INT NOT NULL UNIQUE,
    territory_name      VARCHAR(50) NOT NULL,
    country_code        VARCHAR(3) NOT NULL,
    region_group        VARCHAR(50) NOT NULL
);

-- ============================================================
-- TABLAS DE HECHOS
-- ============================================================

-- Fact principal: Ventas (granularidad: línea de detalle de orden)
CREATE TABLE dw.fact_sales (
    sales_key               BIGSERIAL PRIMARY KEY,
    -- Claves foráneas a dimensiones
    date_key                INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_key            INT NOT NULL REFERENCES dw.dim_customer(customer_key),
    product_key             INT NOT NULL REFERENCES dw.dim_product(product_key),
    territory_key           INT NOT NULL REFERENCES dw.dim_territory(territory_key),
    -- Claves naturales (para trazabilidad)
    sales_order_id          INT NOT NULL,
    sales_order_detail_id   INT NOT NULL,
    -- Métricas aditivas
    order_qty               SMALLINT NOT NULL,
    unit_price              NUMERIC(19,4) NOT NULL,
    unit_price_discount     NUMERIC(19,4) NOT NULL,
    standard_cost           NUMERIC(19,4) NOT NULL,
    line_total              NUMERIC(19,4) NOT NULL,     -- ingreso neto de la línea
    cost_total              NUMERIC(19,4) NOT NULL,     -- costo total de la línea
    gross_margin            NUMERIC(19,4) NOT NULL,     -- line_total - cost_total
    gross_margin_pct        NUMERIC(8,4),               -- gross_margin / line_total * 100
    -- Flags analíticos
    is_online               BOOLEAN NOT NULL DEFAULT FALSE,
    -- Timestamp de carga ETL
    etl_loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Fact: Órdenes (granularidad: cabecera de orden)
-- Para análisis de clientes recurrentes y cohortes
CREATE TABLE dw.fact_orders (
    order_key               BIGSERIAL PRIMARY KEY,
    -- Claves foráneas
    date_key                INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_key            INT NOT NULL REFERENCES dw.dim_customer(customer_key),
    territory_key           INT NOT NULL REFERENCES dw.dim_territory(territory_key),
    -- Clave natural
    sales_order_id          INT NOT NULL UNIQUE,
    -- Métricas
    sub_total               NUMERIC(19,4) NOT NULL,
    tax_amt                 NUMERIC(19,4) NOT NULL,
    freight                 NUMERIC(19,4) NOT NULL,
    total_due               NUMERIC(19,4) NOT NULL,
    line_count              INT NOT NULL,               -- cantidad de productos distintos
    -- Calculados
    customer_order_number   INT,                        -- Qué orden es para este cliente (1=primera, 2=segunda...)
    is_first_order          BOOLEAN NOT NULL DEFAULT FALSE,
    is_recurring            BOOLEAN NOT NULL DEFAULT FALSE, -- cliente ya había comprado antes
    months_since_first      INT,                        -- meses desde su primera compra
    -- ETL
    etl_loaded_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================
-- TABLAS DE AGREGACIÓN (pre-calculadas para performance)
-- ============================================================

-- Análisis de canasta - pares de productos comprados juntos
CREATE TABLE dw.agg_market_basket (
    basket_key      BIGSERIAL PRIMARY KEY,
    product_key_a   INT NOT NULL REFERENCES dw.dim_product(product_key),
    product_key_b   INT NOT NULL REFERENCES dw.dim_product(product_key),
    co_occurrences  INT NOT NULL,              -- veces comprados juntos
    support         NUMERIC(8,6),              -- co_occurrences / total_orders
    -- ETL
    etl_loaded_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_basket_pair UNIQUE (product_key_a, product_key_b),
    CONSTRAINT chk_product_order CHECK (product_key_a < product_key_b)
);

-- Análisis de cohortes - retención mensual
CREATE TABLE dw.agg_cohort_retention (
    cohort_key          CHAR(7) NOT NULL,      -- 'YYYY-MM' de la primera compra
    cohort_year         SMALLINT NOT NULL,
    cohort_month        SMALLINT NOT NULL,
    period_number       INT NOT NULL,          -- 0=mes adquisición, 1=mes+1, etc.
    active_period_key   CHAR(7) NOT NULL,      -- 'YYYY-MM' del periodo activo
    customer_count      INT NOT NULL,
    initial_customers   INT NOT NULL,          -- clientes en periodo 0
    retention_rate      NUMERIC(8,4),          -- customer_count / initial_customers
    total_revenue       NUMERIC(19,4),
    total_margin        NUMERIC(19,4),
    avg_revenue_per_customer NUMERIC(19,4),
    -- ETL
    etl_loaded_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cohort_key, period_number)
);

-- Margen por producto y periodo (para pregunta 2)
CREATE TABLE dw.agg_product_margin (
    product_key         INT NOT NULL REFERENCES dw.dim_product(product_key),
    year                SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    total_qty           INT NOT NULL,
    total_revenue       NUMERIC(19,4) NOT NULL,
    total_cost          NUMERIC(19,4) NOT NULL,
    total_margin        NUMERIC(19,4) NOT NULL,
    margin_pct          NUMERIC(8,4),
    -- ETL
    etl_loaded_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_key, year, month)
);

-- Resumen de clientes recurrentes vs no-recurrentes por periodo
CREATE TABLE dw.agg_customer_recurrence (
    year                SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL,
    customer_type       VARCHAR(20) NOT NULL, -- 'Recurring', 'One-Time'
    customer_count      INT NOT NULL,
    order_count         INT NOT NULL,
    total_revenue       NUMERIC(19,4) NOT NULL,
    revenue_pct         NUMERIC(8,4),         -- % del total del periodo
    -- ETL
    etl_loaded_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year, quarter, customer_type)
);

-- ============================================================
-- ÍNDICES para optimizar las consultas analíticas
-- ============================================================

-- fact_sales
CREATE INDEX idx_fs_date_key       ON dw.fact_sales(date_key);
CREATE INDEX idx_fs_customer_key   ON dw.fact_sales(customer_key);
CREATE INDEX idx_fs_product_key    ON dw.fact_sales(product_key);
CREATE INDEX idx_fs_territory_key  ON dw.fact_sales(territory_key);
CREATE INDEX idx_fs_order_id       ON dw.fact_sales(sales_order_id);

-- fact_orders
CREATE INDEX idx_fo_date_key       ON dw.fact_orders(date_key);
CREATE INDEX idx_fo_customer_key   ON dw.fact_orders(customer_key);
CREATE INDEX idx_fo_is_first       ON dw.fact_orders(is_first_order);
CREATE INDEX idx_fo_is_recurring   ON dw.fact_orders(is_recurring);

-- dim_customer
CREATE INDEX idx_dc_cohort_key     ON dw.dim_customer(cohort_key);
CREATE INDEX idx_dc_territory      ON dw.dim_customer(territory_id);

-- dim_date
CREATE INDEX idx_dd_year_month     ON dw.dim_date(year, month);

-- agg_market_basket
CREATE INDEX idx_mb_product_a      ON dw.agg_market_basket(product_key_a);
CREATE INDEX idx_mb_product_b      ON dw.agg_market_basket(product_key_b);
CREATE INDEX idx_mb_occurrences    ON dw.agg_market_basket(co_occurrences DESC);

-- agg_product_margin
CREATE INDEX idx_pm_year_month     ON dw.agg_product_margin(year, month);
