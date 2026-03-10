# Diagrama del Data Warehouse — Lab 02

## Modelo Star Schema

```mermaid
erDiagram
    dim_date {
        int date_key PK
        date full_date
        smallint year
        smallint quarter
        smallint month
        varchar month_name
        boolean is_weekend
        smallint fiscal_year
        smallint fiscal_quarter
    }

    dim_customer {
        int customer_key PK
        int customer_id
        varchar account_number
        varchar full_name
        int territory_id
        varchar territory_name
        varchar region_group
        varchar country_code
        date first_order_date
        smallint cohort_year
        smallint cohort_month
        char cohort_key
    }

    dim_product {
        int product_key PK
        int product_id
        varchar product_number
        varchar product_name
        numeric list_price
        numeric standard_cost
        varchar price_range
        varchar subcategory_name
        varchar category_name
        boolean is_bike
        boolean is_accessory
    }

    dim_territory {
        int territory_key PK
        int territory_id
        varchar territory_name
        varchar country_code
        varchar region_group
    }

    fact_sales {
        bigint sales_key PK
        int date_key FK
        int customer_key FK
        int product_key FK
        int territory_key FK
        int sales_order_id
        smallint order_qty
        numeric line_total
        numeric cost_total
        numeric gross_margin
        numeric gross_margin_pct
        boolean is_online
    }

    fact_orders {
        bigint order_key PK
        int date_key FK
        int customer_key FK
        int territory_key FK
        int sales_order_id
        numeric total_due
        int line_count
        int customer_order_number
        boolean is_first_order
        boolean is_recurring
        int months_since_first
    }

    agg_market_basket {
        bigint basket_key PK
        int product_key_a FK
        int product_key_b FK
        int co_occurrences
        numeric support
    }

    agg_cohort_retention {
        char cohort_key PK
        int period_number PK
        int customer_count
        int initial_customers
        numeric retention_rate
        numeric total_revenue
        numeric total_margin
    }

    agg_product_margin {
        int product_key PK
        smallint year PK
        smallint month PK
        numeric total_revenue
        numeric total_cost
        numeric total_margin
        numeric margin_pct
    }

    agg_customer_recurrence {
        smallint year PK
        smallint quarter PK
        varchar customer_type PK
        int customer_count
        numeric total_revenue
        numeric revenue_pct
    }

    dim_date      ||--o{ fact_sales : "date_key"
    dim_customer  ||--o{ fact_sales : "customer_key"
    dim_product   ||--o{ fact_sales : "product_key"
    dim_territory ||--o{ fact_sales : "territory_key"

    dim_date      ||--o{ fact_orders : "date_key"
    dim_customer  ||--o{ fact_orders : "customer_key"
    dim_territory ||--o{ fact_orders : "territory_key"

    dim_product   ||--o{ agg_market_basket : "product_key_a"
    dim_product   ||--o{ agg_market_basket : "product_key_b"
    dim_product   ||--o{ agg_product_margin : "product_key"
```

---

## Decisiones de Diseño

### ¿Por qué Star Schema?
Se eligió star schema sobre snowflake por su simplicidad de consulta y rendimiento. Las dimensiones están desnormalizadas (p.ej., `category_name` incluido en `dim_product` en vez de tener una tabla separada), lo que reduce los JOINs necesarios.

### Tablas de Hechos
- **`fact_sales`** (granularidad: línea de detalle) — para análisis de margen por producto y market basket.
- **`fact_orders`** (granularidad: cabecera de orden) — para análisis de clientes recurrentes y cohortes.

### Tablas de Agregación Pre-calculadas
Para las preguntas complejas (market basket, cohortes), se pre-calculan los resultados en el ETL y se almacenan en tablas `agg_*`. Esto permite que la web app responda en milisegundos sin ejecutar queries costosos en tiempo real.

### Dimensión Fecha
Generada sintéticamente en el ETL para el rango 2011-2015, con atributos de año fiscal (inicio julio) para análisis por temporada.

### Cohortes en `dim_customer`
Los campos `cohort_key`, `cohort_year`, `cohort_month` se almacenan directamente en la dimensión cliente para facilitar los JOINs de cohorte sin subconsultas.
