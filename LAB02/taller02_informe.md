# Taller 2 — ETL de Adventure Works
**Bases de Datos Avanzadas · Informe de Desarrollo**

---

## 1. Introducción

El presente taller construye un pipeline ETL (*Extract, Transform, Load*) sobre la base de datos operacional AdventureWorks —una empresa multinacional de fabricación y venta de bicicletas— con el objetivo de trasladar sus datos transaccionales a un Data Warehouse OLAP en PostgreSQL. Sobre ese DW se levanta una aplicación web Flask que responde cuatro preguntas de negocio mediante consultas SQLAlchemy ORM.

---

## 2. Arquitectura del Sistema

El sistema se compone de tres contenedores Docker/Podman orquestados mediante `compose.yaml`:

| Contenedor | Imagen | Puerto | Rol |
|---|---|---|---|
| `lab02_oltp_postgres` | postgres:16 | 5434 | Base operacional OLTP |
| `lab02_olap_postgres` | postgres:16 | 5433 | Data Warehouse OLAP |
| `lab02_etl_web`       | python:3.11 | 8001 | ETL + Flask Web App |

El flujo de datos es unidireccional: el proceso ETL lee del OLTP, transforma los datos y los carga en el OLAP. La aplicación web consulta exclusivamente el OLAP.

```
OLTP (PostgreSQL) ──► ETL Python ──► OLAP / DW (PostgreSQL) ──► Flask App
```

---

## 3. Modelo de Datos OLTP

Se extrajeron únicamente las tablas necesarias para las cuatro preguntas de negocio, organizadas en tres esquemas PostgreSQL:

- **`person`**: `country_region`, `state_province`, `address`, `person`, `business_entity_address`
- **`production`**: `product_category`, `product_subcategory`, `product`, `product_cost_history`
- **`sales`**: `sales_territory`, `customer`, `sales_order_header`, `sales_order_detail`

Las relaciones clave son: `customer → person`, `sales_order_header → customer`, `sales_order_detail → product` y `product → product_subcategory → product_category`.

---

## 4. Diseño del Data Warehouse (Star Schema)

Se eligió un **star schema** sobre snowflake por su simplicidad de consulta y mayor rendimiento analítico. Las dimensiones están desnormalizadas para reducir JOINs.

### 4.1 Dimensiones

| Tabla | Descripción |
|---|---|
| `dim_date` | Calendario 2011–2015 con atributos de año fiscal, trimestre, día de semana |
| `dim_customer` | Clientes con atributos de cohorte (`cohort_key`, `cohort_year`, `cohort_month`) |
| `dim_product` | Productos con categoría, subcategoría, rango de precio, flags `is_bike` / `is_accessory` |
| `dim_territory` | Territorios de venta con país y grupo de región |

### 4.2 Tablas de Hechos

- **`fact_sales`** (granularidad: línea de detalle de orden): almacena `line_total`, `cost_total`, `gross_margin` y `gross_margin_pct` por producto y orden.
- **`fact_orders`** (granularidad: cabecera de orden): almacena `total_due`, `is_first_order`, `is_recurring` y `months_since_first` para análisis de comportamiento de cliente.

### 4.3 Tablas de Agregación Pre-calculadas

Para optimizar los tiempos de respuesta de la web app, cuatro agregaciones se calculan durante el ETL:

| Tabla | Propósito |
|---|---|
| `agg_customer_recurrence` | Ingresos por tipo de cliente (recurrente vs primera compra) por año-trimestre |
| `agg_product_margin` | Margen mensual por producto |
| `agg_market_basket` | Co-ocurrencias de pares de productos en la misma orden |
| `agg_cohort_retention` | Retención, ingresos y margen por cohorte y periodo |

---

## 5. Proceso ETL

El ETL sigue la estructura clásica en tres fases, implementadas en módulos independientes:

### 5.1 Extracción (`src/extract/`)

`SQLExtractor` hereda de `ExtractorBase` (clase abstracta) y ejecuta queries SQL sobre el OLTP mediante SQLAlchemy. Los resultados se devuelven en *batches* de 1000 registros para controlar el uso de memoria.

### 5.2 Transformación (`src/transform/`)

Funciones puras (sin efectos secundarios) que convierten cada fila del OLTP al formato del DW:

- `transform_date(d)` — genera todos los atributos de `dim_date` a partir de un objeto `date`.
- `transform_product(row)` — desnormaliza categoría, subcategoría y clasifica rango de precio.
- `transform_customer(row, first_order_date)` — calcula `cohort_key` y atributos de cohorte.
- `transform_fact_sales(row, ...)` — calcula `line_total`, `cost_total`, `gross_margin` y `gross_margin_pct`.
- `transform_fact_orders(row, ...)` — determina `is_first_order`, `is_recurring` y `months_since_first`.

### 5.3 Carga (`src/load/`)

Implementa *upsert* (`INSERT ... ON CONFLICT DO UPDATE`) para dimensiones y *full refresh* (truncate + insert) para hechos. Las cuatro tablas de agregación se calculan directamente con SQL en el OLAP tras cargar los hechos.

### 5.4 Pipelines y Orquestación

Dos pipelines coordinan el flujo:

- **`CustomerPipeline`**: carga `dim_customer` y calcula `agg_cohort_retention` y `agg_customer_recurrence`.
- **`SalesPipeline`**: carga dimensiones (fecha, territorio, producto), `fact_sales`, `fact_orders`, `agg_market_basket` y `agg_product_margin`.

`src/main.py` los orquesta en orden correcto: primero `dim_customer`, luego los hechos de ventas, y finalmente las agregaciones de cliente que dependen de los hechos.

---

## 6. Preguntas de Negocio

### P1 — Ingresos por Clientes Recurrentes vs No-Recurrentes

**Definición:** cliente recurrente = más de una compra dentro del mismo año-trimestre.

La tabla `agg_customer_recurrence` agrupa por `(year, quarter, customer_type)` y pre-calcula `total_revenue` y `revenue_pct`. La web app consulta esta tabla y muestra la distribución de ingresos entre ambos segmentos. Si los clientes recurrentes representan un porcentaje de ingresos significativamente mayor que su proporción en cantidad, justifica inversión en programas de fidelización.

### P2 — Productos con Mayor Varianza en Margen

**Margen** = `(line_total − cost_total) / line_total × 100`

`agg_product_margin` almacena el margen mensual por producto. La consulta de la web app calcula `STDDEV(margin_pct)` sobre los periodos de cada producto. Alta desviación estándar indica riesgo para compras de inventario navideño: el margen puede ser alto un mes y deteriorarse por descuentos o cambios de costo. La gerente de compras debe preferir productos de varianza baja con margen consistentemente positivo.

### P3 — Análisis de Canasta (Market Basket)

El ETL realiza un auto-join de `fact_sales` sobre `sales_order_id` para encontrar todos los pares de productos comprados en la misma orden:

```sql
SELECT LEAST(a.product_key, b.product_key)    AS product_key_a,
       GREATEST(a.product_key, b.product_key) AS product_key_b,
       COUNT(DISTINCT a.sales_order_id)        AS co_occurrences
FROM dw.fact_sales a
JOIN dw.fact_sales b ON a.sales_order_id = b.sales_order_id
                     AND a.product_key  <> b.product_key
GROUP BY 1, 2
```

El uso de `LEAST/GREATEST` garantiza que cada par aparezca una sola vez. El resultado se almacena en `agg_market_basket` con la métrica de **soporte** (`co_occurrences / total_orders`). Los 10 pares con mayor co-ocurrencia son candidatos a combos, recomendaciones automáticas o ubicación contigua en tienda.

### P4 — Análisis de Cohortes

Una cohorte agrupa los clientes que realizaron su primera compra en el mismo año-mes (`cohort_key = 'YYYY-MM'`). El campo `months_since_first` en `fact_orders` indica cuántos meses después de la primera compra ocurrió cada orden.

`agg_cohort_retention` pre-calcula por cohorte y periodo: `customer_count`, `retention_rate` (activos / iniciales), `total_revenue`, `total_margin` y `avg_revenue_per_customer`. Las tres cohortes con mayor margen acumulado se presentan en la web app con su tabla de retención mensual. El **LTV proyectado** de una campaña en un mes dado se puede estimar como:

```
LTV ≈ avg_revenue_per_customer[0] × Σ retention_rate[N]  para N = 0..T
```

---

## 7. Aplicación Web

La aplicación Flask (`app/app.py`) replica el estilo visual del Taller 1. Expone las rutas:

- `GET /` — interfaz principal con las cuatro secciones
- `GET /api/query/<q1|q2|q3|q4>` — ejecuta la consulta y retorna HTML para renderizar
- `POST /api/run-etl` — dispara `src/main.py` como subproceso
- `GET /health` — verifica conectividad con el DW
- `GET /api/stats` — retorna conteo de registros por tabla

Todas las consultas a la base de datos usan **SQLAlchemy ORM** o consultas SQL ejecutadas a través del engine de SQLAlchemy, sin SQL crudo directo en las rutas Flask.

---

## 8. Decisiones de Diseño

- **Star schema sobre snowflake:** elimina JOINs adicionales en consultas analíticas. El costo es redundancia en dimensiones, aceptable dado el tamaño del DW.
- **Agregaciones pre-calculadas:** market basket y cohortes son computacionalmente costosas. Pre-calcularlas en el ETL garantiza tiempos de respuesta < 100ms en la web app.
- **Full refresh para hechos:** dada la naturaleza del taller (datos estáticos), se optó por truncar y recargar en lugar de carga incremental. En producción se implementaría carga incremental por `order_date`.
- **`months_since_first` en `fact_orders`:** almacenar este valor calculado en la tabla de hechos simplifica enormemente las consultas de cohortes y evita subconsultas costosas en tiempo de ejecución.

---

## 9. Conclusiones

El taller demuestra el flujo completo de un proyecto de Data Warehouse: desde el diseño del modelo dimensional, pasando por un ETL modular y extensible en Python, hasta la presentación de resultados analíticos en una aplicación web. Las cuatro preguntas de negocio —retención de clientes, varianza de margen, análisis de canasta y cohortes— representan patrones analíticos fundamentales que justifican la existencia de un DW separado del sistema transaccional, tanto por la complejidad de las consultas como por la necesidad de no impactar el rendimiento del OLTP.
