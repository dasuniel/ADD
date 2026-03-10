# AdventureWorks ETL — Lab 02

Pipeline ETL completo de AdventureWorks: extrae datos de una base OLTP, transforma y carga un Data Warehouse OLAP (star schema), y expone una aplicación web Flask para análisis de negocio.

---

## Arquitectura

```
┌─────────────────┐      ETL Python       ┌─────────────────┐
│  OLTP (Postgres)│  ──────────────────►  │  OLAP (Postgres)│
│  Puerto: 5434   │  extract/transform/   │  Puerto: 5433   │
│  AdventureWorks │     load              │  DW Star Schema │
└─────────────────┘                       └─────────────────┘
                                                   │
                                          SQLAlchemy ORM
                                                   │
                                          ┌─────────────────┐
                                          │  Flask Web App  │
                                          │  Puerto: 8001   │
                                          └─────────────────┘
```

**3 contenedores:**
| Contenedor | Imagen | Puerto host | Propósito |
|---|---|---|---|
| `lab02_oltp_postgres` | postgres:16 | 5434 | Base operacional OLTP |
| `lab02_olap_postgres` | postgres:16 | 5433 | Data Warehouse OLAP |
| `lab02_etl_web`       | python:3.11 | 8001 | ETL + Flask Web App |

---

##  Estructura del Proyecto

```
lab02/
├── app/
│   ├── app.py              # Flask app con las 4 preguntas de negocio
│   └── Dockerfile
├── config/
│   ├── config.yaml         # Configuración general
│   ├── logging.yaml        # Configuración de logs
│   └── settings.py         # Lector de configuración
├── db/
│   ├── oltp/
│   │   ├── oltp_schema.sql # DDL del esquema operacional
│   │   └── oltp_data.sql   # Datos de ejemplo
│   └── olap/
│       └── olap_schema.sql # Star schema del DW
├── src/
│   ├── main.py             # Orquestador del ETL
│   ├── extract/            # Extractores del OLTP
│   ├── transform/          # Transformaciones
│   ├── load/               # Cargadores al OLAP
│   ├── models/             # ORM SQLAlchemy + Pydantic
│   ├── pipelines/          # Pipelines por dominio
│   └── utils/              # DB, logging, helpers
├── podman/
│   └── compose.yaml        # Orquestación de contenedores
├── .env                    # Variables de entorno
└── requirements.txt
```

---

##  Instalación y Ejecución

### Pre-requisitos
- Docker o Podman instalado
- Python 3.11+ (para desarrollo local)

### 1. Clonar / descomprimir el proyecto

```bash
cd lab02
```

### 2. Configurar variables de entorno

```bash
cp .env .env.local   # Si quieres personalizar
```

### 3. Levantar los contenedores

```bash
# Con Docker
docker compose -f podman/compose.yaml up -d --build

# Con Podman
podman-compose -f podman/compose.yaml up -d --build
```

Los contenedores de PostgreSQL cargan automáticamente el schema y los datos al iniciar (gracias a `/docker-entrypoint-initdb.d`).

### 4. Verificar que los contenedores estén corriendo

```bash
docker ps
# o
podman ps
```

### 5. Ejecutar el ETL

**Opción A — Desde la web app** (recomendado):
- Abre http://localhost:8001
- Clic en **"▶ Ejecutar ETL"**

**Opción B — Desde la terminal**:
```bash
docker exec lab02_etl_web python -m src.main
```

**Opción C — Desarrollo local**:
```bash
pip install -r requirements.txt
# Editar .env para que los hosts apunten a localhost
python -m src.main
```

### 6. Acceder a la aplicación web

- **Web App:** http://localhost:8001
- **Health check:** http://localhost:8001/health
- **Stats del DW:** http://localhost:8001/api/stats

---

## Preguntas de Negocio

### P1: Clientes Recurrentes vs No-Recurrentes
Mide qué porcentaje de los ingresos proviene de clientes que han comprado más de una vez dentro de un periodo. Tabla: `dw.agg_customer_recurrence`.

### P2: Varianza en Margen por Producto
Identifica los productos con mayor desviación estándar en su margen de ganancia a lo largo del tiempo. Útil para decisiones de compra navideña. Tabla: `dw.agg_product_margin`.

### P3: Market Basket Analysis
Encuentra los 10 pares de productos más comprados juntos en la misma transacción. Tabla: `dw.agg_market_basket`.

### P4: Análisis de Cohortes
Agrupa clientes por mes de primera compra y mide su retención y valor a largo plazo. Identifica las 3 mejores cohortes por margen. Tabla: `dw.agg_cohort_retention`.

---

## Modelo de Datos OLAP

**Star Schema — Tabla de hechos central `fact_sales`**

```
           dim_date ────────┐
                            │
dim_customer ───────── fact_sales ───── dim_product
                            │
           dim_territory ───┘
                            │
                       fact_orders
```

**Tablas de agregación pre-calculadas:**
- `agg_market_basket` — pares de productos
- `agg_product_margin` — margen por producto/periodo
- `agg_cohort_retention` — retención por cohorte
- `agg_customer_recurrence` — resumen recurrencia

---

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Base OLTP | PostgreSQL 16 |
| Base OLAP | PostgreSQL 16 (star schema) |
| ORM | SQLAlchemy 2.0 |
| Validación | Pydantic 2 |
| Web App | Flask 3 + Gunicorn |
| Configuración | PyYAML + python-dotenv |
| Contenedores | Docker / Podman |

---

## Monitoreo

```bash
# Ver logs del ETL
docker logs lab02_etl_web -f

# Verificar datos cargados
docker exec -it lab02_olap_postgres psql -U postgres -d adventureworks_dw \
  -c "SELECT schemaname, tablename, n_live_tup FROM pg_stat_user_tables WHERE schemaname='dw';"
```
