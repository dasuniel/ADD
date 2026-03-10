"""
Módulo de carga al Data Warehouse OLAP.
Implementa upsert (insert-or-update) para todas las tablas del DW.
"""
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from src.models.entities import (
    DimDate, DimCustomer, DimProduct, DimTerritory,
    FactSales, FactOrders,
    AggMarketBasket, AggCohortRetention, AggProductMargin, AggCustomerRecurrence
)
from src.utils.exceptions import LoadError

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _bulk_upsert(session: Session, model, rows: list[dict], conflict_cols: list[str],
                 update_cols: list[str] | None = None):
    """
    Inserta o actualiza (upsert) un lote de filas usando ON CONFLICT DO UPDATE.
    """
    if not rows:
        return
    try:
        stmt = insert(model.__table__).values(rows)
        if update_cols:
            update_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
        else:
            # Actualizar todas las columnas excepto las de conflicto
            update_dict = {
                col.name: getattr(stmt.excluded, col.name)
                for col in model.__table__.columns
                if col.name not in conflict_cols and col.name not in ("etl_loaded_at",)
            }
        stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=update_dict)
        session.execute(stmt)
        logger.debug("Upserted %d rows en %s", len(rows), model.__tablename__)
    except Exception as e:
        raise LoadError(f"Error en upsert a {model.__tablename__}: {e}") from e


# ── Dimension loaders ───────────────────────────────────────────────────────

def load_dim_dates(session: Session, rows: list[dict]):
    """Carga dim_date."""
    _bulk_upsert(session, DimDate, rows, conflict_cols=["date_key"])
    logger.info("Cargados %d registros en dim_date", len(rows))


def load_dim_territories(session: Session, rows: list[dict]):
    """Carga dim_territory."""
    _bulk_upsert(session, DimTerritory, rows, conflict_cols=["territory_id"])
    logger.info("Cargados %d registros en dim_territory", len(rows))


def load_dim_customers(session: Session, rows: list[dict]):
    """Carga dim_customer."""
    _bulk_upsert(session, DimCustomer, rows, conflict_cols=["customer_id"])
    logger.info("Cargados %d registros en dim_customer", len(rows))


def load_dim_products(session: Session, rows: list[dict]):
    """Carga dim_product."""
    _bulk_upsert(session, DimProduct, rows, conflict_cols=["product_id"])
    logger.info("Cargados %d registros en dim_product", len(rows))


# ── Fact loaders ────────────────────────────────────────────────────────────

def load_fact_sales(session: Session, rows: list[dict]):
    """Carga fact_sales (insert only, no upsert)."""
    if not rows:
        return
    try:
        # Truncar y recargar (full refresh)
        session.execute(text("TRUNCATE dw.fact_sales RESTART IDENTITY CASCADE"))
        session.execute(FactSales.__table__.insert(), rows)
        logger.info("Cargados %d registros en fact_sales", len(rows))
    except Exception as e:
        raise LoadError(f"Error cargando fact_sales: {e}") from e


def load_fact_orders(session: Session, rows: list[dict]):
    """Carga fact_orders (insert only, full refresh)."""
    if not rows:
        return
    try:
        session.execute(text("TRUNCATE dw.fact_orders RESTART IDENTITY CASCADE"))
        session.execute(FactOrders.__table__.insert(), rows)
        logger.info("Cargados %d registros en fact_orders", len(rows))
    except Exception as e:
        raise LoadError(f"Error cargando fact_orders: {e}") from e


# ── Aggregation loaders ─────────────────────────────────────────────────────

def load_agg_market_basket(session: Session):
    """Calcula y carga la tabla de análisis de canasta desde fact_sales."""
    logger.info("Calculando análisis de canasta...")
    try:
        session.execute(text("TRUNCATE dw.agg_market_basket RESTART IDENTITY"))
        sql = """
        INSERT INTO dw.agg_market_basket (product_key_a, product_key_b, co_occurrences, support)
        WITH total_orders AS (
            SELECT COUNT(DISTINCT sales_order_id) AS n FROM dw.fact_sales
        ),
        pairs AS (
            SELECT
                LEAST(a.product_key, b.product_key)    AS product_key_a,
                GREATEST(a.product_key, b.product_key) AS product_key_b,
                COUNT(DISTINCT a.sales_order_id)        AS co_occurrences
            FROM dw.fact_sales a
            JOIN dw.fact_sales b
                ON a.sales_order_id = b.sales_order_id
               AND a.product_key   <> b.product_key
            GROUP BY 1, 2
            HAVING COUNT(DISTINCT a.sales_order_id) > 0
        )
        SELECT
            p.product_key_a,
            p.product_key_b,
            p.co_occurrences,
            ROUND(p.co_occurrences::numeric / t.n, 6) AS support
        FROM pairs p CROSS JOIN total_orders t
        ORDER BY p.co_occurrences DESC
        ON CONFLICT (product_key_a, product_key_b) DO UPDATE
            SET co_occurrences = EXCLUDED.co_occurrences,
                support        = EXCLUDED.support,
                etl_loaded_at  = NOW()
        """
        session.execute(text(sql))
        logger.info("Análisis de canasta cargado.")
    except Exception as e:
        raise LoadError(f"Error calculando market basket: {e}") from e


def load_agg_product_margin(session: Session):
    """Calcula y carga márgenes por producto y periodo."""
    logger.info("Calculando márgenes por producto...")
    try:
        session.execute(text("TRUNCATE dw.agg_product_margin"))
        sql = """
        INSERT INTO dw.agg_product_margin
            (product_key, year, month, total_qty, total_revenue, total_cost, total_margin, margin_pct)
        SELECT
            fs.product_key,
            dd.year,
            dd.month,
            SUM(fs.order_qty)::int           AS total_qty,
            SUM(fs.line_total)               AS total_revenue,
            SUM(fs.cost_total)               AS total_cost,
            SUM(fs.gross_margin)             AS total_margin,
            CASE WHEN SUM(fs.line_total) > 0
                 THEN ROUND(SUM(fs.gross_margin) / SUM(fs.line_total) * 100, 4)
                 ELSE 0 END                  AS margin_pct
        FROM dw.fact_sales fs
        JOIN dw.dim_date dd ON dd.date_key = fs.date_key
        GROUP BY fs.product_key, dd.year, dd.month
        ON CONFLICT (product_key, year, month) DO UPDATE
            SET total_qty     = EXCLUDED.total_qty,
                total_revenue = EXCLUDED.total_revenue,
                total_cost    = EXCLUDED.total_cost,
                total_margin  = EXCLUDED.total_margin,
                margin_pct    = EXCLUDED.margin_pct,
                etl_loaded_at = NOW()
        """
        session.execute(text(sql))
        logger.info("Márgenes por producto cargados.")
    except Exception as e:
        raise LoadError(f"Error calculando márgenes: {e}") from e


def load_agg_cohort_retention(session: Session):
    """Calcula y carga análisis de cohortes."""
    logger.info("Calculando análisis de cohortes...")
    try:
        session.execute(text("TRUNCATE dw.agg_cohort_retention"))
        sql = """
        INSERT INTO dw.agg_cohort_retention
            (cohort_key, cohort_year, cohort_month, period_number, active_period_key,
             customer_count, initial_customers, retention_rate, total_revenue, total_margin,
             avg_revenue_per_customer)
        WITH cohort_base AS (
            SELECT
                dc.cohort_key,
                dc.cohort_year,
                dc.cohort_month,
                COUNT(DISTINCT dc.customer_key) AS initial_customers
            FROM dw.dim_customer dc
            WHERE dc.cohort_key IS NOT NULL
            GROUP BY dc.cohort_key, dc.cohort_year, dc.cohort_month
        ),
        cohort_activity AS (
            SELECT
                dc.cohort_key,
                fo.months_since_first                                AS period_number,
                TO_CHAR(dd.full_date, 'YYYY-MM')                    AS active_period_key,
                COUNT(DISTINCT fo.customer_key)                      AS customer_count,
                SUM(fo.total_due)                                    AS total_revenue,
                SUM(fs.gross_margin)                                 AS total_margin
            FROM dw.fact_orders fo
            JOIN dw.dim_customer dc  ON dc.customer_key = fo.customer_key
            JOIN dw.dim_date    dd   ON dd.date_key     = fo.date_key
            LEFT JOIN dw.fact_sales fs ON fs.sales_order_id = fo.sales_order_id
            WHERE dc.cohort_key IS NOT NULL AND fo.months_since_first IS NOT NULL
            GROUP BY dc.cohort_key, fo.months_since_first, TO_CHAR(dd.full_date, 'YYYY-MM')
        )
        SELECT
            ca.cohort_key,
            cb.cohort_year,
            cb.cohort_month,
            ca.period_number,
            ca.active_period_key,
            ca.customer_count,
            cb.initial_customers,
            ROUND(ca.customer_count::numeric / cb.initial_customers, 4) AS retention_rate,
            ca.total_revenue,
            ca.total_margin,
            ROUND(ca.total_revenue / ca.customer_count, 4) AS avg_revenue_per_customer
        FROM cohort_activity ca
        JOIN cohort_base cb ON cb.cohort_key = ca.cohort_key
        ORDER BY ca.cohort_key, ca.period_number
        ON CONFLICT (cohort_key, period_number) DO UPDATE
            SET customer_count           = EXCLUDED.customer_count,
                retention_rate           = EXCLUDED.retention_rate,
                total_revenue            = EXCLUDED.total_revenue,
                total_margin             = EXCLUDED.total_margin,
                avg_revenue_per_customer = EXCLUDED.avg_revenue_per_customer,
                etl_loaded_at            = NOW()
        """
        session.execute(text(sql))
        logger.info("Análisis de cohortes cargado.")
    except Exception as e:
        raise LoadError(f"Error calculando cohortes: {e}") from e


def load_agg_customer_recurrence(session: Session):
    """Calcula y carga resumen de clientes recurrentes vs no-recurrentes."""
    logger.info("Calculando recurrencia de clientes...")
    try:
        session.execute(text("TRUNCATE dw.agg_customer_recurrence"))
        sql = """
        INSERT INTO dw.agg_customer_recurrence
            (year, quarter, customer_type, customer_count, order_count, total_revenue, revenue_pct)
        WITH customer_order_counts AS (
            SELECT
                fo.customer_key,
                dd.year,
                dd.quarter,
                COUNT(DISTINCT fo.sales_order_id)  AS order_count,
                SUM(fo.total_due)                  AS total_revenue
            FROM dw.fact_orders fo
            JOIN dw.dim_date dd ON dd.date_key = fo.date_key
            GROUP BY fo.customer_key, dd.year, dd.quarter
        ),
        classified AS (
            SELECT
                year,
                quarter,
                CASE WHEN order_count > 1 THEN 'Recurring' ELSE 'One-Time' END AS customer_type,
                COUNT(DISTINCT customer_key) AS customer_count,
                COUNT(*)                     AS order_count,
                SUM(total_revenue)           AS total_revenue
            FROM customer_order_counts
            GROUP BY year, quarter, CASE WHEN order_count > 1 THEN 'Recurring' ELSE 'One-Time' END
        ),
        totals AS (
            SELECT year, quarter, SUM(total_revenue) AS grand_total
            FROM classified GROUP BY year, quarter
        )
        SELECT
            c.year,
            c.quarter,
            c.customer_type,
            c.customer_count,
            c.order_count,
            c.total_revenue,
            ROUND(c.total_revenue / NULLIF(t.grand_total, 0) * 100, 4) AS revenue_pct
        FROM classified c
        JOIN totals t ON t.year = c.year AND t.quarter = c.quarter
        ON CONFLICT (year, quarter, customer_type) DO UPDATE
            SET customer_count = EXCLUDED.customer_count,
                order_count    = EXCLUDED.order_count,
                total_revenue  = EXCLUDED.total_revenue,
                revenue_pct    = EXCLUDED.revenue_pct,
                etl_loaded_at  = NOW()
        """
        session.execute(text(sql))
        logger.info("Recurrencia de clientes cargada.")
    except Exception as e:
        raise LoadError(f"Error calculando recurrencia: {e}") from e
