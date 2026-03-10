"""
Extractor de datos desde la base OLTP (PostgreSQL - AdventureWorks).
Ejecuta queries SQL y retorna resultados en batches de dicts.
"""
import logging
from typing import Iterator
from sqlalchemy import text

from src.extract.extractor_base import ExtractorBase
from src.utils.db import oltp_session
from src.utils.exceptions import ExtractionError

logger = logging.getLogger(__name__)


class SQLExtractor(ExtractorBase):
    """Extrae datos del OLTP usando SQLAlchemy + queries SQL."""

    # ── Queries de extracción ────────────────────────────────────────────────

    QUERY_CUSTOMERS = """
        SELECT
            c.customer_id,
            c.account_number,
            c.territory_id,
            p.first_name,
            p.last_name,
            TRIM(COALESCE(p.first_name,'') || ' ' || COALESCE(p.last_name,'')) AS full_name,
            st.name          AS territory_name,
            st.group         AS region_group,
            st.country_region_code AS country_code
        FROM sales.customer c
        LEFT JOIN person.person p              ON p.business_entity_id = c.person_id
        LEFT JOIN sales.sales_territory st     ON st.territory_id      = c.territory_id
        ORDER BY c.customer_id
    """

    QUERY_PRODUCTS = """
        SELECT
            p.product_id,
            p.product_number,
            p.name           AS product_name,
            p.color,
            p.size,
            p.list_price,
            p.standard_cost,
            p.product_subcategory_id AS subcategory_id,
            ps.name          AS subcategory_name,
            pc.product_category_id   AS category_id,
            pc.name          AS category_name
        FROM production.product p
        LEFT JOIN production.product_subcategory ps ON ps.product_subcategory_id = p.product_subcategory_id
        LEFT JOIN production.product_category    pc ON pc.product_category_id    = ps.product_category_id
        ORDER BY p.product_id
    """

    QUERY_TERRITORIES = """
        SELECT
            territory_id,
            name              AS territory_name,
            country_region_code AS country_code,
            "group"           AS region_group
        FROM sales.sales_territory
        ORDER BY territory_id
    """

    QUERY_ORDER_DETAILS = """
        SELECT
            sod.sales_order_id,
            sod.sales_order_detail_id,
            soh.order_date,
            soh.customer_id,
            soh.territory_id,
            soh.online_order_flag AS is_online,
            sod.product_id,
            sod.order_qty,
            sod.unit_price,
            sod.unit_price_discount,
            p.standard_cost,
            p.list_price,
            p.product_subcategory_id AS subcategory_id,
            ps.name                  AS subcategory_name,
            pc.product_category_id   AS category_id,
            pc.name                  AS category_name
        FROM sales.sales_order_detail sod
        JOIN sales.sales_order_header soh   ON soh.sales_order_id = sod.sales_order_id
        JOIN production.product p           ON p.product_id        = sod.product_id
        LEFT JOIN production.product_subcategory ps ON ps.product_subcategory_id = p.product_subcategory_id
        LEFT JOIN production.product_category    pc ON pc.product_category_id    = ps.product_category_id
        WHERE soh.status = 5
        ORDER BY soh.order_date, sod.sales_order_id, sod.sales_order_detail_id
    """

    QUERY_ORDER_HEADERS = """
        SELECT
            soh.sales_order_id,
            soh.order_date,
            soh.customer_id,
            soh.territory_id,
            soh.online_order_flag AS is_online,
            soh.sub_total,
            soh.tax_amt,
            soh.freight,
            soh.total_due,
            COUNT(sod.sales_order_detail_id) AS line_count
        FROM sales.sales_order_header soh
        JOIN sales.sales_order_detail sod ON sod.sales_order_id = soh.sales_order_id
        WHERE soh.status = 5
        GROUP BY soh.sales_order_id, soh.order_date, soh.customer_id,
                 soh.territory_id, soh.online_order_flag,
                 soh.sub_total, soh.tax_amt, soh.freight, soh.total_due
        ORDER BY soh.order_date, soh.sales_order_id
    """

    QUERY_FIRST_ORDERS = """
        SELECT
            customer_id,
            MIN(order_date)::date AS first_order_date
        FROM sales.sales_order_header
        WHERE status = 5
        GROUP BY customer_id
    """

    def extract(self, query: str, **kwargs) -> Iterator[list[dict]]:
        """Ejecuta un query y retorna resultados en batches."""
        self.log_start(query[:60] + "...")
        total = 0
        try:
            with oltp_session() as session:
                result = session.execute(text(query))
                keys = list(result.keys())
                batch = []
                for row in result:
                    batch.append(dict(zip(keys, row)))
                    if len(batch) >= self.batch_size:
                        total += len(batch)
                        yield batch
                        batch = []
                if batch:
                    total += len(batch)
                    yield batch
        except Exception as e:
            raise ExtractionError(f"Error extrayendo datos: {e}") from e
        self.log_done(total, "OLTP")

    def extract_customers(self) -> Iterator[list[dict]]:
        yield from self.extract(self.QUERY_CUSTOMERS)

    def extract_products(self) -> Iterator[list[dict]]:
        yield from self.extract(self.QUERY_PRODUCTS)

    def extract_territories(self) -> Iterator[list[dict]]:
        yield from self.extract(self.QUERY_TERRITORIES)

    def extract_order_details(self) -> Iterator[list[dict]]:
        yield from self.extract(self.QUERY_ORDER_DETAILS)

    def extract_order_headers(self) -> Iterator[list[dict]]:
        yield from self.extract(self.QUERY_ORDER_HEADERS)

    def extract_first_orders(self) -> dict:
        """Retorna un dict {customer_id: first_order_date}."""
        result = {}
        with oltp_session() as session:
            rows = session.execute(text(self.QUERY_FIRST_ORDERS))
            for row in rows:
                result[row.customer_id] = row.first_order_date
        return result
