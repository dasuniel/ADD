"""
Pipeline de ventas: extrae órdenes OLTP y carga fact_sales + fact_orders.
"""
import logging
from datetime import date, datetime
from collections import defaultdict

from src.extract.sql_extractor import SQLExtractor
from src.transform import (
    transform_fact_sales, transform_fact_orders,
    transform_date, transform_territory, transform_product
)
from src.load import (
    load_dim_dates, load_dim_territories, load_dim_products,
    load_fact_sales, load_fact_orders,
    load_agg_market_basket, load_agg_product_margin
)
from src.utils.db import olap_session
from src.utils.helpers import generate_date_range
from src.utils.exceptions import ETLException

logger = logging.getLogger(__name__)


class SalesPipeline:
    """
    Orquesta el ETL para hechos de ventas.
    1. Carga dimensiones: fecha, territorio, producto
    2. Carga fact_sales y fact_orders
    3. Calcula agregaciones: market basket, márgenes
    """

    def __init__(self):
        self.extractor = SQLExtractor(batch_size=1000)

    def run(self):
        logger.info("=== Iniciando SalesPipeline ===")
        try:
            self._load_dim_date()
            territory_map = self._load_dim_territory()
            product_map   = self._load_dim_product()
            customer_map  = self._get_customer_key_map()
            self._load_facts(territory_map, product_map, customer_map)
            self._load_aggregations()
            logger.info("=== SalesPipeline completado ===")
        except Exception as e:
            logger.error("Error en SalesPipeline: %s", e)
            raise ETLException(f"SalesPipeline fallido: {e}") from e

    # ── Private methods ──────────────────────────────────────────────────────

    def _load_dim_date(self):
        """Genera y carga la dimensión fecha cubriendo el rango real de datos."""
        logger.info("Detectando rango de fechas en OLTP...")
        from sqlalchemy import text as sa_text
        from src.utils.db import oltp_session as _oltp
        with _oltp() as session:
            row = session.execute(sa_text(
                "SELECT MIN(order_date)::date AS min_d, MAX(order_date)::date AS max_d "
                "FROM sales.sales_order_header"
            )).fetchone()
            min_d, max_d = row.min_d, row.max_d

        # Extender un año antes y después para cobertura
        start = date(min_d.year - 1, 1, 1)
        end   = date(max_d.year + 1, 12, 31)
        logger.info("Generando dim_date desde %s hasta %s", start, end)
        rows  = [transform_date(d) for d in generate_date_range(start, end)]
        with olap_session() as session:
            load_dim_dates(session, rows)

    def _load_dim_territory(self) -> dict:
        """Carga dim_territory y retorna {territory_id: territory_key}."""
        logger.info("Cargando dim_territory...")
        rows = []
        for batch in self.extractor.extract_territories():
            rows.extend([transform_territory(r) for r in batch])

        with olap_session() as session:
            load_dim_territories(session, rows)
            # Leer keys asignadas
            from sqlalchemy import text
            result = session.execute(text("SELECT territory_id, territory_key FROM dw.dim_territory"))
            return {row.territory_id: row.territory_key for row in result}

    def _load_dim_product(self) -> dict:
        """Carga dim_product y retorna {product_id: product_key}."""
        logger.info("Cargando dim_product...")
        from src.transform import transform_product
        rows = []
        for batch in self.extractor.extract_products():
            rows.extend([transform_product(r) for r in batch])

        with olap_session() as session:
            load_dim_products(session, rows)
            from sqlalchemy import text
            result = session.execute(text("SELECT product_id, product_key FROM dw.dim_product"))
            return {row.product_id: row.product_key for row in result}

    def _get_customer_key_map(self) -> dict:
        """Lee {customer_id: customer_key} desde dim_customer ya cargado."""
        from sqlalchemy import text
        with olap_session() as session:
            result = session.execute(text("SELECT customer_id, customer_key FROM dw.dim_customer"))
            return {row.customer_id: row.customer_key for row in result}

    def _load_facts(self, territory_map: dict, product_map: dict, customer_map: dict):
        """Carga fact_sales y fact_orders."""
        logger.info("Cargando fact_sales...")
        sales_rows  = []
        for batch in self.extractor.extract_order_details():
            for row in batch:
                c_key = customer_map.get(row["customer_id"])
                p_key = product_map.get(row["product_id"])
                t_key = territory_map.get(row.get("territory_id"))
                if not all([c_key, p_key, t_key]):
                    logger.warning("Skipping detail order %s - key not found", row["sales_order_id"])
                    continue
                sales_rows.append(transform_fact_sales(row, c_key, p_key, t_key))

        with olap_session() as session:
            load_fact_sales(session, sales_rows)

        # fact_orders
        logger.info("Cargando fact_orders...")
        first_orders = self.extractor.extract_first_orders()
        customer_order_counter = defaultdict(int)
        order_rows = []

        for batch in self.extractor.extract_order_headers():
            for row in batch:
                c_id  = row["customer_id"]
                c_key = customer_map.get(c_id)
                t_key = territory_map.get(row.get("territory_id"))
                if not c_key or not t_key:
                    continue
                customer_order_counter[c_id] += 1
                num    = customer_order_counter[c_id]
                fod    = first_orders.get(c_id)
                order_rows.append(transform_fact_orders(row, c_key, t_key, num, fod))

        with olap_session() as session:
            load_fact_orders(session, order_rows)

    def _load_aggregations(self):
        """Calcula todas las tablas de agregación."""
        with olap_session() as session:
            load_agg_market_basket(session)
            load_agg_product_margin(session)
