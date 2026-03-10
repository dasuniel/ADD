"""
Pipeline de clientes: extrae clientes OLTP, carga dim_customer
y calcula agregaciones de cohortes y recurrencia.
"""
import logging

from src.extract.sql_extractor import SQLExtractor
from src.transform import transform_customer
from src.load import (
    load_dim_customers,
    load_agg_cohort_retention,
    load_agg_customer_recurrence,
)
from src.utils.db import olap_session
from src.utils.exceptions import ETLException

logger = logging.getLogger(__name__)


class CustomerPipeline:
    """
    Orquesta el ETL para la dimensión de clientes.
    1. Extrae clientes del OLTP
    2. Enriquece con fecha de primera orden (para cohortes)
    3. Carga dim_customer
    4. Calcula cohortes y recurrencia
    """

    def __init__(self):
        self.extractor = SQLExtractor(batch_size=1000)

    def run(self):
        logger.info("=== Iniciando CustomerPipeline ===")
        try:
            first_orders = self.extractor.extract_first_orders()
            self._load_dim_customer(first_orders)
            self._load_aggregations()
            logger.info("=== CustomerPipeline completado ===")
        except Exception as e:
            logger.error("Error en CustomerPipeline: %s", e)
            raise ETLException(f"CustomerPipeline fallido: {e}") from e

    def _load_dim_customer(self, first_orders: dict):
        """Extrae y carga dim_customer con datos de cohorte."""
        logger.info("Cargando dim_customer...")
        rows = []
        for batch in self.extractor.extract_customers():
            for row in batch:
                fod = first_orders.get(row["customer_id"])
                rows.append(transform_customer(row, fod))

        with olap_session() as session:
            load_dim_customers(session, rows)
        logger.info("dim_customer: %d clientes cargados", len(rows))

    def _load_aggregations(self):
        """Calcula cohortes y recurrencia (después de cargar facts)."""
        with olap_session() as session:
            load_agg_cohort_retention(session)
            load_agg_customer_recurrence(session)
