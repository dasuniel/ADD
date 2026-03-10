"""
Punto de entrada del proceso ETL completo.
Ejecuta los pipelines en orden correcto:
  1. CustomerPipeline  → dim_customer
  2. SalesPipeline     → dims (fecha, territorio, producto) + facts + aggs de ventas
  3. CustomerPipeline  → aggs de cohortes (necesitan facts cargados)
"""
import sys
import logging
import time

from config.settings import setup_logging
from src.utils.db import test_connections
from src.pipelines.customer_pipeline import CustomerPipeline
from src.pipelines.sales_pipeline import SalesPipeline

# Configurar logging antes de cualquier otra cosa
setup_logging()
logger = logging.getLogger("main")


def main():
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   AdventureWorks ETL Pipeline - Lab02   ║")
    logger.info("╚══════════════════════════════════════════╝")

    # 1. Verificar conectividad
    logger.info("Verificando conexiones a las bases de datos...")
    conn_status = test_connections()
    for db, status in conn_status.items():
        if status["status"] != "ok":
            logger.error("No se puede conectar a %s: %s", db, status.get("message"))
            sys.exit(1)
    logger.info("Todas las conexiones OK.")

    start_time = time.time()

    # 2. Pipeline de clientes (dim_customer sin cohortes aún)
    logger.info("─── Fase 1: Cargando dimensión de clientes ───")
    customer_pipeline = CustomerPipeline()
    customer_pipeline._load_dim_customer(
        customer_pipeline.extractor.extract_first_orders()
    )

    # 3. Pipeline de ventas (dimensiones + hechos + aggs de ventas)
    logger.info("─── Fase 2: Pipeline de ventas ───")
    sales_pipeline = SalesPipeline()
    sales_pipeline.run()

    # 4. Agregaciones de clientes (necesitan facts cargados)
    logger.info("─── Fase 3: Agregaciones de clientes ───")
    customer_pipeline._load_aggregations()

    elapsed = time.time() - start_time
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  ETL completado en %.1f segundos        ║", elapsed)
    logger.info("╚══════════════════════════════════════════╝")


if __name__ == "__main__":
    main()
