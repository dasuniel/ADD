"""Configuración de logging para el proyecto ETL."""
import logging
from config.settings import setup_logging

def get_logger(name: str) -> logging.Logger:
    """Retorna un logger configurado para el módulo indicado."""
    setup_logging()
    return logging.getLogger(name)
