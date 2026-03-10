"""
Clase base abstracta para todos los extractores del ETL.
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class ExtractorBase(ABC):
    """
    Interfaz que deben implementar todos los extractores.
    Define el contrato: extract() retorna un iterador de dicts.
    """

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self, **kwargs) -> Iterator[list[dict]]:
        """
        Extrae datos de la fuente en lotes (batches).
        Cada yield devuelve una lista de dicts.
        """
        raise NotImplementedError

    def log_start(self, source: str):
        self.logger.info("Iniciando extracción desde: %s", source)

    def log_done(self, count: int, source: str):
        self.logger.info("Extracción completada - %d registros desde %s", count, source)
