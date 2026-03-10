"""Excepciones personalizadas del proyecto ETL."""

class ETLException(Exception):
    """Excepción base del ETL."""

class ExtractionError(ETLException):
    """Error durante la fase de extracción."""

class TransformationError(ETLException):
    """Error durante la fase de transformación."""

class LoadError(ETLException):
    """Error durante la fase de carga."""

class ConnectionError(ETLException):
    """Error de conexión a base de datos."""

class ValidationError(ETLException):
    """Error de validación de datos (Pydantic)."""
