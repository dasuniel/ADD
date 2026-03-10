"""
Lectura y validación de configuración del proyecto.
Combina variables de entorno (.env) con config.yaml.
"""
import os
import yaml
import logging
import logging.config
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logger = logging.getLogger(__name__)


def _expand_env(value: str) -> str:
    """Reemplaza ${VAR} por el valor de la variable de entorno."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        var_name = value[2:-1]
        return os.getenv(var_name, value)
    return value


def _resolve_config(config: dict) -> dict:
    """Recorre recursivamente el config y expande variables de entorno."""
    resolved = {}
    for k, v in config.items():
        if isinstance(v, dict):
            resolved[k] = _resolve_config(v)
        elif isinstance(v, list):
            resolved[k] = [_resolve_config(i) if isinstance(i, dict) else i for i in v]
        else:
            resolved[k] = _expand_env(str(v)) if v is not None else v
    return resolved


def load_config() -> dict:
    """Carga config.yaml con variables de entorno resueltas."""
    config_path = BASE_DIR / "config" / "config.yaml"
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return _resolve_config(raw)


def setup_logging():
    """Configura el sistema de logging desde logging.yaml."""
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    logging_config_path = BASE_DIR / "config" / "logging.yaml"
    with open(logging_config_path, "r") as f:
        log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)
    logger.info("Logging configurado correctamente.")


def get_oltp_url() -> str:
    """Retorna la URL de conexión para la base OLTP."""
    host     = os.getenv("OLTP_HOST", "localhost")
    port     = os.getenv("OLTP_PORT", "5432")
    db       = os.getenv("OLTP_DB", "adventureworks_oltp")
    user     = os.getenv("OLTP_USER", "postgres")
    password = os.getenv("OLTP_PASSWORD", "postgres123")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_olap_url() -> str:
    """Retorna la URL de conexión para el Data Warehouse OLAP."""
    host     = os.getenv("OLAP_HOST", "localhost")
    port     = os.getenv("OLAP_PORT", "5432")
    db       = os.getenv("OLAP_DB", "adventureworks_dw")
    user     = os.getenv("OLAP_USER", "postgres")
    password = os.getenv("OLAP_PASSWORD", "postgres123")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
