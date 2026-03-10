"""
Gestión de conexiones a las bases de datos OLTP y OLAP.
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from config.settings import get_oltp_url, get_olap_url

logger = logging.getLogger(__name__)

# ── Engines ────────────────────────────────────────────────────────────────

def create_oltp_engine():
    """Crea el engine SQLAlchemy para la base OLTP."""
    url = get_oltp_url()
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
    )
    logger.info("Engine OLTP creado: %s", url.split("@")[-1])
    return engine


def create_olap_engine():
    """Crea el engine SQLAlchemy para el Data Warehouse OLAP."""
    url = get_olap_url()
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
    )
    logger.info("Engine OLAP creado: %s", url.split("@")[-1])
    return engine


# ── Session factories ───────────────────────────────────────────────────────

_oltp_engine = None
_olap_engine = None
_OLTPSession = None
_OLAPSession = None


def get_oltp_engine():
    global _oltp_engine
    if _oltp_engine is None:
        _oltp_engine = create_oltp_engine()
    return _oltp_engine


def get_olap_engine():
    global _olap_engine
    if _olap_engine is None:
        _olap_engine = create_olap_engine()
    return _olap_engine


def get_oltp_session_factory() -> sessionmaker:
    global _OLTPSession
    if _OLTPSession is None:
        _OLTPSession = sessionmaker(bind=get_oltp_engine(), autocommit=False, autoflush=False)
    return _OLTPSession


def get_olap_session_factory() -> sessionmaker:
    global _OLAPSession
    if _OLAPSession is None:
        _OLAPSession = sessionmaker(bind=get_olap_engine(), autocommit=False, autoflush=False)
    return _OLAPSession


# ── Context managers ────────────────────────────────────────────────────────

@contextmanager
def oltp_session() -> Session:
    """Context manager para sesiones OLTP con commit/rollback automático."""
    factory = get_oltp_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("Error en sesión OLTP: %s", e)
        raise
    finally:
        session.close()


@contextmanager
def olap_session() -> Session:
    """Context manager para sesiones OLAP con commit/rollback automático."""
    factory = get_olap_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("Error en sesión OLAP: %s", e)
        raise
    finally:
        session.close()


def test_connections() -> dict:
    """Verifica la conectividad con ambas bases de datos."""
    results = {}
    for name, engine_fn in [("OLTP", get_oltp_engine), ("OLAP", get_olap_engine)]:
        try:
            with engine_fn().connect() as conn:
                conn.execute(text("SELECT 1"))
            results[name] = {"status": "ok"}
            logger.info("Conexión %s: OK", name)
        except Exception as e:
            results[name] = {"status": "error", "message": str(e)}
            logger.error("Conexión %s: FALLO - %s", name, e)
    return results
