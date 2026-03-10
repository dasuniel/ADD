"""Funciones auxiliares reutilizables en el ETL."""
from datetime import date, timedelta


def date_to_key(d: date) -> int:
    """Convierte una fecha a clave entera YYYYMMDD."""
    return int(d.strftime("%Y%m%d"))


def generate_date_range(start: date, end: date):
    """Generador de fechas entre start y end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def get_quarter(month: int) -> int:
    """Retorna el trimestre (1-4) para un mes dado."""
    return (month - 1) // 3 + 1


def get_fiscal_year(d: date, fiscal_start_month: int = 7) -> int:
    """Calcula el año fiscal dado un mes de inicio del año fiscal."""
    if d.month >= fiscal_start_month:
        return d.year + 1
    return d.year


def get_fiscal_quarter(d: date, fiscal_start_month: int = 7) -> int:
    """Calcula el trimestre fiscal."""
    adj_month = (d.month - fiscal_start_month) % 12
    return (adj_month // 3) + 1


def chunked(iterable, size: int):
    """Divide un iterable en listas de tamaño `size`."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def price_range(price: float) -> str:
    """Clasifica un precio en rango Low/Mid/High."""
    if price < 100:
        return "Low"
    elif price < 1000:
        return "Mid"
    return "High"
