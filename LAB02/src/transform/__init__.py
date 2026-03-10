"""
Transformaciones para el ETL de AdventureWorks.
Convierte datos OLTP crudos al formato OLAP (dimensiones y hechos).
"""
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from src.utils.helpers import (
    date_to_key, get_quarter, get_fiscal_year,
    get_fiscal_quarter, price_range
)
from src.utils.exceptions import TransformationError

logger = logging.getLogger(__name__)

MONTHS = ["January","February","March","April","May","June",
          "July","August","September","October","November","December"]
DAYS   = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]


def transform_date(d: date) -> dict:
    """Transforma una fecha al formato de dim_date."""
    dow = d.isoweekday() % 7  # 0=Sunday ... 6=Saturday  → isoweekday: Mon=1...Sun=7
    return {
        "date_key":       date_to_key(d),
        "full_date":      d,
        "year":           d.year,
        "quarter":        get_quarter(d.month),
        "month":          d.month,
        "month_name":     MONTHS[d.month - 1],
        "week_of_year":   int(d.strftime("%W")),
        "day_of_month":   d.day,
        "day_of_week":    dow,
        "day_name":       DAYS[dow],
        "is_weekend":     dow in (0, 6),
        "fiscal_year":    get_fiscal_year(d),
        "fiscal_quarter": get_fiscal_quarter(d),
    }


def transform_territory(row: dict) -> dict:
    """Transforma una fila de territory al formato de dim_territory."""
    return {
        "territory_id":   row["territory_id"],
        "territory_name": row["territory_name"],
        "country_code":   row["country_code"],
        "region_group":   row["region_group"],
    }


def transform_product(row: dict) -> dict:
    """Transforma una fila de producto al formato de dim_product."""
    cat_name = row.get("category_name") or ""
    return {
        "product_id":       row["product_id"],
        "product_number":   row["product_number"],
        "product_name":     row["product_name"],
        "color":            row.get("color"),
        "size":             row.get("size"),
        "list_price":       row.get("list_price"),
        "standard_cost":    row.get("standard_cost"),
        "price_range":      price_range(float(row.get("list_price") or 0)),
        "subcategory_id":   row.get("subcategory_id"),
        "subcategory_name": row.get("subcategory_name"),
        "category_id":      row.get("category_id"),
        "category_name":    cat_name,
        "is_bike":          cat_name == "Bikes",
        "is_accessory":     cat_name == "Accessories",
    }


def transform_customer(row: dict, first_order_date: Optional[date] = None) -> dict:
    """Transforma una fila de cliente al formato de dim_customer."""
    cohort_key = None
    cohort_year = None
    cohort_month = None
    if first_order_date:
        cohort_key   = f"{first_order_date.year}-{first_order_date.month:02d}"
        cohort_year  = first_order_date.year
        cohort_month = first_order_date.month

    return {
        "customer_id":      row["customer_id"],
        "account_number":   row["account_number"],
        "first_name":       row.get("first_name"),
        "last_name":        row.get("last_name"),
        "full_name":        row.get("full_name"),
        "territory_id":     row.get("territory_id"),
        "territory_name":   row.get("territory_name"),
        "region_group":     row.get("region_group"),
        "country_code":     row.get("country_code"),
        "first_order_date": first_order_date,
        "cohort_year":      cohort_year,
        "cohort_month":     cohort_month,
        "cohort_key":       cohort_key,
    }


def transform_fact_sales(row: dict, customer_key: int, product_key: int,
                          territory_key: int) -> dict:
    """Transforma una línea de detalle de orden al formato de fact_sales."""
    try:
        qty           = int(row["order_qty"])
        unit_price    = Decimal(str(row["unit_price"]))
        discount      = Decimal(str(row["unit_price_discount"]))
        std_cost      = Decimal(str(row["standard_cost"]))
        line_total    = qty * unit_price * (1 - discount)
        cost_total    = qty * std_cost
        gross_margin  = line_total - cost_total
        margin_pct    = (gross_margin / line_total * 100) if line_total > 0 else Decimal(0)

        order_date = row["order_date"]
        if isinstance(order_date, datetime):
            order_date = order_date.date()

        return {
            "date_key":             date_to_key(order_date),
            "customer_key":         customer_key,
            "product_key":          product_key,
            "territory_key":        territory_key,
            "sales_order_id":       row["sales_order_id"],
            "sales_order_detail_id":row["sales_order_detail_id"],
            "order_qty":            qty,
            "unit_price":           unit_price,
            "unit_price_discount":  discount,
            "standard_cost":        std_cost,
            "line_total":           line_total,
            "cost_total":           cost_total,
            "gross_margin":         gross_margin,
            "gross_margin_pct":     round(margin_pct, 4),
            "is_online":            bool(row.get("is_online", False)),
        }
    except Exception as e:
        raise TransformationError(f"Error transformando fact_sales: {e}") from e


def transform_fact_orders(row: dict, customer_key: int, territory_key: int,
                           customer_order_number: int, first_order_date: Optional[date]) -> dict:
    """Transforma una cabecera de orden al formato de fact_orders."""
    try:
        order_date = row["order_date"]
        if isinstance(order_date, datetime):
            order_date = order_date.date()

        is_first   = customer_order_number == 1
        is_recurring = customer_order_number > 1

        months_since = None
        if first_order_date:
            delta = (order_date.year - first_order_date.year) * 12 + \
                    (order_date.month - first_order_date.month)
            months_since = delta

        return {
            "date_key":             date_to_key(order_date),
            "customer_key":         customer_key,
            "territory_key":        territory_key,
            "sales_order_id":       row["sales_order_id"],
            "sub_total":            Decimal(str(row["sub_total"])),
            "tax_amt":              Decimal(str(row["tax_amt"])),
            "freight":              Decimal(str(row["freight"])),
            "total_due":            Decimal(str(row["total_due"])),
            "line_count":           int(row["line_count"]),
            "customer_order_number":customer_order_number,
            "is_first_order":       is_first,
            "is_recurring":         is_recurring,
            "months_since_first":   months_since,
        }
    except Exception as e:
        raise TransformationError(f"Error transformando fact_orders: {e}") from e
