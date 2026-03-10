"""
Esquemas Pydantic para validación de datos en el pipeline ETL.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator


# ── OLTP Source schemas ─────────────────────────────────────────────────────

class RawOrderDetail(BaseModel):
    sales_order_id:        int
    sales_order_detail_id: int
    order_date:            datetime
    customer_id:           int
    territory_id:          Optional[int]
    product_id:            int
    order_qty:             int
    unit_price:            Decimal
    unit_price_discount:   Decimal
    standard_cost:         Decimal
    list_price:            Decimal
    is_online:             bool
    subcategory_id:        Optional[int]
    subcategory_name:      Optional[str]
    category_id:           Optional[int]
    category_name:         Optional[str]

    @field_validator("order_qty")
    @classmethod
    def qty_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("order_qty debe ser positivo")
        return v

    @field_validator("unit_price", "standard_cost")
    @classmethod
    def price_non_negative(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("El precio/costo no puede ser negativo")
        return v


# ── OLAP Dimension schemas ──────────────────────────────────────────────────

class DimDateSchema(BaseModel):
    date_key:       int
    full_date:      date
    year:           int
    quarter:        int
    month:          int
    month_name:     str
    week_of_year:   int
    day_of_month:   int
    day_of_week:    int
    day_name:       str
    is_weekend:     bool
    fiscal_year:    int
    fiscal_quarter: int


class DimCustomerSchema(BaseModel):
    customer_id:     int
    account_number:  str
    first_name:      Optional[str]
    last_name:       Optional[str]
    full_name:       Optional[str]
    territory_id:    Optional[int]
    territory_name:  Optional[str]
    region_group:    Optional[str]
    country_code:    Optional[str]
    first_order_date:Optional[date]
    cohort_year:     Optional[int]
    cohort_month:    Optional[int]
    cohort_key:      Optional[str]


class DimProductSchema(BaseModel):
    product_id:      int
    product_number:  str
    product_name:    str
    color:           Optional[str]
    size:            Optional[str]
    list_price:      Optional[Decimal]
    standard_cost:   Optional[Decimal]
    price_range:     Optional[str]
    subcategory_id:  Optional[int]
    subcategory_name:Optional[str]
    category_id:     Optional[int]
    category_name:   Optional[str]
    is_bike:         bool = False
    is_accessory:    bool = False


class FactSalesSchema(BaseModel):
    date_key:             int
    customer_key:         int
    product_key:          int
    territory_key:        int
    sales_order_id:       int
    sales_order_detail_id:int
    order_qty:            int
    unit_price:           Decimal
    unit_price_discount:  Decimal
    standard_cost:        Decimal
    line_total:           Decimal
    cost_total:           Decimal
    gross_margin:         Decimal
    gross_margin_pct:     Optional[Decimal]
    is_online:            bool

    @model_validator(mode="after")
    def compute_margin(self) -> "FactSalesSchema":
        if self.line_total > 0:
            self.gross_margin_pct = round(self.gross_margin / self.line_total * 100, 4)
        return self
