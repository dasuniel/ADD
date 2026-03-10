"""
Modelos SQLAlchemy ORM para OLTP y OLAP.
OLTP: esquema de AdventureWorks (operacional)
OLAP: esquema dw (Data Warehouse star schema)
"""
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import (
    Column, Integer, SmallInteger, BigInteger, String, Boolean,
    Numeric, DateTime, Date, ForeignKey, Text, CHAR
)
from sqlalchemy.orm import relationship, DeclarativeBase


# ── Base classes ────────────────────────────────────────────────────────────

class OLTPBase(DeclarativeBase):
    pass

class OLAPBase(DeclarativeBase):
    pass


# ============================================================
# MODELOS OLTP
# ============================================================

class OLTPCountryRegion(OLTPBase):
    __tablename__ = "country_region"
    __table_args__ = {"schema": "person"}

    country_region_code = Column(String(3), primary_key=True)
    name                = Column(String(50), nullable=False)
    modified_date       = Column(DateTime, default=datetime.now)

    state_provinces = relationship("OLTPStateProvince", back_populates="country_region")


class OLTPStateProvince(OLTPBase):
    __tablename__ = "state_province"
    __table_args__ = {"schema": "person"}

    state_province_id   = Column(Integer, primary_key=True)
    state_province_code = Column(CHAR(3), nullable=False)
    country_region_code = Column(String(3), ForeignKey("person.country_region.country_region_code"), nullable=False)
    name                = Column(String(50), nullable=False)
    territory_id        = Column(Integer, nullable=False)
    modified_date       = Column(DateTime, default=datetime.now)

    country_region = relationship("OLTPCountryRegion", back_populates="state_provinces")
    addresses      = relationship("OLTPAddress", back_populates="state_province")


class OLTPAddress(OLTPBase):
    __tablename__ = "address"
    __table_args__ = {"schema": "person"}

    address_id        = Column(Integer, primary_key=True)
    address_line1     = Column(String(60), nullable=False)
    address_line2     = Column(String(60))
    city              = Column(String(30), nullable=False)
    state_province_id = Column(Integer, ForeignKey("person.state_province.state_province_id"), nullable=False)
    postal_code       = Column(String(15), nullable=False)
    modified_date     = Column(DateTime, default=datetime.now)

    state_province = relationship("OLTPStateProvince", back_populates="addresses")


class OLTPPerson(OLTPBase):
    __tablename__ = "person"
    __table_args__ = {"schema": "person"}

    business_entity_id = Column(Integer, primary_key=True)
    person_type        = Column(CHAR(2), nullable=False)
    first_name         = Column(String(50), nullable=False)
    middle_name        = Column(String(50))
    last_name          = Column(String(50), nullable=False)
    email_promotion    = Column(Integer, default=0)
    modified_date      = Column(DateTime, default=datetime.now)

    customer = relationship("OLTPCustomer", back_populates="person", uselist=False)


class OLTPSalesTerritory(OLTPBase):
    __tablename__ = "sales_territory"
    __table_args__ = {"schema": "sales"}

    territory_id        = Column(Integer, primary_key=True)
    name                = Column(String(50), nullable=False)
    country_region_code = Column(String(3), ForeignKey("person.country_region.country_region_code"), nullable=False)
    group               = Column("group", String(50), nullable=False)
    sales_ytd           = Column(Numeric(19, 4), default=0)
    sales_last_year     = Column(Numeric(19, 4), default=0)
    modified_date       = Column(DateTime, default=datetime.now)

    customers = relationship("OLTPCustomer", back_populates="territory")
    orders    = relationship("OLTPSalesOrderHeader", back_populates="territory")


class OLTPCustomer(OLTPBase):
    __tablename__ = "customer"
    __table_args__ = {"schema": "sales"}

    customer_id    = Column(Integer, primary_key=True)
    person_id      = Column(Integer, ForeignKey("person.person.business_entity_id"))
    store_id       = Column(Integer)
    territory_id   = Column(Integer, ForeignKey("sales.sales_territory.territory_id"))
    account_number = Column(String(10), nullable=False)
    modified_date  = Column(DateTime, default=datetime.now)

    person    = relationship("OLTPPerson", back_populates="customer")
    territory = relationship("OLTPSalesTerritory", back_populates="customers")
    orders    = relationship("OLTPSalesOrderHeader", back_populates="customer")


class OLTPProductCategory(OLTPBase):
    __tablename__ = "product_category"
    __table_args__ = {"schema": "production"}

    product_category_id = Column(Integer, primary_key=True)
    name                = Column(String(50), nullable=False)
    modified_date       = Column(DateTime, default=datetime.now)

    subcategories = relationship("OLTPProductSubcategory", back_populates="category")


class OLTPProductSubcategory(OLTPBase):
    __tablename__ = "product_subcategory"
    __table_args__ = {"schema": "production"}

    product_subcategory_id = Column(Integer, primary_key=True)
    product_category_id    = Column(Integer, ForeignKey("production.product_category.product_category_id"), nullable=False)
    name                   = Column(String(50), nullable=False)
    modified_date          = Column(DateTime, default=datetime.now)

    category = relationship("OLTPProductCategory", back_populates="subcategories")
    products = relationship("OLTPProduct", back_populates="subcategory")


class OLTPProduct(OLTPBase):
    __tablename__ = "product"
    __table_args__ = {"schema": "production"}

    product_id             = Column(Integer, primary_key=True)
    name                   = Column(String(50), nullable=False)
    product_number         = Column(String(25), nullable=False, unique=True)
    make_flag              = Column(Boolean, default=True)
    finished_goods_flag    = Column(Boolean, default=True)
    color                  = Column(String(15))
    safety_stock_level     = Column(SmallInteger, nullable=False)
    reorder_point          = Column(SmallInteger, nullable=False)
    standard_cost          = Column(Numeric(19, 4), nullable=False)
    list_price             = Column(Numeric(19, 4), nullable=False)
    size                   = Column(String(5))
    days_to_manufacture    = Column(Integer, nullable=False)
    product_subcategory_id = Column(Integer, ForeignKey("production.product_subcategory.product_subcategory_id"))
    sell_start_date        = Column(DateTime, nullable=False)
    sell_end_date          = Column(DateTime)
    modified_date          = Column(DateTime, default=datetime.now)

    subcategory   = relationship("OLTPProductSubcategory", back_populates="products")
    order_details = relationship("OLTPSalesOrderDetail", back_populates="product")


class OLTPSalesOrderHeader(OLTPBase):
    __tablename__ = "sales_order_header"
    __table_args__ = {"schema": "sales"}

    sales_order_id        = Column(Integer, primary_key=True)
    revision_number       = Column(SmallInteger, default=0)
    order_date            = Column(DateTime, nullable=False)
    due_date              = Column(DateTime, nullable=False)
    ship_date             = Column(DateTime)
    status                = Column(SmallInteger, default=1)
    online_order_flag     = Column(Boolean, default=True)
    purchase_order_number = Column(String(25))
    customer_id           = Column(Integer, ForeignKey("sales.customer.customer_id"), nullable=False)
    territory_id          = Column(Integer, ForeignKey("sales.sales_territory.territory_id"))
    sub_total             = Column(Numeric(19, 4), default=0)
    tax_amt               = Column(Numeric(19, 4), default=0)
    freight               = Column(Numeric(19, 4), default=0)
    total_due             = Column(Numeric(19, 4), default=0)
    modified_date         = Column(DateTime, default=datetime.now)

    customer       = relationship("OLTPCustomer", back_populates="orders")
    territory      = relationship("OLTPSalesTerritory", back_populates="orders")
    order_details  = relationship("OLTPSalesOrderDetail", back_populates="order_header")


class OLTPSalesOrderDetail(OLTPBase):
    __tablename__ = "sales_order_detail"
    __table_args__ = {"schema": "sales"}

    sales_order_id        = Column(Integer, ForeignKey("sales.sales_order_header.sales_order_id"), primary_key=True)
    sales_order_detail_id = Column(Integer, primary_key=True)
    order_qty             = Column(SmallInteger, nullable=False)
    product_id            = Column(Integer, ForeignKey("production.product.product_id"), nullable=False)
    special_offer_id      = Column(Integer, default=1)
    unit_price            = Column(Numeric(19, 4), nullable=False)
    unit_price_discount   = Column(Numeric(19, 4), default=0)
    modified_date         = Column(DateTime, default=datetime.now)

    order_header = relationship("OLTPSalesOrderHeader", back_populates="order_details")
    product      = relationship("OLTPProduct", back_populates="order_details")


# ============================================================
# MODELOS OLAP (Data Warehouse)
# ============================================================

class DimDate(OLAPBase):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "dw"}

    date_key       = Column(Integer, primary_key=True)
    full_date      = Column(Date, nullable=False)
    year           = Column(SmallInteger, nullable=False)
    quarter        = Column(SmallInteger, nullable=False)
    month          = Column(SmallInteger, nullable=False)
    month_name     = Column(String(15), nullable=False)
    week_of_year   = Column(SmallInteger, nullable=False)
    day_of_month   = Column(SmallInteger, nullable=False)
    day_of_week    = Column(SmallInteger, nullable=False)
    day_name       = Column(String(15), nullable=False)
    is_weekend     = Column(Boolean, nullable=False)
    fiscal_year    = Column(SmallInteger, nullable=False)
    fiscal_quarter = Column(SmallInteger, nullable=False)

    fact_sales  = relationship("FactSales",  back_populates="date_dim")
    fact_orders = relationship("FactOrders", back_populates="date_dim")


class DimCustomer(OLAPBase):
    __tablename__ = "dim_customer"
    __table_args__ = {"schema": "dw"}

    customer_key      = Column(Integer, primary_key=True)
    customer_id       = Column(Integer, nullable=False, unique=True)
    account_number    = Column(String(10), nullable=False)
    first_name        = Column(String(50))
    last_name         = Column(String(50))
    full_name         = Column(String(101))
    territory_id      = Column(Integer)
    territory_name    = Column(String(50))
    region_group      = Column(String(50))
    country_code      = Column(String(3))
    first_order_date  = Column(Date)
    cohort_year       = Column(SmallInteger)
    cohort_month      = Column(SmallInteger)
    cohort_key        = Column(CHAR(7))
    created_at        = Column(DateTime, default=datetime.now)
    updated_at        = Column(DateTime, default=datetime.now)

    fact_sales  = relationship("FactSales",  back_populates="customer_dim")
    fact_orders = relationship("FactOrders", back_populates="customer_dim")


class DimProduct(OLAPBase):
    __tablename__ = "dim_product"
    __table_args__ = {"schema": "dw"}

    product_key      = Column(Integer, primary_key=True)
    product_id       = Column(Integer, nullable=False, unique=True)
    product_number   = Column(String(25), nullable=False)
    product_name     = Column(String(50), nullable=False)
    color            = Column(String(15))
    size             = Column(String(5))
    list_price       = Column(Numeric(19, 4))
    standard_cost    = Column(Numeric(19, 4))
    price_range      = Column(String(20))
    subcategory_id   = Column(Integer)
    subcategory_name = Column(String(50))
    category_id      = Column(Integer)
    category_name    = Column(String(50))
    is_bike          = Column(Boolean, default=False)
    is_accessory     = Column(Boolean, default=False)
    created_at       = Column(DateTime, default=datetime.now)
    updated_at       = Column(DateTime, default=datetime.now)

    fact_sales = relationship("FactSales", back_populates="product_dim")


class DimTerritory(OLAPBase):
    __tablename__ = "dim_territory"
    __table_args__ = {"schema": "dw"}

    territory_key  = Column(Integer, primary_key=True)
    territory_id   = Column(Integer, nullable=False, unique=True)
    territory_name = Column(String(50), nullable=False)
    country_code   = Column(String(3), nullable=False)
    region_group   = Column(String(50), nullable=False)

    fact_sales  = relationship("FactSales",  back_populates="territory_dim")
    fact_orders = relationship("FactOrders", back_populates="territory_dim")


class FactSales(OLAPBase):
    __tablename__ = "fact_sales"
    __table_args__ = {"schema": "dw"}

    sales_key             = Column(BigInteger, primary_key=True)
    date_key              = Column(Integer, ForeignKey("dw.dim_date.date_key"), nullable=False)
    customer_key          = Column(Integer, ForeignKey("dw.dim_customer.customer_key"), nullable=False)
    product_key           = Column(Integer, ForeignKey("dw.dim_product.product_key"), nullable=False)
    territory_key         = Column(Integer, ForeignKey("dw.dim_territory.territory_key"), nullable=False)
    sales_order_id        = Column(Integer, nullable=False)
    sales_order_detail_id = Column(Integer, nullable=False)
    order_qty             = Column(SmallInteger, nullable=False)
    unit_price            = Column(Numeric(19, 4), nullable=False)
    unit_price_discount   = Column(Numeric(19, 4), nullable=False)
    standard_cost         = Column(Numeric(19, 4), nullable=False)
    line_total            = Column(Numeric(19, 4), nullable=False)
    cost_total            = Column(Numeric(19, 4), nullable=False)
    gross_margin          = Column(Numeric(19, 4), nullable=False)
    gross_margin_pct      = Column(Numeric(8, 4))
    is_online             = Column(Boolean, default=False)
    etl_loaded_at         = Column(DateTime, default=datetime.now)

    date_dim      = relationship("DimDate",      back_populates="fact_sales")
    customer_dim  = relationship("DimCustomer",  back_populates="fact_sales")
    product_dim   = relationship("DimProduct",   back_populates="fact_sales")
    territory_dim = relationship("DimTerritory", back_populates="fact_sales")


class FactOrders(OLAPBase):
    __tablename__ = "fact_orders"
    __table_args__ = {"schema": "dw"}

    order_key             = Column(BigInteger, primary_key=True)
    date_key              = Column(Integer, ForeignKey("dw.dim_date.date_key"), nullable=False)
    customer_key          = Column(Integer, ForeignKey("dw.dim_customer.customer_key"), nullable=False)
    territory_key         = Column(Integer, ForeignKey("dw.dim_territory.territory_key"), nullable=False)
    sales_order_id        = Column(Integer, nullable=False, unique=True)
    sub_total             = Column(Numeric(19, 4), nullable=False)
    tax_amt               = Column(Numeric(19, 4), nullable=False)
    freight               = Column(Numeric(19, 4), nullable=False)
    total_due             = Column(Numeric(19, 4), nullable=False)
    line_count            = Column(Integer, nullable=False)
    customer_order_number = Column(Integer)
    is_first_order        = Column(Boolean, default=False)
    is_recurring          = Column(Boolean, default=False)
    months_since_first    = Column(Integer)
    etl_loaded_at         = Column(DateTime, default=datetime.now)

    date_dim      = relationship("DimDate",      back_populates="fact_orders")
    customer_dim  = relationship("DimCustomer",  back_populates="fact_orders")
    territory_dim = relationship("DimTerritory", back_populates="fact_orders")


class AggMarketBasket(OLAPBase):
    __tablename__ = "agg_market_basket"
    __table_args__ = {"schema": "dw"}

    basket_key     = Column(BigInteger, primary_key=True)
    product_key_a  = Column(Integer, ForeignKey("dw.dim_product.product_key"), nullable=False)
    product_key_b  = Column(Integer, ForeignKey("dw.dim_product.product_key"), nullable=False)
    co_occurrences = Column(Integer, nullable=False)
    support        = Column(Numeric(8, 6))
    etl_loaded_at  = Column(DateTime, default=datetime.now)


class AggCohortRetention(OLAPBase):
    __tablename__ = "agg_cohort_retention"
    __table_args__ = {"schema": "dw"}

    cohort_key               = Column(CHAR(7), primary_key=True)
    cohort_year              = Column(SmallInteger, nullable=False)
    cohort_month             = Column(SmallInteger, nullable=False)
    period_number            = Column(Integer, primary_key=True)
    active_period_key        = Column(CHAR(7), nullable=False)
    customer_count           = Column(Integer, nullable=False)
    initial_customers        = Column(Integer, nullable=False)
    retention_rate           = Column(Numeric(8, 4))
    total_revenue            = Column(Numeric(19, 4))
    total_margin             = Column(Numeric(19, 4))
    avg_revenue_per_customer = Column(Numeric(19, 4))
    etl_loaded_at            = Column(DateTime, default=datetime.now)


class AggProductMargin(OLAPBase):
    __tablename__ = "agg_product_margin"
    __table_args__ = {"schema": "dw"}

    product_key   = Column(Integer, ForeignKey("dw.dim_product.product_key"), primary_key=True)
    year          = Column(SmallInteger, primary_key=True)
    month         = Column(SmallInteger, primary_key=True)
    total_qty     = Column(Integer, nullable=False)
    total_revenue = Column(Numeric(19, 4), nullable=False)
    total_cost    = Column(Numeric(19, 4), nullable=False)
    total_margin  = Column(Numeric(19, 4), nullable=False)
    margin_pct    = Column(Numeric(8, 4))
    etl_loaded_at = Column(DateTime, default=datetime.now)


class AggCustomerRecurrence(OLAPBase):
    __tablename__ = "agg_customer_recurrence"
    __table_args__ = {"schema": "dw"}

    year          = Column(SmallInteger, primary_key=True)
    quarter       = Column(SmallInteger, primary_key=True)
    customer_type = Column(String(20), primary_key=True)
    customer_count= Column(Integer, nullable=False)
    order_count   = Column(Integer, nullable=False)
    total_revenue = Column(Numeric(19, 4), nullable=False)
    revenue_pct   = Column(Numeric(8, 4))
    etl_loaded_at = Column(DateTime, default=datetime.now)
