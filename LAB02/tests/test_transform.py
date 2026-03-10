"""Tests para el módulo de transformación."""
import unittest
from datetime import date
from decimal import Decimal
from src.transform import (
    transform_date, transform_product, transform_customer,
    transform_fact_sales, transform_fact_orders
)
from src.utils.helpers import date_to_key, get_quarter, price_range


class TestTransformDate(unittest.TestCase):
    def test_transform_date_basic(self):
        d = date(2013, 6, 15)
        result = transform_date(d)
        self.assertEqual(result["date_key"], 20130615)
        self.assertEqual(result["year"], 2013)
        self.assertEqual(result["month"], 6)
        self.assertEqual(result["quarter"], 2)
        self.assertEqual(result["month_name"], "June")
        self.assertFalse(result["is_weekend"])

    def test_transform_date_weekend(self):
        d = date(2013, 6, 16)  # Sunday
        result = transform_date(d)
        self.assertTrue(result["is_weekend"])

    def test_date_key(self):
        self.assertEqual(date_to_key(date(2011, 1, 1)), 20110101)
        self.assertEqual(date_to_key(date(2014, 12, 31)), 20141231)

    def test_quarter(self):
        self.assertEqual(get_quarter(1), 1)
        self.assertEqual(get_quarter(4), 2)
        self.assertEqual(get_quarter(7), 3)
        self.assertEqual(get_quarter(10), 4)


class TestTransformProduct(unittest.TestCase):
    def setUp(self):
        self.row = {
            "product_id": 1, "product_number": "BK-M68B-38",
            "product_name": "Mountain-200 Black, 38",
            "color": "Black", "size": "38",
            "list_price": Decimal("2294.99"), "standard_cost": Decimal("1251.49"),
            "subcategory_id": 1, "subcategory_name": "Mountain Bikes",
            "category_id": 1, "category_name": "Bikes"
        }

    def test_transform_product_bike(self):
        result = transform_product(self.row)
        self.assertEqual(result["product_id"], 1)
        self.assertTrue(result["is_bike"])
        self.assertFalse(result["is_accessory"])
        self.assertEqual(result["price_range"], "High")

    def test_price_range(self):
        self.assertEqual(price_range(50), "Low")
        self.assertEqual(price_range(500), "Mid")
        self.assertEqual(price_range(2000), "High")


class TestTransformCustomer(unittest.TestCase):
    def setUp(self):
        self.row = {
            "customer_id": 1, "account_number": "AW00000001",
            "first_name": "Ken", "last_name": "Sánchez",
            "full_name": "Ken Sánchez", "territory_id": 1,
            "territory_name": "Northwest", "region_group": "North America",
            "country_code": "US"
        }

    def test_transform_customer_with_cohort(self):
        fod = date(2011, 5, 31)
        result = transform_customer(self.row, fod)
        self.assertEqual(result["cohort_key"], "2011-05")
        self.assertEqual(result["cohort_year"], 2011)
        self.assertEqual(result["cohort_month"], 5)

    def test_transform_customer_no_orders(self):
        result = transform_customer(self.row, None)
        self.assertIsNone(result["cohort_key"])


class TestTransformFactSales(unittest.TestCase):
    def setUp(self):
        self.row = {
            "sales_order_id": 1, "sales_order_detail_id": 1,
            "order_date": date(2011, 5, 31),
            "customer_id": 1, "territory_id": 1,
            "product_id": 4, "order_qty": 1,
            "unit_price": Decimal("3578.27"), "unit_price_discount": Decimal("0"),
            "standard_cost": Decimal("2171.29"), "list_price": Decimal("3578.27"),
            "is_online": True, "subcategory_id": 2,
            "subcategory_name": "Road Bikes", "category_id": 1, "category_name": "Bikes"
        }

    def test_margin_calculation(self):
        result = transform_fact_sales(self.row, customer_key=1, product_key=1, territory_key=1)
        self.assertEqual(result["line_total"], Decimal("3578.27"))
        self.assertEqual(result["cost_total"], Decimal("2171.29"))
        expected_margin = Decimal("3578.27") - Decimal("2171.29")
        self.assertAlmostEqual(float(result["gross_margin"]), float(expected_margin), places=2)

    def test_date_key(self):
        result = transform_fact_sales(self.row, customer_key=1, product_key=1, territory_key=1)
        self.assertEqual(result["date_key"], 20110531)


if __name__ == "__main__":
    unittest.main()
