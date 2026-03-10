"""Tests para el módulo de carga (sin base de datos real — mock)."""
import unittest
from unittest.mock import MagicMock, patch, call
from src.load import load_dim_dates, load_dim_customers, load_dim_products
from src.transform import transform_date
from datetime import date


class TestLoadDimDate(unittest.TestCase):
    def test_load_empty_rows(self):
        """No debe fallar con lista vacía."""
        mock_session = MagicMock()
        # Con rows vacío, no se llama execute
        load_dim_dates(mock_session, [])
        mock_session.execute.assert_not_called()

    def test_transform_and_load_dates(self):
        """Verifica que las fechas se transforman correctamente antes de cargar."""
        dates = [transform_date(date(2011, 1, d)) for d in range(1, 4)]
        self.assertEqual(len(dates), 3)
        self.assertEqual(dates[0]["date_key"], 20110101)
        self.assertEqual(dates[1]["date_key"], 20110102)
        self.assertEqual(dates[2]["date_key"], 20110103)


class TestLoadDimCustomer(unittest.TestCase):
    def test_customer_rows_structure(self):
        """Verifica estructura de un registro de cliente."""
        from src.transform import transform_customer
        row = {
            "customer_id": 1, "account_number": "AW00000001",
            "first_name": "Ken", "last_name": "Sánchez", "full_name": "Ken Sánchez",
            "territory_id": 1, "territory_name": "Northwest",
            "region_group": "North America", "country_code": "US"
        }
        result = transform_customer(row, date(2011, 5, 31))
        required_keys = ["customer_id", "account_number", "full_name",
                         "cohort_key", "cohort_year", "cohort_month"]
        for key in required_keys:
            self.assertIn(key, result, f"Falta clave: {key}")


class TestLoadDimProduct(unittest.TestCase):
    def test_product_classification(self):
        """Verifica que los productos se clasifican correctamente."""
        from src.transform import transform_product
        bike_row = {
            "product_id": 1, "product_number": "BK-M68B-38",
            "product_name": "Mountain-200", "color": "Black", "size": "38",
            "list_price": 2294.99, "standard_cost": 1251.49,
            "subcategory_id": 1, "subcategory_name": "Mountain Bikes",
            "category_id": 1, "category_name": "Bikes"
        }
        acc_row = {**bike_row, "product_id": 2, "product_name": "Helmet",
                   "category_name": "Accessories", "subcategory_name": "Helmets",
                   "list_price": 34.99, "standard_cost": 13.09}

        bike_result = transform_product(bike_row)
        acc_result  = transform_product(acc_row)

        self.assertTrue(bike_result["is_bike"])
        self.assertFalse(bike_result["is_accessory"])
        self.assertFalse(acc_result["is_bike"])
        self.assertTrue(acc_result["is_accessory"])


if __name__ == "__main__":
    unittest.main()
