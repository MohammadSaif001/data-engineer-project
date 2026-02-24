import os
import sys
import unittest

import pandas as pd

# Ensure python/ is on the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
python_folder = os.path.join(project_root, "python")
if python_folder not in sys.path:
    sys.path.insert(0, python_folder)

from silver.crm.crm_sales import validate_data, clean_sales_data


class TestSalesDataQuality(unittest.TestCase):
    """Tests for sales data validation and cleaning."""

    def _make_valid_df(self):
        return pd.DataFrame({
            "sales_price": [10.0, 20.0],
            "sales_quantity": pd.array([2, 3], dtype="Int64"),
            "sales_sales": [20.0, 60.0],
            "sales_order_date_raw": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "sales_ship_date_raw": pd.to_datetime(["2024-01-10", "2024-02-10"]),
        })

    def test_valid_records_pass(self):
        df = self._make_valid_df()
        valid, invalid = validate_data(df)
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 0)

    def test_negative_price_flagged(self):
        df = self._make_valid_df()
        df.loc[0, "sales_price"] = -5.0
        valid, invalid = validate_data(df)
        self.assertEqual(len(invalid), 1)

    def test_clean_sales_recalculates(self):
        df = self._make_valid_df()
        df["sales_sales"] = [0.0, 0.0]
        result = clean_sales_data(df)
        self.assertAlmostEqual(result["sales_sales"].iloc[0], 20.0)
        self.assertAlmostEqual(result["sales_sales"].iloc[1], 60.0)

    def test_clean_sales_abs_negatives(self):
        df = self._make_valid_df()
        df["sales_price"] = [-10.0, -20.0]
        result = clean_sales_data(df)
        self.assertTrue((result["sales_price"] >= 0).all())


if __name__ == "__main__":
    unittest.main()
