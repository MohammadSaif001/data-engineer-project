import os
import sys
import unittest

import pandas as pd
import numpy as np

# Ensure python/ is on the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
python_folder = os.path.join(project_root, "python")
if python_folder not in sys.path:
    sys.path.insert(0, python_folder)

from silver.crm.crm_customers import normalize_data, standardize_data, enforce_schema
from silver.crm.crm_customers import schema as customer_schema


class TestCustomerNormalization(unittest.TestCase):
    """Tests for customer data normalization in the silver layer."""

    def _make_df(self):
        return pd.DataFrame({
            "cst_id": ["1", "2"],
            "cst_key": ["K1", "K2"],
            "cst_firstname": ["  alice  ", "  bob  "],
            "cst_lastname": ["  smith  ", "  jones  "],
            "cst_marital_status": ["s", "m"],
            "cst_gndr": ["f", "m"],
            "cst_create_date_raw": ["2024-01-01", "2024-06-15"],
            "raw_row": ['{"a":1}', '{"b":2}'],
        })

    def test_normalize_strips_whitespace_and_drops_raw_row(self):
        df = self._make_df()
        df = enforce_schema(df, customer_schema)
        result = normalize_data(df)
        self.assertNotIn("raw_row", result.columns)
        self.assertEqual(result["cst_firstname"].iloc[0], "Alice")
        self.assertEqual(result["cst_lastname"].iloc[1], "Jones")

    def test_standardize_gender(self):
        df = self._make_df()
        df = enforce_schema(df, customer_schema)
        df = normalize_data(df)
        result = standardize_data(df)
        self.assertEqual(result["cst_gndr"].iloc[0], "Female")
        self.assertEqual(result["cst_gndr"].iloc[1], "Male")

    def test_standardize_marital_status(self):
        df = self._make_df()
        df = enforce_schema(df, customer_schema)
        df = normalize_data(df)
        result = standardize_data(df)
        self.assertEqual(result["cst_marital_status"].iloc[0], "Single")
        self.assertEqual(result["cst_marital_status"].iloc[1], "Married")

    def test_standardize_unknown_gender(self):
        df = self._make_df()
        df["cst_gndr"] = ["X", None]
        df = enforce_schema(df, customer_schema)
        df = normalize_data(df)
        result = standardize_data(df)
        self.assertTrue((result["cst_gndr"] == "Unknown").all())


if __name__ == "__main__":
    unittest.main()
