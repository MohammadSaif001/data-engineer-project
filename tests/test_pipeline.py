import os
import sys
import unittest
import tempfile

import pandas as pd

# Ensure python/ is on the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
python_folder = os.path.join(project_root, "python")
if python_folder not in sys.path:
    sys.path.insert(0, python_folder)

from utils.paths import get_project_root, get_config_path, get_raw_data_path, get_logs_path
from utils.db_connection import load_config
from bronze.load_bronze import read_bronze_csv, add_raw_row


class TestPaths(unittest.TestCase):
    """Tests for cross-platform path utilities."""

    def test_project_root_exists(self):
        root = get_project_root()
        self.assertTrue(os.path.isdir(root))

    def test_config_path_exists(self):
        self.assertTrue(os.path.isfile(get_config_path()))

    def test_logs_path_is_under_project(self):
        log = get_logs_path("test.log")
        self.assertIn(get_project_root(), log)


class TestDbConfig(unittest.TestCase):
    """Tests for database configuration loading."""

    def test_load_config_has_mysql_key(self):
        cfg = load_config()
        self.assertIn("mysql", cfg)

    def test_password_not_hardcoded_in_config_file(self):
        cfg = load_config()
        password = cfg["mysql"].get("password", "")
        self.assertEqual(password, "",
                         "Config file should not contain a password; "
                         "use DB_PASSWORD environment variable instead")


class TestBronzeHelpers(unittest.TestCase):
    """Tests for bronze layer CSV helpers."""

    def test_read_bronze_csv(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("Name,Age\nAlice,30\nBob,25\n")
            f.flush()
            df = read_bronze_csv(f.name)

        os.unlink(f.name)
        self.assertEqual(list(df.columns), ["name", "age"])
        self.assertEqual(len(df), 2)

    def test_add_raw_row(self):
        df = pd.DataFrame({"a": ["1"], "b": ["2"]})
        result = add_raw_row(df)
        self.assertIn("raw_row", result.columns)


class TestExtractFromBronzeValidation(unittest.TestCase):
    """Tests for SQL injection protection in extract_from_bronze."""

    def test_disallowed_table_name_raises(self):
        from silver.crm.crm_customers import extract_from_bronze
        with self.assertRaises(ValueError):
            extract_from_bronze("DROP TABLE users; --")


if __name__ == "__main__":
    unittest.main()
