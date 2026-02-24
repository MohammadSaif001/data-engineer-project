# File: python/utils/db_connection.py
import json
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

try:
    from .paths import get_config_path
except ImportError:
    from utils.paths import get_config_path

def load_config():
    config_path = get_config_path()

    with open(config_path, "r") as file:
        return json.load(file)

def get_engine(layer="bronze"):
    full_config = load_config()
    
    # Ensure 'mysql' key exists
    if 'mysql' not in full_config:
        raise KeyError("'mysql' section missing in db_config.json")

    cfg = full_config['mysql']
    
    # Prefer environment variables over config file values
    user = os.environ.get('DB_USER', cfg['user'])
    pwd = os.environ.get('DB_PASSWORD', cfg.get('password', ''))
    host = os.environ.get('DB_HOST', cfg['host'])
    port = int(os.environ.get('DB_PORT', cfg.get('port', 3306)))
    
    db_map = {
        "bronze": os.environ.get('DB_BRONZE', cfg['bronze_db']),
        "silver": os.environ.get('DB_SILVER', cfg['silver_db']),
        "gold": os.environ.get('DB_GOLD', cfg['gold_db'])
    }
    
    if layer not in db_map:
        raise ValueError(f"Unknown layer: {layer}")
        
    dbname = db_map[layer]
    pwd_quoted = quote_plus(pwd)
    
    url = f"mysql+pymysql://{user}:{pwd_quoted}@{host}:{port}/{dbname}"
    
    return create_engine(url, pool_pre_ping=True)