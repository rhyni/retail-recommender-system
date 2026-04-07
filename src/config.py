config_code = '''
from datetime import datetime, timezone

DATASET_DIR = "/kaggle/input/datasets/psparks/instacart-market-basket-analysis"

BRONZE_BUCKET = "bronze-data"
BRONZE_BASE = "instacart/bronze"
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BRONZE_PREFIX = f"{BRONZE_BASE}/{RUN_ID}"

FILE_CONFIG = [
    {"name": "aisles", "chunksize": None},
    {"name": "departments", "chunksize": None},
    {"name": "products", "chunksize": None},
    {"name": "orders", "chunksize": 500_000},
    {"name": "order_products__prior", "chunksize": 500_000},
    {"name": "order_products__train", "chunksize": 500_000},
]

EXPECTED_COLS = {}
EXPECTED_ROWS = {}
'''.strip()

file_path = "/kaggle/working/retail-recommender-system/src/config.py"

with open(file_path, "w", encoding="utf-8") as f:
    f.write(config_code)

print("written:", file_path)
