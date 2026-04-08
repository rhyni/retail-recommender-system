"""
Configuration Constants

Centralized config cho toàn bộ project.
"""

from datetime import datetime, timezone

# ── Kaggle Dataset ───────────────────────────────────────────────────────────
DATASET_DIR = "/kaggle/input/datasets/psparks/instacart-market-basket-analysis"

# ── Layer Buckets (đã move sang run_config.py, nhưng giữ lại để backward compat) ─
BRONZE_BUCKET = "bronze-data"
SILVER_BUCKET = "silver-data"
GOLD_BUCKET = "gold-data"
ANALYSIS_BUCKET = "analysis-data"
BI_BUCKET = "bi-export"

# ── Base Paths ───────────────────────────────────────────────────────────────
BRONZE_BASE = "instacart/bronze"
SILVER_BASE = "instacart/silver"
GOLD_BASE = "instacart/gold"
ANALYSIS_BASE = "instacart/analysis"
BI_BASE = "instacart/bi"

# ── File Configuration ───────────────────────────────────────────────────────
FILE_CONFIG = [
    {
        "name": "aisles",
        "chunksize": None,
        "dtypes": {"aisle_id": "int16", "aisle": "str"},
    },
    {
        "name": "departments",
        "chunksize": None,
        "dtypes": {"department_id": "int8", "department": "str"},
    },
    {
        "name": "products",
        "chunksize": None,
        "dtypes": {
            "product_id": "int32",
            "product_name": "str",
            "aisle_id": "int16",
            "department_id": "int8",
        },
    },
    {
        "name": "orders",
        "chunksize": 500_000,
        "dtypes": {
            "order_id": "int32",
            "user_id": "int32",
            "eval_set": "category",
            "order_number": "int16",
            "order_dow": "int8",
            "order_hour_of_day": "int8",
            "days_since_prior_order": "float32",
        },
    },
    {
        "name": "order_products__train",
        "chunksize": 500_000,
        "dtypes": {
            "order_id": "int32",
            "product_id": "int32",
            "add_to_cart_order": "int16",
            "reordered": "int8",
        },
    },
    {
        "name": "order_products__prior",
        "chunksize": 1_000_000,
        "dtypes": {
            "order_id": "int32",
            "product_id": "int32",
            "add_to_cart_order": "int16",
            "reordered": "int8",
        },
    },
]

# ── Expected Schema ──────────────────────────────────────────────────────────
EXPECTED_COLS = {
    "aisles": ["aisle_id", "aisle"],
    "departments": ["department_id", "department"],
    "products": ["product_id", "product_name", "aisle_id", "department_id"],
    "orders": [
        "order_id", "user_id", "eval_set", "order_number",
        "order_dow", "order_hour_of_day", "days_since_prior_order"
    ],
    "order_products__train": ["order_id", "product_id", "add_to_cart_order", "reordered"],
    "order_products__prior": ["order_id", "product_id", "add_to_cart_order", "reordered"],
}

# ── Expected Row Counts (for validation) ─────────────────────────────────────
EXPECTED_ROWS = {
    "aisles": 134,
    "departments": 21,
    "products": 49_688,
    "orders": 3_421_083,
    "order_products__train": 1_384_617,
    "order_products__prior": 32_434_489,
}
