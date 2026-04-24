# src/config.py
# Hằng số dùng chung cho toàn bộ pipeline
# Person A quản lý — Person B không sửa trực tiếp

from datetime import datetime, timezone

# ─────────────────────────────────────────────────────
# KAGGLE — đường dẫn file CSV gốc
# ─────────────────────────────────────────────────────

KAGGLE_BASE = "/kaggle/input/datasets/psparks/instacart-market-basket-analysis"

# Mapping: tên bảng → đường dẫn CSV
BRONZE_TABLES = {
    "orders":                f"{KAGGLE_BASE}/orders.csv",
    "order_products__prior": f"{KAGGLE_BASE}/order_products__prior.csv",
    "order_products__train": f"{KAGGLE_BASE}/order_products__train.csv",
    "products":              f"{KAGGLE_BASE}/products.csv",
    "aisles":                f"{KAGGLE_BASE}/aisles.csv",
    "departments":           f"{KAGGLE_BASE}/departments.csv",
}

# ─────────────────────────────────────────────────────
# SUPABASE — kết nối
# ─────────────────────────────────────────────────────

SUPABASE_URL = None   # được set từ notebook
SUPABASE_KEY = None   # được set từ notebook

# Tên bucket
BRONZE_BUCKET = "bronze-data"
SILVER_BUCKET = "silver-data"
GOLD_BUCKET   = "gold-data"

# Prefix path bên trong bucket
# Prefix path bên trong bucket
BRONZE_PREFIX          = "instacart/bronze"
SILVER_PREFIX          = "instacart/silver"          # giữ nguyên — code cũ vẫn dùng
SILVER_MASTER_PREFIX   = "instacart/silver-master"   # ← thêm dòng này
SILVER_FEATURES_PREFIX = "instacart/silver-features" # ← thêm dòng này
GOLD_PREFIX            = "instacart/gold"

# ─────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────

# Số rows tối đa mỗi chunk
# orders ~3.2M       → ~7 chunks
# prior  ~32M        → ~64 chunks
CHUNK_THRESHOLD = 500_000

# ─────────────────────────────────────────────────────
# DTYPE MAP — ép kiểu để tối ưu RAM
# ─────────────────────────────────────────────────────
# Cột không có trong map → giữ nguyên dtype mặc định

DTYPE_MAP = {
    # ── orders ───────────────────────────────────────
    "order_id":               "int32",    # max ~3.4M   → int32 đủ
    "user_id":                "int32",    # max ~206K   → int32 đủ
    "order_number":           "int16",    # thường < 100
    "order_dow":              "int8",     # 0–6
    "order_hour_of_day":      "int8",     # 0–23
    "days_since_prior_order": "float32",  # có NaN → phải dùng float

    # ── order_products__prior + train ─────────────────
    "product_id":             "int32",    # max ~49K    → int32 đủ
    "add_to_cart_order":      "int16",    # thường < 150
    "reordered":              "int8",     # chỉ 0 hoặc 1

    # ── products ──────────────────────────────────────
    "aisle_id":               "int16",    # max 134     → int16 đủ
    "department_id":          "int8",     # max 21      → int8 đủ

    # eval_set, product_name, aisle, department
    # → giữ nguyên object (string), không cần ép
}
