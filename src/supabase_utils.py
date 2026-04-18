# src/supabase_utils.py
# Các hàm tương tác với Supabase Storage
# upload, download, list files, ghi manifest
# Person A quản lý — dùng chung cho cả 3 layer

import io
import json
from datetime import datetime, timezone

import pandas as pd
from supabase import create_client

from src import config

# ─────────────────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────────────────

_client = None

def get_client():
    """
    Khởi tạo Supabase client 1 lần duy nhất (singleton).
    Các lần gọi sau trả về client đã có sẵn.
    """
    global _client
    if _client is None:
        _client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    return _client


# ─────────────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────────────

def upload_parquet(df: pd.DataFrame, bucket: str, path: str):
    """
    Upload 1 DataFrame thành 1 file .parquet lên Supabase Storage.

    Args:
        df     : DataFrame cần upload
        bucket : tên bucket    (vd: "bronze-data")
        path   : path trong bucket (vd: "instacart/bronze/{run_id}/products.parquet")
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    get_client().storage.from_(bucket).upload(
        path,
        buf.read(),
        file_options={
            "content-type": "application/octet-stream",
            "upsert": "true",   # ghi đè nếu đã tồn tại — safe khi re-run
        }
    )


# ─────────────────────────────────────────────────────
# DOWNLOAD
# ─────────────────────────────────────────────────────

def download_parquet(bucket: str, path: str) -> pd.DataFrame:
    """
    Download 1 file .parquet từ Supabase Storage về DataFrame.

    Args:
        bucket : tên bucket
        path   : path đầy đủ trong bucket
    Returns:
        pd.DataFrame
    """
    data = get_client().storage.from_(bucket).download(path)
    return pd.read_parquet(io.BytesIO(data))


# ─────────────────────────────────────────────────────
# LIST FILES
# ─────────────────────────────────────────────────────

def list_files(bucket: str, prefix: str) -> list[str]:
    """
    Liệt kê các file .parquet trong 1 folder trên Supabase.

    Args:
        bucket : tên bucket
        prefix : đường dẫn folder (vd: "instacart/bronze/{run_id}/orders")
    Returns:
        list tên file .parquet, ví dụ ["part-00001.parquet", "part-00002.parquet"]
    """
    res = get_client().storage.from_(bucket).list(prefix)
    return [f["name"] for f in res if f["name"].endswith(".parquet")]


# ─────────────────────────────────────────────────────
# MANIFEST
# ─────────────────────────────────────────────────────

def write_manifest(
    bucket:        str,
    prefix:        str,
    run_id:        str,
    layer:         str,
    created_by:    str,
    tables:        dict,
    source_run_id: str = None,
):
    """
    Ghi file _manifest.json sau khi 1 layer chạy xong.

    File lưu tại: {bucket}/{prefix}/{run_id}/_manifest.json

    Args:
        bucket        : tên bucket
        prefix        : prefix path  (vd: "instacart/bronze")
        run_id        : RUN_ID hiện tại
        layer         : "bronze" | "silver" | "gold"
        created_by    : "person_a"  | "person_b"
        tables        : {"table_name": {"rows": int}}
        source_run_id : run_id của layer trước
                        → None ở Bronze (không có layer trước)
                        → BRONZE_RUN_ID ở Silver
                        → SILVER_RUN_ID ở Gold

    Ví dụ output:
        {
            "run_id":        "20260418T090000Z",
            "layer":         "silver",
            "created_by":    "person_a",
            "created_at":    "2026-04-18T09:00:00+00:00",
            "source_run_id": "20260418T083000Z",
            "tables": {
                "user_stats": {"rows": 206209}
            }
        }
    """
    manifest = {
        "run_id":     run_id,
        "layer":      layer,
        "created_by": created_by,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tables":     tables,
    }

    # Chỉ thêm source_run_id nếu có (Silver và Gold)
    if source_run_id:
        manifest["source_run_id"] = source_run_id

    path = f"{prefix}/{run_id}/_manifest.json"
    data = json.dumps(manifest, indent=2).encode("utf-8")

    get_client().storage.from_(bucket).upload(
        path,
        data,
        file_options={
            "content-type": "application/json",
            "upsert": "true",
        }
    )
    print(f"  ✓ manifest → {bucket}/{path}")
