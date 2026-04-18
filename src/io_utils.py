# src/io_utils.py
# Các hàm xử lý DataFrame dùng chung
# normalize, check, optimize dtype, upload/load tự detect chunk
# Person A quản lý — dùng chung cho cả 3 layer

import math
import pandas as pd

from src import config
from src.supabase_utils import upload_parquet, download_parquet, list_files


# ─────────────────────────────────────────────────────
# NORMALIZE — chuẩn hóa tên cột
# ─────────────────────────────────────────────────────

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa tên cột về dạng snake_case:
        - lowercase
        - strip khoảng trắng 2 đầu
        - thay space và dash → underscore

    Ví dụ:
        "Order ID"      → "order_id"
        "order-number"  → "order_number"
        " User_ID "     → "user_id"
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
    )
    return df


# ─────────────────────────────────────────────────────
# CHECK — kiểm tra cấu trúc cơ bản
# ─────────────────────────────────────────────────────

def check_basic_structure(df: pd.DataFrame, table_name: str):
    """
    Kiểm tra DataFrame sau khi đọc CSV:
        - không được rỗng (0 rows)
        - phải có ít nhất 1 cột

    Raise AssertionError nếu không hợp lệ.

    Args:
        df         : DataFrame cần kiểm tra
        table_name : tên bảng để hiện trong thông báo lỗi
    """
    assert len(df) > 0,         f"[{table_name}] ❌ DataFrame rỗng"
    assert len(df.columns) > 0, f"[{table_name}] ❌ Không có cột nào"
    print(f"  ✓ {table_name}: {len(df):>10,} rows | {len(df.columns)} cols")


# ─────────────────────────────────────────────────────
# OPTIMIZE DTYPE — ép kiểu tiết kiệm RAM
# ─────────────────────────────────────────────────────

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ép kiểu các cột theo DTYPE_MAP trong config.py.
    Chỉ ép cột nào có trong DataFrame, bỏ qua cột không tồn tại.

    Ngoài DTYPE_MAP, hàm còn:
        - ép eval_set → category (chỉ 3 giá trị: prior/train/test)

    Lợi ích:
        int64  → int32/int16/int8  : giảm 2–8x RAM
        float64→ float32           : giảm 2x RAM
        object → category          : giảm RAM nếu cardinality thấp
    """
    # ép theo DTYPE_MAP
    for col, dtype in config.DTYPE_MAP.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    # eval_set chỉ có 3 giá trị → category tiết kiệm RAM
    if "eval_set" in df.columns:
        df["eval_set"] = df["eval_set"].astype("category")

    return df


# ─────────────────────────────────────────────────────
# UPLOAD — tự detect chunk hay file đơn
# ─────────────────────────────────────────────────────

def upload_dataframe(
    df:         pd.DataFrame,
    bucket:     str,
    prefix:     str,
    run_id:     str,
    table_name: str,
):
    """
    Upload DataFrame lên Supabase, tự quyết định chunk hay file đơn:

        len(df) > CHUNK_THRESHOLD (500,000 rows)
            → chia chunk, upload vào folder
            → path: {prefix}/{run_id}/{table_name}/part-00001.parquet

        len(df) <= CHUNK_THRESHOLD
            → upload 1 file parquet
            → path: {prefix}/{run_id}/{table_name}.parquet

    Args:
        df         : DataFrame cần upload
        bucket     : tên bucket     (vd: "bronze-data")
        prefix     : prefix path    (vd: "instacart/bronze")
        run_id     : RUN_ID hiện tại
        table_name : tên bảng       (vd: "orders")
    """
    if len(df) > config.CHUNK_THRESHOLD:
        n_chunks = math.ceil(len(df) / config.CHUNK_THRESHOLD)

        for i in range(n_chunks):
            start = i * config.CHUNK_THRESHOLD
            end   = min(start + config.CHUNK_THRESHOLD, len(df))
            chunk = df.iloc[start:end]

            path = f"{prefix}/{run_id}/{table_name}/part-{i+1:05d}.parquet"
            upload_parquet(chunk, bucket, path)

        print(f"  ✓ uploaded [{table_name}]:"
              f" {n_chunks} chunks | {len(df):,} rows")
    else:
        path = f"{prefix}/{run_id}/{table_name}.parquet"
        upload_parquet(df, bucket, path)
        print(f"  ✓ uploaded [{table_name}]:"
              f" 1 file | {len(df):,} rows")


# ─────────────────────────────────────────────────────
# LOAD — tự detect chunk hay file đơn
# ─────────────────────────────────────────────────────

def load_dataframe(
    bucket:     str,
    prefix:     str,
    run_id:     str,
    table_name: str,
) -> pd.DataFrame:
    """
    Load DataFrame từ Supabase, tự detect folder chunk hay file đơn:

        Nếu tồn tại folder {prefix}/{run_id}/{table_name}/
            → đọc tất cả part-*.parquet theo thứ tự rồi concat
        Ngược lại
            → đọc file đơn {prefix}/{run_id}/{table_name}.parquet

    Dùng cho:
        Silver đọc Bronze  → load_dataframe(BRONZE_BUCKET, ..., BRONZE_RUN_ID, "orders")
        Gold   đọc Silver  → load_dataframe(SILVER_BUCKET, ..., SILVER_RUN_ID, "user_stats")

    Args:
        bucket     : tên bucket
        prefix     : prefix path
        run_id     : RUN_ID của layer cần đọc
        table_name : tên bảng
    Returns:
        pd.DataFrame
    """
    folder_prefix = f"{prefix}/{run_id}/{table_name}"
    files = list_files(bucket, folder_prefix)

    if files:
        # đọc từng chunk theo thứ tự tên file
        dfs = [
            download_parquet(bucket, f"{folder_prefix}/{f}")
            for f in sorted(files)
        ]
        df = pd.concat(dfs, ignore_index=True)
        print(f"  ✓ loaded [{table_name}]:"
              f" {len(files)} chunks | {len(df):,} rows")
        return df
    else:
        df = download_parquet(
            bucket, f"{prefix}/{run_id}/{table_name}.parquet"
        )
        print(f"  ✓ loaded [{table_name}]:"
              f" 1 file | {len(df):,} rows")
        return df
