# src/io_utils.py
import io
import pandas as pd
from src.config import EXPECTED_COLS

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hóa tên cột: lowercase + underscore."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def verify_schema(df: pd.DataFrame, name: str, part_num: int):
    """
    Kiểm tra schema trên chunk.
    - Thiếu cột bắt buộc -> Dừng pipeline.
    - Dư cột -> Cảnh báo.
    """
    expected = EXPECTED_COLS[name]
    actual   = df.columns.tolist()
    missing  = [c for c in expected if c not in actual]

    if missing:
        raise ValueError(f"[SCHEMA ERROR] Bảng '{name}' chunk {part_num:05d} thiếu cột: {missing}")

    extra = [c for c in actual if c not in expected]
    if extra:
        print(f"  [WARN] Chunk {part_num:05d}: có {len(extra)} cột thừa: {extra}")
    else:
        print(f"  schema OK ({len(expected)} cols) — chunk {part_num:05d}")

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame -> Parquet bytes (Snappy)."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    return buf.getvalue()
