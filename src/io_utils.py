"""
I/O Utilities for Parquet and Supabase Storage

Core functions:
- normalize_columns()
- to_parquet_bytes()
- upload_to_storage() / upload_to_storage_incremental()
- read_parquet_from_storage()
- iter_parquet_parts()
"""

import io
import gc
import time
import pandas as pd
from typing import Iterator, Tuple, Optional


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names: lowercase + underscore.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with normalized column names
    
    Example:
        >>> df.columns = ['Product ID', 'Product Name']
        >>> df = normalize_columns(df)
        >>> df.columns
        Index(['product_id', 'product_name'], dtype='object')
    """
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert DataFrame to Parquet bytes (Snappy compression).
    
    Không ghi ra disk → nhanh hơn, tiết kiệm I/O.
    
    Args:
        df: DataFrame to convert
    
    Returns:
        Parquet bytes
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    return buf.getvalue()


def upload_to_storage(
    supabase,
    bucket: str,
    storage_path: str,
    payload: bytes,
    max_retries: int = 3
) -> None:
    """
    Upload bytes lên Supabase Storage với retry logic.
    
    Args:
        supabase: Supabase client
        bucket: Bucket name
        storage_path: Path trong bucket
        payload: Bytes to upload
        max_retries: Max retry attempts
    
    Raises:
        RuntimeError: Nếu upload thất bại sau max_retries
    
    Note:
        - 409 duplicate → bỏ qua (không raise error)
        - Network error → retry với exponential backoff
    """
    for attempt in range(1, max_retries + 1):
        try:
            supabase.storage.from_(bucket).upload(
                storage_path,
                payload,
                file_options={
                    "content-type": "application/octet-stream",
                    "upsert": "false"
                },
            )
            return
        
        except Exception as e:
            msg = str(e).lower()
            
            # Duplicate → OK, skip
            if any(k in msg for k in ("duplicate", "already exists", "409")):
                print(f"  [SKIP] Already exists: {storage_path}")
                return
            
            # Other error → retry
            print(f"  [WARN] Attempt {attempt}/{max_retries}: {e}")
            
            if attempt < max_retries:
                time.sleep(attempt * 2)  # backoff: 2s, 4s, 6s
            else:
                raise RuntimeError(
                    f"Upload failed after {max_retries} attempts: {storage_path}"
                ) from e


def upload_to_storage_incremental(
    supabase,
    bucket: str,
    storage_path: str,
    payload: bytes,
    max_retries: int = 3
) -> str:
    """
    Incremental upload: check exist trước, skip nếu đã có.
    
    Khác với upload_to_storage():
    - Trả về status ("uploaded" | "skipped" | "failed")
    - Không raise error
    - Dùng cho retry scenarios
    
    Args:
        supabase: Supabase client
        bucket: Bucket name
        storage_path: Path trong bucket
        payload: Bytes to upload
        max_retries: Max retry attempts
    
    Returns:
        Status: "uploaded" | "skipped" | "failed"
    
    Example:
        >>> status = upload_to_storage_incremental(...)
        >>> if status == "uploaded":
        ...     uploaded_count += 1
        >>> elif status == "skipped":
        ...     skipped_count += 1
    """
    # Check exist
    try:
        supabase.storage.from_(bucket).download(storage_path)
        return "skipped"
    except:
        pass  # File chưa tồn tại → upload
    
    # Upload
    for attempt in range(1, max_retries + 1):
        try:
            supabase.storage.from_(bucket).upload(
                storage_path,
                payload,
                file_options={
                    "content-type": "application/octet-stream",
                    "upsert": "false"
                },
            )
            return "uploaded"
        
        except Exception as e:
            msg = str(e).lower()
            
            if any(k in msg for k in ("duplicate", "already exists", "409")):
                return "skipped"
            
            if attempt < max_retries:
                time.sleep(attempt * 2)
            else:
                return "failed"


def read_parquet_from_storage(
    supabase,
    bucket: str,
    path: str,
    n_parts: Optional[int] = None
) -> pd.DataFrame:
    """
    Đọc parquet từ Supabase Storage về DataFrame.
    
    Args:
        supabase: Supabase client
        bucket: Bucket name
        path: Path trong bucket (chứa các part-xxxxx.parquet)
        n_parts: Limit số parts để đọc (None = đọc hết)
    
    Returns:
        Concatenated DataFrame
    
    Raises:
        FileNotFoundError: Nếu path không có parquet files
    
    Example:
        >>> df = read_parquet_from_storage(
        ...     supabase,
        ...     "silver-data",
        ...     "instacart/silver/20260408_thuy/products"
        ... )
    """
    files = supabase.storage.from_(bucket).list(path)
    parts = sorted([f["name"] for f in files if f["name"].endswith(".parquet")])
    
    if not parts:
        raise FileNotFoundError(f"No parquet files at: {bucket}/{path}")
    
    if n_parts:
        parts = parts[:n_parts]
    
    result = []
    for fname in parts:
        raw = supabase.storage.from_(bucket).download(f"{path}/{fname}")
        result.append(pd.read_parquet(io.BytesIO(raw)))
        del raw
        gc.collect()
    
    return pd.concat(result, ignore_index=True)


def iter_parquet_parts(
    supabase,
    bucket: str,
    path: str
) -> Iterator[Tuple[str, pd.DataFrame]]:
    """
    Iterator cho parquet parts (để xử lý từng chunk).
    
    Args:
        supabase: Supabase client
        bucket: Bucket name
        path: Path trong bucket
    
    Yields:
        (filename, DataFrame) tuples
    
    Example:
        >>> for fname, chunk in iter_parquet_parts(supabase, bucket, path):
        ...     cleaned = clean_chunk(chunk)
        ...     upload(cleaned)
    """
    files = supabase.storage.from_(bucket).list(path)
    parts = sorted([f["name"] for f in files if f["name"].endswith(".parquet")])
    
    for fname in parts:
        raw = supabase.storage.from_(bucket).download(f"{path}/{fname}")
        chunk = pd.read_parquet(io.BytesIO(raw))
        
        yield fname, chunk
        
        del raw, chunk
        gc.collect()
