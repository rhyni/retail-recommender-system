import io
import gc
import time
import pandas as pd

def normalize_columns(df):
    """Lowercase + underscore tên cột."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def to_parquet_bytes(df):
    """DataFrame → Parquet bytes (Snappy), không ghi ra disk."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    return buf.getvalue()


def upload_to_storage(supabase, bucket, storage_path, payload, max_retries=3):
    """
    Upload bytes lên Supabase Storage.
    - 409 duplicate → bỏ qua
    - Lỗi mạng → retry 3 lần, backoff 2s/4s/6s
    """
    for attempt in range(1, max_retries + 1):
        try:
            supabase.storage.from_(bucket).upload(
                storage_path, payload,
                file_options={
                    "content-type": "application/octet-stream",
                    "upsert": "false"
                },
            )
            return
        except Exception as e:
            msg = str(e).lower()
            if any(k in msg for k in ("duplicate", "already exists", "409")):
                print(f"  [BỎ QUA] Đã tồn tại: {storage_path}")
                return
            print(f"  [WARN] Lần {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                time.sleep(attempt * 2)
            else:
                raise RuntimeError(
                    f"Upload thất bại sau {max_retries} lần: {storage_path}"
                ) from e


def read_parquet_from_storage(supabase, bucket, path, n_parts=None):
    """Đọc parquet từ Supabase Storage về DataFrame."""
    files = supabase.storage.from_(bucket).list(path)
    parts = sorted([f["name"] for f in files if f["name"].endswith(".parquet")])

    if not parts:
        raise FileNotFoundError(f"Không tìm thấy parquet tại: {path}")

    if n_parts:
        parts = parts[:n_parts]

    result = []
    for fname in parts:
        raw = supabase.storage.from_(bucket).download(f"{path}/{fname}")
        result.append(pd.read_parquet(io.BytesIO(raw)))
        del raw
        gc.collect()

    return pd.concat(result, ignore_index=True)


def get_latest_run_id(supabase, bucket, base_path):
    """Lấy RUN_ID mới nhất từ bucket."""
    folders = supabase.storage.from_(bucket).list(base_path)
    run_ids = sorted(
        [f["name"] for f in folders
         if f["name"] and f["name"][0].isdigit()],
        reverse=True
    )
    if not run_ids:
        raise RuntimeError(f"Không tìm thấy run_id tại: {base_path}")
    return run_ids[0]
