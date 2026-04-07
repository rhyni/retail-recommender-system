import io
import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa tên cột:
    - strip
    - lower
    - space -> underscore
    """
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace(" ", "_")
        for col in df.columns
    ]
    return df


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert DataFrame -> parquet bytes bằng pyarrow + snappy.
    """
    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )
    buffer.seek(0)
    return buffer.read()


def upload_to_storage(
    supabase,
    bucket: str,
    storage_path: str,
    payload: bytes,
    content_type: str = "application/octet-stream",
    upsert: str = "false",
):
    """
    Upload bytes lên Supabase Storage.
    """
    return supabase.storage.from_(bucket).upload(
        storage_path,
        payload,
        file_options={
            "content-type": content_type,
            "upsert": upsert,
        },
    )


def read_parquet_from_storage(supabase, bucket: str, storage_path: str) -> pd.DataFrame:
    """
    Download parquet từ Supabase Storage và đọc thành DataFrame.
    """
    data = supabase.storage.from_(bucket).download(storage_path)
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")
