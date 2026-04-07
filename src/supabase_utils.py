# src/supabase_utils.py
import time
from supabase import create_client, Client

# Singleton pattern
_supabase_client = None

def get_supabase_client(url: str, key: str) -> Client:
    """Khởi tạo và trả về client Supabase duy nhất"""
    global _supabase_client
    if _supabase_client is None:
        if not url or not key:
            raise ValueError("Thiếu URL hoặc Key của Supabase.")
        _supabase_client = create_client(url, key)
    return _supabase_client

def upload_to_storage(client: Client, bucket: str, storage_path: str, payload: bytes, max_retries: int = 3):
    """
    Upload file bytes lên Supabase Storage với cơ chế Retry.
    """
    for attempt in range(1, max_retries + 1):
        try:
            client.storage.from_(bucket).upload(
                storage_path, payload,
                file_options={"content-type": "application/octet-stream", "upsert": "false"},
            )
            return True
        except Exception as e:
            msg = str(e).lower()
            if any(k in msg for k in ("duplicate", "already exists", "409")):
                print(f"  [BỎ QUA] Đã tồn tại: {storage_path}")
                return True
            
            print(f"  [WARN] Upload thất bại lần {attempt}/{max_retries}: {e}")
            if attempt < max_retries:
                time.sleep(attempt * 2)
            else:
                raise RuntimeError(f"Upload thất bại sau {max_retries} lần: {storage_path}") from e
