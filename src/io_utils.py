from supabase import create_client

def get_supabase_client():
    """
    Tạo Supabase client từ Kaggle Secrets.
    Import hàm này ở mọi notebook thay vì viết lại.
    """
    try:
        from kaggle_secrets import UserSecretsClient
        secrets      = UserSecretsClient()
        SUPABASE_URL = secrets.get_secret("SUPABASE_URL")
        SUPABASE_KEY = secrets.get_secret("SUPABASE_KEY")
    except Exception:
        # Chạy local (không phải Kaggle) → dùng env variable
        import os
        SUPABASE_URL = os.environ.get("SUPABASE_URL")
        SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")

    return create_client(SUPABASE_URL, SUPABASE_KEY)
