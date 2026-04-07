from supabase import create_client


def get_supabase_client():
    """
    Tạo Supabase client từ Kaggle Secrets.
    Nếu không chạy trên Kaggle thì fallback sang environment variables.
    """
    try:
        from kaggle_secrets import UserSecretsClient

        secrets = UserSecretsClient()
        supabase_url = secrets.get_secret("SUPABASE_URL")
        supabase_key = secrets.get_secret("SUPABASE_KEY")
    except Exception:
        import os

        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise ValueError("Thiếu SUPABASE_URL hoặc SUPABASE_KEY")

    return create_client(supabase_url, supabase_key)
