from supabase import create_client
from kaggle_secrets import UserSecretsClient

def get_client():
    user_secrets = UserSecretsClient()
    supabase_url = user_secrets.get_secret("SUPABASE_URL")
    supabase_key = user_secrets.get_secret("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise ValueError("Thiếu SUPABASE_URL hoặc SUPABASE_KEY trong Kaggle Secrets.")

    return create_client(supabase_url, supabase_key)
