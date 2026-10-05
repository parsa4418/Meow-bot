"""ذخیره امن Session: رمزنگاری با Fernet و نگهداری در Supabase (یا فایل محلی/دیسک Render)."""
import os
import httpx
from cryptography.fernet import Fernet, InvalidToken

_fernet = Fernet(os.environ["SESSION_ENC_KEY"].encode())
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FILE_PATH = os.environ.get("SESSION_FILE", "/data/session.enc")
ROW_ID = "main"


def _headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"}


async def save_session(s: str) -> None:
    enc = _fernet.encrypt(s.encode()).decode()
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{SUPABASE_URL}/rest/v1/tg_session",
                             headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
                             json={"id": ROW_ID, "data": enc})
            r.raise_for_status()
    else:
        os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)
        with open(FILE_PATH, "w") as f:
            f.write(enc)


async def load_session():
    enc = None
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/tg_session",
                            headers=_headers(), params={"id": f"eq.{ROW_ID}", "select": "data"})
            r.raise_for_status()
            rows = r.json()
            enc = rows[0]["data"] if rows else None
    elif os.path.exists(FILE_PATH):
        enc = open(FILE_PATH).read()
    if not enc:
        return None
    try:
        return _fernet.decrypt(enc.encode()).decode()
    except InvalidToken:
        return None


async def delete_session() -> None:
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            await c.delete(f"{SUPABASE_URL}/rest/v1/tg_session",
                           headers=_headers(), params={"id": f"eq.{ROW_ID}"})
    elif os.path.exists(FILE_PATH):
        os.remove(FILE_PATH)
          
