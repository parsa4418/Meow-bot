"""ذخیره Session در فایل (یا Supabase اگر تنظیم شده باشد). رمزنگاری اختیاری است."""
import os
import httpx
from cryptography.fernet import Fernet, InvalidToken

_key = os.environ.get("SESSION_ENC_KEY")
_fernet = Fernet(_key.encode()) if _key else None
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
FILE_PATH = os.environ.get("SESSION_FILE", "/tmp/session.enc")
ROW_ID = "main"


def _enc(s: str) -> str:
    return _fernet.encrypt(s.encode()).decode() if _fernet else s


def _dec(s: str):
    if not _fernet:
        return s
    try:
        return _fernet.decrypt(s.encode()).decode()
    except InvalidToken:
        return None


def _headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"}


async def save_session(s: str) -> None:
    data = _enc(s)
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{SUPABASE_URL}/rest/v1/tg_session",
                             headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
                             json={"id": ROW_ID, "data": data})
            r.raise_for_status()
    else:
        os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)
        with open(FILE_PATH, "w") as f:
            f.write(data)


async def load_session():
    data = None
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/tg_session",
                            headers=_headers(), params={"id": f"eq.{ROW_ID}", "select": "data"})
            r.raise_for_status()
            rows = r.json()
            data = rows[0]["data"] if rows else None
    elif os.path.exists(FILE_PATH):
        data = open(FILE_PATH).read()
    return _dec(data) if data else None


async def delete_session() -> None:
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            await c.delete(f"{SUPABASE_URL}/rest/v1/tg_session",
                           headers=_headers(), params={"id": f"eq.{ROW_ID}"})
    elif os.path.exists(FILE_PATH):
        os.remove(FILE_PATH)
        
