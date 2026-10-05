"""ذخیره Session به‌ازای هر کاربر. رمزنگاری اختیاری؛ Supabase اختیاری؛ در غیر این صورت فایل."""
import os
import httpx
from cryptography.fernet import Fernet, InvalidToken

_key = os.environ.get("SESSION_ENC_KEY")
_fernet = Fernet(_key.encode()) if _key else None
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
SESSION_DIR = os.environ.get("SESSION_DIR", "/tmp/sessions")


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


def _path(uid: int) -> str:
    return os.path.join(SESSION_DIR, f"{uid}.enc")


async def save_session(uid: int, s: str) -> None:
    data = _enc(s)
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{SUPABASE_URL}/rest/v1/tg_session",
                             headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
                             json={"id": str(uid), "data": data})
            r.raise_for_status()
    else:
        os.makedirs(SESSION_DIR, exist_ok=True)
        with open(_path(uid), "w") as f:
            f.write(data)


async def load_session(uid: int):
    data = None
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/tg_session",
                            headers=_headers(), params={"id": f"eq.{uid}", "select": "data"})
            r.raise_for_status()
            rows = r.json()
            data = rows[0]["data"] if rows else None
    elif os.path.exists(_path(uid)):
        data = open(_path(uid)).read()
    return _dec(data) if data else None


async def list_users() -> list:
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/tg_session",
                            headers=_headers(), params={"select": "id"})
            r.raise_for_status()
            return [int(x["id"]) for x in r.json() if str(x["id"]).isdigit()]
    if not os.path.isdir(SESSION_DIR):
        return []
    return [int(f[:-4]) for f in os.listdir(SESSION_DIR) if f.endswith(".enc") and f[:-4].isdigit()]


async def delete_session(uid: int) -> None:
    if SUPABASE_URL:
        async with httpx.AsyncClient(timeout=15) as c:
            await c.delete(f"{SUPABASE_URL}/rest/v1/tg_session",
                           headers=_headers(), params={"id": f"eq.{uid}"})
    elif os.path.exists(_path(uid)):
        os.remove(_path(uid))
        
