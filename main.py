import os
import asyncio
import logging
from aiohttp import web
from telethon import TelegramClient, events, Button, types
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PasswordHashInvalidError, FloodWaitError, AuthKeyUnregisteredError,
    PhoneNumberInvalidError,
)
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("userbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
KEYWORD = "بدزد"


def _ids(*names):
    out = set()
    for n in names:
        for x in os.environ.get(n, "").replace(" ", "").split(","):
            if x.isdigit():
                out.add(int(x))
    return out


# مجاز‌ها: ALLOWED_IDS="111,222" (OWNER_ID هم هنوز پذیرفته می‌شود)
ALLOWED = _ids("ALLOWED_IDS", "OWNER_ID")
if not ALLOWED:
    raise SystemExit("ALLOWED_IDS (or OWNER_ID) is required")

bot = TelegramClient(StringSession(), API_ID, API_HASH)
clients = {}   # uid -> TelegramClient (اکانت فعال هر کاربر)
logins = {}    # uid -> وضعیت لاگین (step: phone | code | 2fa)


# ───────────────────────── مانیتور گروه‌ها ─────────────────────────
def make_handler(uid):
    seen = set()

    async def on_group_msg(event):
        msg = event.message
        if not msg.buttons:
            return
        for i, row in enumerate(msg.buttons):
            for j, b in enumerate(row):
                kind = type(b.button).__name__
                text = b.text or ""
                log.info("[%s] button chat=%s msg=%s [%s,%s] type=%s text=%r",
                         uid, event.chat_id, msg.id, i, j, kind, text)
                if KEYWORD not in text:
                    continue
                if not isinstance(b.button, types.KeyboardButtonCallback):
                    log.info("[%s]   match but not API-clickable (%s) -> skipped", uid, kind)
                    continue
                if getattr(b.button, "requires_password", False):
                    log.info("[%s]   match but requires password -> skipped", uid)
                    continue
                key = (event.chat_id, msg.id, i, j)
                if key in seen:
                    continue
                seen.add(key)
                if len(seen) > 5000:
                    seen.clear()
                try:
                    await b.click()
                    log.info("[%s]   CLICKED chat=%s msg=%s", uid, event.chat_id, msg.id)
                except Exception as e:
                    log.warning("[%s]   click failed: %r", uid, e)
                return

    return on_group_msg


async def attach(uid, client):
    old = clients.get(uid)
    if old and old is not client:
        try:
            await old.disconnect()
        except Exception:
            pass
    h = make_handler(uid)
    flt = lambda e: not e.is_private
    client.add_event_handler(h, events.NewMessage(incoming=True, func=flt))
    client.add_event_handler(h, events.MessageEdited(incoming=True, func=flt))
    clients[uid] = client


async def restore():
    for uid in await storage.list_users():
        if uid not in ALLOWED:
            continue
        s = await storage.load_session(uid)
        if not s:
            continue
        c = TelegramClient(StringSession(s), API_ID, API_HASH)
        await c.connect()
        try:
            ok = await c.is_user_authorized()
        except AuthKeyUnregisteredError:
            ok = False
        if not ok:
            await storage.delete_session(uid)
            await c.disconnect()
            await bot.send_message(uid, "⚠️ Session منقضی شده. دوباره /login بزن.")
            continue
        await attach(uid, c)
        me = await c.get_me()
        try:
            await bot.send_message(uid, f"✅ مانیتور فعال شد: {me.first_name}")
        except Exception:
            pass


# ───────────────────────── بات کنترل ─────────────────────────
def keypad():
    rows = [[Button.inline(str(n), f"k:{n}".encode()) for n in r]
            for r in ((1, 2, 3), (4, 5, 6), (7, 8, 9))]
    rows.append([Button.inline("⌫ حذف", b"k:del"), Button.inline("0", b"k:0"),
                 Button.inline("✅ تأیید", b"k:ok")])
    rows.append([Button.inline("❌ لغو", b"k:cancel")])
    return rows


def panel(lg, note=""):
    code = lg.get("code", "")
    shown = " ".join(code) if code else "—"
    return f"{note}📲 کدی که تلگرام فرستاد را با کیپد وارد کن:\n\n`{shown}`"


async def reset_login(uid):
    lg = logins.pop(uid, None) or {}
    c = lg.get("client")
    if c and c is not clients.get(uid):
        await c.disconnect()


async def finish_login(uid, c, event):
    await attach(uid, c)  # اول مانیتور فعال شود، حتی اگر ذخیره Session خطا بدهد
    me = await c.get_me()
    logins.pop(uid, None)
    note = ""
    try:
        await storage.save_session(uid, c.session.save())
    except Exception as e:
        log.error("[%s] save_session failed: %r", uid, e)
        note = "\n⚠️ ذخیره Session انجام نشد؛ بعد از ری‌استارت باید دوباره لاگین کنی."
    await event.respond(f"✅ وارد شدی: {me.first_name}\nمانیتور گروه‌ها فعال است.{note}")


@bot.on(events.NewMessage(func=lambda e: e.is_private and e.sender_id in ALLOWED))
async def on_text(event):
    uid = event.sender_id
    lg = logins.get(uid, {})
    step = lg.get("step")
    t = (event.raw_text or "").strip()

    if t == "/start":
        await event.respond("/login ورود\n/status وضعیت\n/logout خروج")
    elif t == "/status":
        c = clients.get(uid)
        await event.respond("🟢 فعال" if c and c.is_connected() else "🔴 غیرفعال")
    elif t == "/logout":
        c = clients.pop(uid, None)
        if c:
            try:
                await c.log_out()
            except Exception:
                pass
        await storage.delete_session(uid)
        await event.respond("خارج شدی و Session پاک شد.")
    elif t == "/login":
        await reset_login(uid)
        logins[uid] = {"step": "phone"}
        await event.respond("شماره را با کد کشور بفرست. مثال: +98912xxxxxxx")
    elif step == "phone":
        phone = t.replace(" ", "")
        if not phone.startswith("+"):
            phone = "+" + phone
        c = TelegramClient(StringSession(), API_ID, API_HASH)
        await c.connect()
        try:
            sent = await c.send_code_request(phone)
        except PhoneNumberInvalidError:
            await c.disconnect()
            return await event.respond("شماره نامعتبر است. دوباره بفرست.")
        except FloodWaitError as e:
            await c.disconnect()
            await reset_login(uid)
            return await event.respond(f"محدودیت تلگرام؛ {e.seconds} ثانیه صبر کن.")
        lg.update(step="code", phone=phone, hash=sent.phone_code_hash, code="", client=c)
        await event.respond(panel(lg), buttons=keypad())
    elif step == "2fa":
        c = lg["client"]
        try:
            await c.sign_in(password=t)
        except PasswordHashInvalidError:
            return await event.respond("❌ رمز اشتباه است. دوباره بفرست.")
        finally:
            try:
                await event.delete()
            except Exception:
                pass
        await finish_login(uid, c, event)


@bot.on(events.CallbackQuery(pattern=b"k:"))
async def on_key(event):
    uid = event.sender_id
    lg = logins.get(uid)
    if uid not in ALLOWED or not lg or lg.get("step") != "code":
        return await event.answer()
    k = event.data.decode()[2:]
    if k.isdigit():
        if len(lg["code"]) < 8:
            lg["code"] += k
    elif k == "del":
        lg["code"] = lg["code"][:-1]
    elif k == "cancel":
        await reset_login(uid)
        return await event.edit("لغو شد.")
    elif k == "ok":
        c = lg["client"]
        try:
            await c.sign_in(lg["phone"], lg["code"], phone_code_hash=lg["hash"])
        except SessionPasswordNeededError:
            lg["step"] = "2fa"
            return await event.edit("🔐 رمز تأیید دو مرحله‌ای را بفرست (پیامت پاک می‌شود).")
        except PhoneCodeInvalidError:
            lg["code"] = ""
            return await event.edit(panel(lg, "❌ کد اشتباه بود.\n\n"), buttons=keypad())
        except PhoneCodeExpiredError:
            await reset_login(uid)
            return await event.edit("کد منقضی شد. دوباره /login بزن.")
        return await finish_login(uid, c, event)
    try:
        await event.edit(panel(lg), buttons=keypad())
    except Exception:
        pass


# ───────────────────────── سرور سلامت برای Render ─────────────────────────
async def health(_):
    return web.Response(text="ok")


async def main():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 10000))).start()

    await bot.start(bot_token=BOT_TOKEN)
    await restore()
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
                             
