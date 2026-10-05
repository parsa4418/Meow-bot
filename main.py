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
OWNER_ID = int(os.environ["OWNER_ID"])
KEYWORD = "بدزد"

bot = TelegramClient(StringSession(), API_ID, API_HASH)
user = {"client": None}
login = {}          # step: phone | code | 2fa
seen = set()        # جلوگیری از کلیک تکراری روی یک دکمه


# ───────────────────────── مانیتور گروه‌ها ─────────────────────────
async def on_group_msg(event):
    msg = event.message
    if not msg.buttons:
        return
    for i, row in enumerate(msg.buttons):
        for j, b in enumerate(row):
            kind = type(b.button).__name__
            text = b.text or ""
            log.info("button chat=%s msg=%s [%s,%s] type=%s text=%r",
                     event.chat_id, msg.id, i, j, kind, text)
            if KEYWORD not in text:
                continue
            if not isinstance(b.button, types.KeyboardButtonCallback):
                log.info("  match but not API-clickable (%s) -> skipped", kind)
                continue
            if getattr(b.button, "requires_password", False):
                log.info("  match but requires password -> skipped")
                continue
            key = (event.chat_id, msg.id, i, j)
            if key in seen:
                continue
            seen.add(key)
            if len(seen) > 5000:
                seen.clear()
            try:
                await b.click()
                log.info("  CLICKED chat=%s msg=%s", event.chat_id, msg.id)
            except Exception as e:  # BotResponseTimeout و ... عادیه
                log.warning("  click failed: %r", e)
            return


def attach(client):
    flt = lambda e: not e.is_private
    client.add_event_handler(on_group_msg, events.NewMessage(incoming=True, func=flt))
    client.add_event_handler(on_group_msg, events.MessageEdited(incoming=True, func=flt))
    user["client"] = client


async def restore():
    s = await storage.load_session()
    if not s:
        log.info("no saved session")
        return
    c = TelegramClient(StringSession(s), API_ID, API_HASH)
    await c.connect()
    try:
        ok = await c.is_user_authorized()
    except AuthKeyUnregisteredError:
        ok = False
    if not ok:
        await storage.delete_session()
        await c.disconnect()
        await bot.send_message(OWNER_ID, "⚠️ Session منقضی شده. دوباره /login بزن.")
        return
    attach(c)
    me = await c.get_me()
    await bot.send_message(OWNER_ID, f"✅ مانیتور فعال شد: {me.first_name} ({me.id})")


# ───────────────────────── بات کنترل ─────────────────────────
def keypad():
    rows = [[Button.inline(str(n), f"k:{n}".encode()) for n in r]
            for r in ((1, 2, 3), (4, 5, 6), (7, 8, 9))]
    rows.append([Button.inline("⌫ حذف", b"k:del"), Button.inline("0", b"k:0"),
                 Button.inline("✅ تأیید", b"k:ok")])
    rows.append([Button.inline("❌ لغو", b"k:cancel")])
    return rows


def panel(note=""):
    code = login.get("code", "")
    shown = " ".join(code) if code else "—"
    return f"{note}📲 کدی که تلگرام فرستاد را با کیپد وارد کن:\n\n`{shown}`"


async def reset_login():
    c = login.get("client")
    if c and c is not user["client"]:
        await c.disconnect()
    login.clear()


async def finish_login(c, event):
    attach(c)  # اول مانیتور فعال شود، حتی اگر ذخیره Session خطا بدهد
    me = await c.get_me()
    login.clear()
    note = ""
    try:
        await storage.save_session(c.session.save())
    except Exception as e:
        log.error("save_session failed: %r", e)
        note = "\n⚠️ ذخیره Session انجام نشد؛ بعد از ری‌استارت باید دوباره لاگین کنی."
    await event.respond(f"✅ وارد شدی: {me.first_name}\nمانیتور گروه‌ها فعال است.{note}")


@bot.on(events.NewMessage(func=lambda e: e.is_private and e.sender_id == OWNER_ID))
async def on_text(event):
    t = (event.raw_text or "").strip()
    step = login.get("step")

    if t == "/start":
        await event.respond("/login ورود\n/status وضعیت\n/logout خروج")
    elif t == "/status":
        c = user["client"]
        await event.respond("🟢 فعال" if c and c.is_connected() else "🔴 غیرفعال")
    elif t == "/logout":
        c = user["client"]
        if c:
            try:
                await c.log_out()
            except Exception:
                pass
            user["client"] = None
        await storage.delete_session()
        await event.respond("خارج شدی و Session پاک شد.")
    elif t == "/login":
        await reset_login()
        login["step"] = "phone"
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
            await reset_login()
            return await event.respond(f"محدودیت تلگرام؛ {e.seconds} ثانیه صبر کن.")
        login.update(step="code", phone=phone, hash=sent.phone_code_hash, code="", client=c)
        await event.respond(panel(), buttons=keypad())
    elif step == "2fa":
        c = login["client"]
        try:
            await c.sign_in(password=t)
        except PasswordHashInvalidError:
            return await event.respond("❌ رمز اشتباه است. دوباره بفرست.")
        finally:
            try:
                await event.delete()  # پاک کردن پیام رمز
            except Exception:
                pass
        await finish_login(c, event)


@bot.on(events.CallbackQuery(pattern=b"k:"))
async def on_key(event):
    if event.sender_id != OWNER_ID or login.get("step") != "code":
        return await event.answer()
    k = event.data.decode()[2:]
    if k.isdigit():
        if len(login["code"]) < 8:
            login["code"] += k
    elif k == "del":
        login["code"] = login["code"][:-1]
    elif k == "cancel":
        await reset_login()
        return await event.edit("لغو شد.")
    elif k == "ok":
        c = login["client"]
        try:
            await c.sign_in(login["phone"], login["code"], phone_code_hash=login["hash"])
        except SessionPasswordNeededError:
            login["step"] = "2fa"
            return await event.edit("🔐 رمز تأیید دو مرحله‌ای را بفرست (پیامت پاک می‌شود).")
        except PhoneCodeInvalidError:
            login["code"] = ""
            return await event.edit(panel("❌ کد اشتباه بود.\n\n"), buttons=keypad())
        except PhoneCodeExpiredError:
            await reset_login()
            return await event.edit("کد منقضی شد. دوباره /login بزن.")
        return await finish_login(c, event)
    try:
        await event.edit(panel(), buttons=keypad())
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
        
