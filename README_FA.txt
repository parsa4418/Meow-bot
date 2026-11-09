هاپویی - نسخه آماده استقرار روی Render
=====================================

رفتار و قابلیت‌های بات دست‌کاری نشده‌اند. فقط تنظیمات استقرار از متغیرهای محیطی خوانده می‌شوند و مسیر SQLite برای دیسک پایدار Render قابل تنظیم شده است.

نوع سرویس Render:
- Background Worker (نه Web Service؛ چون بات با polling کار می‌کند و پورت HTTP لازم ندارد)

Build Command:
pip install -r requirements.txt

Start Command:
python main.py

متغیرهای محیطی الزامی:
BOT_TOKEN              توکن بات از BotFather
BOT_USERNAME           یوزرنیم بات بدون @
ADMIN_IDS              آیدی عددی ادمین؛ چند آیدی با کاما، مثال: 123456789,987654321
DB_PATH                /var/data/happy_bot.db (در render.yaml تنظیم شده)

متغیرهای اختیاری عضویت اجباری:
FORCE_JOIN_CHANNEL       یوزرنیم کانال بدون @
FORCE_JOIN_CHANNEL_LINK  لینک کانال، مثال https://t.me/channel
FORCE_JOIN_CHANNEL_NAME  نام نمایشی کانال

نکته مهم دیتابیس:
خود سورس اصلی از SQLite استفاده می‌کند؛ برای حفظ اطلاعات کاربران، شهرها، بانک، سگ‌ها و سایر داده‌های بازی، فایل دیتابیس روی Persistent Disk در /var/data ذخیره می‌شود. دیتابیس جدیدی به پروژه اضافه نشده؛ همان SQLite خود سورس است.

مراحل:
1) فایل‌های این پوشه را در یک مخزن GitHub آپلود کن.
2) در Render گزینه New + و سپس Blueprint را انتخاب و مخزن را وصل کن؛ یا یک Background Worker بساز.
3) اگر Blueprint را انتخاب کردی، Render سرویس و دیسک را از render.yaml می‌سازد.
4) مقادیر BOT_TOKEN و BOT_USERNAME و ADMIN_IDS را در Environment وارد کن.
5) اگر عضویت اجباری کانال لازم نیست، سه متغیر FORCE_JOIN را خالی بگذار یا حذف کن.
6) Deploy را بزن و لاگ‌ها را بررسی کن.

هشدار: دیسک پایدار Render فقط روی پلن‌های پولی قابل استفاده است. بدون دیسک پایدار، فایل SQLite ممکن است با deploy/restart پاک شود.
