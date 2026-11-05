# ===== کتابخانه‌ها =====
import sqlite3
import time
import random
import re
import json
import threading
import shutil
import os
import traceback
import uuid
from datetime import datetime
import requests
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor
import psycopg2  # 🔄 جدید برای PostgreSQL

DB_NAME = "meowie.db"
BACKUP_INTERVAL = 6 * 3600

# ===== تنظیمات =====
BALE_TOKEN = os.environ.get("BALE_TOKEN", "").strip()
BALE_API = f"https://tapi.bale.ai/bot{BALE_TOKEN}"
# بله مارک‌داون تک‌ستاره می‌خونه؛ اگه ** روی بله درست نشون داده شد این رو 0 کن
# شناسه عددی ادمین‌ها در بله، با کاما جدا کن (مثلاً 12345,67890). با دستور «آیدی من» پیداش کن
ADMIN_IDS = {x.strip() for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}
banned_users = set()
BALE_CONVERT_BOLD = os.environ.get("BALE_CONVERT_BOLD", "1") == "1"

MEOW_COOLDOWN = 120
POINTS_MIN = 15
POINTS_MAX = 35
CAT_PRICE = 150
HOOK_PRICE = 250
FISHING_TIME = 180
GAMBLE_MIN_LEVEL = 3
GAMBLE_MIN_CAT_LEVEL = 2
HUNGER_INTERVAL = 7200
STRAY_CAT_INTERVAL = 1800
STRAY_CAT_CHANCE = 0.3
CUDDLE_COOLDOWN = 600

SMUGGLE_TIME = 3600
JAIL_BAIL = 30000
JAIL_TIME = 3 * 3600

BANK_INTEREST_RATE = 0.05
BANK_INTEREST_INTERVAL = 3600

DAILY_TRANSFER_LIMIT = 5
MAX_TRANSFER_AMOUNT = 500000
MIN_TRANSFER_AMOUNT = 100

HOOK_LEVELS = {
    1: {"name": "قلاب چوبی", "emoji": "🪵", "price": 500},
    2: {"name": "قلاب آهنی", "emoji": "🔩", "price": 2000},
    3: {"name": "قلاب نقره‌ای", "emoji": "🥈", "price": 8000},
    4: {"name": "قلاب طلایی", "emoji": "🥇", "price": 25000},
    5: {"name": "قلاب الماسی", "emoji": "💎", "price": 75000},
}

FISH_TYPES = {
    "معمولی": {"emoji": "🐟", "weight_range": (1.0, 2.0), "price_range": (10000, 15000)},
    "رنگین‌کمان": {"emoji": "🐠", "weight_range": (2.0, 3.5), "price_range": (15000, 25000)},
    "بادکنکی": {"emoji": "🐡", "weight_range": (1.0, 2.5), "price_range": (25000, 40000)},
    "کوسه‌ای": {"emoji": "🦈", "weight_range": (4.0, 7.0), "price_range": (50000, 80000)},
    "نهنگ": {"emoji": "🐋", "weight_range": (8.0, 15.0), "price_range": (100000, 200000)},
}

FISH_CHANCES = {
    1: {"معمولی": 60, "رنگین‌کمان": 25, "بادکنکی": 10, "کوسه‌ای": 4, "نهنگ": 1},
    2: {"معمولی": 40, "رنگین‌کمان": 30, "بادکنکی": 18, "کوسه‌ای": 8, "نهنگ": 4},
    3: {"معمولی": 25, "رنگین‌کمان": 30, "بادکنکی": 25, "کوسه‌ای": 12, "نهنگ": 8},
    4: {"معمولی": 15, "رنگین‌کمان": 25, "بادکنکی": 30, "کوسه‌ای": 18, "نهنگ": 12},
    5: {"معمولی": 5, "رنگین‌کمان": 15, "بادکنکی": 25, "کوسه‌ای": 30, "نهنگ": 25},
}

UPGRADE_COSTS = {
    1: 1000, 2: 2500, 3: 5000, 4: 10000, 5: 20000,
    6: 35000, 7: 50000, 8: 75000, 9: 100000
}

ALLOWED_COLUMNS = {
    'name', 'points', 'level', 'meow_count', 'last_meow',
    'cat_name', 'cat_level', 'cat_exp', 'total_produced',
    'last_collect', 'cat_hunger', 'last_hunger_time',
    'has_hook', 'fish_count', 'fishing_start', 'stray_cat_saved',
    'fridge_level', 'fridge_fish', 'fridge_cooked',
    'bank_balance', 'bank_last_interest', 'bank_last_transaction',
    'cooked_food', 'raw_fish', 'hook_level', 'last_cuddle',
    'is_jailed', 'jail_time', 'stray_cat_available', 'fish_inventory',
    'smuggle_start_time', 'smuggle_amount', 'smuggle_waiting',
    'daily_transfer', 'last_transfer_date'
}

# ===== قفل‌ها =====
bank_lock = threading.Lock()
casino_lock = threading.Lock()
tas_lock = threading.Lock()
smuggle_lock = threading.Lock()

# ===== متغیرهای گلوبال =====
tas_tables = {}
last_stray_time = 0
waiting_for_smuggle = {}
smuggle_in_progress = {}
waiting_for_casino_game = {}
waiting_for_casino_amount = {}
waiting_for_shansi_amount = {}
waiting_for_shansi_players = {}
waiting_for_tas_amount = {}
waiting_for_name = {}
waiting_for_rename = {}
processed_messages = {}
user_last_action = {}

# ===== تنظیمات دیتابیس PostgreSQL =====
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    """اتصال به دیتابیس PostgreSQL (Supabase)"""
    if not DATABASE_URL:
        raise Exception("DATABASE_URL environment variable not set!")
    return psycopg2.connect(DATABASE_URL)

def enable_wal():
    # در PostgreSQL نیازی به WAL نیست
    pass

def to_int(value):
    try:
        return int(float(value))
    except:
        return 0

def log_error(error, context=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open("error_log.txt", "a", encoding="utf-8") as f:
            f.write(f"\n{'='*50}\n")
            f.write(f"زمان: {timestamp}\n")
            f.write(f"متن: {context}\n")
            f.write(f"خطا: {str(error)}\n")
            f.write(traceback.format_exc())
            f.write(f"{'='*50}\n")
    except:
        pass
# ===== دیتابیس PostgreSQL (Supabase) =====
def init_db():
    conn = get_connection()
    c = conn.cursor()
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        name TEXT,
        points BIGINT DEFAULT 0,
        level INTEGER DEFAULT 1,
        meow_count BIGINT DEFAULT 0,
        last_meow DOUBLE PRECISION DEFAULT 0,
        cat_name TEXT DEFAULT NULL,
        cat_level INTEGER DEFAULT 0,
        cat_exp BIGINT DEFAULT 0,
        total_produced BIGINT DEFAULT 0,
        last_collect DOUBLE PRECISION DEFAULT 0,
        created_at TEXT,
        cat_hunger INTEGER DEFAULT 6,
        last_hunger_time DOUBLE PRECISION DEFAULT 0,
        has_hook INTEGER DEFAULT 0,
        fish_count BIGINT DEFAULT 0,
        fishing_start DOUBLE PRECISION DEFAULT 0,
        fridge_level INTEGER DEFAULT 0,
        fridge_fish BIGINT DEFAULT 0,
        fridge_cooked BIGINT DEFAULT 0,
        stray_cat_saved BIGINT DEFAULT 0,
        bank_balance BIGINT DEFAULT 0,
        bank_last_interest DOUBLE PRECISION DEFAULT 0,
        bank_last_transaction DOUBLE PRECISION DEFAULT 0,
        cooked_food BIGINT DEFAULT 0,
        raw_fish BIGINT DEFAULT 0,
        hook_level INTEGER DEFAULT 1,
        last_cuddle DOUBLE PRECISION DEFAULT 0,
        is_jailed INTEGER DEFAULT 0,
        jail_time DOUBLE PRECISION DEFAULT 0,
        stray_cat_available INTEGER DEFAULT 0,
        fish_inventory TEXT DEFAULT '{}',
        smuggle_start_time DOUBLE PRECISION DEFAULT 0,
        smuggle_amount INTEGER DEFAULT 0,
        smuggle_waiting INTEGER DEFAULT 0,
        daily_transfer INTEGER DEFAULT 0,
        last_transfer_date DOUBLE PRECISION DEFAULT 0
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS casino_tables (
        table_id TEXT PRIMARY KEY,
        creator_id TEXT,
        chat_id TEXT,
        amount BIGINT,
        max_players INTEGER,
        players TEXT DEFAULT '[]',
        status TEXT DEFAULT 'waiting',
        created_at DOUBLE PRECISION,
        message_id TEXT,
        game_type TEXT DEFAULT 'shansi'
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        user_id TEXT,
        chat_id TEXT,
        created_at DOUBLE PRECISION
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT,
        type TEXT,
        amount BIGINT,
        balance_before BIGINT,
        balance_after BIGINT,
        created_at DOUBLE PRECISION,
        description TEXT,
        transfer_id TEXT
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS tas_tables (
        table_id TEXT PRIMARY KEY,
        creator_id TEXT,
        chat_id TEXT,
        amount BIGINT,
        players TEXT DEFAULT '[]',
        choices TEXT DEFAULT '{}',
        status TEXT DEFAULT 'waiting',
        created_at DOUBLE PRECISION,
        message_id TEXT,
        finished INTEGER DEFAULT 0,
        choice_started_at DOUBLE PRECISION DEFAULT 0
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS bank_cards (
        user_id TEXT PRIMARY KEY,
        card_number TEXT UNIQUE,
        created_at DOUBLE PRECISION
    )
    ''')
    
    c.execute('''
    CREATE TABLE IF NOT EXISTS transfer_log (
        transfer_id TEXT PRIMARY KEY,
        sender_id TEXT,
        receiver_id TEXT,
        amount BIGINT,
        created_at DOUBLE PRECISION,
        status TEXT DEFAULT 'pending'
    )
    ''')
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_users_points ON users(points DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_casino_status ON casino_tables(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_casino_chat_status ON casino_tables(chat_id, status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_casino_message ON casino_tables(message_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_time ON transactions(user_id, created_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tas_status ON tas_tables(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tas_chat_status ON tas_tables(chat_id, status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tas_message ON tas_tables(message_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_bank_cards_number ON bank_cards(card_number)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_transfer_log_id ON transfer_log(transfer_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_transfer_log_sender ON transfer_log(sender_id)")
    
    conn.commit()
    conn.close()
  # ===== توابع کمکی =====
def get_user(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = %s", (str(user_id),))
    row = c.fetchone()
    conn.close()
    return row

def get_user_dict(user_id):
    user = get_user(user_id)
    if not user:
        return None
    
    columns = [
        'user_id', 'name', 'points', 'level', 'meow_count', 'last_meow',
        'cat_name', 'cat_level', 'cat_exp', 'total_produced', 'last_collect',
        'created_at', 'cat_hunger', 'last_hunger_time', 'has_hook', 'fish_count',
        'fishing_start', 'fridge_level', 'fridge_fish', 'fridge_cooked',
        'stray_cat_saved', 'bank_balance', 'bank_last_interest',
        'bank_last_transaction', 'cooked_food', 'raw_fish', 'hook_level',
        'last_cuddle', 'is_jailed', 'jail_time', 'stray_cat_available',
        'fish_inventory', 'smuggle_start_time', 'smuggle_amount', 'smuggle_waiting',
        'daily_transfer', 'last_transfer_date'
    ]
    
    return dict(zip(columns, user))

def safe_get(user, index, default=0):
    try:
        if user is None:
            return default
        val = user[index]
        return default if val is None else val
    except:
        return default

def create_user(user_id, name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO users (user_id, name, created_at, last_hunger_time, last_collect, hook_level, fish_inventory) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING",
        (str(user_id), name, datetime.now().isoformat(), time.time(), time.time(), 1, '{}')
    )
    conn.commit()
    conn.close()

def update_user(user_id, **kwargs):
    valid_kwargs = {k: v for k, v in kwargs.items() if k in ALLOWED_COLUMNS}
    if not valid_kwargs:
        return
    
    conn = get_connection()
    c = conn.cursor()
    fields = ", ".join([f"{k} = %s" for k in valid_kwargs])
    values = list(valid_kwargs.values()) + [str(user_id)]
    c.execute(f"UPDATE users SET {fields} WHERE user_id = %s", values)
    conn.commit()
    conn.close()

def save_message(message_id, user_id, chat_id):
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute("INSERT INTO messages (message_id, user_id, chat_id, created_at) VALUES (%s, %s, %s, %s) ON CONFLICT (message_id) DO UPDATE SET user_id = %s, chat_id = %s, created_at = %s",
                  (str(message_id), str(user_id), str(chat_id), time.time(), str(user_id), str(chat_id), time.time()))
        conn.commit()
        conn.close()
    except:
        pass

def get_user_id_from_reply(reply_to_id):
    try:
        if not reply_to_id:
            return None
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM messages WHERE message_id = %s", (str(reply_to_id),))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    except:
        return None

def log_transaction(user_id, trans_type, amount, before, after, desc="", transfer_id=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO transactions (user_id, type, amount, balance_before, balance_after, created_at, description, transfer_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (str(user_id), trans_type, amount, before, after, time.time(), desc, transfer_id))
    conn.commit()
    conn.close()

def get_leaderboard(limit=10):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT name, points, level, user_id FROM users ORDER BY points DESC LIMIT %s", (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_required_meows(level):
    if level <= 1:
        return 5
    return 5 + (level - 1) * 15

def get_user_level(meow_count):
    level = 1
    total_needed = 0
    while True:
        needed = get_required_meows(level)
        if meow_count < total_needed + needed:
            return level
        total_needed += needed
        level += 1

def format_time(seconds):
    if seconds < 0:
        seconds = 0
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}:{secs:02d}"

def parse_amount(text):
    if not text:
        return None
    cleaned = re.sub(r'[^0-9]', '', text)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except:
        return None

def parse_bank_amount(text):
    if not text:
        return None
    text = text.strip().lower()
    persian = "۰۱۲۳۴۵۶۷۸۹"
    english = "0123456789"
    text = text.translate(str.maketrans(persian, english))
    text = text.replace("کی", "k").replace("ک", "k")
    match = re.search(r"(\d+(?:\.\d+)?)\s*k?", text)
    if not match:
        return None
    amount = float(match.group(1))
    if "k" in text:
        amount *= 1000
    result = int(amount)
    if result <= 0:
        return None
    return result

def normalize_card_number(card_number):
    return re.sub(r'[^0-9]', '', str(card_number))

def update_hunger(user):
    if not user or not user[6]:
        return user
    user_id = user[0]
    hunger = safe_get(user, 12, 6)
    last_time = safe_get(user, 13, time.time())
    now = time.time()
    elapsed = now - last_time
    decrease = int(elapsed // HUNGER_INTERVAL)
    if decrease > 0:
        new_hunger = max(0, hunger - decrease)
        update_user(user_id, cat_hunger=new_hunger, last_hunger_time=now)
        user = list(user)
        user[12] = new_hunger
        user[13] = now
        return tuple(user)
    return user

def update_production(user):
    if not user or not user[6]:
        return user
    user_id = user[0]
    cat_level = safe_get(user, 7, 1)
    total_produced = safe_get(user, 9, 0)
    last_collect = safe_get(user, 10, time.time())
    now = time.time()
    speed_map = {1:0.5,2:1.5,3:3,4:5,5:8,6:12,7:17,8:23,9:30,10:40}
    income_per_sec = speed_map.get(cat_level, cat_level * 0.5)
    capacity_map = {1:6000,2:8000,3:10000,4:12000,5:15000,6:18000,7:22000,8:26000,9:30000,10:35000}
    capacity = capacity_map.get(cat_level, 5000 + (cat_level * 1000))
    elapsed = max(0, now - last_collect)
    produced = int(elapsed * income_per_sec)
    if produced > 0:
        new_total = min(capacity, total_produced + produced)
        update_user(user_id, total_produced=new_total, last_collect=now)
        user = list(user)
        user[9] = new_total
        user[10] = now
        return tuple(user)
    return user

def get_hunger_text(hunger):
    if hunger <= 0:
        return "😿 گرسنه (0/6)"
    elif hunger <= 2:
        return f"😟 کمی گرسنه ({hunger}/6)"
    elif hunger <= 4:
        return f"😐 معمولی ({hunger}/6)"
    else:
        return f"😸 شیکمم پره ({hunger}/6)"

def get_upgrade_cost(cat_level):
    return UPGRADE_COSTS.get(cat_level, cat_level * 10000)

CAT_RANKS = {
    1: "🥚 میو نوب", 2: "🐣 میو ساده", 3: "😺 میو حرفه‌ای",
    4: "😻 میو سلطنتی", 5: "🦁 میو شیر", 6: "🐯 میو ببر",
    7: "🐆 میو پلنگ", 8: "👑 میو شاه", 9: "🌟 میو افسانه‌ای",
    10: "🔥 میو اسطوره‌ای"
}

def auto_backup():
    # در PostgreSQL نیازی به بکاپ فایل نیست
    pass
  # ===== سیستم بانک و کارت به کارت =====
def apply_bank_interest(user):
    if not user: return user
    user_id = user[0]
    balance = safe_get(user, 21, 0)
    last_interest = safe_get(user, 22, 0)
    now = time.time()
    if balance <= 0: return user
    if last_interest == 0:
        update_user(user_id, bank_last_interest=now)
        return user
    elapsed = now - last_interest
    periods = int(elapsed // BANK_INTEREST_INTERVAL)
    if periods <= 0: return user
    new_balance = balance
    for _ in range(periods):
        new_balance = int(new_balance * (1 + BANK_INTEREST_RATE))
    new_last = last_interest + periods * BANK_INTEREST_INTERVAL
    update_user(user_id, bank_balance=new_balance, bank_last_interest=new_last)
    user = list(user)
    user[21] = new_balance
    user[22] = new_last
    return tuple(user)

def get_next_interest_info(balance, last_interest):
    if balance <= 0: return 0, "—"
    now = time.time()
    if last_interest == 0: last_interest = now
    next_time = last_interest + BANK_INTEREST_INTERVAL
    if next_time < now:
        periods = int((now - last_interest) // BANK_INTEREST_INTERVAL) + 1
        next_time = last_interest + periods * BANK_INTEREST_INTERVAL
    next_interest = int(balance * BANK_INTEREST_RATE)
    remaining = max(0, next_time - now)
    hours = int(remaining // 3600)
    mins = int((remaining % 3600) // 60)
    return next_interest, f"{hours} ساعت و {mins} دقیقه دیگر"

def get_user_by_card(card_number):
    card_number = normalize_card_number(card_number)
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT user_id FROM bank_cards WHERE card_number = %s", (card_number,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def check_daily_transfer(user_id):
    user = get_user(user_id)
    if not user:
        return False, 0
    
    daily_transfer = safe_get(user, 35, 0)
    last_date = safe_get(user, 36, 0)
    now = time.time()
    
    if now - last_date >= 86400:
        update_user(user_id, daily_transfer=0, last_transfer_date=now)
        return True, DAILY_TRANSFER_LIMIT
    
    if daily_transfer >= DAILY_TRANSFER_LIMIT:
        return False, DAILY_TRANSFER_LIMIT - daily_transfer
    
    return True, DAILY_TRANSFER_LIMIT - daily_transfer

def handle_card_to_card(user_id, name, chat_id, text, reply_to_id=None):
    try:
        parts = text.split()
        if len(parts) < 4:
            safe_send_message(chat_id, "❌ فرمت درست:\n`کارت به کارت میویی [شماره کارت] [مبلغ]`\nمثال: `کارت به کارت میویی 1234567890 10000`", reply_to_id)
            return
        
        card_number_raw = parts[3]
        card_number = normalize_card_number(card_number_raw)
        if len(card_number) < 10:
            safe_send_message(chat_id, "❌ شماره کارت نامعتبر! (حداقل ۱۰ رقم)", reply_to_id)
            return
        
        amount_text = parts[4] if len(parts) > 4 else ""
        if not amount_text:
            safe_send_message(chat_id, "❌ مبلغ را وارد کن!\nمثال: `کارت به کارت میویی 1234567890 10000`", reply_to_id)
            return
        
        amount = parse_bank_amount(amount_text)
        if amount is None or amount <= 0:
            safe_send_message(chat_id, "❌ مبلغ معتبر وارد کن! (مثلاً 10000 یا 10k)", reply_to_id)
            return
        
        if amount < MIN_TRANSFER_AMOUNT:
            safe_send_message(chat_id, f"❌ حداقل مبلغ {MIN_TRANSFER_AMOUNT:,} پوینت است!", reply_to_id)
            return
        if amount > MAX_TRANSFER_AMOUNT:
            safe_send_message(chat_id, f"❌ حداکثر مبلغ {MAX_TRANSFER_AMOUNT:,} پوینت است!", reply_to_id)
            return
        
        receiver_id = get_user_by_card(card_number)
        if not receiver_id:
            safe_send_message(chat_id, f"❌ شماره کارت {card_number} یافت نشد!", reply_to_id)
            return
        
        if str(receiver_id) == str(user_id):
            safe_send_message(chat_id, "❌ نمی‌تونی به کارت خودت پول بزنی!", reply_to_id)
            return
        
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "❌ اول یه میو بگو!", reply_to_id)
            return
        
        points = safe_get(user, 2, 0)
        if points < amount:
            safe_send_message(chat_id, f"💰 پوینت کافی نیست!\nنیاز: {amount:,} 🪙\nموجودی: {points:,} 🪙", reply_to_id)
            return
        
        can_transfer, remaining = check_daily_transfer(user_id)
        if not can_transfer:
            safe_send_message(chat_id, f"❌ امروز {DAILY_TRANSFER_LIMIT} بار کارت به کارت انجام دادی! فردا امتحان کن.", reply_to_id)
            return
        
        transfer_id = f"transfer_{user_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        with bank_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")  # شروع تراکنش
                
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "💰 پوینت کافی نیست!", reply_to_id)
                    return
                
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "❌ خطا در انجام تراکنش!", reply_to_id)
                    return
                
                c.execute("UPDATE users SET bank_balance = bank_balance + %s WHERE user_id = %s", 
                         (amount, str(receiver_id)))
                
                now = time.time()
                c.execute("UPDATE users SET daily_transfer = daily_transfer + 1, last_transfer_date = %s WHERE user_id = %s", 
                         (now, str(user_id)))
                
                c.execute("INSERT INTO transfer_log (transfer_id, sender_id, receiver_id, amount, created_at, status) VALUES (%s, %s, %s, %s, %s, %s)",
                         (transfer_id, str(user_id), str(receiver_id), amount, now, 'completed'))
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "handle_card_to_card")
                safe_send_message(chat_id, "❌ خطا در انجام کارت به کارت!", reply_to_id)
                return
        
        receiver = get_user(receiver_id)
        receiver_name = receiver[1] if receiver else "کاربر"
        
        new_points = points - amount
        log_transaction(user_id, 'card_transfer_send', amount, points, new_points, f'کارت به کارت به {receiver_name}', transfer_id)
        log_transaction(receiver_id, 'card_transfer_receive', amount, 0, 0, f'دریافت از {name}', transfer_id)
        
        safe_send_message(chat_id, f"""✅ **کارت به کارت موفق!** 🎉

📤 از: {name}
📥 به: {receiver_name}
💳 شماره کارت: {card_number}
💰 مبلغ: {amount:,} 🪙
💳 به حساب بانکی گیرنده واریز شد!
🆔 شناسه: {transfer_id}

📊 موجودی جدید شما: {new_points:,} 🪙""", reply_to_id)
        
        try:
            safe_send_message(receiver_id, f"""💳 **کارت به کارت دریافت شد!**

📤 از: {name}
📥 به: {receiver_name}
💰 مبلغ: {amount:,} 🪙
💳 به حساب بانکی شما واریز شد!
🆔 شناسه: {transfer_id}

📌 برای مشاهده موجودی بانک: `بانک میویی`""")
        except:
            pass
        
    except Exception as e:
        log_error(e, "handle_card_to_card")
        safe_send_message(chat_id, "❌ خطا در کارت به کارت!", reply_to_id)

def handle_register_card(user_id, name, chat_id, text, reply_to_id=None):
    try:
        parts = text.split()
        if len(parts) < 2:
            safe_send_message(chat_id, "❌ فرمت درست:\n`ثبت کارت [شماره کارت]`\nمثال: `ثبت کارت 1234567890`", reply_to_id)
            return
        
        card_number_raw = parts[1]
        card_number = normalize_card_number(card_number_raw)
        if len(card_number) < 10:
            safe_send_message(chat_id, "❌ شماره کارت نامعتبر! (حداقل ۱۰ رقم)", reply_to_id)
            return
        
        existing = get_user_by_card(card_number)
        if existing:
            if str(existing) == str(user_id):
                safe_send_message(chat_id, "✅ شما قبلاً این شماره کارت را ثبت کرده‌اید!", reply_to_id)
                return
            else:
                safe_send_message(chat_id, f"❌ این شماره کارت قبلاً توسط کاربر دیگری ثبت شده!", reply_to_id)
                return
        
        conn = get_connection()
        c = conn.cursor()
        try:
            c.execute("INSERT INTO bank_cards (user_id, card_number, created_at) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET card_number = %s, created_at = %s",
                     (str(user_id), card_number, time.time(), card_number, time.time()))
            conn.commit()
            conn.close()
        except Exception as e:
            conn.rollback()
            conn.close()
            log_error(e, "handle_register_card")
            safe_send_message(chat_id, "❌ خطا در ثبت شماره کارت!", reply_to_id)
            return
        
        safe_send_message(chat_id, f"""✅ **شماره کارت ثبت شد!** 💳

👤 {name}
💳 شماره کارت: {card_number}

📌 حالا دیگران می‌توانند با این شماره کارت به شما پول بزنند.
📌 برای مشاهده کارت خود: `کارت من`""", reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_register_card")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)

def handle_my_card(user_id, name, chat_id, reply_to_id=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT card_number FROM bank_cards WHERE user_id = %s", (str(user_id),))
    row = c.fetchone()
    conn.close()
    
    if row:
        safe_send_message(chat_id, f"""💳 **کارت بانکی شما**

👤 {name}
💳 شماره کارت: {row[0]}

📌 برای انتقال پول به دیگران:
`کارت به کارت میویی [شماره کارت] [مبلغ]`""", reply_to_id)
    else:
        safe_send_message(chat_id, f"❌ شما هنوز شماره کارت ثبت نکردید!\n📌 برای ثبت: `ثبت کارت [شماره کارت]`", reply_to_id)

def handle_bank(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        
        user = apply_bank_interest(user)
        balance = safe_get(user, 21, 0)
        last_interest = safe_get(user, 22, 0)
        next_interest, next_time = get_next_interest_info(balance, last_interest)
        
        conn = get_connection()
        c = conn.cursor()
        c.execute("""
            SELECT type, amount, description, created_at, transfer_id
            FROM transactions 
            WHERE user_id = %s 
            ORDER BY created_at DESC 
            LIMIT 5
        """, (str(user_id),))
        rows = c.fetchall()
        conn.close()
        
        transactions_text = ""
        if rows:
            for row in rows:
                trans_type, amount, desc, created_at, transfer_id = row
                time_str = datetime.fromtimestamp(created_at).strftime("%H:%M")
                emoji = "📥" if "receive" in trans_type or "deposit" in trans_type else "📤"
                tid = f" ({transfer_id[:8]})" if transfer_id else ""
                transactions_text += f"\n{emoji} {time_str} | {desc}{tid} | {amount:,} 🪙"
        else:
            transactions_text = "\n📭 هیچ تراکنشی ثبت نشده!"
        
        conn2 = get_connection()
        c2 = conn2.cursor()
        c2.execute("SELECT card_number FROM bank_cards WHERE user_id = %s", (str(user_id),))
        card_row = c2.fetchone()
        conn2.close()
        card_text = f"\n💳 شماره کارت شما: {card_row[0]}" if card_row else "\n📌 شماره کارت ثبت نشده! (`ثبت کارت`)"
        
        can_transfer, remaining = check_daily_transfer(user_id)
        daily_text = f"\n📊 انتقال امروز: {DAILY_TRANSFER_LIMIT - remaining}/{DAILY_TRANSFER_LIMIT}" if can_transfer else f"\n⚠️ امروز {DAILY_TRANSFER_LIMIT} بار انتقال انجام دادی! فردا امتحان کن."
        
        text = f"""🐱 **بانک میویی** 🏦

💳 شماره حساب : `{user_id}`
👤 به نام : {user[1] or name}

💰 موجودی حساب : {balance:,} 🪙{card_text}

🤑 **سود بانکی**
┘─ درصد سود : ۵٪ (هر ۱ ساعت)
┘─ مبلغ سود بعدی : {next_interest:,} 🪙
┘─ زمان واریز : {next_time}

📋 **۵ تراکنش آخر:**{transactions_text}
{daily_text}

❗️ برای مدیریت حساب بانکی از گزینه‌های زیر استفاده کنید.

📥 `واریز میویی مبلغ`
📤 `برداشت میویی مبلغ`
💳 `کارت به کارت میویی [شماره کارت] [مبلغ]`
📌 `ثبت کارت [شماره کارت]` - برای دریافت پول"""
        safe_send_message(chat_id, text, reply_to_id)
    except Exception as e:
        log_error(e, "handle_bank")
        safe_send_message(chat_id, "❌ خطا در نمایش بانک!", reply_to_id)
      # ===== توابع اصلی =====
MAX_PROCESSED_MESSAGES = 3000

def init_app():
    init_db()
    _conn = get_connection()
    _c = _conn.cursor()
    _c.execute("CREATE TABLE IF NOT EXISTS banned_users (user_id TEXT PRIMARY KEY, banned_at DOUBLE PRECISION)")
    _conn.commit()
    _conn.close()
    print("✅ دیتابیس PostgreSQL آماده شد!")

init_app()

# ===== لایه ارتباط با API بله =====
# شناسه پیام‌ها به شکل "chat_id:message_id" نگه داشته می‌شن تا بین چت‌های مختلف تداخل نکنن
def _bale_call(method, payload=None, timeout=20):
    r = requests.post(f"{BALE_API}/{method}", json=payload or {}, timeout=timeout)
    try:
        data = r.json()
    except Exception:
        raise Exception(f"Bale {method} HTTP {r.status_code}: {r.text[:200]}")
    if not data.get("ok"):
        raise Exception(f"Bale {method} error: {data}")
    return data.get("result")

def _split_composite(mid):
    mid = str(mid)
    if ":" in mid:
        return mid.rsplit(":", 1)[1]
    return mid

def _fix_chat_id(chat_id):
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id

def safe_send_message(chat_id, text, reply_to_id=None):
    try:
        if BALE_CONVERT_BOLD:
            text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text, flags=re.S)
        payload = {"chat_id": _fix_chat_id(chat_id), "text": text[:4000]}
        if reply_to_id:
            try:
                payload["reply_to_message_id"] = int(_split_composite(reply_to_id))
            except ValueError:
                pass
        try:
            res = _bale_call("sendMessage", payload)
        except Exception as e:
            # اگه ریپلای به پیام حذف‌شده بود، بدون ریپلای بفرست
            if "reply_to_message_id" in payload:
                payload.pop("reply_to_message_id")
                res = _bale_call("sendMessage", payload)
            else:
                raise e
        if isinstance(res, dict) and res.get("message_id") is not None:
            return {"message_id": f"{chat_id}:{res['message_id']}", "id": f"{chat_id}:{res['message_id']}"}
        return res
    except Exception as e:
        if "can't write" in str(e).lower():
            return None
        log_error(e, f"safe_send_message to {chat_id}")
        return None

# ===== TaskScheduler =====
class TaskScheduler:
    def __init__(self):
        self.tasks = []
        self.running = True
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
    
    def add_task(self, delay, func, *args, **kwargs):
        with self.lock:
            self.tasks.append({
                'run_at': time.time() + delay,
                'func': func,
                'args': args,
                'kwargs': kwargs
            })
    
    def _run(self):
        while self.running:
            with self.lock:
                now = time.time()
                ready = [t for t in self.tasks if t['run_at'] <= now]
                self.tasks = [t for t in self.tasks if t['run_at'] > now]
            
            for task in ready:
                try:
                    task['func'](*task['args'], **task['kwargs'])
                except Exception as e:
                    log_error(e, "TaskScheduler")
            
            time.sleep(1)
    
    def stop(self):
        self.running = False

scheduler = TaskScheduler()

def keep_alive():
    while True:
        time.sleep(600)
        pass

threading.Thread(target=keep_alive, daemon=True).start()

def cleanup_waiting_states():
    now = time.time()
    for state_dict in [waiting_for_casino_game, waiting_for_casino_amount, 
                       waiting_for_shansi_amount, waiting_for_shansi_players,
                       waiting_for_tas_amount, waiting_for_smuggle,
                       waiting_for_name, waiting_for_rename]:
        for user_id in list(state_dict.keys()):
            if user_id in user_last_action and now - user_last_action[user_id] > 300:
                del state_dict[user_id]
                if user_id in waiting_for_smuggle:
                    update_user(user_id, smuggle_waiting=0)

def cleanup_thread():
    while True:
        time.sleep(300)
        try:
            cleanup_waiting_states()
        except:
            pass

threading.Thread(target=cleanup_thread, daemon=True).start()
# ===== سیستم قاچاق و زندان =====
def get_smuggle_chance(count):
    base = 0.85 - (count * 0.08)
    return max(0.30, base)

def check_jail_status(user_id):
    user = get_user(user_id)
    if not user:
        return False
    
    is_jailed = safe_get(user, 28, 0)
    jail_time = safe_get(user, 29, 0)
    
    if is_jailed == 1 and time.time() >= jail_time:
        with smuggle_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("UPDATE users SET is_jailed = 0, jail_time = 0 WHERE user_id = %s AND is_jailed = 1 AND jail_time <= %s", 
                         (str(user_id), time.time()))
                if c.rowcount > 0:
                    conn.commit()
                conn.close()
                return False
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "check_jail_status_transaction")
                return True
    
    return is_jailed == 1

def get_jail_remaining(user_id):
    user = get_user(user_id)
    if not user:
        return 0
    is_jailed = safe_get(user, 28, 0)
    jail_time = safe_get(user, 29, 0)
    if is_jailed != 1:
        return 0
    remaining = jail_time - time.time()
    return max(0, remaining)

def sync_smuggle_state(user_id):
    user = get_user(user_id)
    if not user:
        return
    
    smuggle_waiting = safe_get(user, 34, 0)
    if smuggle_waiting == 1 and user_id not in waiting_for_smuggle:
        waiting_for_smuggle[user_id] = True
    elif smuggle_waiting == 0 and user_id in waiting_for_smuggle:
        del waiting_for_smuggle[user_id]
    
    smuggle_start = safe_get(user, 32, 0)
    smuggle_amount = safe_get(user, 33, 0)
    
    if smuggle_start > 0 and smuggle_amount > 0:
        if user_id not in smuggle_in_progress:
            smuggle_in_progress[user_id] = True
    else:
        if user_id in smuggle_in_progress:
            del smuggle_in_progress[user_id]

def handle_smuggle(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if user:
            smuggle_start = safe_get(user, 32, 0)
            smuggle_amount = safe_get(user, 33, 0)
            
            if (smuggle_start == 0 or smuggle_amount == 0) and user_id in smuggle_in_progress:
                with smuggle_lock:
                    if user_id in smuggle_in_progress:
                        del smuggle_in_progress[user_id]
            
            if smuggle_start > 0 and smuggle_amount > 0 and user_id not in smuggle_in_progress:
                smuggle_in_progress[user_id] = True
        
        sync_smuggle_state(user_id)
        
        if not user:
            safe_send_message(chat_id, "❌ اول یه میو بگو!", reply_to_id)
            return
        
        if check_jail_status(user_id):
            remaining = get_jail_remaining(user_id)
            hours = int(remaining // 3600)
            mins = int((remaining % 3600) // 60)
            safe_send_message(chat_id, f"""⛓️ **شما در زندان هستید!**

{hours} ساعت و {mins} دقیقه دیگه آزاد میشید.

📌 برای آزادی فوری: `آزادی` ({JAIL_BAIL:,} 🪙)""", reply_to_id)
            return
        
        stray_saved = safe_get(user, 20, 0)
        if stray_saved <= 0:
            safe_send_message(chat_id, "😿 شما هیچ پیشی خیابونی برای قاچاق ندارید!\n📌 با «نجات گربه» پیشی‌های خیابونی رو نجات بدید.", reply_to_id)
            return
        
        smuggle_start = safe_get(user, 32, 0)
        smuggle_amount = safe_get(user, 33, 0)
        
        if smuggle_start > 0 and smuggle_amount > 0:
            elapsed = time.time() - smuggle_start
            if elapsed >= SMUGGLE_TIME:
                safe_send_message(chat_id, "⏳ در حال بررسی عملیات قاچاق قبلی شما...", reply_to_id)
                check_pending_smuggle(user_id, chat_id, reply_to_id)
                return
            else:
                remaining = SMUGGLE_TIME - elapsed
                hours = int(remaining // 3600)
                mins = int((remaining % 3600) // 60)
                safe_send_message(chat_id, f"⏳ شما یک عملیات قاچاق در حال اجرا دارید!\n{hours} ساعت و {mins} دقیقه دیگه تموم میشه.", reply_to_id)
                return
        
        with smuggle_lock:
            if user_id in smuggle_in_progress:
                safe_send_message(chat_id, "⏳ شما در حال حاضر یک عملیات قاچاق در حال اجرا دارید!", reply_to_id)
                return
        
        max_select = min(3, stray_saved)
        update_user(user_id, smuggle_waiting=1)
        waiting_for_smuggle[user_id] = True
        
        safe_send_message(chat_id, f"""😼 **قاچاق پیشی** 🥷

❓ با قاچاق پیشی های خیابونی و تبدیل کردنشون به کالباس پولدار شو.
🚨 اما مراقب باش.. ممکنه گیر بیوفتی

🐈 شما {stray_saved} پیشی خیابونی آماده برای قاچاق دارید

📌 تعداد پیشی برای قاچاق رو انتخاب کن (1 تا {max_select}):""", reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_smuggle")
        safe_send_message(chat_id, "❌ خطا در شروع قاچاق!", reply_to_id)

def handle_smuggle_confirm(user_id, name, chat_id, amount, reply_to_id=None):
    try:
        sync_smuggle_state(user_id)
        
        if user_id in waiting_for_smuggle:
            user = get_user(user_id)
            if user and safe_get(user, 34, 0) == 0:
                del waiting_for_smuggle[user_id]
                safe_send_message(chat_id, "❌ وضعیت قاچاق منقضی شده. دوباره تلاش کن.", reply_to_id)
                return
        
        with smuggle_lock:
            if user_id in smuggle_in_progress:
                safe_send_message(chat_id, "⏳ شما در حال حاضر یک عملیات قاچاق در حال اجرا دارید!", reply_to_id)
                return
            
            user = get_user(user_id)
            if not user:
                safe_send_message(chat_id, "❌ اول یه میو بگو!", reply_to_id)
                return
            
            if check_jail_status(user_id):
                safe_send_message(chat_id, "⛓️ شما در زندان هستید! نمی‌تونید قاچاق کنید.", reply_to_id)
                if user_id in waiting_for_smuggle:
                    del waiting_for_smuggle[user_id]
                    update_user(user_id, smuggle_waiting=0)
                return
            
            if safe_get(user, 32, 0) > 0:
                safe_send_message(chat_id, "⏳ شما یک عملیات قاچاق ناتمام دارید!", reply_to_id)
                if user_id in waiting_for_smuggle:
                    del waiting_for_smuggle[user_id]
                    update_user(user_id, smuggle_waiting=0)
                return
            
            stray_saved = safe_get(user, 20, 0)
            
            if amount > stray_saved:
                safe_send_message(chat_id, f"❌ شما فقط {stray_saved} پیشی خیابونی دارید!", reply_to_id)
                return
            
            if amount <= 0 or amount > 3:
                safe_send_message(chat_id, "❌ تعداد باید بین ۱ تا ۳ باشد!", reply_to_id)
                return
            
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("""
                    UPDATE users 
                    SET stray_cat_saved = stray_cat_saved - %s,
                        smuggle_start_time = %s,
                        smuggle_amount = %s,
                        smuggle_waiting = 0
                    WHERE user_id = %s AND stray_cat_saved >= %s AND smuggle_start_time = 0
                """, (amount, time.time(), amount, str(user_id), amount))
                
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "❌ خطا در شروع قاچاق! لطفاً دوباره تلاش کنید.", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "handle_smuggle_confirm_transaction")
                safe_send_message(chat_id, "❌ خطا در شروع قاچاق!", reply_to_id)
                return
            
            if user_id in waiting_for_smuggle:
                del waiting_for_smuggle[user_id]
            
            smuggle_in_progress[user_id] = True
            
            safe_send_message(chat_id, f"""😼 **قاچاق شروع شد!** 🥷

⏳ عملیات قاچاق ۱ ساعت طول میکشه...
لطفاً صبر کن... 🕐

پیشی‌های قاچاق‌شده: {amount} عدد""", reply_to_id)
            
            scheduler.add_task(SMUGGLE_TIME, _smuggle_finish, user_id, chat_id, reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_smuggle_confirm")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)
        if user_id in waiting_for_smuggle:
            del waiting_for_smuggle[user_id]
            update_user(user_id, smuggle_waiting=0)

def _smuggle_finish(user_id, chat_id, reply_to_id=None):
    try:
        sync_smuggle_state(user_id)
        
        user_after = get_user(user_id)
        if not user_after:
            with smuggle_lock:
                if user_id in smuggle_in_progress:
                    del smuggle_in_progress[user_id]
            return
        
        smuggle_amount = safe_get(user_after, 33, 0)
        if smuggle_amount == 0:
            with smuggle_lock:
                if user_id in smuggle_in_progress:
                    del smuggle_in_progress[user_id]
            return
        
        with smuggle_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT smuggle_amount, smuggle_start_time FROM users WHERE user_id = %s", (str(user_id),))
                row = c.fetchone()
                if not row or row[0] == 0:
                    conn.rollback()
                    conn.close()
                    if user_id in smuggle_in_progress:
                        del smuggle_in_progress[user_id]
                    return
                
                c.execute("UPDATE users SET smuggle_start_time = 0, smuggle_amount = 0 WHERE user_id = %s", (str(user_id),))
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "smuggle_lock_transaction")
                if user_id in smuggle_in_progress:
                    del smuggle_in_progress[user_id]
                return
        
        if check_jail_status(user_id):
            with smuggle_lock:
                if user_id in smuggle_in_progress:
                    del smuggle_in_progress[user_id]
            return
        
        chance = get_smuggle_chance(smuggle_amount)
        
        if random.random() < chance:
            reward = smuggle_amount * 50000
            with bank_lock:
                conn = get_connection()
                c = conn.cursor()
                try:
                    c.execute("BEGIN")
                    c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (reward, str(user_id)))
                    conn.commit()
                    conn.close()
                except Exception as e:
                    conn.rollback()
                    conn.close()
                    log_error(e, "smuggle_success_pay")
                    safe_send_message(chat_id, "❌ خطا در پرداخت جایزه!", reply_to_id)
                    return
            
            user = get_user(user_id)
            new_points = safe_get(user, 2, 0)
            safe_send_message(chat_id, f"""✅ **عملیات قاچاق موفق!** 🥷💰

🎉 شما {smuggle_amount} پیشی رو به کالباس تبدیل کردید!
💰 +{reward:,} 🪙

🪙 موجودی جدید: {new_points:,}""", reply_to_id)
            log_transaction(user_id, 'smuggle_success', reward, 0, new_points, f'قاچاق {smuggle_amount} پیشی')
        else:
            jail_time = time.time() + JAIL_TIME
            update_user(user_id, is_jailed=1, jail_time=jail_time)
            safe_send_message(chat_id, f"""🚨 **گیر افتادی!** 🚨

⛓️ شما به جرم قاچاق پیشی دستگیر شدید!
برای آزادی باید {JAIL_BAIL:,} پوینت پرداخت کنید.
📌 بنویس: `آزادی`

(اگر پول نداری، ۳ ساعت بعد خودکار آزاد میشی)""", reply_to_id)
            log_transaction(user_id, 'smuggle_fail', 0, 0, 0, 'شکست در قاچاق')
        
        with smuggle_lock:
            if user_id in smuggle_in_progress:
                del smuggle_in_progress[user_id]
    except Exception as e:
        log_error(e, "_smuggle_finish")
        with smuggle_lock:
            if user_id in smuggle_in_progress:
                del smuggle_in_progress[user_id]
              # ===== کازینو و شانسی =====
def handle_casino(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید! نمی‌تونید کازینو بازی کنید.", reply_to_id)
            return
        
        if safe_get(user, 3, 1) < GAMBLE_MIN_LEVEL:
            safe_send_message(chat_id, f"🎰 برای کازینو باید سطح {GAMBLE_MIN_LEVEL} باشی!", reply_to_id)
            return
        if safe_get(user, 7, 0) < GAMBLE_MIN_CAT_LEVEL:
            safe_send_message(chat_id, f"🎰 برای کازینو باید سطح پیشی {GAMBLE_MIN_CAT_LEVEL} باشی!", reply_to_id)
            return
        
        waiting_for_casino_game[user_id] = True
        msg = f"""🎰 **کازینو میویی** 🎰

👤 {name} عزیز خوش اومدی! 🍀

🕹️ **لیست بازی‌ها:**
┌─────────────────────┐
│ 1️⃣ 🃏 گردونه شانس   │
│ 2️⃣ 🍷 شانسی          │
│ 3️⃣ 🎲 تاس            │
└─────────────────────┘

💰 **امتیاز فعلی:** {safe_get(user, 2, 0):,} 🪙
🎯 **سطح بازیکن:** {safe_get(user, 3, 1)}
🐱 **سطح پیشی:** {safe_get(user, 7, 0)}

📌 شماره بازی رو بفرست (مثلاً 1، 2 یا 3):"""
        safe_send_message(chat_id, msg, reply_to_id)
    except Exception as e:
        log_error(e, "handle_casino")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)

def handle_spin_slot(user_id, name, chat_id, amount, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user: return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        with casino_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "💰 پوینت کافی نیست!", reply_to_id)
                    return
                
                initial_points = row[0]
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "💰 پوینت کافی نیست!", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "handle_spin_slot_transaction")
                safe_send_message(chat_id, "❌ خطا!", reply_to_id)
                return
            
            log_transaction(user_id, 'casino_spin', amount, initial_points, initial_points - amount, 'شرط گردونه شانس')
            
            symbols = ["🍇", "🍋", "🍒", "🍉", "⭐", "💎"]
            result = [random.choice(symbols) for _ in range(3)]
            result_text = "  ".join(result)
            
            if result[0] == result[1] == result[2]:
                if result[0] == "💎":
                    win_amount = amount * 4
                    multiplier = "4.0x"
                    emoji = "💎🤩"
                else:
                    win_amount = amount * 2
                    multiplier = "2.0x"
                    emoji = "🎉🍀"
                
                with bank_lock:
                    conn = get_connection()
                    c = conn.cursor()
                    try:
                        c.execute("BEGIN")
                        c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (win_amount, str(user_id)))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        conn.rollback()
                        conn.close()
                        log_error(e, "casino_win_transaction")
                        safe_send_message(chat_id, "❌ خطا در پرداخت جایزه!", reply_to_id)
                        return
                
                log_transaction(user_id, 'casino_win', win_amount, 0, 0, f'برد گردونه شانس {multiplier}')
                
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : {win_amount:,} 🪙 ({multiplier})

🎰 {result_text}

{emoji} **تبریک!** 🎊
🏆 بردی: {win_amount:,} 🪙 🎁

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
                
            elif result[0] == result[1] or result[1] == result[2] or result[0] == result[2]:
                with bank_lock:
                    conn = get_connection()
                    c = conn.cursor()
                    try:
                        c.execute("BEGIN")
                        c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (amount, str(user_id)))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        conn.rollback()
                        conn.close()
                        log_error(e, "casino_refund_transaction")
                        safe_send_message(chat_id, "❌ خطا در برگشت پول!", reply_to_id)
                        return
                
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : {amount:,} 🪙 (1.0x)

🎰 {result_text}

🟡 **دو تا یکی شدن!** 💫
پولت برگشت!

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
            else:
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : 0 🪙 (0.0x)

🎰 {result_text}

😢 **هیچکدوم یکی نشد...**
پولت از بین رفت!

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
                    
    except Exception as e:
        log_error(e, "handle_spin_slot")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)

def create_shansi_table(creator_id, chat_id, amount, max_players, message_id=None):
    table_id = f"shansi_{creator_id}_{int(time.time())}"
    players = json.dumps([creator_id])
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO casino_tables 
                 (table_id, creator_id, chat_id, amount, max_players, players, status, created_at, message_id, game_type)
                 VALUES (%s, %s, %s, %s, %s, %s, 'waiting', %s, %s, 'shansi')''',
              (table_id, creator_id, chat_id, amount, max_players, players, time.time(), message_id))
    conn.commit()
    conn.close()
    return table_id

def get_shansi_table(table_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM casino_tables WHERE table_id = %s", (table_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_shansi_table_by_message(message_id, chat_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM casino_tables WHERE chat_id = %s AND message_id = %s AND status = 'waiting' AND game_type = 'shansi'", 
             (str(chat_id), str(message_id)))
    row = c.fetchone()
    conn.close()
    return row

def close_shansi_table(table_id, status="finished"):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE casino_tables SET status = %s WHERE table_id = %s AND status != 'finished' AND status != 'expired'", 
             (status, table_id))
    if c.rowcount > 0:
        conn.commit()
    conn.close()

def handle_shansi_start(user_id, name, chat_id, amount, max_players, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user: return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        with casino_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "shansi_start_transaction")
                safe_send_message(chat_id, "❌ خطا در ساخت میز!", reply_to_id)
                return
        
        log_transaction(user_id, 'shansi_bet', amount, 0, 0, 'شرط بازی شانسی')
        
        text = f"""🃏 **شانسی** 🍷

🐈 تعداد بازیکن : {max_players}
🏆 جایزه برنده : {amount * max_players:,} 🪙
💰 مبلغ ورودی : {amount:,} 🪙

1️⃣ بازیکن : {name}
"""
        for i in range(2, max_players + 1):
            text += f"{i}️⃣ بازیکن : ...\n"
        text += f"""
⏳ این میز فقط ۶۰ ثانیه اعتبار دارد.

📌 برای ورود به میز روی همین پیام **ریپلای** کنید و بنویسید:
`پیوستن`"""
        
        sent_msg = safe_send_message(chat_id, text, reply_to_id)
        
        msg_id = None
        if sent_msg and hasattr(sent_msg, 'get'):
            msg_id = sent_msg.get('message_id') or sent_msg.get('id')
        
        table_id = create_shansi_table(user_id, chat_id, amount, max_players, msg_id)
        
        scheduler.add_task(60, _shansi_expire, table_id, chat_id, reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_shansi_start")
        safe_send_message(chat_id, "❌ خطا در ساخت میز!", reply_to_id)
      # ===== کازینو و شانسی =====
def handle_casino(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید! نمی‌تونید کازینو بازی کنید.", reply_to_id)
            return
        
        if safe_get(user, 3, 1) < GAMBLE_MIN_LEVEL:
            safe_send_message(chat_id, f"🎰 برای کازینو باید سطح {GAMBLE_MIN_LEVEL} باشی!", reply_to_id)
            return
        if safe_get(user, 7, 0) < GAMBLE_MIN_CAT_LEVEL:
            safe_send_message(chat_id, f"🎰 برای کازینو باید سطح پیشی {GAMBLE_MIN_CAT_LEVEL} باشی!", reply_to_id)
            return
        
        waiting_for_casino_game[user_id] = True
        msg = f"""🎰 **کازینو میویی** 🎰

👤 {name} عزیز خوش اومدی! 🍀

🕹️ **لیست بازی‌ها:**
┌─────────────────────┐
│ 1️⃣ 🃏 گردونه شانس   │
│ 2️⃣ 🍷 شانسی          │
│ 3️⃣ 🎲 تاس            │
└─────────────────────┘

💰 **امتیاز فعلی:** {safe_get(user, 2, 0):,} 🪙
🎯 **سطح بازیکن:** {safe_get(user, 3, 1)}
🐱 **سطح پیشی:** {safe_get(user, 7, 0)}

📌 شماره بازی رو بفرست (مثلاً 1، 2 یا 3):"""
        safe_send_message(chat_id, msg, reply_to_id)
    except Exception as e:
        log_error(e, "handle_casino")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)

def handle_spin_slot(user_id, name, chat_id, amount, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user: return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        with casino_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "💰 پوینت کافی نیست!", reply_to_id)
                    return
                
                initial_points = row[0]
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, "💰 پوینت کافی نیست!", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "handle_spin_slot_transaction")
                safe_send_message(chat_id, "❌ خطا!", reply_to_id)
                return
            
            log_transaction(user_id, 'casino_spin', amount, initial_points, initial_points - amount, 'شرط گردونه شانس')
            
            symbols = ["🍇", "🍋", "🍒", "🍉", "⭐", "💎"]
            result = [random.choice(symbols) for _ in range(3)]
            result_text = "  ".join(result)
            
            if result[0] == result[1] == result[2]:
                if result[0] == "💎":
                    win_amount = amount * 4
                    multiplier = "4.0x"
                    emoji = "💎🤩"
                else:
                    win_amount = amount * 2
                    multiplier = "2.0x"
                    emoji = "🎉🍀"
                
                with bank_lock:
                    conn = get_connection()
                    c = conn.cursor()
                    try:
                        c.execute("BEGIN")
                        c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (win_amount, str(user_id)))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        conn.rollback()
                        conn.close()
                        log_error(e, "casino_win_transaction")
                        safe_send_message(chat_id, "❌ خطا در پرداخت جایزه!", reply_to_id)
                        return
                
                log_transaction(user_id, 'casino_win', win_amount, 0, 0, f'برد گردونه شانس {multiplier}')
                
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : {win_amount:,} 🪙 ({multiplier})

🎰 {result_text}

{emoji} **تبریک!** 🎊
🏆 بردی: {win_amount:,} 🪙 🎁

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
                
            elif result[0] == result[1] or result[1] == result[2] or result[0] == result[2]:
                with bank_lock:
                    conn = get_connection()
                    c = conn.cursor()
                    try:
                        c.execute("BEGIN")
                        c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (amount, str(user_id)))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        conn.rollback()
                        conn.close()
                        log_error(e, "casino_refund_transaction")
                        safe_send_message(chat_id, "❌ خطا در برگشت پول!", reply_to_id)
                        return
                
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : {amount:,} 🪙 (1.0x)

🎰 {result_text}

🟡 **دو تا یکی شدن!** 💫
پولت برگشت!

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
            else:
                user = get_user(user_id)
                new_points = safe_get(user, 2, 0)
                
                safe_send_message(chat_id, f"""🃏 **گردونه شانس** 🎰

💰 مبلغ ورودی : {amount:,} 🪙
🏆 مبلغ دریافتی : 0 🪙 (0.0x)

🎰 {result_text}

😢 **هیچکدوم یکی نشد...**
پولت از بین رفت!

💰 موجودی جدید: {new_points:,} 🪙""", reply_to_id)
                    
    except Exception as e:
        log_error(e, "handle_spin_slot")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)

def create_shansi_table(creator_id, chat_id, amount, max_players, message_id=None):
    table_id = f"shansi_{creator_id}_{int(time.time())}"
    players = json.dumps([creator_id])
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO casino_tables 
                 (table_id, creator_id, chat_id, amount, max_players, players, status, created_at, message_id, game_type)
                 VALUES (%s, %s, %s, %s, %s, %s, 'waiting', %s, %s, 'shansi')''',
              (table_id, creator_id, chat_id, amount, max_players, players, time.time(), message_id))
    conn.commit()
    conn.close()
    return table_id

def get_shansi_table(table_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM casino_tables WHERE table_id = %s", (table_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_shansi_table_by_message(message_id, chat_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM casino_tables WHERE chat_id = %s AND message_id = %s AND status = 'waiting' AND game_type = 'shansi'", 
             (str(chat_id), str(message_id)))
    row = c.fetchone()
    conn.close()
    return row

def close_shansi_table(table_id, status="finished"):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE casino_tables SET status = %s WHERE table_id = %s AND status != 'finished' AND status != 'expired'", 
             (status, table_id))
    if c.rowcount > 0:
        conn.commit()
    conn.close()

def handle_shansi_start(user_id, name, chat_id, amount, max_players, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user: return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        with casino_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "shansi_start_transaction")
                safe_send_message(chat_id, "❌ خطا در ساخت میز!", reply_to_id)
                return
        
        log_transaction(user_id, 'shansi_bet', amount, 0, 0, 'شرط بازی شانسی')
        
        text = f"""🃏 **شانسی** 🍷

🐈 تعداد بازیکن : {max_players}
🏆 جایزه برنده : {amount * max_players:,} 🪙
💰 مبلغ ورودی : {amount:,} 🪙

1️⃣ بازیکن : {name}
"""
        for i in range(2, max_players + 1):
            text += f"{i}️⃣ بازیکن : ...\n"
        text += f"""
⏳ این میز فقط ۶۰ ثانیه اعتبار دارد.

📌 برای ورود به میز روی همین پیام **ریپلای** کنید و بنویسید:
`پیوستن`"""
        
        sent_msg = safe_send_message(chat_id, text, reply_to_id)
        
        msg_id = None
        if sent_msg and hasattr(sent_msg, 'get'):
            msg_id = sent_msg.get('message_id') or sent_msg.get('id')
        
        table_id = create_shansi_table(user_id, chat_id, amount, max_players, msg_id)
        
        scheduler.add_task(60, _shansi_expire, table_id, chat_id, reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_shansi_start")
        safe_send_message(chat_id, "❌ خطا در ساخت میز!", reply_to_id)
      # ===== سیستم تاس =====
TAS_TABLE_ID = 0
TAS_CREATOR_ID = 1
TAS_CHAT_ID = 2
TAS_AMOUNT = 3
TAS_PLAYERS = 4
TAS_CHOICES = 5
TAS_STATUS = 6
TAS_CREATED_AT = 7
TAS_MESSAGE_ID = 8
TAS_FINISHED = 9
TAS_CHOICE_STARTED = 10

def get_tas_table_dict(table):
    if not table:
        return None
    return {
        'table_id': table[TAS_TABLE_ID],
        'creator_id': table[TAS_CREATOR_ID],
        'chat_id': table[TAS_CHAT_ID],
        'amount': table[TAS_AMOUNT],
        'players': json.loads(table[TAS_PLAYERS]),
        'choices': json.loads(table[TAS_CHOICES]),
        'status': table[TAS_STATUS],
        'created_at': table[TAS_CREATED_AT],
        'message_id': table[TAS_MESSAGE_ID],
        'finished': table[TAS_FINISHED],
        'choice_started_at': table[TAS_CHOICE_STARTED] if len(table) > TAS_CHOICE_STARTED else 0
    }

def create_tas_table(creator_id, chat_id, amount, table_id, message_id=None):
    players = json.dumps([creator_id])
    choices = json.dumps({})
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO tas_tables 
                 (table_id, creator_id, chat_id, amount, players, choices, status, created_at, message_id, finished, choice_started_at)
                 VALUES (%s, %s, %s, %s, %s, %s, 'waiting', %s, %s, 0, 0)''',
              (table_id, creator_id, chat_id, amount, players, choices, time.time(), message_id))
    conn.commit()
    conn.close()

def get_tas_table(table_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM tas_tables WHERE table_id = %s", (table_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_tas_table_by_message(message_id, chat_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM tas_tables WHERE chat_id = %s AND message_id = %s AND status = 'waiting' AND finished = 0", 
             (str(chat_id), str(message_id)))
    row = c.fetchone()
    conn.close()
    return row

def get_tas_table_for_user(user_id, chat_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT table_id FROM tas_tables 
        WHERE chat_id = %s AND status = 'waiting' AND finished = 0
        ORDER BY created_at DESC LIMIT 1
    """, (str(chat_id),))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0]
    return None

def update_tas_table(table_id, **kwargs):
    conn = get_connection()
    c = conn.cursor()
    fields = ", ".join([f"{k} = %s" for k in kwargs])
    values = list(kwargs.values()) + [table_id]
    c.execute(f"UPDATE tas_tables SET {fields} WHERE table_id = %s", values)
    if c.rowcount > 0:
        conn.commit()
    conn.close()

def handle_tas_start(user_id, name, chat_id, amount, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "❌ کاربر پیدا نشد!", reply_to_id)
            return
        
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        with casino_lock:
            conn = get_connection()
            c = conn.cursor()
            try:
                c.execute("BEGIN")
                c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                row = c.fetchone()
                if not row:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                         (amount, str(user_id), amount))
                if c.rowcount == 0:
                    conn.rollback()
                    conn.close()
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                    return
                
                conn.commit()
                conn.close()
            except Exception as e:
                conn.rollback()
                conn.close()
                log_error(e, "handle_tas_start_transaction")
                safe_send_message(chat_id, "❌ خطا در ساخت میز تاس!", reply_to_id)
                return
        
        table_id = f"tas_{user_id}_{int(time.time())}"
        log_transaction(user_id, 'tas_bet', amount, 0, 0, 'شرط بازی تاس')
        
        prize = amount * 2
        
        text = f"""🎲 **تاس** 🎲

🐈 تعداد بازیکن : 1/2
💰 مبلغ ورودی : {amount:,} 🪙
🏆 جایزه برنده : {prize:,} 🪙

1️⃣ بازیکن : {name}

📌 برای ورود به میز روی همین پیام **ریپلای** کنید و بنویسید:
`پیوستن تاس`"""
        
        sent_msg = safe_send_message(chat_id, text, reply_to_id)
        
        msg_id = None
        if sent_msg and hasattr(sent_msg, 'get'):
            msg_id = sent_msg.get('message_id') or sent_msg.get('id')
        
        create_tas_table(user_id, chat_id, amount, table_id, msg_id)
        
        tas_tables[table_id] = {
            "players": [user_id],
            "choices": {},
            "amount": amount,
            "creator_id": user_id,
            "chat_id": chat_id,
            "message_id": msg_id,
            "finished": False,
            "created_at": time.time(),
            "choice_started_at": 0
        }
        
        scheduler.add_task(30, _tas_expire, table_id, chat_id, reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_tas_start")
        safe_send_message(chat_id, "❌ خطا در ساخت میز تاس!", reply_to_id)

def _tas_expire(table_id, chat_id, reply_to_id=None):
    with tas_lock:
        db_table = get_tas_table(table_id)
        if db_table:
            table_dict = get_tas_table_dict(db_table)
            if table_dict and table_dict['status'] == 'waiting' and table_dict['finished'] == 0:
                players = table_dict['players']
                amount = table_dict['amount']
                update_tas_table(table_id, status="expired")
                for pid in players:
                    with bank_lock:
                        conn = get_connection()
                        c = conn.cursor()
                        try:
                            c.execute("BEGIN")
                            c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (amount, str(pid)))
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            conn.rollback()
                            conn.close()
                            log_error(e, "tas_expire_refund")
                try:
                    safe_send_message(chat_id, "⏰ میز تاس منقضی شد! پول بازگردانده شد.", reply_to_id)
                except:
                    pass
        
        if table_id in tas_tables:
            del tas_tables[table_id]

def handle_tas_join(user_id, name, chat_id, reply_to_id=None):
    try:
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        
        table_id = None
        
        if reply_to_id:
            db_table = get_tas_table_by_message(reply_to_id, chat_id)
            if db_table:
                table_id = db_table[0]
        
        if not table_id:
            conn = get_connection()
            c = conn.cursor()
            c.execute("""
                SELECT table_id FROM tas_tables 
                WHERE chat_id = %s AND status = 'waiting' AND finished = 0 
                ORDER BY created_at DESC LIMIT 1
            """, (str(chat_id),))
            row = c.fetchone()
            conn.close()
            if row:
                table_id = row[0]
        
        if not table_id:
            safe_send_message(chat_id, "❌ میز تاس فعالی پیدا نشد!", reply_to_id)
            return
        
        with tas_lock:
            db_table = get_tas_table(table_id)
            if not db_table:
                safe_send_message(chat_id, "❌ میز دیگر فعال نیست!", reply_to_id)
                return
            
            table_dict = get_tas_table_dict(db_table)
            if table_dict['status'] != 'waiting' or table_dict['finished'] == 1:
                safe_send_message(chat_id, "❌ میز دیگر فعال نیست!", reply_to_id)
                return
            
            players = table_dict['players']
            amount = table_dict['amount']
            
            if user_id in players:
                safe_send_message(chat_id, "❌ شما از قبل توی این میز هستید!", reply_to_id)
                return
            
            if len(players) >= 2:
                safe_send_message(chat_id, "❌ میز پر شده!", reply_to_id)
                return
            
            with casino_lock:
                conn = get_connection()
                c = conn.cursor()
                try:
                    c.execute("BEGIN")
                    c.execute("SELECT points FROM users WHERE user_id = %s AND points >= %s", (str(user_id), amount))
                    row = c.fetchone()
                    if not row:
                        conn.rollback()
                        conn.close()
                        safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                        return
                    
                    c.execute("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s", 
                             (amount, str(user_id), amount))
                    if c.rowcount == 0:
                        conn.rollback()
                        conn.close()
                        safe_send_message(chat_id, f"💰 پوینت کافی نداری!", reply_to_id)
                        return
                    
                    conn.commit()
                    conn.close()
                except Exception as e:
                    conn.rollback()
                    conn.close()
                    log_error(e, "tas_join_transaction")
                    safe_send_message(chat_id, "❌ خطا در پیوستن به تاس!", reply_to_id)
                    return
            
            players.append(user_id)
            update_tas_table(table_id, players=json.dumps(players))
            log_transaction(user_id, 'tas_bet', amount, 0, 0, 'شرط بازی تاس')
            
            if table_id in tas_tables:
                tas_tables[table_id]["players"] = players
        
        db_table = get_tas_table(table_id)
        table_dict = get_tas_table_dict(db_table)
        players = table_dict['players']
        amount = table_dict['amount']
        prize = amount * 2
        p1 = get_user(players[0])
        p1_name = p1[1] if p1 else players[0][:8]
        
        choice_started = time.time()
        update_tas_table(table_id, choice_started_at=choice_started)
        if table_id in tas_tables:
            tas_tables[table_id]["choice_started_at"] = choice_started
        
        text = f"""🎲 **تاس** 🎲

🐈 تعداد بازیکن : 2/2
💰 مبلغ ورودی : {amount:,} 🪙
🏆 جایزه برنده : {prize:,} 🪙

👤 بازیکن 1 : {p1_name}
👤 بازیکن 2 : {name}

📌 هر بازیکن انتخاب خود را بفرستد:
**کوچیک** (1-3) یا **بزرگ** (4-6)"""
        
        safe_send_message(chat_id, text, reply_to_id)
        
        scheduler.add_task(20, _tas_choice_timer, table_id, chat_id, reply_to_id)
        
    except Exception as e:
        log_error(e, "handle_tas_join")
        safe_send_message(chat_id, "❌ خطا در پیوستن!", reply_to_id)

def _tas_choice_timer(table_id, chat_id, reply_to_id=None):
    with tas_lock:
        if table_id not in tas_tables:
            return
        
        db_table = get_tas_table(table_id)
        if not db_table:
            return
        
        table_dict = get_tas_table_dict(db_table)
        if table_dict['finished'] == 1:
            return
        
        choices = table_dict['choices']
        if len(choices) < 2:
            players = table_dict['players']
            amount = table_dict['amount']
            update_tas_table(table_id, status="expired", finished=1)
            for pid in players:
                with bank_lock:
                    conn = get_connection()
                    c = conn.cursor()
                    try:
                        c.execute("BEGIN")
                        c.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (amount, str(pid)))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        conn.rollback()
                        conn.close()
                        log_error(e, "tas_choice_refund")
            try:
                safe_send_message(chat_id, "⏰ زمان انتخاب به پایان رسید! میز باطل شد و پول‌ها بازگردانده شد.", reply_to_id)
            except:
                pass
            if table_id in tas_tables:
                del tas_tables[table_id]
              # ===== on_message =====
# ===== توابع کمکی بازی (بازنویسی‌شده) =====
stray_lock = threading.Lock()
stray_cats = {}  # chat_id -> زمان پایان فرصت نجات

CAT_SPEED = {1: 0.5, 2: 1.5, 3: 3, 4: 5, 5: 8, 6: 12, 7: 17, 8: 23, 9: 30, 10: 40}
CAT_CAPACITY = {1: 6000, 2: 8000, 3: 10000, 4: 12000, 5: 15000, 6: 18000, 7: 22000, 8: 26000, 9: 30000, 10: 35000}
CAT_MAX_LEVEL = 10
HOOK_MAX_LEVEL = 5
FRIDGE_CAPACITY = 10
MAX_RAW_FISH = 30
HUNGER_MAX = 6
FEED_AMOUNT = 2
DEFAULT_CAT_NAME = "پیشی بی‌نام"


def db_exec(sql, params=()):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute(sql, params)
        n = c.rowcount
        conn.commit()
        return n
    finally:
        conn.close()


def db_query(sql, params=()):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute(sql, params)
        return c.fetchall()
    finally:
        conn.close()


def add_points(user_id, delta):
    db_exec("UPDATE users SET points = GREATEST(0, points + %s) WHERE user_id = %s", (delta, str(user_id)))


def spend_points(user_id, amount):
    """کم کردن اتمیک پوینت؛ اگه موجودی کافی نباشه False برمی‌گردونه"""
    return db_exec("UPDATE users SET points = points - %s WHERE user_id = %s AND points >= %s",
                   (amount, str(user_id), amount)) > 0


def display_name(user_id):
    u = get_user(user_id)
    return (u[1] if u and u[1] else str(user_id)[:8])


def meows_to_next_level(meow_count):
    level = 1
    total = 0
    while True:
        needed = get_required_meows(level)
        if meow_count < total + needed:
            return level, total + needed - meow_count
        total += needed
        level += 1


def _refresh_cat(user):
    """تولید و گرسنگی پیشی رو به‌روز می‌کنه. پیشی گرسنه (۰) تولید نمی‌کنه."""
    if not user or not user[6]:
        return user
    if safe_get(user, 12, HUNGER_MAX) <= 0:
        now = time.time()
        update_user(user[0], last_collect=now)
        user = list(user)
        user[10] = now
        user = tuple(user)
    else:
        user = update_production(user)
    return update_hunger(user)


# ===== میو و پروفایل =====
def handle_meow(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            create_user(user_id, name)
            user = get_user(user_id)
        now = time.time()
        last = safe_get(user, 5, 0)
        if now - last < MEOW_COOLDOWN:
            safe_send_message(chat_id, f"⏳ هنوز زوده! {format_time(MEOW_COOLDOWN - (now - last))} دیگه صبر کن 🐱", reply_to_id)
            return
        gain = random.randint(POINTS_MIN, POINTS_MAX)
        meows = safe_get(user, 4, 0) + 1
        old_level = safe_get(user, 3, 1)
        new_level = get_user_level(meows)
        db_exec("UPDATE users SET points = points + %s, meow_count = %s, last_meow = %s, level = %s, name = %s WHERE user_id = %s",
                (gain, meows, now, new_level, name, str(user_id)))
        total = safe_get(user, 2, 0) + gain
        _, rem = meows_to_next_level(meows)
        text = f"🐱 میووو! +{gain} 🪙\n\n💰 موجودی: {total:,} 🪙\n🎯 سطح {new_level} | تا سطح بعد: {rem} میو"
        if new_level > old_level:
            text += f"\n\n🎉 تبریک! به سطح {new_level} رسیدی!"
        safe_send_message(chat_id, text, reply_to_id)
    except Exception as e:
        log_error(e, "handle_meow")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def _profile_text(user, fallback_name):
    user = apply_bank_interest(user)
    meows = safe_get(user, 4, 0)
    level = safe_get(user, 3, 1)
    _, rem = meows_to_next_level(meows)
    cat_name = safe_get(user, 6, None)
    cat_level = safe_get(user, 7, 0)
    if cat_name:
        cat_line = f"🐈 پیشی: {cat_name} | {CAT_RANKS.get(cat_level, '')} (سطح {cat_level})"
    else:
        cat_line = "🐈 پیشی: ندارد"
    if safe_get(user, 14, 0):
        hl = safe_get(user, 26, 1)
        h = HOOK_LEVELS.get(hl, HOOK_LEVELS[1])
        hook_line = f"🎣 قلاب: {h['emoji']} {h['name']} (سطح {hl})"
    else:
        hook_line = "🎣 قلاب: ندارد"
    jail_line = ""
    if safe_get(user, 28, 0) == 1:
        rem_j = get_jail_remaining(user[0])
        if rem_j > 0:
            jail_line = f"\n⛓️ زندان: {int(rem_j // 60)} دقیقه مونده"
    return f"""👤 **پروفایل {user[1] or fallback_name}**

🎯 سطح: {level} | 🐾 میو: {meows:,}
📈 تا سطح بعد: {rem} میو
💰 پوینت: {safe_get(user, 2, 0):,} 🪙
🏦 بانک: {safe_get(user, 21, 0):,} 🪙
{cat_line}
{hook_line}
😼 پیشی خیابونی نجات‌داده: {safe_get(user, 20, 0)}{jail_line}"""


def handle_profile(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        safe_send_message(chat_id, _profile_text(user, name), reply_to_id)
    except Exception as e:
        log_error(e, "handle_profile")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_profile_other(user_id, target_user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(target_user_id)
        if not user:
            safe_send_message(chat_id, "❌ این کاربر هنوز شروع نکرده!", reply_to_id)
            return
        safe_send_message(chat_id, _profile_text(user, "کاربر"), reply_to_id)
    except Exception as e:
        log_error(e, "handle_profile_other")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_leaderboard(chat_id, reply_to_id=None):
    try:
        rows = get_leaderboard(10)
        if not rows:
            safe_send_message(chat_id, "📭 هنوز کسی توی لیست نیست!", reply_to_id)
            return
        medals = ["🥇", "🥈", "🥉"]
        lines = ["🏆 **برترین پیشی‌ها** 🐱\n"]
        for i, (nm, pts, lvl, _uid) in enumerate(rows):
            icon = medals[i] if i < 3 else f"{i + 1}."
            lines.append(f"{icon} {nm or 'کاربر'} | سطح {lvl} | {int(pts or 0):,} 🪙")
        safe_send_message(chat_id, "\n".join(lines), reply_to_id)
    except Exception as e:
        log_error(e, "handle_leaderboard")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


# ===== پیشی =====
def handle_buy_cat(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if safe_get(user, 6, None):
            safe_send_message(chat_id, f"🐱 تو از قبل پیشی داری: {user[6]}", reply_to_id)
            return
        pts = safe_get(user, 2, 0)
        if not spend_points(user_id, CAT_PRICE):
            safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nقیمت پیشی: {CAT_PRICE} 🪙\nموجودی: {pts:,} 🪙", reply_to_id)
            return
        now = time.time()
        update_user(user_id, cat_name=DEFAULT_CAT_NAME, cat_level=1, last_collect=now,
                    cat_hunger=HUNGER_MAX, last_hunger_time=now, total_produced=0)
        log_transaction(user_id, 'buy_cat', CAT_PRICE, pts, pts - CAT_PRICE, 'خرید پیشی')
        waiting_for_name[user_id] = True
        safe_send_message(chat_id, "🎉 پیشی خریدی!\n\n✏️ حالا اسم پیشی رو بفرست (۲ تا ۳۰ حرف):", reply_to_id)
    except Exception as e:
        log_error(e, "handle_buy_cat")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_cat_panel(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not user[6]:
            safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
            return
        user = _refresh_cat(user)
        lvl = safe_get(user, 7, 1)
        speed = CAT_SPEED.get(lvl, lvl * 0.5)
        cap = CAT_CAPACITY.get(lvl, 5000 + lvl * 1000)
        produced = safe_get(user, 9, 0)
        if lvl >= CAT_MAX_LEVEL:
            up = "🔝 به بالاترین سطح رسیده!"
        else:
            up = f"⬆️ ارتقا به سطح {lvl + 1}: {get_upgrade_cost(lvl):,} 🪙 (`ارتقا مقام پیشی`)"
        text = f"""🐈 **{user[6]}**

🏅 مقام: {CAT_RANKS.get(lvl, '')} (سطح {lvl})
🍖 وضعیت: {get_hunger_text(safe_get(user, 12, 0))}
⚡ تولید: {speed:g} پوینت در ثانیه
📦 انبار: {produced:,} / {cap:,} 🪙
{up}

📌 `برداشت میویی` | `غذا دادن` | `ناز کردن` | `تغییر اسم پیشی`"""
        safe_send_message(chat_id, text, reply_to_id)
    except Exception as e:
        log_error(e, "handle_cat_panel")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_rename_cat(user_id, chat_id, reply_to_id=None):
    user = get_user(user_id)
    if not user or not user[6]:
        safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
        return
    waiting_for_rename[user_id] = True
    safe_send_message(chat_id, "✏️ اسم جدید پیشی رو بفرست (۲ تا ۳۰ حرف):", reply_to_id)


def handle_upgrade_cat(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not user[6]:
            safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
            return
        user = _refresh_cat(user)
        lvl = safe_get(user, 7, 1)
        if lvl >= CAT_MAX_LEVEL:
            safe_send_message(chat_id, "🔝 پیشی‌ت به بالاترین مقام رسیده!", reply_to_id)
            return
        cost = get_upgrade_cost(lvl)
        pts = safe_get(user, 2, 0)
        if not spend_points(user_id, cost):
            safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nهزینه ارتقا: {cost:,} 🪙\nموجودی: {pts:,} 🪙", reply_to_id)
            return
        update_user(user_id, cat_level=lvl + 1)
        log_transaction(user_id, 'upgrade_cat', cost, pts, pts - cost, f'ارتقای پیشی به سطح {lvl + 1}')
        safe_send_message(chat_id, f"🎉 پیشی‌ت ارتقا پیدا کرد!\n\n{CAT_RANKS.get(lvl + 1, '')} (سطح {lvl + 1})\n⚡ تولید جدید: {CAT_SPEED.get(lvl + 1, 0):g} در ثانیه", reply_to_id)
    except Exception as e:
        log_error(e, "handle_upgrade_cat")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_collect(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not user[6]:
            safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
            return
        hungry = safe_get(user, 12, HUNGER_MAX) <= 0
        user = _refresh_cat(user)
        amount = int(safe_get(user, 9, 0))
        if amount <= 0:
            extra = "\n😿 پیشی‌ت گرسنه‌ست و تولید نمی‌کنه! `غذا دادن`" if hungry else ""
            safe_send_message(chat_id, f"📭 چیزی برای برداشت نیست!{extra}", reply_to_id)
            return
        n = db_exec("UPDATE users SET points = points + %s, total_produced = total_produced - %s WHERE user_id = %s AND total_produced >= %s",
                    (amount, amount, str(user_id), amount))
        if n == 0:
            safe_send_message(chat_id, "❌ خطا! دوباره تلاش کن.", reply_to_id)
            return
        pts = safe_get(get_user(user_id), 2, 0)
        log_transaction(user_id, 'collect', amount, pts - amount, pts, 'برداشت تولید پیشی')
        safe_send_message(chat_id, f"💰 {amount:,} 🪙 از تولید پیشی برداشت شد!\n\n🪙 موجودی: {pts:,}", reply_to_id)
    except Exception as e:
        log_error(e, "handle_collect")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_cuddle(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not user[6]:
            safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
            return
        now = time.time()
        last = safe_get(user, 27, 0)
        if now - last < CUDDLE_COOLDOWN:
            safe_send_message(chat_id, f"😽 پیشی هنوز خسته‌ست! {format_time(CUDDLE_COOLDOWN - (now - last))} دیگه صبر کن.", reply_to_id)
            return
        gain = random.randint(10, 40)
        n = db_exec("UPDATE users SET points = points + %s, last_cuddle = %s WHERE user_id = %s AND last_cuddle = %s",
                    (gain, now, str(user_id), last))
        if n == 0:
            return
        safe_send_message(chat_id, f"😻 {user[6]} ازت ناز گرفت و خرخر کرد!\n+{gain} 🪙", reply_to_id)
    except Exception as e:
        log_error(e, "handle_cuddle")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


# ===== قلاب و ماهیگیری =====
def _load_fish(user):
    raw = safe_get(user, 31, "[]")
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        data = []
    return data if isinstance(data, list) else []


def _save_fish(user_id, inv, **extra):
    update_user(user_id, fish_inventory=json.dumps(inv, ensure_ascii=False), raw_fish=len(inv), **extra)


def handle_buy_hook(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if safe_get(user, 14, 0):
            safe_send_message(chat_id, "🎣 تو از قبل قلاب داری! (`قلاب`)", reply_to_id)
            return
        pts = safe_get(user, 2, 0)
        if not spend_points(user_id, HOOK_PRICE):
            safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nقیمت قلاب: {HOOK_PRICE} 🪙\nموجودی: {pts:,} 🪙", reply_to_id)
            return
        update_user(user_id, has_hook=1, hook_level=1)
        log_transaction(user_id, 'buy_hook', HOOK_PRICE, pts, pts - HOOK_PRICE, 'خرید قلاب')
        safe_send_message(chat_id, "🎉 قلاب چوبی خریدی! 🪵\n📌 برای شروع بنویس: `ماهیگیری`", reply_to_id)
    except Exception as e:
        log_error(e, "handle_buy_hook")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_hook_panel(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if not safe_get(user, 14, 0):
            safe_send_message(chat_id, f"🎣 هنوز قلاب نداری!\n📌 `خرید قلاب` ({HOOK_PRICE} 🪙)", reply_to_id)
            return
        lvl = safe_get(user, 26, 1)
        h = HOOK_LEVELS.get(lvl, HOOK_LEVELS[1])
        chances = FISH_CHANCES.get(lvl, FISH_CHANCES[1])
        odds = "\n".join(f"{FISH_TYPES[k]['emoji']} {k}: {v}٪" for k, v in chances.items())
        if lvl >= HOOK_MAX_LEVEL:
            up = "🔝 قلاب به بالاترین سطح رسیده!"
        else:
            nxt = HOOK_LEVELS[lvl + 1]
            up = f"⬆️ ارتقا به {nxt['emoji']} {nxt['name']}: {nxt['price']:,} 🪙 (`ارتقا قلاب`)"
        safe_send_message(chat_id, f"""🎣 **قلاب شما**

{h['emoji']} {h['name']} (سطح {lvl})

🎯 شانس صید:
{odds}

{up}
📌 `ماهیگیری` | `فروش ماهی` | `ذخیره ماهی`""", reply_to_id)
    except Exception as e:
        log_error(e, "handle_hook_panel")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_upgrade_hook(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not safe_get(user, 14, 0):
            safe_send_message(chat_id, f"🎣 هنوز قلاب نداری!\n📌 `خرید قلاب` ({HOOK_PRICE} 🪙)", reply_to_id)
            return
        lvl = safe_get(user, 26, 1)
        if lvl >= HOOK_MAX_LEVEL:
            safe_send_message(chat_id, "🔝 قلاب به بالاترین سطح رسیده!", reply_to_id)
            return
        nxt = HOOK_LEVELS[lvl + 1]
        pts = safe_get(user, 2, 0)
        if not spend_points(user_id, nxt['price']):
            safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nهزینه: {nxt['price']:,} 🪙\nموجودی: {pts:,} 🪙", reply_to_id)
            return
        update_user(user_id, hook_level=lvl + 1)
        log_transaction(user_id, 'upgrade_hook', nxt['price'], pts, pts - nxt['price'], f"ارتقای قلاب به {nxt['name']}")
        safe_send_message(chat_id, f"🎉 قلاب ارتقا پیدا کرد!\n{nxt['emoji']} {nxt['name']} (سطح {lvl + 1})", reply_to_id)
    except Exception as e:
        log_error(e, "handle_upgrade_hook")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def _fishing_finish(user_id, chat_id, reply_to_id=None):
    try:
        if db_exec("UPDATE users SET fishing_start = 0 WHERE user_id = %s AND fishing_start > 0", (str(user_id),)) == 0:
            return
        user = get_user(user_id)
        if not user:
            return
        lvl = safe_get(user, 26, 1)
        chances = FISH_CHANCES.get(lvl, FISH_CHANCES[1])
        ftype = random.choices(list(chances.keys()), weights=list(chances.values()))[0]
        info = FISH_TYPES[ftype]
        weight = round(random.uniform(*info["weight_range"]), 1)
        price = random.randint(*info["price_range"])
        inv = _load_fish(user)
        inv.append({"t": ftype, "w": weight, "p": price})
        _save_fish(user_id, inv, fish_count=safe_get(user, 15, 0) + 1)
        safe_send_message(chat_id, f"""🎣 ماهیگیری تموم شد!

{info['emoji']} یه ماهی {ftype} گرفتی!
⚖️ وزن: {weight} کیلو
💰 ارزش: {price:,} 🪙

🧺 تعداد ماهی‌ها: {len(inv)}
📌 `فروش ماهی` | `ذخیره ماهی`""", reply_to_id)
    except Exception as e:
        log_error(e, "_fishing_finish")


def handle_fishing_new(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if not safe_get(user, 14, 0):
            safe_send_message(chat_id, f"🎣 اول باید قلاب بخری!\n📌 `خرید قلاب` ({HOOK_PRICE} 🪙)", reply_to_id)
            return
        start = safe_get(user, 16, 0)
        now = time.time()
        if start > 0:
            if now - start >= FISHING_TIME:
                _fishing_finish(user_id, chat_id, reply_to_id)
            else:
                safe_send_message(chat_id, f"🎣 در حال ماهیگیری هستی! {format_time(FISHING_TIME - (now - start))} دیگه مونده...", reply_to_id)
            return
        if len(_load_fish(user)) >= MAX_RAW_FISH:
            safe_send_message(chat_id, "🧺 کیسه ماهی‌هات پره! اول `فروش ماهی` یا `ذخیره ماهی`", reply_to_id)
            return
        if db_exec("UPDATE users SET fishing_start = %s WHERE user_id = %s AND fishing_start = 0", (now, str(user_id))) == 0:
            return
        safe_send_message(chat_id, f"🎣 قلاب رو انداختی توی آب...\n⏳ {FISHING_TIME // 60} دقیقه دیگه نتیجه رو می‌فهمی!", reply_to_id)
        scheduler.add_task(FISHING_TIME, _fishing_finish, user_id, chat_id, reply_to_id)
    except Exception as e:
        log_error(e, "handle_fishing_new")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_sell_fish(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        inv = _load_fish(user)
        if not inv:
            safe_send_message(chat_id, "🧺 ماهی خامی برای فروش نداری!", reply_to_id)
            return
        total = sum(int(f.get("p", 0)) for f in inv)
        pts = safe_get(user, 2, 0)
        n = db_exec("UPDATE users SET points = points + %s, fish_inventory = '[]', raw_fish = 0 WHERE user_id = %s AND raw_fish = %s",
                    (total, str(user_id), len(inv)))
        if n == 0:
            safe_send_message(chat_id, "❌ خطا! دوباره تلاش کن.", reply_to_id)
            return
        log_transaction(user_id, 'sell_fish', total, pts, pts + total, f'فروش {len(inv)} ماهی')
        safe_send_message(chat_id, f"💰 {len(inv)} ماهی فروختی!\n+{total:,} 🪙\n\n🪙 موجودی: {pts + total:,}", reply_to_id)
    except Exception as e:
        log_error(e, "handle_sell_fish")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_store_fish(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        inv = _load_fish(user)
        if not inv:
            safe_send_message(chat_id, "🧺 ماهی خامی برای ذخیره نداری!", reply_to_id)
            return
        stored = safe_get(user, 18, 0)
        cap = FRIDGE_CAPACITY + safe_get(user, 17, 0) * 5
        space = cap - stored
        if space <= 0:
            safe_send_message(chat_id, f"🧊 یخچال پره! ({stored}/{cap})\n📌 `پخت میویی` تا جا باز شه.", reply_to_id)
            return
        inv.sort(key=lambda f: int(f.get("p", 0)))  # اول ارزان‌ترها
        moving = inv[:space]
        rest = inv[space:]
        _save_fish(user_id, rest, fridge_fish=stored + len(moving))
        safe_send_message(chat_id, f"🧊 {len(moving)} ماهی توی یخچال ذخیره شد! ({stored + len(moving)}/{cap})\n📌 `پخت میویی` برای پختنشون.", reply_to_id)
    except Exception as e:
        log_error(e, "handle_store_fish")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_cook_fish(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        stored = safe_get(user, 18, 0)
        if stored <= 0:
            safe_send_message(chat_id, "🧊 یخچالت خالیه! اول `ذخیره ماهی`", reply_to_id)
            return
        n = db_exec("UPDATE users SET fridge_fish = fridge_fish - %s, cooked_food = cooked_food + %s WHERE user_id = %s AND fridge_fish >= %s",
                    (stored, stored, str(user_id), stored))
        if n == 0:
            return
        cooked = safe_get(user, 24, 0) + stored
        safe_send_message(chat_id, f"🍳 {stored} ماهی پخته شد!\n🍱 غذای آماده: {cooked}\n📌 `غذا دادن` به پیشی", reply_to_id)
    except Exception as e:
        log_error(e, "handle_cook_fish")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_feed(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user or not user[6]:
            safe_send_message(chat_id, f"🐱 هنوز پیشی نداری!\n📌 `خرید پیشی` ({CAT_PRICE} 🪙)", reply_to_id)
            return
        user = _refresh_cat(user)
        hunger = safe_get(user, 12, 0)
        if hunger >= HUNGER_MAX:
            safe_send_message(chat_id, f"😸 {user[6]} سیره و غذا نمی‌خواد!", reply_to_id)
            return
        if safe_get(user, 24, 0) <= 0:
            safe_send_message(chat_id, "🍱 غذای پخته نداری!\n📌 `ماهیگیری` ← `ذخیره ماهی` ← `پخت میویی`", reply_to_id)
            return
        new_h = min(HUNGER_MAX, hunger + FEED_AMOUNT)
        n = db_exec("UPDATE users SET cooked_food = cooked_food - 1, cat_hunger = %s, last_hunger_time = %s WHERE user_id = %s AND cooked_food > 0",
                    (new_h, time.time(), str(user_id)))
        if n == 0:
            return
        safe_send_message(chat_id, f"😋 {user[6]} غذاشو خورد!\n🍖 وضعیت: {get_hunger_text(new_h)}\n🍱 غذای باقی‌مونده: {safe_get(user, 24, 1) - 1}", reply_to_id)
    except Exception as e:
        log_error(e, "handle_feed")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


# ===== گربه خیابونی =====
def handle_stray_cat(chat_id, user_id, reply_to_id=None):
    global last_stray_time
    try:
        now = time.time()
        if now - last_stray_time < STRAY_CAT_INTERVAL:
            return
        if random.random() > STRAY_CAT_CHANCE:
            return
        if not get_user(user_id):
            return
        with stray_lock:
            if stray_cats.get(chat_id, 0) > now:
                return
            stray_cats[chat_id] = now + 300
            last_stray_time = now
        safe_send_message(chat_id, "😼 یه گربه خیابونی پیدا شد!\n\n🏃 اولین نفری که بنویسه `نجات گربه` نجاتش میده!\n⏳ ۵ دقیقه وقت داری...")
    except Exception as e:
        log_error(e, "handle_stray_cat")


def handle_save_stray(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        now = time.time()
        with stray_lock:
            exp = stray_cats.get(chat_id, 0)
            if exp <= now:
                safe_send_message(chat_id, "😿 الان هیچ گربه خیابونی‌ای اینجا نیست!", reply_to_id)
                return
            del stray_cats[chat_id]
        db_exec("UPDATE users SET stray_cat_saved = stray_cat_saved + 1 WHERE user_id = %s", (str(user_id),))
        total = safe_get(user, 20, 0) + 1
        safe_send_message(chat_id, f"🎉 {name} گربه خیابونی رو نجات داد! 😻\n\n😼 پیشی‌های نجات‌داده‌شده: {total}\n📌 با `قاچاق` می‌تونی ازشون پول در بیاری!", reply_to_id)
    except Exception as e:
        log_error(e, "handle_save_stray")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


# ===== زندان و قاچاق =====
def handle_jail_break(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if not check_jail_status(user_id):
            safe_send_message(chat_id, "😺 تو که زندان نیستی!", reply_to_id)
            return
        pts = safe_get(user, 2, 0)
        if not spend_points(user_id, JAIL_BAIL):
            rem = int(get_jail_remaining(user_id) // 60)
            safe_send_message(chat_id, f"💰 پول کافی برای وثیقه نداری!\nوثیقه: {JAIL_BAIL:,} 🪙 | موجودی: {pts:,} 🪙\n⏳ {rem} دقیقه دیگه خودکار آزاد می‌شی.", reply_to_id)
            return
        update_user(user_id, is_jailed=0, jail_time=0)
        log_transaction(user_id, 'jail_bail', JAIL_BAIL, pts, pts - JAIL_BAIL, 'وثیقه آزادی')
        safe_send_message(chat_id, f"🔓 وثیقه {JAIL_BAIL:,} 🪙 پرداخت شد و آزاد شدی!", reply_to_id)
    except Exception as e:
        log_error(e, "handle_jail_break")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def check_pending_smuggle(user_id, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        start = safe_get(user, 32, 0)
        amount = safe_get(user, 33, 0)
        if not user or start <= 0 or amount <= 0:
            sync_smuggle_state(user_id)
            return
        elapsed = time.time() - start
        if elapsed >= SMUGGLE_TIME:
            _smuggle_finish(user_id, chat_id, reply_to_id)
        else:
            safe_send_message(chat_id, f"⏳ عملیات قاچاق در حال انجامه! {int((SMUGGLE_TIME - elapsed) // 60) + 1} دقیقه دیگه مونده.", reply_to_id)
    except Exception as e:
        log_error(e, "check_pending_smuggle")


def check_all_pending_smuggles():
    """بعد از ری‌استارت، قاچاق‌های نیمه‌کاره رو دوباره زمان‌بندی می‌کنه"""
    try:
        rows = db_query("SELECT user_id, smuggle_start_time FROM users WHERE smuggle_start_time > 0 AND smuggle_amount > 0")
        now = time.time()
        for uid, start in rows:
            chat_rows = db_query("SELECT chat_id FROM messages WHERE user_id = %s ORDER BY created_at DESC LIMIT 1", (str(uid),))
            chat = chat_rows[0][0] if chat_rows else uid
            smuggle_in_progress[uid] = True
            scheduler.add_task(max(3, start + SMUGGLE_TIME - now), _smuggle_finish, uid, chat, None)
    except Exception as e:
        log_error(e, "check_all_pending_smuggles")


# ===== بانک =====
def _bank_amount_from_text(text):
    rest = re.sub(r"^\s*(واریز|برداشت)\s*میویی", "", text.strip()).strip()
    if rest in ("همه", "کل", "all"):
        return -1
    return parse_bank_amount(rest)


def handle_bank_deposit(user_id, name, chat_id, text, reply_to_id=None):
    try:
        with bank_lock:
            user = get_user(user_id)
            if not user:
                safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
                return
            user = apply_bank_interest(user)
            amount = _bank_amount_from_text(text)
            pts = safe_get(user, 2, 0)
            balance = safe_get(user, 21, 0)
            if amount == -1:
                amount = pts
            if not amount or amount <= 0:
                safe_send_message(chat_id, "❌ مبلغ معتبر وارد کن!\nمثلاً: `واریز میویی 1000`", reply_to_id)
                return
            if pts < amount:
                safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nموجودی: {pts:,} 🪙", reply_to_id)
                return
            now = time.time()
            n = db_exec("""UPDATE users SET points = points - %s, bank_balance = bank_balance + %s,
                           bank_last_interest = CASE WHEN bank_balance <= 0 THEN %s ELSE bank_last_interest END,
                           bank_last_transaction = %s
                           WHERE user_id = %s AND points >= %s""",
                        (amount, amount, now, now, str(user_id), amount))
            if n == 0:
                safe_send_message(chat_id, "❌ خطا! دوباره تلاش کن.", reply_to_id)
                return
        log_transaction(user_id, 'deposit', amount, balance, balance + amount, 'واریز به بانک')
        safe_send_message(chat_id, f"📥 {amount:,} 🪙 به بانک واریز شد!\n\n🏦 موجودی بانک: {balance + amount:,} 🪙\n💰 موجودی کیف پول: {pts - amount:,} 🪙", reply_to_id)
    except Exception as e:
        log_error(e, "handle_bank_deposit")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


def handle_bank_withdraw(user_id, name, chat_id, text, reply_to_id=None):
    try:
        with bank_lock:
            user = get_user(user_id)
            if not user:
                safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
                return
            user = apply_bank_interest(user)
            amount = _bank_amount_from_text(text)
            pts = safe_get(user, 2, 0)
            balance = safe_get(user, 21, 0)
            if amount == -1:
                amount = balance
            if not amount or amount <= 0:
                safe_send_message(chat_id, "❌ مبلغ معتبر وارد کن!\nمثلاً: `برداشت میویی 1000`", reply_to_id)
                return
            if balance < amount:
                safe_send_message(chat_id, f"🏦 موجودی بانک کافی نیست!\nموجودی بانک: {balance:,} 🪙", reply_to_id)
                return
            n = db_exec("""UPDATE users SET points = points + %s, bank_balance = bank_balance - %s, bank_last_transaction = %s
                           WHERE user_id = %s AND bank_balance >= %s""",
                        (amount, amount, time.time(), str(user_id), amount))
            if n == 0:
                safe_send_message(chat_id, "❌ خطا! دوباره تلاش کن.", reply_to_id)
                return
        log_transaction(user_id, 'withdraw', amount, balance, balance - amount, 'برداشت از بانک')
        safe_send_message(chat_id, f"📤 {amount:,} 🪙 از بانک برداشت شد!\n\n🏦 موجودی بانک: {balance - amount:,} 🪙\n💰 موجودی کیف پول: {pts + amount:,} 🪙", reply_to_id)
    except Exception as e:
        log_error(e, "handle_bank_withdraw")
        safe_send_message(chat_id, "❌ خطا!", reply_to_id)


# ===== شانسی =====
def handle_shansi_join(user_id, name, chat_id, reply_to_id=None):
    try:
        user = get_user(user_id)
        if not user:
            safe_send_message(chat_id, "اول یه میو بگو!", reply_to_id)
            return
        if check_jail_status(user_id):
            safe_send_message(chat_id, "⛓️ شما در زندان هستید!", reply_to_id)
            return
        table = None
        if reply_to_id:
            table = get_shansi_table_by_message(reply_to_id, chat_id)
        if not table:
            rows = db_query("SELECT * FROM casino_tables WHERE chat_id = %s AND status = 'waiting' AND game_type = 'shansi' ORDER BY created_at DESC LIMIT 1", (str(chat_id),))
            table = rows[0] if rows else None
        if not table:
            safe_send_message(chat_id, "❌ میز شانسی فعالی پیدا نشد!", reply_to_id)
            return
        table_id = table[0]
        amount = int(table[3])
        max_players = int(table[4])
        players = json.loads(table[5])
        if user_id in players:
            safe_send_message(chat_id, "❌ شما از قبل توی این میز هستید!", reply_to_id)
            return
        if len(players) >= max_players:
            safe_send_message(chat_id, "❌ میز پر شده!", reply_to_id)
            return
        if not spend_points(user_id, amount):
            safe_send_message(chat_id, "💰 پوینت کافی نداری!", reply_to_id)
            return
        finished = False
        with casino_lock:
            row = get_shansi_table(table_id)
            players = json.loads(row[5]) if row else []
            if not row or row[6] != 'waiting' or len(players) >= max_players or user_id in players:
                add_points(user_id, amount)
                safe_send_message(chat_id, "❌ میز دیگه فعال نیست! پولت برگشت.", reply_to_id)
                return
            players.append(user_id)
            db_exec("UPDATE casino_tables SET players = %s WHERE table_id = %s", (json.dumps(players), table_id))
            if len(players) >= max_players:
                finished = db_exec("UPDATE casino_tables SET status = 'finished' WHERE table_id = %s AND status = 'waiting'", (table_id,)) > 0
        log_transaction(user_id, 'shansi_bet', amount, 0, 0, 'شرط بازی شانسی')
        if not finished:
            lines = "\n".join(f"{i + 1}️⃣ بازیکن : {display_name(p)}" for i, p in enumerate(players))
            for i in range(len(players), max_players):
                lines += f"\n{i + 1}️⃣ بازیکن : ..."
            safe_send_message(chat_id, f"🃏 **شانسی** 🍷\n\n✅ {name} به میز پیوست! ({len(players)}/{max_players})\n\n{lines}", reply_to_id)
            return
        winner = random.choice(players)
        prize = amount * len(players)
        add_points(winner, prize)
        log_transaction(winner, 'shansi_win', prize, 0, 0, 'برد بازی شانسی')
        names = "\n".join(f"{i + 1}️⃣ {display_name(p)}" for i, p in enumerate(players))
        safe_send_message(chat_id, f"🃏 **شانسی** 🍷\n\n{names}\n\n🏆 برنده: {display_name(winner)}\n💰 جایزه: {prize:,} 🪙", reply_to_id)
    except Exception as e:
        log_error(e, "handle_shansi_join")
        safe_send_message(chat_id, "❌ خطا در پیوستن!", reply_to_id)


def _shansi_expire(table_id, chat_id, reply_to_id=None):
    try:
        with casino_lock:
            row = get_shansi_table(table_id)
            if not row or row[6] != 'waiting':
                return
            if db_exec("UPDATE casino_tables SET status = 'expired' WHERE table_id = %s AND status = 'waiting'", (table_id,)) == 0:
                return
            players = json.loads(row[5])
            amount = int(row[3])
        for pid in players:
            add_points(pid, amount)
        safe_send_message(chat_id, "⏰ میز شانسی منقضی شد! پول‌ها بازگردانده شد.", reply_to_id)
    except Exception as e:
        log_error(e, "_shansi_expire")


# ===== تاس =====
def handle_tas_choice(user_id, chat_id, text, reply_to_id=None):
    try:
        choice = "کوچیک" if "کوچیک" in text else "بزرگ"
        with tas_lock:
            table_id = get_tas_table_for_user(user_id, chat_id)
            if not table_id:
                return
            t = get_tas_table_dict(get_tas_table(table_id))
            if not t or t['finished'] == 1 or t['status'] != 'waiting':
                return
            if user_id not in t['players']:
                return
            if len(t['players']) < 2 or not t['choice_started_at']:
                safe_send_message(chat_id, "⏳ هنوز بازیکن دوم به میز نپیوسته!", reply_to_id)
                return
            choices = t['choices']
            if user_id in choices:
                safe_send_message(chat_id, "❌ انتخابت قبلاً ثبت شده!", reply_to_id)
                return
            choices[user_id] = choice
            update_tas_table(table_id, choices=json.dumps(choices))
            if len(choices) < 2:
                safe_send_message(chat_id, "✅ انتخابت ثبت شد! منتظر بازیکن دوم...", reply_to_id)
                return
            update_tas_table(table_id, status="finished", finished=1)
            players = t['players']
            amount = t['amount']
            if table_id in tas_tables:
                del tas_tables[table_id]
        die = random.randint(1, 6)
        result = "کوچیک" if die <= 3 else "بزرگ"
        p1, p2 = players[0], players[1]
        c1, c2 = choices.get(p1), choices.get(p2)
        head = f"🎲 **تاس** 🎲\n\n🎯 تاس آمد: {die} ({result})\n👤 {display_name(p1)}: {c1}\n👤 {display_name(p2)}: {c2}\n\n"
        if c1 == c2:
            add_points(p1, amount)
            add_points(p2, amount)
            safe_send_message(chat_id, head + "🤝 هر دو یه انتخاب داشتید! مساوی شد و پول‌ها برگشت.", reply_to_id)
            return
        winner = p1 if c1 == result else p2
        prize = amount * 2
        add_points(winner, prize)
        log_transaction(winner, 'tas_win', prize, 0, 0, 'برد بازی تاس')
        safe_send_message(chat_id, head + f"🏆 برنده: {display_name(winner)}\n💰 جایزه: {prize:,} 🪙", reply_to_id)
    except Exception as e:
        log_error(e, "handle_tas_choice")


# ===== بازیابی بعد از ری‌استارت =====
def _recover_tables(select_sql, update_sql, label):
    try:
        rows = db_query(select_sql)
        for table_id, chat_id, amount, players_json in rows:
            if db_exec(update_sql, (table_id,)) == 0:
                continue
            for pid in json.loads(players_json):
                add_points(pid, int(amount))
            safe_send_message(chat_id, f"♻️ ربات ری‌استارت شد؛ میز {label} باطل شد و پول‌ها برگشت.")
    except Exception as e:
        log_error(e, f"recover_{label}")


def recover_tas_tables():
    _recover_tables("SELECT table_id, chat_id, amount, players FROM tas_tables WHERE finished = 0 AND status = 'waiting'",
                    "UPDATE tas_tables SET status = 'expired', finished = 1 WHERE table_id = %s AND finished = 0 AND status = 'waiting'",
                    "تاس")


def recover_shansi_tables():
    _recover_tables("SELECT table_id, chat_id, amount, players FROM casino_tables WHERE status = 'waiting'",
                    "UPDATE casino_tables SET status = 'expired' WHERE table_id = %s AND status = 'waiting'",
                    "شانسی")


# ===== بخش ادمین =====
_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")

def load_banned():
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM banned_users")
        banned_users.clear()
        banned_users.update(str(r[0]) for r in c.fetchall())
        conn.close()
    except Exception as e:
        log_error(e, "load_banned")

def _admin_amount(text, prefix):
    rest = text[len(prefix):].translate(_DIGITS)
    digits = re.sub(r"[^0-9]", "", rest)
    return int(digits) if digits else None

def _admin_target(reply_to_id):
    target = get_user_id_from_reply(reply_to_id)
    if target and not get_user(target):
        create_user(target, "کاربر")
    return target

def _broadcast(text):
    try:
        conn = get_connection()
        c = conn.cursor()
        c.execute("SELECT DISTINCT chat_id FROM messages")
        chats = [r[0] for r in c.fetchall()]
        conn.close()
    except Exception as e:
        log_error(e, "broadcast")
        return 0
    sent = 0
    for ch in chats:
        if safe_send_message(ch, text):
            sent += 1
        time.sleep(0.4)
    return sent

def handle_admin(user_id, name, chat_id, text, text_lower, reply_to_id):
    """True برگردونه یعنی پیام یه دستور ادمین بوده و پردازش شده"""
    try:
        if text_lower in ["آیدی من", "شناسه من"]:
            safe_send_message(chat_id, f"🆔 آیدی شما: {user_id}", reply_to_id)
            return True
        if user_id not in ADMIN_IDS:
            return False

        if text_lower in ["ادمین", "پنل ادمین"]:
            safe_send_message(chat_id, """🛠 پنل ادمین میویی

پوینت اضافه [مبلغ]  (ریپلای)
پوینت کم [مبلغ]  (ریپلای)
پوینت تنظیم [مبلغ]  (ریپلای)
آزاد کن  (ریپلای) → خروج از زندان
ریست کاربر  (ریپلای) → پاک کردن کامل اکانت
بن  (ریپلای) / آنبن  (ریپلای)
لیست بن
آمار
پیام همگانی [متن]""", reply_to_id)
            return True

        if text_lower == "آمار":
            conn = get_connection()
            c = conn.cursor()
            c.execute("SELECT COUNT(*), COALESCE(SUM(points),0), COALESCE(SUM(bank_balance),0) FROM users")
            cnt, pts, bank = c.fetchone()
            c.execute("SELECT COUNT(DISTINCT chat_id) FROM messages")
            chats = c.fetchone()[0]
            conn.close()
            safe_send_message(chat_id, f"📊 آمار ربات\n\n👥 کاربران: {cnt:,}\n💬 چت‌ها: {chats:,}\n🪙 مجموع پوینت: {int(pts):,}\n🏦 مجموع بانک: {int(bank):,}\n🚫 بن‌شده‌ها: {len(banned_users)}", reply_to_id)
            return True

        if text_lower == "لیست بن":
            lst = "\n".join(sorted(banned_users)) or "خالیه"
            safe_send_message(chat_id, f"🚫 لیست بن:\n{lst}", reply_to_id)
            return True

        if text_lower.startswith("پیام همگانی"):
            msg = text[len("پیام همگانی"):].strip()
            if not msg:
                safe_send_message(chat_id, "❌ متن پیام رو بعد از دستور بنویس.", reply_to_id)
                return True
            safe_send_message(chat_id, "📢 در حال ارسال...", reply_to_id)
            n = _broadcast("📢 " + msg)
            safe_send_message(chat_id, f"✅ برای {n} چت ارسال شد.", reply_to_id)
            return True

        admin_cmds = ["پوینت اضافه", "پوینت کم", "پوینت تنظیم", "آزاد کن", "ریست کاربر", "بن", "آنبن"]
        cmd = next((c_ for c_ in admin_cmds if text_lower == c_ or (c_.startswith("پوینت") and text_lower.startswith(c_))), None)
        if not cmd:
            return False

        target = _admin_target(reply_to_id)
        if not target:
            safe_send_message(chat_id, "❌ روی پیام کاربر مورد نظر ریپلای کن.", reply_to_id)
            return True

        if cmd.startswith("پوینت"):
            amount = _admin_amount(text_lower, cmd)
            if amount is None:
                safe_send_message(chat_id, "❌ مبلغ رو بنویس. مثلاً: پوینت اضافه 5000", reply_to_id)
                return True
            cur = to_int(safe_get(get_user(target), 2, 0))
            new = cur + amount if cmd == "پوینت اضافه" else max(0, cur - amount) if cmd == "پوینت کم" else amount
            update_user(target, points=new)
            safe_send_message(chat_id, f"✅ پوینت کاربر {target}\n{cur:,} ← {new:,} 🪙", reply_to_id)
        elif cmd == "آزاد کن":
            update_user(target, is_jailed=0, jail_time=0)
            safe_send_message(chat_id, f"✅ کاربر {target} از زندان آزاد شد.", reply_to_id)
        elif cmd == "ریست کاربر":
            conn = get_connection()
            c = conn.cursor()
            c.execute("DELETE FROM users WHERE user_id = %s", (str(target),))
            conn.commit()
            conn.close()
            safe_send_message(chat_id, f"♻️ اکانت کاربر {target} پاک شد.", reply_to_id)
        elif cmd == "بن":
            if target in ADMIN_IDS:
                safe_send_message(chat_id, "❌ نمی‌تونی ادمین رو بن کنی.", reply_to_id)
                return True
            conn = get_connection()
            c = conn.cursor()
            c.execute("INSERT INTO banned_users (user_id, banned_at) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING", (str(target), time.time()))
            conn.commit()
            conn.close()
            banned_users.add(str(target))
            safe_send_message(chat_id, f"🚫 کاربر {target} بن شد.", reply_to_id)
        elif cmd == "آنبن":
            conn = get_connection()
            c = conn.cursor()
            c.execute("DELETE FROM banned_users WHERE user_id = %s", (str(target),))
            conn.commit()
            conn.close()
            banned_users.discard(str(target))
            safe_send_message(chat_id, f"✅ کاربر {target} آنبن شد.", reply_to_id)
        return True
    except Exception as e:
        log_error(e, "handle_admin")
        safe_send_message(chat_id, "❌ خطا در دستور ادمین!", reply_to_id)
        return True

def on_message(event):
    global last_stray_time
    try:
        data = event.data
        if not data.get("text"):
            return

        msg_id = data.get("message_id") or data.get("id") or data.get("msg_id")
        user_id = str(data.get("sender_id", ""))
        text = data["text"].strip()
        text_lower = text.lower()
        now = time.time()
        chat_id = data.get("chat_id") or data.get("chat_name")
        reply_to_id = data.get("reply_to_id")

        if msg_id and user_id:
            save_message(msg_id, user_id, chat_id)

        if msg_id:
            if msg_id in processed_messages:
                return
            processed_messages[msg_id] = now

        if len(processed_messages) >= MAX_PROCESSED_MESSAGES:
            old_keys = [k for k, t in processed_messages.items() if now - t > 300]
            for k in old_keys:
                del processed_messages[k]
            if len(processed_messages) >= MAX_PROCESSED_MESSAGES:
                sorted_keys = sorted(processed_messages, key=processed_messages.get)[:1000]
                for k in sorted_keys:
                    del processed_messages[k]

        if len(user_last_action) > 2500:
            old = [k for k, t in user_last_action.items() if now - t > 90]
            for k in old:
                del user_last_action[k]
        
        user_last_action[user_id] = now

        name = data.get("sender_name") or "کاربر"

        if handle_admin(user_id, name, chat_id, text, text_lower, reply_to_id):
            return
        if user_id in banned_users:
            return

        # ===== انتخاب تاس =====
        if text_lower in ["کوچیک", "بزرگ"]:
            table_id = get_tas_table_for_user(user_id, chat_id)
            if table_id:
                handle_tas_choice(user_id, chat_id, text, reply_to_id)
                return

        # ===== کازینو =====
        if user_id in waiting_for_casino_game:
            game_number = text.strip()
            if game_number == "1":
                waiting_for_casino_amount[user_id] = True
                del waiting_for_casino_game[user_id]
                safe_send_message(chat_id, "💰 مبلغ شرط رو بفرست (مثلاً 100):", reply_to_id)
            elif game_number == "2":
                waiting_for_shansi_amount[user_id] = True
                del waiting_for_casino_game[user_id]
                safe_send_message(chat_id, "🃏 شانسی 🍷\n\n❗ لطفا میز قمار را بچینید.\n\n💰 مبلغ بازی را وارد کنید.", reply_to_id)
            elif game_number == "3":
                waiting_for_tas_amount[user_id] = True
                del waiting_for_casino_game[user_id]
                safe_send_message(chat_id, "🎲 **تاس** 🎲\n\n❗ لطفا مبلغ بازی را وارد کنید.\nمثلاً: 500", reply_to_id)
            else:
                safe_send_message(chat_id, "❌ شماره بازی نامعتبر! فقط ۱، ۲ یا ۳.", reply_to_id)
            return

        if user_id in waiting_for_casino_amount:
            try:
                amount = int(re.sub(r'[^0-9]', '', text))
                if amount <= 0:
                    safe_send_message(chat_id, "❌ مبلغ باید بیشتر از ۰ باشه!", reply_to_id)
                    return
                user = get_user(user_id)
                if safe_get(user, 2, 0) < amount:
                    safe_send_message(chat_id, f"💰 پوینت کافی نداری!\nموجودی: {safe_get(user, 2, 0):,} 🪙", reply_to_id)
                    return
                del waiting_for_casino_amount[user_id]
                handle_spin_slot(user_id, name, chat_id, amount, reply_to_id)
            except:
                safe_send_message(chat_id, "❌ عدد معتبر وارد کن!", reply_to_id)
                if user_id in waiting_for_casino_amount:
                    del waiting_for_casino_amount[user_id]
            return

        if user_id in waiting_for_shansi_amount:
            amount = parse_bank_amount(text)
            if amount is None or amount <= 0:
                safe_send_message(chat_id, "❌ مبلغ معتبر وارد کن!", reply_to_id)
                return
            waiting_for_shansi_players[user_id] = {"amount": amount}
            del waiting_for_shansi_amount[user_id]
            safe_send_message(chat_id, "تعداد بازیکن را وارد کنید.\n\nبین ۲ تا ۴ نفر.", reply_to_id)
            return

        if user_id in waiting_for_shansi_players and "max" not in waiting_for_shansi_players.get(user_id, {}):
            try:
                max_p = int(re.sub(r'[^0-9]', '', text))
                if max_p < 2 or max_p > 4:
                    safe_send_message(chat_id, "تعداد باید بین ۲ تا ۴ باشد!", reply_to_id)
                    return
                amount = waiting_for_shansi_players[user_id]["amount"]
                del waiting_for_shansi_players[user_id]
                handle_shansi_start(user_id, name, chat_id, amount, max_p, reply_to_id)
            except:
                safe_send_message(chat_id, "عدد معتبر وارد کن!", reply_to_id)
            return

        if user_id in waiting_for_tas_amount:
            try:
                amount = int(re.sub(r'[^0-9]', '', text))
                if amount <= 0:
                    safe_send_message(chat_id, "❌ مبلغ باید بیشتر از ۰ باشه!", reply_to_id)
                    return
                del waiting_for_tas_amount[user_id]
                handle_tas_start(user_id, name, chat_id, amount, reply_to_id)
            except:
                safe_send_message(chat_id, "❌ عدد معتبر وارد کن!", reply_to_id)
                if user_id in waiting_for_tas_amount:
                    del waiting_for_tas_amount[user_id]
            return

        # ===== کارت به کارت =====
        if text_lower.startswith("کارت به کارت میویی") or text_lower.startswith("کارت بکارت میویی"):
            handle_card_to_card(user_id, name, chat_id, text, reply_to_id)
            return

        # ===== ثبت کارت =====
        if text_lower.startswith("ثبت کارت"):
            handle_register_card(user_id, name, chat_id, text, reply_to_id)
            return

        # ===== کارت من =====
        if text_lower == "کارت من":
            handle_my_card(user_id, name, chat_id, reply_to_id)
            return

        # ===== بازیابی قاچاق =====
        if text.isdigit() and user_id not in waiting_for_smuggle:
            user = get_user(user_id)
            if user and safe_get(user, 34, 0) == 1:
                waiting_for_smuggle[user_id] = True
                try:
                    amount = int(re.sub(r'[^0-9]', '', text))
                    if 1 <= amount <= 3:
                        handle_smuggle_confirm(user_id, name, chat_id, amount, reply_to_id)
                        return
                    else:
                        del waiting_for_smuggle[user_id]
                        update_user(user_id, smuggle_waiting=0)
                        safe_send_message(chat_id, "❌ تعداد باید بین ۱ تا ۳ باشد!", reply_to_id)
                        return
                except:
                    pass

        # ===== قاچاق =====
        if user_id in waiting_for_smuggle:
            try:
                amount = int(re.sub(r'[^0-9]', '', text))
                
                if amount < 1 or amount > 3:
                    safe_send_message(chat_id, "❌ تعداد باید بین ۱ تا ۳ باشد!", reply_to_id)
                    return
                
                with smuggle_lock:
                    if user_id in smuggle_in_progress:
                        safe_send_message(chat_id, "⏳ شما در حال حاضر یک عملیات قاچاق در حال اجرا دارید!", reply_to_id)
                        if user_id in waiting_for_smuggle:
                            del waiting_for_smuggle[user_id]
                            update_user(user_id, smuggle_waiting=0)
                        return
                    
                    user = get_user(user_id)
                    if not user:
                        safe_send_message(chat_id, "❌ کاربر پیدا نشد!", reply_to_id)
                        if user_id in waiting_for_smuggle:
                            del waiting_for_smuggle[user_id]
                            update_user(user_id, smuggle_waiting=0)
                        return
                    
                    if check_jail_status(user_id):
                        safe_send_message(chat_id, "⛓️ شما در زندان هستید! نمی‌تونید قاچاق کنید.", reply_to_id)
                        if user_id in waiting_for_smuggle:
                            del waiting_for_smuggle[user_id]
                            update_user(user_id, smuggle_waiting=0)
                        return
                    
                    stray_saved = safe_get(user, 20, 0)
                    if amount > min(3, stray_saved):
                        safe_send_message(chat_id, f"❌ شما فقط {stray_saved} پیشی خیابونی دارید! (حداکثر {min(3, stray_saved)})", reply_to_id)
                        return
                    
                    if safe_get(user, 32, 0) > 0:
                        safe_send_message(chat_id, "⏳ شما یک عملیات قاچاق ناتمام دارید که در حال بررسی است...", reply_to_id)
                        if user_id in waiting_for_smuggle:
                            del waiting_for_smuggle[user_id]
                            update_user(user_id, smuggle_waiting=0)
                        check_pending_smuggle(user_id, chat_id, reply_to_id)
                        return
                    
                    if user_id in waiting_for_smuggle:
                        del waiting_for_smuggle[user_id]
                        update_user(user_id, smuggle_waiting=0)
                    
                    handle_smuggle_confirm(user_id, name, chat_id, amount, reply_to_id)
                    
            except ValueError:
                safe_send_message(chat_id, "❌ لطفاً یک عدد معتبر وارد کن!", reply_to_id)
            except Exception as e:
                log_error(e, "on_message_smuggle")
                safe_send_message(chat_id, "❌ خطا در پردازش قاچاق!", reply_to_id)
                if user_id in waiting_for_smuggle:
                    del waiting_for_smuggle[user_id]
                    update_user(user_id, smuggle_waiting=0)
            return

        # ===== نام‌ها =====
        if user_id in waiting_for_name:
            cat_name = text.strip()
            if len(cat_name) > 30 or len(cat_name) < 2:
                safe_send_message(chat_id, "❌ اسم پیشی باید بین ۲ تا ۳۰ حرف باشد!", reply_to_id)
                return
            update_user(user_id, cat_name=cat_name, cat_level=1, last_collect=time.time(),
                        cat_hunger=6, last_hunger_time=time.time())
            del waiting_for_name[user_id]
            safe_send_message(chat_id, f"✅ اسم پیشی: {cat_name}", reply_to_id)
            return

        if user_id in waiting_for_rename:
            new_name = text.strip()
            if len(new_name) > 30 or len(new_name) < 2:
                safe_send_message(chat_id, "❌ اسم باید بین ۲ تا ۳۰ حرف باشد!", reply_to_id)
                return
            update_user(user_id, cat_name=new_name)
            del waiting_for_rename[user_id]
            safe_send_message(chat_id, f"✅ اسم پیشی شما به «{new_name}» تغییر کرد! 🐱", reply_to_id)
            return

        # ===== دستورات اصلی =====
        if text_lower in ["میو", "مع"]:
            handle_meow(user_id, name, chat_id, reply_to_id)
        elif text_lower == "پروفایل":
            handle_profile(user_id, name, chat_id, reply_to_id)
        elif text_lower == "پروفایلش":
            if reply_to_id:
                target_user_id = get_user_id_from_reply(reply_to_id)
                if target_user_id:
                    handle_profile_other(user_id, target_user_id, chat_id, reply_to_id)
                else:
                    safe_send_message(chat_id, "❌ کاربر مورد نظر پیدا نشد!", reply_to_id)
            else:
                safe_send_message(chat_id, "❌ لطفاً روی پیام کاربر مورد نظر ریپلای کنید و بنویسید «پروفایلش»", reply_to_id)
        elif text_lower in ["خرید پیشی", "خرید گربه"]:
            handle_buy_cat(user_id, name, chat_id, reply_to_id)
        elif text_lower in ["پیشی", "گربه"] or (get_user(user_id) and get_user(user_id)[6] and text == get_user(user_id)[6]):
            handle_cat_panel(user_id, name, chat_id, reply_to_id)
        elif text_lower in ["تغییر اسم پیشی", "تغیر اسم پیشی"]:
            handle_rename_cat(user_id, chat_id, reply_to_id)
        elif text_lower in ["ارتقا مقام پیشی", "ارتقا پیشی"]:
            handle_upgrade_cat(user_id, chat_id, reply_to_id)
        elif text_lower in ["برداشت میویی", "برداشت"] and not any(c.isdigit() for c in text):
            handle_collect(user_id, name, chat_id, reply_to_id)
        elif text_lower.startswith("برداشت میویی") and any(c.isdigit() for c in text):
            handle_bank_withdraw(user_id, name, chat_id, text, reply_to_id)
        elif text_lower.startswith("واریز میویی"):
            handle_bank_deposit(user_id, name, chat_id, text, reply_to_id)
        elif text_lower in ["بانک میویی", "بانک"]:
            handle_bank(user_id, name, chat_id, reply_to_id)
        elif text_lower == "خرید قلاب":
            handle_buy_hook(user_id, name, chat_id, reply_to_id)
        elif text_lower == "قلاب":
            handle_hook_panel(user_id, chat_id, reply_to_id)
        elif text_lower == "ارتقا قلاب":
            handle_upgrade_hook(user_id, chat_id, reply_to_id)
        elif text_lower == "ماهیگیری":
            handle_fishing_new(user_id, name, chat_id, reply_to_id)
        elif text_lower in ["پخت میویی", "پخت"]:
            handle_cook_fish(user_id, chat_id, reply_to_id)
        elif text_lower in ["غذا دادن", "غذا"]:
            handle_feed(user_id, chat_id, reply_to_id)
        elif text_lower == "فروش ماهی":
            handle_sell_fish(user_id, chat_id, reply_to_id)
        elif text_lower == "ذخیره ماهی":
            handle_store_fish(user_id, chat_id, reply_to_id)
        elif text_lower in ["نجات گربه", "نجات"]:
            handle_save_stray(user_id, name, chat_id, reply_to_id)
        elif text_lower == "کازینو":
            handle_casino(user_id, name, chat_id, reply_to_id)
        elif text_lower in ["لیدربرد", "پیشی های برتر"]:
            handle_leaderboard(chat_id, reply_to_id)
        elif text_lower == "پیوستن":
            handle_shansi_join(user_id, name, chat_id, reply_to_id)
        elif text_lower == "پیوستن تاس":
            handle_tas_join(user_id, name, chat_id, reply_to_id)
        elif text_lower == "قاچاق" or text_lower == "قاچاق میویی":
            handle_smuggle(user_id, name, chat_id, reply_to_id)
        elif text_lower == "آزادی":
            handle_jail_break(user_id, chat_id, reply_to_id)
        elif text_lower in ["ناز", "ناز کردن"]:
            handle_cuddle(user_id, chat_id, reply_to_id)
        elif text_lower in ["راهنما", "help"]:
            handle_help(chat_id, reply_to_id)
        elif text_lower == "تست":
            safe_send_message(chat_id, "✅ ربات فعال است!", reply_to_id)
        
        handle_stray_cat(chat_id, user_id, reply_to_id)
    except Exception as e:
        log_error(e, "on_message")
      # ===== راهنما =====
def handle_help(chat_id, reply_to_id=None):
    text = """📋 **راهنمای ربات میویی** 🐱

═══════════════════════════════
🔹 **دستورات اصلی**
───────────────────────────────
میو / مع         → میو کردن و دریافت پوینت
پروفایل          → نمایش پروفایل خودت
پروفایلش (ریپلای) → نمایش پروفایل دیگری
لیدربرد          → لیست برترین کاربران

═══════════════════════════════
🔹 **سیستم پیشی** 🐈
───────────────────────────────
خرید پیشی       → خرید گربه (150🪙)
پیشی / گربه     → پنل مدیریت پیشی
تغییر اسم پیشی  → تغییر نام گربه
ارتقا مقام پیشی → ارتقا سطح گربه
برداشت میویی    → برداشت تولید گربه
ناز کردن        → ناز کردن پیشی + پوینت (۱۰دقیقه)

═══════════════════════════════
🔹 **قلاب و ماهیگیری** 🎣
───────────────────────────────
قلاب            → نمایش پنل قلاب و ارتقا
ارتقا قلاب      → ارتقا سطح قلاب
ماهیگیری        → شروع ماهیگیری (۳ دقیقه)
فروش ماهی       → فروش ماهی‌های خام
ذخیره ماهی      → ذخیره ماهی برای پخت
پخت میویی       → پخت ماهی‌ها
غذا دادن        → غذا دادن به پیشی

═══════════════════════════════
🔹 **گربه خیابونی و قاچاق** 😼
───────────────────────────────
نجات گربه      → نجات گربه خیابونی
قاچاق          → قاچاق پیشی‌های خیابونی
آزادی          → خروج از زندان

═══════════════════════════════
🔹 **بانک و اقتصاد** 🏦
───────────────────────────────
بانک میویی      → نمایش اطلاعات بانک + ۵ تراکنش آخر
واریز میویی [م] → واریز به بانک
برداشت میویی [م] → برداشت از بانک
ثبت کارت [شماره] → ثبت شماره کارت بانکی
کارت من         → نمایش شماره کارت خودت
کارت به کارت میویی [شماره] [مبلغ] → انتقال پول به کارت بانکی دیگران

═══════════════════════════════
🔹 **کازینو** 🎰
───────────────────────────────
کازینو          → ورود به کازینو
1️⃣ گردونه شانس  → بازی گردونه
2️⃣ شانسی        → بازی شانسی
3️⃣ تاس          → بازی تاس (کوچیک/بزرگ)

═══════════════════════════════
🔹 **متفرقه**
───────────────────────────────
تست             → تست ربات
راهنما          → این راهنما
═══════════════════════════════"""
    safe_send_message(chat_id, text, reply_to_id)

# ===== حلقه اصلی =====
class _Event:
    def __init__(self, data):
        self.data = data

executor = ThreadPoolExecutor(max_workers=8)

def _convert_update(upd):
    m = upd.get("message")
    if not m or not m.get("text"):
        return None
    frm = m.get("from") or {}
    if frm.get("is_bot"):
        return None
    chat = m.get("chat") or {}
    chat_id = chat.get("id")
    text = m["text"]
    if text.strip() == "/start":
        text = "راهنما"
    name = (frm.get("first_name") or "") + (" " + frm["last_name"] if frm.get("last_name") else "")
    name = name.strip() or frm.get("username") or "کاربر"
    reply = m.get("reply_to_message")
    return {
        "text": text,
        "message_id": f"{chat_id}:{m.get('message_id')}",
        "sender_id": frm.get("id"),
        "sender_name": name,
        "chat_id": chat_id,
        "reply_to_id": f"{chat_id}:{reply.get('message_id')}" if reply and reply.get("message_id") is not None else None,
    }

def poll_updates():
    offset = None
    while True:
        try:
            payload = {"timeout": 25}
            if offset is not None:
                payload["offset"] = offset
            updates = _bale_call("getUpdates", payload, timeout=40) or []
            for upd in updates:
                offset = upd["update_id"] + 1
                data = _convert_update(upd)
                if data:
                    executor.submit(on_message, _Event(data))
        except Exception as e:
            log_error(e, "poll_updates")
            time.sleep(5)

# وب‌سرور ساده برای Render (Web Service نیاز به پورت باز داره)
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"meowie bot is running")
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()
    def log_message(self, *a):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    srv = HTTPServer(("0.0.0.0", port), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

if not BALE_TOKEN:
    raise SystemExit("❌ متغیر محیطی BALE_TOKEN تنظیم نشده!")

load_banned()
start_health_server()
try:
    me = _bale_call("getMe")
    print(f"✅ وصل شد به بله: @{me.get('username')}")
except Exception as e:
    raise SystemExit(f"❌ توکن بله نامعتبره یا اتصال برقرار نشد: {e}")
try:
    _bale_call("deleteWebhook")
except Exception:
    pass
print("🐱 ربات میویی نسخه ۴.۳.۰ - نسخه بله با PostgreSQL روشن شد!")

# ===== Recovery =====
def recover_all():
    check_all_pending_smuggles()
    recover_tas_tables()
    recover_shansi_tables()

recover_all()

threading.Thread(target=poll_updates, daemon=True).start()

last_backup = 0
while True:
    try:
        time.sleep(5)
        if time.time() - last_backup > BACKUP_INTERVAL:
            auto_backup()
            last_backup = time.time()
    except Exception as e:
        log_error(e, "main_loop")
        time.sleep(10)
        continue
