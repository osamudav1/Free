#!/usr/bin/env python3
import os
import time
import sqlite3
import threading
import traceback
import telebot
from telebot import types

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 1735522859
MAIN_GROUP = -1002849045181
FORCE_CHANNEL = "osamu1123"
DB_FILE = "bot.db"
PAGE_SIZE = 5

bot = telebot.TeleBot(BOT_TOKEN)

# ================= DB (sqlite3, threadsafe via lock) =================
db_lock = threading.Lock()
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row

def init_db():
    with db_lock:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
          user_id INTEGER PRIMARY KEY,
          approved INTEGER DEFAULT 0,
          points INTEGER DEFAULT 10,
          vip INTEGER DEFAULT 0
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS movies (
          movie_id TEXT PRIMARY KEY,
          title TEXT,
          description TEXT,
          cover_file_id TEXT,
          status TEXT,
          created_at INTEGER
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS parts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          movie_id TEXT,
          message_id INTEGER
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
          upload_id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          title TEXT,
          description TEXT,
          cover_file_id TEXT,
          status TEXT DEFAULT 'in_progress',
          created_at INTEGER
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS upload_parts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          upload_id INTEGER,
          message_id INTEGER
        )""")
        conn.commit()

init_db()

# In-memory transient states for editing flows
edit_states = {}       # user_id -> {"action":"edit_title"/"edit_desc", "movie_id":...}
current_uploads = {}   # user_id -> upload_id (for fast access)

def db_execute(query, params=(), fetch=False, many=False):
    with db_lock:
        cur = conn.cursor()
        if many:
            cur.executemany(query, params)
        else:
            cur.execute(query, params)
        conn.commit()
        if fetch:
            return cur.fetchall()
        return cur

# ================= HELPERS =================
def get_user_id_from_message(message):
    try:
        return int(message.from_user.id)
    except Exception:
        return int(message.chat.id)

def check_join(user_id):
    try:
        member = bot.get_chat_member(f"@{FORCE_CHANNEL}", user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

def main_menu(chat_id, user_id):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add("🎥 Movies", "🔍 Search")
    kb.add("⭐ My Points", "💎 VIP")
    if user_id == OWNER_ID:
        kb.add("➕ Add Movie", "📢 Broadcast")
        kb.add("⚙️ Owner Dashboard")
    bot.send_message(chat_id, "🏠 Main Menu", reply_markup=kb)

# ================= START / FORCE JOIN =================
@bot.message_handler(commands=["start"])
def start(message):
    user_id = get_user_id_from_message(message)
    # Ensure user exists in users table
    db_execute("INSERT OR IGNORE INTO users(user_id, approved, points, vip) VALUES(?,?,?,?)",
               (user_id, 0, 10, 0))
    if not check_join(user_id):
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔔 Join Channel", url=f"https://t.me/{FORCE_CHANNEL}"))
        kb.add(types.InlineKeyboardButton("✅ Done", callback_data="check_join"))
        bot.send_message(message.chat.id, "🚫 Channel Join လုပ်ပါ", reply_markup=kb)
        return
    db_execute("UPDATE users SET approved=1 WHERE user_id=?", (user_id,))
    main_menu(message.chat.id, user_id)

@bot.callback_query_handler(func=lambda c: c.data == "check_join")
def recheck(call):
    user_id = call.from_user.id
    if check_join(user_id):
        db_execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)", (user_id,))
        db_execute("UPDATE users SET approved=1 WHERE user_id=?", (user_id,))
        main_menu(call.message.chat.id, user_id)
    else:
        bot.answer_callback_query(call.id, "Join မလုပ်သေးပါ")

# ================= ADD MOVIE FLOW (persisted uploads) =================
@bot.message_handler(func=lambda m: m.text == "➕ Add Movie")
def trigger_add_movie(message):
    user_id = get_user_id_from_message(message)
    if user_id != OWNER_ID:
        return
    # create an uploads row
    now = int(time.time())
    cur = db_execute("INSERT INTO uploads(user_id, created_at) VALUES(?,?)", (user_id, now))
    upload_id = cur.lastrowid
    current_uploads[user_id] = upload_id
    msg = bot.send_message(message.chat.id, "🎬 Movie နာမည်ရိုက်ထည့်ပါ (ဥပမာ - John Wick)")
    bot.register_next_step_handler(msg, get_movie_title)

def get_movie_title(message):
    user_id = get_user_id_from_message(message)
    upload_id = current_uploads.get(user_id)
    if not upload_id:
        bot.send_message(message.chat.id, "⚠️ Upload session မတွေ့ပါ။ ➕ Add Movie ကိုနှိပ်ပါ")
        return
    title = message.text.strip()
    db_execute("UPDATE uploads SET title=? WHERE upload_id=?", (title, upload_id))
    msg = bot.send_message(message.chat.id, "📝 Movie Description (Action/2024)")
    bot.register_next_step_handler(msg, get_movie_desc)

def get_movie_desc(message):
    user_id = get_user_id_from_message(message)
    upload_id = current_uploads.get(user_id)
    if not upload_id:
        bot.send_message(message.chat.id, "⚠️ Upload session မတွေ့ပါ။ ➕ Add Movie ကိုနှိပ်ပါ")
        return
    desc = message.text.strip()
    db_execute("UPDATE uploads SET description=? WHERE upload_id=?", (desc, upload_id))
    msg = bot.send_message(message.chat.id, "🖼 Movie Cover Photo ပို့ပါ")
    bot.register_next_step_handler(msg, get_movie_cover)

def get_movie_cover(message):
    user_id = get_user_id_from_message(message)
    upload_id = current_uploads.get(user_id)
    if not upload_id:
        bot.send_message(message.chat.id, "⚠️ Upload session မတွေ့ပါ။ ➕ Add Movie ကိုနှိပ်ပါ")
        return
    if message.content_type == 'photo':
        file_id = message.photo[-1].file_id
        db_execute("UPDATE uploads SET cover_file_id=? WHERE upload_id=?", (file_id, upload_id))
        db_execute("UPDATE uploads SET status='uploading_parts' WHERE upload_id=?", (upload_id,))
        msg = bot.send_message(message.chat.id, "📹 Movie Video ပို့ပါ (ပြီးရင် /done)")
        bot.register_next_step_handler(msg, get_movie_videos)
    else:
        msg = bot.send_message(message.chat.id, "⚠️ ဓာတ်ပုံပို့ပါ")
        bot.register_next_step_handler(msg, get_movie_cover)

def get_movie_videos(message):
    user_id = get_user_id_from_message(message)
    upload_id = current_uploads.get(user_id)
    if not upload_id:
        bot.send_message(message.chat.id, "⚠️ Upload session မတွေ့ပါ။ ➕ Add Movie ကိုနှိပ်ပါ")
        return

    if message.text and message.text.strip() == "/done":
        finalize_upload(upload_id, user_id, message.chat.id)
        return

    if message.content_type in ['video', 'document']:
        try:
            forwarded = bot.forward_message(MAIN_GROUP, message.chat.id, message.message_id)
            forwarded_id = forwarded.message_id
            db_execute("INSERT INTO upload_parts(upload_id, message_id) VALUES(?,?)", (upload_id, forwarded_id))
            # count parts
            rows = db_execute("SELECT COUNT(*) as cnt FROM upload_parts WHERE upload_id=?", (upload_id,), fetch=True)
            cnt = rows[0]['cnt'] if rows else 0
            bot.send_message(message.chat.id, f"✅ အပိုင်း {cnt} ရပြီး။ နောက်ပိုင်း ပို့ပါ (/done)")
        except Exception:
            traceback.print_exc()
            bot.send_message(message.chat.id, "⛔ အပိုင်းပို့ရာ၌ ပြဿနာရှိသွားသည်")
    else:
        bot.send_message(message.chat.id, "🔹 ဗီဒီယို သို့မဟုတ် ဖိုင်ပို့ပါ (/done ပြီးရင်)")

    bot.register_next_step_handler(message, get_movie_videos)

def finalize_upload(upload_id, user_id, chat_id_for_response=None):
    # fetch upload
    rows = db_execute("SELECT * FROM uploads WHERE upload_id=?", (upload_id,), fetch=True)
    if not rows:
        if chat_id_for_response:
            bot.send_message(chat_id_for_response, "⚠️ Upload မတွေ့ပါ")
        return
    up = rows[0]
    title = up['title'] or "Untitled"
    desc = up['description'] or ""
    cover = up['cover_file_id']
    now = int(time.time())
    movie_id = f"MOV_{now}"
    # create movie row
    db_execute("INSERT INTO movies(movie_id, title, description, cover_file_id, status, created_at) VALUES(?,?,?,?,?,?)",
               (movie_id, title, desc, cover, "Ended", now))
    # move parts
    parts = db_execute("SELECT message_id FROM upload_parts WHERE upload_id=?", (upload_id,), fetch=True)
    if parts:
        for p in parts:
            db_execute("INSERT INTO parts(movie_id, message_id) VALUES(?,?)", (movie_id, p['message_id']))
    # cleanup upload rows
    db_execute("DELETE FROM upload_parts WHERE upload_id=?", (upload_id,))
    db_execute("DELETE FROM uploads WHERE upload_id=?", (upload_id,))
    current_uploads.pop(user_id, None)
    if chat_id_for_response:
        bot.send_message(chat_id_for_response, f"🎊 '{title}' တင်ပြီးပါပြီ\nMovie ID: {movie_id}")

# ================= SHOW MOVIES (pagination) =================
def build_movies_keyboard(page, total, query=None):
    kb = types.InlineKeyboardMarkup()
    offset = (page - 1) * PAGE_SIZE
    rows = []
    if query:
        rows = db_execute("SELECT movie_id, title, description, cover_file_id FROM movies WHERE title LIKE ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                          (f"%{query}%", PAGE_SIZE, offset), fetch=True)
    else:
        rows = db_execute("SELECT movie_id, title, description, cover_file_id FROM movies ORDER BY created_at DESC LIMIT ? OFFSET ?",
                          (PAGE_SIZE, offset), fetch=True)
    for r in rows:
        kb.add(types.InlineKeyboardButton(f"📺 {r['title']}", callback_data=f"watch_{r['movie_id']}"))
    # navigation
    prev_page = max(1, page - 1)
    next_page = page + 1
    # compute total count
    if query:
        cnt_row = db_execute("SELECT COUNT(*) as cnt FROM movies WHERE title LIKE ?", (f"%{query}%",), fetch=True)[0]
    else:
        cnt_row = db_execute("SELECT COUNT(*) as cnt FROM movies", fetch=True)[0]
    total_count = cnt_row['cnt']
    max_page = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
    nav_buttons = []
    if page > 1:
        nav_buttons.append(types.InlineKeyboardButton("⬅️ Prev", callback_data=f"movies_page_{prev_page}__{query or ''}"))
    if page < max_page:
        nav_buttons.append(types.InlineKeyboardButton("Next ➡️", callback_data=f"movies_page_{next_page}__{query or ''}"))
    if nav_buttons:
        kb.row(*nav_buttons)
    return kb, total_count

@bot.message_handler(func=lambda m: m.text == "🎥 Movies")
def show_movie_list(message):
    kb, total = build_movies_keyboard(1, None)
    if total == 0:
        bot.send_message(message.chat.id, "Movie မရှိသေးပါ")
        return
    bot.send_message(message.chat.id, "Movies:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("movies_page_"))
def on_movies_page(call):
    payload = call.data.replace("movies_page_", "", 1)
    # format page__query
    if "__" in payload:
        page_str, query = payload.split("__", 1)
        query = query or None
    else:
        page_str = payload
        query = None
    page = int(page_str)
    kb, total = build_movies_keyboard(page, None, query)
    bot.edit_message_text("Movies:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("watch_"))
def watch_movie(call):
    m_id = call.data.replace("watch_", "", 1)
    row = db_execute("SELECT * FROM movies WHERE movie_id=?", (m_id,), fetch=True)
    if not row:
        bot.answer_callback_query(call.id, "Movie မတွေ့ပါ")
        return
    movie = row[0]
    bot.send_message(call.message.chat.id, f"🎬 {movie['title']} ပို့နေပါ...")
    parts = db_execute("SELECT message_id FROM parts WHERE movie_id=? ORDER BY id", (m_id,), fetch=True)
    for p in parts:
        try:
            bot.forward_message(call.message.chat.id, MAIN_GROUP, p['message_id'])
            time.sleep(1)
        except Exception:
            traceback.print_exc()
    bot.send_message(call.message.chat.id, "✅ ပို့ပြီးပါပြီ")

# ================= SEARCH =================
@bot.message_handler(func=lambda m: m.text == "🔍 Search")
def trigger_search(message):
    msg = bot.send_message(message.chat.id, "🔎 Search ဟုရိုက်ပါ (title)")
    bot.register_next_step_handler(msg, do_search)

def do_search(message):
    query = message.text.strip()
    kb, total = build_movies_keyboard(1, None, query)
    if total == 0:
        bot.send_message(message.chat.id, "Search မတွေ့ပါ")
    else:
        bot.send_message(message.chat.id, f"Results for '{query}':", reply_markup=kb)

# ================= OWNER: manage movies (edit/delete) =================
@bot.message_handler(func=lambda m: m.text == "⚙️ Owner Dashboard")
def admin_panel(message):
    user_id = get_user_id_from_message(message)
    if user_id != OWNER_ID:
        return
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("👥 Users", callback_data="manage_users"),
        types.InlineKeyboardButton("🎬 Manage Movies", callback_data="manage_movies"),
        types.InlineKeyboardButton("📢 Broadcast", callback_data="broadcast"),
        types.InlineKeyboardButton("🔐 Force Join", callback_data="force_join")
    )
    bot.send_message(OWNER_ID, "🛠 Owner Dashboard", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data in ["manage_movies", "manage_users", "broadcast", "force_join"])
def owner_callbacks(call):
    uid = call.from_user.id
    if uid != OWNER_ID:
        bot.answer_callback_query(call.id, "Unauthorized")
        return
    if call.data == "manage_movies":
        # list first page of movies with edit/delete buttons
        rows = db_execute("SELECT movie_id, title FROM movies ORDER BY created_at DESC LIMIT ?", (PAGE_SIZE,), fetch=True)
        if not rows:
            bot.answer_callback_query(call.id, "No movies")
            return
        for r in rows:
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton("Edit", callback_data=f"edit_{r['movie_id']}"),
                types.InlineKeyboardButton("Delete", callback_data=f"delete_{r['movie_id']}")
            )
            bot.send_message(call.message.chat.id, f"🎬 {r['title']}", reply_markup=kb)
    elif call.data == "manage_users":
        cnt = db_execute("SELECT COUNT(*) as cnt FROM users", fetch=True)[0]['cnt']
        bot.answer_callback_query(call.id, f"Users: {cnt}")
    elif call.data == "broadcast":
        bot.answer_callback_query(call.id, "Use /broadcast <text>")
    elif call.data == "force_join":
        bot.answer_callback_query(call.id, "Force Join channel: @" + FORCE_CHANNEL)

@bot.callback_query_handler(func=lambda c: c.data and (c.data.startswith("edit_") or c.data.startswith("delete_")))
def edit_delete_handler(call):
    uid = call.from_user.id
    if uid != OWNER_ID:
        bot.answer_callback_query(call.id, "Unauthorized")
        return
    data = call.data
    if data.startswith("delete_"):
        movie_id = data.replace("delete_", "", 1)
        # delete parts and movie
        db_execute("DELETE FROM parts WHERE movie_id=?", (movie_id,))
        db_execute("DELETE FROM movies WHERE movie_id=?", (movie_id,))
        bot.answer_callback_query(call.id, "Deleted")
        bot.edit_message_text("Deleted movie.", chat_id=call.message.chat.id, message_id=call.message.message_id)
    else:
        movie_id = data.replace("edit_", "", 1)
        # start edit flow: ask what to edit
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Title", callback_data=f"edit_field_title__{movie_id}"))
        kb.add(types.InlineKeyboardButton("Description", callback_data=f"edit_field_description__{movie_id}"))
        kb.add(types.InlineKeyboardButton("Cancel", callback_data="edit_cancel"))
        bot.send_message(call.message.chat.id, "Select field to edit:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("edit_field_"))
def edit_field_choice(call):
    uid = call.from_user.id
    if uid != OWNER_ID:
        bot.answer_callback_query(call.id, "Unauthorized")
        return
    # format: edit_field_{field}__{movie_id}
    payload = call.data.replace("edit_field_", "", 1)
    if "__" not in payload:
        bot.answer_callback_query(call.id, "Bad payload")
        return
    field, movie_id = payload.split("__", 1)
    if field not in ("title", "description"):
        bot.answer_callback_query(call.id, "Unsupported")
        return
    # store state and ask next
    edit_states[uid] = {"action": f"edit_{field}", "movie_id": movie_id}
    prompt = "Enter new title:" if field == "title" else "Enter new description:"
    msg = bot.send_message(call.message.chat.id, prompt)
    bot.register_next_step_handler(msg, perform_edit)

@bot.message_handler(func=lambda m: True)
def perform_edit(message):
    uid = get_user_id_from_message(message)
    state = edit_states.get(uid)
    if not state:
        return  # ignore other messages normally
    action = state["action"]
    movie_id = state["movie_id"]
    text = message.text.strip()
    if action == "edit_title":
        db_execute("UPDATE movies SET title=? WHERE movie_id=?", (text, movie_id))
        bot.send_message(message.chat.id, "Title updated.")
    elif action == "edit_description":
        db_execute("UPDATE movies SET description=? WHERE movie_id=?", (text, movie_id))
        bot.send_message(message.chat.id, "Description updated.")
    else:
        bot.send_message(message.chat.id, "Unknown edit action.")
    edit_states.pop(uid, None)

# ================= BROADCAST (owner) =================
@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    user_id = get_user_id_from_message(message)
    if user_id != OWNER_ID:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /broadcast <text>")
        return
    text = parts[1]
    rows = db_execute("SELECT user_id FROM users", fetch=True)
    sent = 0
    for r in rows:
        try:
            bot.send_message(r['user_id'], text)
            sent += 1
        except Exception:
            pass
    bot.send_message(message.chat.id, f"Broadcast sent to {sent} users.")

# ================= RUN =================
def run_polling():
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except Exception:
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    run_polling()
