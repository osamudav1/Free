import telebot
from telebot import types
import time
import json
import os

# ================= CONFIG =================
BOT_TOKEN = "7292122932:AAG8hCvjbcF-MuM9IUxivPUGyF-MvdW84HQ"   # BotFather token ထည့်
OWNER_ID = 1735522859
MAIN_GROUP = -1002849045181
BACKUP_GROUP = -1003502685671

bot = telebot.TeleBot(BOT_TOKEN)

# ================= JSON DATABASE =================
DB_FILE = "database.json"

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "users": {},
        "movies": {},
        "current_upload": {}
    }

def save_db():
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

db = load_db()
# ================= FORCE JOIN CHECK =================
FORCE_CHANNEL = "osamu1123"  # @ မထည့်

def check_join(user_id):
    try:
        member = bot.get_chat_member(f"@{FORCE_CHANNEL}", user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

# ================= MAIN MENU =================
def main_menu(uid):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add("🎥 Movies", "🔍 Search")
    kb.add("⭐ My Points", "💎 VIP")
    if str(uid) == str(OWNER_ID):
        kb.add("➕ Add Movie", "📢 Broadcast")
    bot.send_message(uid, "🏠 Main Menu", reply_markup=kb)

# ================= START =================
@bot.message_handler(commands=["start"])
def start(message):
    uid = str(message.chat.id)

    if uid not in db["users"]:
        db["users"][uid] = {
            "approved": False,
            "points": 10,
            "vip": False
        }
        save_db()

    if not check_join(message.chat.id):
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔔 Join Channel", url=f"https://t.me/{FORCE_CHANNEL}"))
        kb.add(types.InlineKeyboardButton("✅ Done", callback_data="check_join"))
        bot.send_message(message.chat.id, "🚫 Channel Join လုပ်ပါ", reply_markup=kb)
        return

    db["users"][uid]["approved"] = True
    save_db()
    main_menu(message.chat.id)

@bot.callback_query_handler(func=lambda c: c.data == "check_join")
def recheck(call):
    if check_join(call.message.chat.id):
        db["users"][str(call.message.chat.id)]["approved"] = True
        save_db()
        main_menu(call.message.chat.id)
    else:
        bot.answer_callback_query(call.id, "Join မလုပ်သေးပါ")
        # ================= ADD MOVIE FLOW =================
@bot.message_handler(func=lambda m: m.text == "➕ Add Movie" and m.chat.id == OWNER_ID)
def add_movie(message):
    uid = str(message.chat.id)
    db["current_upload"][uid] = {}
    save_db()
    msg = bot.send_message(uid, "🎬 Movie နာမည် ရိုက်ထည့်ပါ")
    bot.register_next_step_handler(msg, get_title)

def get_title(message):
    uid = str(message.chat.id)
    db["current_upload"][uid]["title"] = message.text
    save_db()
    msg = bot.send_message(uid, "📝 Description ရိုက်ပါ")
    bot.register_next_step_handler(msg, get_desc)

def get_desc(message):
    uid = str(message.chat.id)
    db["current_upload"][uid]["desc"] = message.text
    save_db()
    msg = bot.send_message(uid, "🖼 Cover Photo ပို့ပါ")
    bot.register_next_step_handler(msg, get_cover)

def get_cover(message):
    uid = str(message.chat.id)
    if message.content_type == "photo":
        db["current_upload"][uid]["cover"] = message.photo[-1].file_id
        db["current_upload"][uid]["parts"] = []
        save_db()
        msg = bot.send_message(uid, "📹 Video တွေ ပို့ပါ (/done ပြီးရင်)")
        bot.register_next_step_handler(msg, get_videos)
    else:
        bot.send_message(uid, "⚠️ ဓာတ်ပုံပဲ ပို့ပါ")
        bot.register_next_step_handler(message, get_cover)

def get_videos(message):
    uid = str(message.chat.id)

    if message.text == "/done":
        save_movie(uid)
        return

    if message.content_type in ["video", "document"]:
        sent = bot.forward_message(MAIN_GROUP, message.chat.id, message.message_id)
        db["current_upload"][uid]["parts"].append(sent.message_id)
        save_db()
        bot.send_message(uid, f"✅ Part {len(db['current_upload'][uid]['parts'])} OK")

    bot.register_next_step_handler(message, get_videos)
    # ================= SAVE MOVIE =================
def save_movie(uid):
    movie_id = f"MOV_{int(time.time())}"
    data = db["current_upload"][uid]

    db["movies"][movie_id] = {
        "title": data["title"],
        "desc": data["desc"],
        "cover": data["cover"],
        "parts": data["parts"]
    }

    del db["current_upload"][uid]
    save_db()

    bot.send_message(uid, f"🎉 Movie တင်ပြီးပါပြီ\nID: {movie_id}")

# ================= SHOW MOVIES =================
@bot.message_handler(func=lambda m: m.text == "🎥 Movies")
def show_movies(message):
    if not db["movies"]:
        bot.send_message(message.chat.id, "Movie မရှိသေးပါ")
        return

    for mid, m in db["movies"].items():
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("▶ Watch", callback_data=f"watch_{mid}"))
        bot.send_photo(
            message.chat.id,
            m["cover"],
            caption=f"🎬 {m['title']}\n📝 {m['desc']}",
            reply_markup=kb
        )

# ================= WATCH MOVIE =================
@bot.callback_query_handler(func=lambda c: c.data.startswith("watch_"))
def watch_movie(call):
    mid = call.data.replace("watch_", "")
    movie = db["movies"].get(mid)

    if movie:
        bot.send_message(call.message.chat.id, f"▶ {movie['title']} ပို့နေပါတယ်...")
        for pid in movie["parts"]:
            bot.forward_message(call.message.chat.id, MAIN_GROUP, pid)
            time.sleep(2)
        bot.send_message(call.message.chat.id, "✅ ပြီးပါပြီ")
        # ================= SEARCH =================
@bot.message_handler(func=lambda m: m.text == "🔍 Search")
def search_prompt(message):
    msg = bot.send_message(message.chat.id, "🔍 Movie နာမည် ရိုက်ပါ")
    bot.register_next_step_handler(msg, do_search)

def do_search(message):
    key = message.text.lower()
    found = False
    for mid, m in db["movies"].items():
        if key in m["title"].lower():
            found = True
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("▶ Watch", callback_data=f"watch_{mid}"))
            bot.send_photo(message.chat.id, m["cover"], caption=m["title"], reply_markup=kb)
    if not found:
        bot.send_message(message.chat.id, "❌ မတွေ့ပါ")

# ================= POINTS / VIP =================
@bot.message_handler(func=lambda m: m.text == "⭐ My Points")
def my_points(message):
    user = db["users"].get(str(message.chat.id))
    bot.send_message(message.chat.id, f"⭐ Points: {user['points']}")

@bot.message_handler(func=lambda m: m.text == "💎 VIP")
def vip(message):
    bot.send_message(message.chat.id, "💎 VIP မဝယ်ရသေးပါ")

# ================= BROADCAST =================
@bot.message_handler(func=lambda m: m.text == "📢 Broadcast" and m.chat.id == OWNER_ID)
def broadcast_prompt(message):
    msg = bot.send_message(message.chat.id, "📢 Broadcast စာရေးပါ")
    bot.register_next_step_handler(msg, do_broadcast)

def do_broadcast(message):
    for uid in db["users"]:
        try:
            bot.send_message(int(uid), message.text)
        except:
            pass
    bot.send_message(message.chat.id, "✅ Broadcast Done")

# ================= RUN =================
bot.infinity_polling()
    
