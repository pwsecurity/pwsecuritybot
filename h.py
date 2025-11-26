import os
import sys
import weakref
import asyncio
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from flask import Flask
from waitress import serve
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackContext,
    CallbackQueryHandler,
    MessageHandler,
    filters
)

# ================================================
# PYTHON 3.13 COMPATIBILITY FIXES
# ================================================
weakref.ProxyTypes = ()
sys.audit = lambda *args, **kwargs: None
os.environ['PYTHON_TELEGRAM_BOT_USE_PY_TZ'] = 'False'
os.environ['PYTHON_TELEGRAM_BOT_SKIP_WEAKREF'] = 'True'

# Monkey-patch JobQueue before imports
import telegram.ext._jobqueue

def safe_set_application(self, application):
    self._application = lambda: application  # Creates strong reference

telegram.ext._jobqueue.JobQueue.set_application = safe_set_application

# ================================================
# CONFIGURATION
# ================================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "7673723315:AAH9l3D-0gI7_o1uLZQYy-ZYK4TRNzLjQwE"
ADMIN_ID = 5261072501

# ProxyChecker.org API Configuration
PROXYCHECKER_API_TOKENS = [
    "TdAROzZAkgybV9z55FjSC6F0ySqOGTWJVe9nkTW4nQ13xIPbWHrTDIDK6d6vXUTY",  # Token 1
    "Y3H2R2t1kEt5Fy3Ql9Lj5vmaEC64jab8285VUDuApceemvhwB7hDO8Gfbv3t21R2"   # Token 2 (fallback)
]
PROXYCHECKER_API_URL = "https://proxychecker.org/api"


# SOCKS5 Proxy List
def load_socks5_ips_from_file(filename="ip.txt"):
    socks5_ips = {}
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
            for idx, line in enumerate(lines, 1):
                socks5_ips[f"Panel Ip {idx}"] = line
    return socks5_ips

# ========== PROFILE CARD GENERATOR =============
def generate_profile_card(username, user_id, status, expiry_date):
    """
    Generate a profile card image with user information.
    Returns a BytesIO object containing the PNG image.
    """
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    
    # Load background image
    bg_path = "profile_bg.png"
    if not os.path.exists(bg_path):
        # Create a simple dark background if file doesn't exist
        img = Image.new('RGB', (1000, 500), color=(10, 20, 30))
    else:
        img = Image.open(bg_path)
        img = img.resize((1000, 500))
    
    draw = ImageDraw.Draw(img)
    
    # Try to load a custom font, fallback to default
    try:
        # Use Courier for hacker/terminal aesthetic - MUCH BIGGER
        title_font = ImageFont.truetype("/System/Library/Fonts/Courier.dfont", 72)  # Was 52
        label_font = ImageFont.truetype("/System/Library/Fonts/Courier.dfont", 44)  # Was 32
        value_font = ImageFont.truetype("/System/Library/Fonts/Courier.dfont", 42)  # Was 30
    except:
        try:
            # Fallback to Arial Bold - MUCH BIGGER
            title_font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf", 72)
            label_font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf", 44)
            value_font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 42)
        except:
            title_font = ImageFont.load_default()
            label_font = ImageFont.load_default()
            value_font = ImageFont.load_default()
    
    # Colors - bright white for maximum visibility
    text_color = (255, 255, 255, 255)  # Pure White
    accent_color = (0, 255, 0, 255)  # Bright Green
    
    # Draw title at the top - bigger and brighter
    draw.text((500, 40), "█ PREMIUM SOCKS5 █", font=title_font, fill=accent_color, anchor="mm")
    
    # Create 4 info rows with MORE spacing
    left_margin = 50
    y_start = 160
    row_height = 85  # Increased from 75
    
    # Row 1: Username
    y = y_start
    draw.text((left_margin, y), "👤 USER:", font=label_font, fill=accent_color, anchor="lm")
    draw.text((left_margin + 270, y), username[:25] + "..." if len(username) > 25 else username, font=value_font, fill=text_color, anchor="lm")
    
    # Row 2: User ID
    y += row_height
    draw.text((left_margin, y), "🆔 USER ID:", font=label_font, fill=accent_color, anchor="lm")
    draw.text((left_margin + 270, y), user_id[:20] + "..." if len(user_id) > 20 else user_id, font=value_font, fill=text_color, anchor="lm")
    
    # Row 3: Status
    y += row_height
    draw.text((left_margin, y), "📊 STATUS:", font=label_font, fill=accent_color, anchor="lm")
    status_emoji = "✅" if status == "approved" else "⏳" if status == "pending" else "🔄"
    draw.text((left_margin + 270, y), f"{status_emoji} {status.upper()}", font=value_font, fill=text_color, anchor="lm")
    
    # Row 4: Expiry
    y += row_height
    draw.text((left_margin, y), "📅 EXPIRES:", font=label_font, fill=accent_color, anchor="lm")
    draw.text((left_margin + 270, y), expiry_date if expiry_date else "N/A", font=value_font, fill=text_color, anchor="lm")
    
    # Save to BytesIO
    output = BytesIO()
    img.save(output, format='PNG')
    output.seek(0)
    return output

# ========== SOCKS5 PROXY CHECKER (ProxyChecker.org API) =============
def check_socks5_proxy_via_api(proxy_string):
    """
    Check SOCKS5 proxy using ProxyChecker.org API.
    Supports dual-token fallback for better rate limiting.
    Works on PythonAnywhere (uses HTTP instead of sockets).
    
    Args:
        proxy_string: Proxy in format "host:port:username:password"
    
    Returns:
        tuple: (is_working, status_message)
    """
    import requests
    
    # Try both API tokens (fallback if rate limited)
    for token_index, api_token in enumerate(PROXYCHECKER_API_TOKENS, 1):
        try:
            headers = {
                'Authorization': f'Bearer {api_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            data = {
                'proxy': proxy_string,
                'check_ssl': False,  # SOCKS5 doesn't need SSL check
                'check_anonymity': True,
                'check_speed': False,  # Faster response
                'check_location': False  # Faster response
            }
            
            response = requests.post(
                f'{PROXYCHECKER_API_URL}/proxy/check',
                headers=headers,
                json=data,
                timeout=15
            )
            
            # Rate limit hit - try next token
            if response.status_code == 429:
                logger.warning(f"API Token {token_index} rate limited, trying fallback...")
                continue
            
            # Other error
            if response.status_code != 200:
                error_msg = response.json().get('message', 'API error')
                logger.error(f"ProxyChecker API error: {error_msg}")
                return (False, f"API Error: {error_msg[:30]}")
            
            # Parse response
            result = response.json()
            
            if result.get('success'):
                data = result.get('data', {})
                
                # Check 'working' boolean field (API returns true/false)
                is_working = data.get('working', False)
                
                if is_working:
                    response_time = data.get('response_time', 'N/A')
                    return (True, f"Online ({response_time})")
                else:
                    return (False, "Proxy offline")
            else:
                error_msg = result.get('message', 'Unknown error')
                return (False, error_msg[:30])
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout with token {token_index}")
            if token_index < len(PROXYCHECKER_API_TOKENS):
                continue  # Try next token
            return (False, "Request timeout")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error with token {token_index}: {e}")
            if token_index < len(PROXYCHECKER_API_TOKENS):
                continue  # Try next token
            return (False, f"Network error")
            
        except Exception as e:
            logger.error(f"Unexpected error checking proxy: {e}")
            return (False, "Check failed")
    
    # All tokens failed (rate limited)
    return (False, "Rate limit (wait)")

USER_DATA_FILE = "users.json"
PROXY_STATUS_FILE = "proxy_status.json"
PROGRESS_BAR_LENGTH = 20
ANIMATION_FRAMES = ['⬜', '🟦']

# Proxy status cache
proxy_status_cache = {}

def load_proxy_status():
    """Load cached proxy status from file"""
    global proxy_status_cache
    if os.path.exists(PROXY_STATUS_FILE):
        try:
            with open(PROXY_STATUS_FILE, "r", encoding='utf-8') as file:
                proxy_status_cache = json.load(file)
        except:
            proxy_status_cache = {}
    return proxy_status_cache

def save_proxy_status():
    """Save proxy status to file"""
    with open(PROXY_STATUS_FILE, "w", encoding='utf-8') as file:
        json.dump(proxy_status_cache, file, indent=4)

# Load proxy status on startup
load_proxy_status()

# ========== EARNINGS SYSTEM =============
EARNINGS_RATE_FILE = "earnings_rate.json"

def load_earnings_rate():
    """Load USD to BDT conversion rate"""
    if os.path.exists(EARNINGS_RATE_FILE):
        try:
            with open(EARNINGS_RATE_FILE, "r") as f:
                data = json.load(f)
                return data.get("rate", 120)  # Default 120 BDT
        except:
            return 120
    return 120

def save_earnings_rate(rate):
    """Save USD to BDT conversion rate"""
    with open(EARNINGS_RATE_FILE, "w") as f:
        json.dump({"rate": rate}, f)

def escape_markdown(text):
    """Escape Markdown special characters for Legacy Markdown mode"""
    if not text:
        return text
    # In Legacy Markdown, only these need escaping: _ * ` [
    # We also escape \ to prevent accidental escaping
    return (str(text)
            .replace("\\", "\\\\")  # Escape backslash first
            .replace("_", "\\_")
            .replace("*", "\\*")
            .replace("`", "\\`")
            .replace("[", "\\["))

# Global rate
usd_to_bdt_rate = load_earnings_rate()

# ================================================
# DATA MANAGEMENT
# ================================================
def load_users() -> dict:
    if os.path.exists(USER_DATA_FILE):
        with open(USER_DATA_FILE, "r", encoding='utf-8') as file:
            return json.load(file)
    return {}

def save_users(users: dict) -> None:
    with open(USER_DATA_FILE, "w", encoding='utf-8') as file:
        json.dump(users, file, indent=4, ensure_ascii=False)

users = load_users()

# ================================================
# BOT HANDLERS
# ================================================
async def start(update: Update, context: CallbackContext) -> None:
    user_id = str(update.effective_user.id)
    keyboard = []
    
    is_active_user = False
    if user_id in users:
        status = users[user_id]["status"]
        if status == "approved":
            expiry = users[user_id]["expiry_date"]
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            if datetime.now() <= expiry_date:
                is_active_user = True

    # Row 1: Register (only if not active) or 2-column layout
    if not is_active_user:
        keyboard.append([InlineKeyboardButton("🔑  Register for Access  ", callback_data='register')])

    # Row 2: Main actions in 2 columns with padding for width
    keyboard.append([
        InlineKeyboardButton("🌐  Get Proxy IPs  ", callback_data='getip'),
        InlineKeyboardButton("📊  My Dashboard  ", callback_data='dashboard')
    ])
    
    # Row 3: Earnings + Training
    keyboard.append([
        InlineKeyboardButton("💰  My Earnings  ", callback_data='my_earnings'),
        InlineKeyboardButton("📚  Training Materials  ", callback_data='training')
    ])
    
    # Row 4: Jarvis Pro (if active)
    if is_active_user:
        keyboard.append([InlineKeyboardButton("🤖  Jarvis AI Pro  ", url="https://t.me/jarvisaipro_bot")])
    
    # Status info (full width)
    if user_id in users:
        status = users[user_id]["status"]
        if status == "approved":
            expiry = users[user_id]["expiry_date"]
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            if datetime.now() > expiry_date:
                keyboard.append([InlineKeyboardButton("⏱️  Subscription Expired", callback_data='none')])
                keyboard.append([InlineKeyboardButton("🔄  Request Renewal Now", callback_data='request_renewal')])
            else:
                keyboard.append([InlineKeyboardButton(f"✅  Active Until: {expiry}", callback_data='none')])
        elif status == "pending":
            keyboard.append([InlineKeyboardButton("⏳  Pending Admin Approval", callback_data='none')])
        elif status == "renewal_requested":
            keyboard.append([InlineKeyboardButton("🔄  Renewal Request Pending", callback_data='none')])
    else:
        keyboard.append([InlineKeyboardButton("❌  Not Registered Yet", callback_data='none')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send banner image if it exists
    if os.path.exists("banner.png"):
        # Use context.bot_data to cache the file_id
        banner_file_id = context.bot_data.get('banner_file_id')
        try:
            if banner_file_id:
                await update.message.reply_photo(
                    photo=banner_file_id,
                    caption="🎮 *Welcome to Premium SOCKS5 Service!*\n\n🚀 *Fast. Secure. Reliable.*\n\nPlease select an option below:",
                    parse_mode="Markdown",
                    reply_markup=reply_markup
                )
            else:
                msg = await update.message.reply_photo(
                    photo=open("banner.png", "rb"),
                    caption="🎮 *Welcome to Premium SOCKS5 Service!*\n\n🚀 *Fast. Secure. Reliable.*\n\nPlease select an option below:",
                    parse_mode="Markdown",
                    reply_markup=reply_markup
                )
                # Cache the file_id for future use
                if msg.photo:
                    context.bot_data['banner_file_id'] = msg.photo[-1].file_id
        except Exception as e:
            logger.error(f"Error sending banner: {e}")
            # Fallback to text if photo fails
            await update.message.reply_text(
                "🎮 *Welcome to Premium SOCKS5 Service!*\n\nPlease select an option below:",
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
    else:
        await update.message.reply_text(
            "🎮 *Welcome to Premium SOCKS5 Service!*\n\nPlease select an option below:",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def button_click(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Error answering callback query: {e}")

    if query.data == "register":
        await register(update, context)
    elif query.data == "getip":
        await get_ip(update, context)
    elif query.data == "dashboard":
        await show_dashboard(update, context)
    elif query.data == "manage_favorites":
        await manage_favorites(update, context)
    elif query.data == "clear_favorites":
        await clear_all_favorites(update, context)
    elif query.data == "request_renewal":
        await request_renewal(update, context)
    elif query.data == "listusers":
        await list_users(update, context)
    elif query.data == "none":
        return

async def register(update: Update, context: CallbackContext) -> None:
    user_id = str(update.effective_user.id)
    username = update.effective_user.username
    if user_id in users:
        status = users[user_id]["status"]
        if status == "approved":
            expiry = users[user_id]["expiry_date"]
            await update.effective_message.reply_text(f"✅ Already registered! Valid until: {expiry}")
        else:
            await update.effective_message.reply_text("⏳ Registration pending approval.")
        return

    users[user_id] = {
        "username": username,
        "status": "pending",
        "expiry_date": None,
        "favorites": [],
        "last_notification": None
    }
    save_users(users)
    await update.effective_message.reply_text("⏳ Registration submitted! Awaiting admin approval.")

    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data=f"approve_{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data=f"decline_{user_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
        ADMIN_ID,
        f"🔔 New registration request:\n\nUsername: @{escape_markdown(username)}\nUser ID: {user_id}",
        reply_markup=reply_markup
    )

# ========== ADMIN INTERACTIVE IP MANAGEMENT =============

# Helper to check admin
ADMIN_ID_STR = str(ADMIN_ID)

def is_admin(user_id):
    return str(user_id) == ADMIN_ID_STR

# Update send_menu to show admin buttons
async def send_menu(update: Update, context: CallbackContext) -> None:
    user_id = str(update.effective_user.id)
    keyboard = []
    
    is_active_user = False
    if user_id in users:
        status = users[user_id]["status"]
        if status == "approved":
            expiry = users[user_id]["expiry_date"]
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            if datetime.now() <= expiry_date:
                is_active_user = True

    # Row 1: Register (only if not active)
    if not is_active_user:
        keyboard.append([InlineKeyboardButton("🔑  Register for Access  ", callback_data='register')])

    # Row 2: Main actions in 2 columns with padding
    keyboard.append([
        InlineKeyboardButton("🌐  Get Proxy IPs  ", callback_data='getip'),
        InlineKeyboardButton("📊  My Dashboard  ", callback_data='dashboard')
    ])
    
    # Row 3: Earnings + Training + IP Due
    keyboard.append([
        InlineKeyboardButton("💰  My Earnings  ", callback_data='my_earnings'),
        InlineKeyboardButton("💳  IP Due  ", callback_data='my_ip_due')
    ])
    keyboard.append([InlineKeyboardButton("📚  Training Materials  ", callback_data='training')])
    
    # Row 4: Jarvis Pro (if active)
    if is_active_user:
        keyboard.append([InlineKeyboardButton("🤖  Jarvis AI Pro  ", url="https://t.me/jarvisaipro_bot")])

    # Admin buttons in 2-column grid with padding
    if is_admin(user_id):
        keyboard.append([
            InlineKeyboardButton("📢  Admin Broadcast  ", callback_data='admin_broadcast'),
            InlineKeyboardButton("🛠️  Manage IPs  ", callback_data='admin_edit_ips')
        ])
        keyboard.append([
            InlineKeyboardButton("📈  View Analytics  ", callback_data='admin_analytics'),
            InlineKeyboardButton("📊  Usage Stats  ", callback_data='user_analytics')
        ])
        keyboard.append([InlineKeyboardButton("🔍  Check All Proxies  ", callback_data='admin_check_proxies')])
    if user_id in users:
        status = users[user_id]["status"]
        if status == "approved":
            expiry = users[user_id]["expiry_date"]
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            if datetime.now() > expiry_date:
                keyboard.append([InlineKeyboardButton("⏱️ Subscription: Expired", callback_data='none')])
                keyboard.append([InlineKeyboardButton("🔄 Request Renewal", callback_data='request_renewal')])
            else:
                keyboard.append([InlineKeyboardButton(f"✅ Subscription: Active (Expires: {expiry})", callback_data='none')])
        elif status == "pending":
            keyboard.append([InlineKeyboardButton("⏳ Subscription: Pending Approval", callback_data='none')])
        elif status == "renewal_requested":
            keyboard.append([InlineKeyboardButton("🔄 Renewal: Pending Approval", callback_data='none')])
    else:
        keyboard.append([InlineKeyboardButton("❌ Subscription: Not Registered", callback_data='none')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send banner image if it exists
    if os.path.exists("banner.png"):
        banner_file_id = context.bot_data.get('banner_file_id')
        try:
            if banner_file_id:
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=banner_file_id,
                    caption="🎮 *Welcome to Premium SOCKS5 Service!*\n\n🚀 *Fast. Secure. Reliable.*\n\nPlease select an option below:",
                    parse_mode="Markdown",
                    reply_markup=reply_markup
                )
            else:
                msg = await context.bot.send_photo(
                    chat_id=user_id,
                    photo=open("banner.png", "rb"),
                    caption="🎮 *Welcome to Premium SOCKS5 Service!*\n\n🚀 *Fast. Secure. Reliable.*\n\nPlease select an option below:",
                    parse_mode="Markdown",
                    reply_markup=reply_markup
                )
                if msg.photo:
                    context.bot_data['banner_file_id'] = msg.photo[-1].file_id
        except Exception as e:
            logger.error(f"Error sending banner in menu: {e}")
            await context.bot.send_message(
                chat_id=user_id,
                text="🎮 *Welcome to Premium SOCKS5 Service!*\n\nPlease select an option below:",
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="🎮 *Welcome to Premium SOCKS5 Service!*\n\nPlease select an option below:",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

# Admin Broadcast via button
async def admin_broadcast_button(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    context.user_data['awaiting_broadcast'] = True
    await safe_edit_message(query, "📢 *Enter the message to broadcast to all users:*")

# UNIFIED ADMIN TEXT HANDLER
async def handle_admin_text_input(update: Update, context: CallbackContext) -> None:
    if not is_admin(update.effective_user.id):
        return

    # Helper to cleanup and refresh
    async def cleanup_and_refresh(user_id):
        try:
            # Delete user's input
            await update.message.delete()
            # Delete bot's prompt if stored
            if 'prompt_message_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.message.chat_id,
                    message_id=context.user_data['prompt_message_id']
                )
            # Refresh card if stored
            if 'card_message_id' in context.user_data:
                await refresh_user_card(
                    context, 
                    update.message.chat_id, 
                    context.user_data['card_message_id'], 
                    user_id
                )
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    # Check if awaiting broadcast message
    if context.user_data.get('awaiting_broadcast'):
        message = update.message.text
        photo = update.message.photo[-1].file_id if update.message.photo else None
        caption = update.message.caption if update.message.caption else message

        count = 0
        for user_id in users:
            try:
                if photo:
                    await context.bot.send_photo(chat_id=user_id, photo=photo, caption=f"📢 Admin Announcement:\n\n{message}")
                else:
                    await context.bot.send_message(chat_id=user_id, text=f"📢 Admin Announcement:\n\n{message}")
                count += 1
            except Exception as e:
                logger.warning(f"Failed to send broadcast to {user_id}: {e}")
        
        await update.message.reply_text(f"✅ Broadcast sent to {count} users.", reply_markup=ReplyKeyboardRemove())
        context.user_data.pop('awaiting_broadcast', None)
        return

    # Check if awaiting IPs to add
    if context.user_data.get('awaiting_add_ip'):
        if not update.message.text:
             await update.message.reply_text("❌ Please send text content for IPs.")
             return
        text = update.message.text
        ips = [ip.strip() for ip in text.replace(',', '\n').replace(' ', '\n').split('\n') if ip.strip()]
        if not ips:
            await update.message.reply_text("❌ No valid IPs found.")
        else:
            with open("ip.txt", "a", encoding="utf-8") as f:
                for ip in ips:
                    f.write(f"{ip}\n")
            await update.message.reply_text(f"✅ Added {len(ips)} IP(s).", reply_markup=ReplyKeyboardRemove())
            await admin_edit_ips(update, context) # Show updated panel
        context.user_data.pop('awaiting_add_ip', None)
        return

    # Check if awaiting earning ID/Name
    if context.user_data.get('awaiting_earning_id'):
        id_name = update.message.text.strip()
        context.user_data['earning_id_name'] = id_name
        context.user_data['awaiting_earning_id'] = False
        context.user_data['awaiting_earning_amount'] = True
        
        # Ask for amount (Seamless: Send new prompt, delete old one)
        try:
            await update.message.delete() # Delete ID input
            if 'prompt_message_id' in context.user_data:
                await context.bot.delete_message(chat_id=update.message.chat_id, message_id=context.user_data['prompt_message_id'])
            
            prompt_msg = await update.message.reply_text(
                f"👇 *Step 2/2: Enter Amount (USD) for '{id_name}':*",
                parse_mode="Markdown"
            )
            context.user_data['prompt_message_id'] = prompt_msg.message_id
        except Exception as e:
            logger.error(f"Error in earning step 1: {e}")
        return
    
    # Check if awaiting earning amount
    if context.user_data.get('awaiting_earning_amount'):
        try:
            amount = float(update.message.text)
            if amount <= 0:
                raise ValueError("Amount must be positive")
            
            user_id = context.user_data.get('earning_user_id')
            id_name = context.user_data.get('earning_id_name')
            
            if user_id not in users:
                raise ValueError("User not found")
            
            # Initialize earnings if needed
            if "earnings" not in users[user_id]:
                users[user_id]["earnings"] = {
                    "total_usd": 0.0,
                    "rate": 120,
                    "history": [],
                    "payments": []
                }
            
            # Add earning record
            earning_record = {
                "id_name": id_name,
                "amount_usd": amount,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "time": datetime.now().strftime("%H:%M:%S")
            }
            
            users[user_id]["earnings"]["history"].append(earning_record)
            users[user_id]["earnings"]["total_usd"] += amount
            save_users(users)
            
            await cleanup_and_refresh(user_id)
            
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid amount: {str(e)}. Please enter a valid number.")
            return
        finally:
            context.user_data.pop('awaiting_earning_amount', None)
            context.user_data.pop('earning_user_id', None)
            context.user_data.pop('earning_id_name', None)
            context.user_data.pop('prompt_message_id', None)
            context.user_data.pop('card_message_id', None)
        return
    
    # Check if awaiting rate
    if context.user_data.get('awaiting_rate'):
        try:
            rate = int(update.message.text)
            if rate <= 0:
                raise ValueError("Rate must be positive")
            
            user_id = context.user_data.get('rate_user_id')
            
            if user_id not in users:
                raise ValueError("User not found")
            
            # Initialize earnings if needed
            if "earnings" not in users[user_id]:
                users[user_id]["earnings"] = {
                    "total_usd": 0.0,
                    "rate": 120,
                    "history": [],
                    "payments": []
                }
            
            users[user_id]["earnings"]["rate"] = rate
            save_users(users)
            
            await cleanup_and_refresh(user_id)
            
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid rate: {str(e)}. Please enter a valid number.")
            return
        finally:
            context.user_data.pop('awaiting_rate', None)
            context.user_data.pop('rate_user_id', None)
            context.user_data.pop('prompt_message_id', None)
            context.user_data.pop('card_message_id', None)
        return

    # Check if awaiting IP Due input
    if context.user_data.get('awaiting_ip_due_input'):
        try:
            amount = float(update.message.text)
            if amount < 0:
                raise ValueError("Amount cannot be negative")
                
            user_id = context.user_data.get('ip_due_user_id')
            action = context.user_data.get('ip_due_action')
            
            if user_id not in users:
                raise ValueError("User not found")
                
            # Initialize if needed
            if "ip_due" not in users[user_id]:
                users[user_id]["ip_due"] = {
                    "current_due": 1400.0,
                    "due_rate": 1400.0,
                    "history": []
                }
            
            ip_data = users[user_id]["ip_due"]
            
            if action == "set":
                ip_data["current_due"] = amount
            elif action == "add":
                ip_data["current_due"] += amount
            elif action == "reduce":
                ip_data["current_due"] = max(0, ip_data["current_due"] - amount)
            elif action == "rate":
                ip_data["due_rate"] = amount
            
            # Log history
            if "history" not in ip_data:
                ip_data["history"] = []
                
            ip_data["history"].append({
                "action": action,
                "amount": amount,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "time": datetime.now().strftime("%H:%M:%S")
            })
            
            save_users(users)
            await cleanup_and_refresh(user_id)
            
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid input: {str(e)}. Please enter a valid number.")
            return
        finally:
            context.user_data.pop('awaiting_ip_due_input', None)
            context.user_data.pop('ip_due_user_id', None)
            context.user_data.pop('ip_due_action', None)
            context.user_data.pop('prompt_message_id', None)
            context.user_data.pop('card_message_id', None)
        return

    # Check if awaiting partial deduction amount
    if context.user_data.get('awaiting_deduct_amount'):
        try:
            amount = float(update.message.text)
            if amount < 0:
                raise ValueError("Amount cannot be negative")
            
            user_id = context.user_data.get('payment_user_id')
            
            # Validate against max due
            current_due = users[user_id].get("ip_due", {}).get("current_due", 0)
            if amount > current_due:
                await update.message.reply_text(f"❌ Amount cannot exceed current due ({current_due:,.0f} BDT).")
                return
                
            # Show confirmation
            # We need to construct a fake query object or call the function directly
            # Since we can't easily fake a query, we'll send a new message with the confirmation
            
            username = users[user_id].get("username", "Unknown")
            earnings = users[user_id].get("earnings", {})
            total_usd = earnings.get("total_usd", 0.0)
            rate = earnings.get("rate", 120)
            total_bdt = total_usd * rate
            
            final_amount = total_bdt - amount
            
            keyboard = [
                [InlineKeyboardButton("✅ Confirm Payment", callback_data=f"confirm_pay_{user_id}_{amount}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="close_msg")]
            ]
            
            await update.message.reply_text(
                f"💸 *FINAL CONFIRMATION*\n"
                f"User: @{escape_markdown(username)}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💰 Earnings: {total_bdt:,.0f} BDT\n"
                f"💳 Deduction: -{amount:,.0f} BDT\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💵 *NET PAY: {final_amount:,.0f} BDT*\n\n"
                f"Confirm processing?",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid amount: {str(e)}. Please enter a valid number.")
            return
        finally:
            context.user_data.pop('awaiting_deduct_amount', None)
            context.user_data.pop('payment_user_id', None)
        return

    # Check if awaiting days for extend/reduce
    if context.user_data.get("admin_action"):
        try:
            days = int(update.message.text)
            if days <= 0:
                raise ValueError("Days must be positive")

            action = context.user_data.get("admin_action")
            user_id = context.user_data.get("target_user_id") or context.user_data.get("admin_user_id")
            
            if not action or not user_id or user_id not in users:
                raise ValueError("Invalid action or user.")

            current_expiry = datetime.strptime(users[user_id]["expiry_date"], "%Y-%m-%d")
            current_date = datetime.now()

            if action == "extend":
                # If expired, start extension from today, otherwise from current expiry
                if current_expiry < current_date:
                    current_expiry = current_date
                new_expiry = current_expiry + timedelta(days=days)
            elif action == "reduce":
                new_expiry = current_expiry - timedelta(days=days)
                # Ensure it doesn't go before today if it's currently active
                if new_expiry < current_date and current_expiry >= current_date:
                    new_expiry = current_date # Set to today if reducing past today
                elif new_expiry < current_date and current_expiry < current_date:
                    # If already expired, allow reducing further into the past
                    pass 

            users[user_id]["expiry_date"] = new_expiry.strftime("%Y-%m-%d")
            users[user_id]["status"] = "approved" # Ensure active status after modification
            save_users(users)
            
            await cleanup_and_refresh(user_id)
            
        except ValueError as e:
            await update.message.reply_text(
                f"❌ Invalid input: {str(e)}. Please enter a positive number.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ Cancel", callback_data="listusers")]])
            )
        except Exception as e:
            logger.error(f"Unexpected error in admin text input: {e}")
            await update.message.reply_text(
                "❌ An unexpected error occurred.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to User List", callback_data="listusers")]])
            )
        finally:
            context.user_data.pop("admin_action", None)
            context.user_data.pop("admin_user_id", None)
            context.user_data.pop("current_expiry", None)
        return

# Admin Edit IPs Panel
async def admin_edit_ips(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = str(update.effective_user.id)
    
    if query:
        await query.answer()
    
    if not is_admin(user_id):
        if query:
            await safe_edit_message(query, "❌ Not authorized.")
        else:
            await update.message.reply_text("❌ Not authorized.")
        return
        
    socks5_ips = load_socks5_ips_from_file()
    keyboard = []
    for idx, (panel, ip) in enumerate(socks5_ips.items(), 1):
        keyboard.append([
            InlineKeyboardButton(f"{panel}", callback_data=f"noop"),
            InlineKeyboardButton("❌ Delete", callback_data=f"admin_del_ip_{idx}")
        ])
    keyboard.append([InlineKeyboardButton("➕ Add IP", callback_data="admin_add_ip")])
    keyboard.append([InlineKeyboardButton("🗑 Delete All", callback_data="admin_del_all_ips")])
    keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "🛠️ *Edit SOCKS5 Proxies*\n\nBelow are your current proxies. You can delete, add, or clear all:"
    
    if query:
        await safe_edit_message(query, text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup)

# Delete single IP
async def admin_del_ip(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    idx = int(query.data.split('_')[-1])
    with open("ip.txt", "r", encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]
    if not (1 <= idx <= len(lines)):
        await query.edit_message_text(f"❌ Invalid panel number. There are {len(lines)} proxies.")
        return
    removed_proxy = lines.pop(idx - 1).strip()
    with open("ip.txt", "w", encoding="utf-8") as f:
        f.writelines(lines)
    await query.edit_message_text(f"✅ Removed Panel Ip {idx}: {removed_proxy}")
    # Show updated panel
    await admin_edit_ips(update, context)

# Delete all IPs
async def admin_del_all_ips(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    # Confirm deletion
    keyboard = [
        [InlineKeyboardButton("⚠️ Confirm Delete All", callback_data="admin_confirm_del_all_ips")],
        [InlineKeyboardButton("🔙 Cancel", callback_data="admin_edit_ips")]
    ]
    await query.edit_message_text(
        "⚠️ Are you sure you want to delete ALL proxies? This cannot be undone!",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_confirm_del_all_ips(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    with open("ip.txt", "w", encoding="utf-8") as f:
        f.write("")
    await query.edit_message_text("✅ All proxies deleted.")
    await admin_edit_ips(update, context)

# Add IPs
async def admin_add_ip(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    context.user_data['awaiting_add_ip'] = True
    await query.edit_message_text(
        "➕ *Send the IP(s) to add.* You can send multiple IPs separated by newlines, commas, or spaces:",
        parse_mode="Markdown"
    )

async def handle_approval(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if str(query.from_user.id) != str(ADMIN_ID):
        await query.edit_message_text("❌ Unauthorized.")
        return

    data_parts = query.data.split("_")
    if "renewal" in query.data:
        action = f"{data_parts[0]}_{data_parts[1]}"
        user_id = data_parts[2]
    else:
        action = data_parts[0]
        user_id = data_parts[1]

    if user_id not in users:
        await query.edit_message_text("❌ User not found.")
        return

    if action == "approve":
        users[user_id]["status"] = "approved"
        users[user_id]["expiry_date"] = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        save_users(users)
        await query.edit_message_text(f"✅ User @{escape_markdown(users[user_id]['username'])} approved!")
        await context.bot.send_message(
            user_id,
            f"✅ Your registration has been approved! Your subscription is valid until: {users[user_id]['expiry_date']}"
        )
        await send_menu(update, context)
    elif action == "decline":
        username = users[user_id]["username"]
        del users[user_id]
        save_users(users)
        await query.edit_message_text(f"❌ User @{escape_markdown(username)} declined.")
        await context.bot.send_message(user_id, "❌ Your registration has been declined.")
    elif action == "approve_renewal":
        # Extend by 30 days from current expiry
        current_expiry = datetime.strptime(users[user_id]["expiry_date"], "%Y-%m-%d")
        # If expired, start from today
        if current_expiry < datetime.now():
            current_expiry = datetime.now()
        
        new_expiry = current_expiry + timedelta(days=30)
        users[user_id]["expiry_date"] = new_expiry.strftime("%Y-%m-%d")
        users[user_id]["status"] = "approved"
        users[user_id]["last_notification"] = None # Reset notification
        
        # Auto-increment IP Due
        if "ip_due" not in users[user_id]:
            users[user_id]["ip_due"] = {
                "current_due": 1400.0,
                "due_rate": 1400.0,
                "history": []
            }
        
        ip_data = users[user_id]["ip_due"]
        due_rate = ip_data.get("due_rate", 1400.0)
        ip_data["current_due"] += due_rate
        
        if "history" not in ip_data:
            ip_data["history"] = []
            
        ip_data["history"].append({
            "action": "renewal_add",
            "amount": due_rate,
            "reason": "Subscription renewed",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S")
        })
        
        save_users(users)
        
        await query.edit_message_text(f"✅ Renewal approved for @{escape_markdown(users[user_id]['username'])}.\nNew expiry: {users[user_id]['expiry_date']}\n💳 IP Due added: +{due_rate:,.0f} BDT")
        
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=int(user_id),
                text=f"✅ *Subscription Renewed!*\n\nYour subscription has been extended until {users[user_id]['expiry_date']}.\n\n💳 IP Due Added: {due_rate:,.0f} BDT",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {user_id}: {e}")
        await send_menu(update, context)
    elif action == "decline_renewal":
        users[user_id]["status"] = "expired"
        save_users(users)
        await query.edit_message_text(f"❌ Renewal for @{escape_markdown(users[user_id]['username'])} declined.")
        await context.bot.send_message(user_id, "❌ Your subscription renewal request has been declined.")

async def get_ip(update: Update, context: CallbackContext) -> None:
    user_id = str(update.effective_user.id)
    if user_id not in users or users[user_id]["status"] != "approved":
        await update.effective_message.reply_text("❌ Not approved. Please register first.")
        return

    expiry_date = datetime.strptime(users[user_id]["expiry_date"], "%Y-%m-%d")
    if datetime.now() > expiry_date:
        keyboard = [[InlineKeyboardButton("🔄 Request Renewal", callback_data='request_renewal')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.effective_message.reply_text(
            "⏱️ Your subscription has expired. Would you like to request a renewal?",
            reply_markup=reply_markup
        )
        return

    # Load proxies from file
    socks5_ips = load_socks5_ips_from_file()
    states = list(socks5_ips.keys())
    keyboard = []
    row = []

    for i, state in enumerate(states):
        # Get status indicator from cache
        status_indicator = ""
        if state in proxy_status_cache:
            cache_status = proxy_status_cache[state].get("status", "unknown")
            if cache_status == "online":
                status_indicator = "🟢 "
            elif cache_status == "offline":
                status_indicator = "🔴 "
            else:
                status_indicator = "⚪ "
        
        row.append(InlineKeyboardButton(f"{status_indicator}{state}", callback_data=f"panel_{i+1}"))
        if len(row) == 2 or i == len(states) - 1:
            keyboard.append(row)
            row = []

    keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text(
        "🛜 *Select a panel to get the SOCKS5 IP:*",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

# New handler for panel button clicks
def get_panel_proxy(panel_idx):
    socks5_ips = load_socks5_ips_from_file()
    keys = list(socks5_ips.keys())
    if 0 <= panel_idx < len(keys):
        return keys[panel_idx], socks5_ips[keys[panel_idx]]
    return None, None

# ========== RATE LIMITING =============
user_last_proxy_time = {}

async def handle_panel_selection(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    now = time.time()
    # Rate limiting: 5 seconds per user
    last_time = user_last_proxy_time.get(user_id, 0)
    if now - last_time < 5:
        await query.answer("⏳ Please wait before requesting another proxy.", show_alert=True)
        return
    user_last_proxy_time[user_id] = now
    if user_id not in users or users[user_id]["status"] != "approved":
        await query.edit_message_text("❌ Not approved. Please register first.")
        return
    try:
        panel_idx = int(query.data.split("_")[1]) - 1
        panel_name, proxy = get_panel_proxy(panel_idx)
        if not proxy:
            await query.edit_message_text("❌ Invalid panel selected. Please try again.")
            return
        
        # Track proxy request for analytics
        if "proxy_requests" not in users[user_id]:
            users[user_id]["proxy_requests"] = []
        users[user_id]["proxy_requests"].append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "panel": panel_name
        })
        save_users(users)
        
        # Send the proxy as a code block, then delete after 5 seconds
        sent = await query.message.reply_text(
            f"""🛜 *{panel_name}*\n\n```
{proxy}
```\n\n_Copy the above credentials to configure your SOCKS5 proxy._""",
            parse_mode="Markdown"
        )
        await asyncio.sleep(5)
        try:
            await sent.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error in handle_panel_selection: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ An error occurred while processing your request. Please try again."
        )

async def remove_user(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) != str(ADMIN_ID):
        if hasattr(update, 'callback_query'):
            await update.callback_query.answer("❌ Not authorized.")
        else:
            await update.message.reply_text("❌ Not authorized.")
        return

    if hasattr(update, 'callback_query'):
        # Handle button click case
        username = context.args[0] if context.args else None
    else:
        # Handle command case
        if not context.args:
            await update.message.reply_text("Usage: /remove @username")
            return
        username = context.args[0].lstrip('@')

    user_found = False
    for user_id, data in list(users.items()):
        if data["username"] == username:
            del users[user_id]
            save_users(users)
            user_found = True
            break

    if user_found:
        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(f"✅ User @{escape_markdown(username)} has been removed.")
        else:
            await update.message.reply_text(f"✅ User @{escape_markdown(username)} has been removed.")
    else:
        if hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(f"❌ User @{escape_markdown(username)} not found.")
        else:
            await update.message.reply_text(f"❌ User @{escape_markdown(username)} not found.")

async def list_users(update: Update, context: CallbackContext, query=None) -> None:
    if str(update.effective_user.id) != str(ADMIN_ID):
        if query:
            await query.edit_message_text("❌ Unauthorized.")
        else:
            await update.message.reply_text("❌ Unauthorized.")
        return

    keyboard = []
    for user_id, data in users.items():
        status_emoji = "✅" if data.get("status") == "approved" else "⏳" if data.get("status") == "pending" else "🔄" if data.get("status") == "renewal_requested" else "❌"
        expiry = data.get("expiry_date", "N/A")
        user_info = f"{status_emoji} @{escape_markdown(data['username'])} - {data.get('status', 'unknown')} - Exp: {expiry}"

        buttons = [InlineKeyboardButton(user_info, callback_data=f"userinfo_{user_id}")]
        keyboard.append(buttons)

        action_buttons = []
        if data.get("status") == "pending":
            action_buttons = [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Decline", callback_data=f"decline_{user_id}")
            ]
        elif data.get("status") == "renewal_requested":
            action_buttons = [
                InlineKeyboardButton("✅ Approve Renewal", callback_data=f"approve_renewal_{user_id}"),
                InlineKeyboardButton("❌ Decline Renewal", callback_data=f"decline_renewal_{user_id}")
            ]
        elif data.get("status") == "approved":
            action_buttons = [
                InlineKeyboardButton("➕ Extend", callback_data=f"sub_extend_{user_id}"),
                InlineKeyboardButton("➖ Reduce", callback_data=f"sub_reduce_{user_id}"),
                InlineKeyboardButton("❌ Remove", callback_data=f"remove_{user_id}")
            ]

        if action_buttons:
            keyboard.append(action_buttons)

    keyboard.append([InlineKeyboardButton("🔙 Back to Main Menu", callback_data='back_to_menu')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        await query.edit_message_text(
            "📜 *User Management Panel*\n\nBelow is the list of all users and their status:",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            "📜 *User Management Panel*\n\nBelow is the list of all users and their status:",
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def handle_extend_reduce(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if str(query.from_user.id) != str(ADMIN_ID):
        await query.edit_message_text("❌ Unauthorized.")
        return

    data_parts = query.data.split("_")
    action = data_parts[0]  # "extend" or "reduce"
    user_id = data_parts[1]

    if user_id not in users:
        await query.edit_message_text("❌ User not found.")
        return

    # Store the action and user_id in context
    context.user_data["admin_action"] = action
    context.user_data["admin_user_id"] = user_id

    # Store the current expiry date for validation
    context.user_data["current_expiry"] = users[user_id].get("expiry_date", datetime.now().strftime("%Y-%m-%d"))

    await query.edit_message_text(
        f"🛠️ Enter number of days to {action} for @{escape_markdown(users[user_id]['username'])}:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ Cancel", callback_data="cancel_admin_action")]
        ])
    )

async def handle_remove_user_button(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if str(query.from_user.id) != str(ADMIN_ID):
        await query.edit_message_text("❌ Unauthorized.")
        return

    user_id = query.data.split("_")[1]

    if user_id in users:
        username = users[user_id]["username"]
        del users[user_id]
        save_users(users)

        await query.edit_message_text(
            f"✅ User @{escape_markdown(username)} has been removed.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to User List", callback_data="listusers")]
            ])
        )
    else:
        await query.edit_message_text("❌ User not found.")

async def cancel_admin_action(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if str(query.from_user.id) != str(ADMIN_ID):
        await query.edit_message_text("❌ Unauthorized.")
        return

    await query.edit_message_text("❌ Action cancelled.")
    await list_users(update, context)

# ========== ADMIN BROADCAST =============
async def list_users(update: Update, context: CallbackContext) -> None:
    # Get the message object (works for both commands and callback queries)
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        message = query.message
        user_id = str(query.from_user.id)
    else:
        message = update.message
        user_id = str(update.effective_user.id)
    
    if not is_admin(user_id):
        await message.reply_text("❌ Not authorized.")
        return

    if not users:
        await message.reply_text("📋 No users registered yet.")
        return

    # Send header
    await message.reply_text(
        f"👥 *REGISTERED USERS*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Total: *{len(users)}* users\n\n"
        f"Loading user cards...",
        parse_mode="Markdown"
    )
    
    # Send each user as individual message with buttons
    for uid, data in users.items():
        raw_username = data.get("username", "Unknown")
        username = escape_markdown(raw_username)
        expiry = data.get("expiry_date", "N/A")
        status = data.get("status", "pending")
        
        status_emoji = "✅" if status == "approved" else "⏳" if status == "pending" else "❌"
        
        # Initialize earnings if not exists
        if "earnings" not in data:
            data["earnings"] = {
                "total_usd": 0.0,
                "rate": 120,
                "history": [],
                "payments": []
            }
            save_users(users)
        
        # Initialize IP Due if not exists
        if "ip_due" not in data:
            data["ip_due"] = {
                "current_due": 1400.0,
                "due_rate": 1400.0,
                "history": [{
                    "action": "initialized",
                    "amount": 1400.0,
                    "reason": "System default",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "time": datetime.now().strftime("%H:%M:%S")
                }]
            }
            save_users(users)
        
        earnings = data["earnings"]
        total_usd = earnings.get("total_usd", 0.0)
        rate = earnings.get("rate", 120)
        total_bdt = total_usd * rate
        
        ip_due_data = data.get("ip_due", {"current_due": 1400.0, "due_rate": 1400.0})
        current_due = ip_due_data.get("current_due", 1400.0)
        
        # Create user card message
        user_message = (
            f"👤 *@{username}*\n"
            f"━━━━━━━━━━━━━━━\n"
            f"{status_emoji} *Status:* {status.upper()}\n"
            f"📅 *Expires:* {expiry}\n"
            f"💰 *Earnings:* ${total_usd:.2f}\n"
            f"💴 *BDT:* {total_bdt:,.0f} (@ {rate})\n"
            f"💳 *IP Due:* {current_due:,.0f} BDT\n"
            f"🆔 *ID:* `{uid}`"
        )
        
        # Create buttons for this user
        keyboard = [
            # Row 1: Earnings buttons
            [
                InlineKeyboardButton("💵 Add $", callback_data=f"add_earn_{uid}"),
                InlineKeyboardButton("💱 Rate", callback_data=f"set_rate_{uid}"),
                InlineKeyboardButton("💸 Pay", callback_data=f"payment_{uid}"),
                InlineKeyboardButton("💳 Due", callback_data=f"ip_due_{uid}")
            ],
            # Row 2: Subscription management
            [
                InlineKeyboardButton("⏰ Extend", callback_data=f"sub_extend_{uid}"),
                InlineKeyboardButton("⏬ Reduce", callback_data=f"sub_reduce_{uid}"),
                InlineKeyboardButton("🗑️ Remove", callback_data=f"remove_{uid}")
            ]
        ]
        
        # Send user card
        await message.reply_text(
            user_message,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def refresh_user_card(context: CallbackContext, chat_id: int, message_id: int, user_id: str) -> None:
    """Helper to refresh a user card message in-place"""
    if user_id not in users:
        return
        
    data = users[user_id]
    username = escape_markdown(data.get("username", "Unknown"))
    status = data.get("status", "pending")
    expiry = data.get("expiry_date", "N/A")
    
    earnings = data.get("earnings", {})
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    
    ip_due_data = data.get("ip_due", {"current_due": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    
    status_emoji = "✅" if status == "approved" else "⏳" if status == "pending" else "❌"
    
    user_message = (
        f"👤 *@{username}*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"{status_emoji} *Status:* {status.upper()}\n"
        f"📅 *Expires:* {expiry}\n"
        f"💰 *Earnings:* ${total_usd:.2f}\n"
        f"💴 *BDT:* {total_bdt:,.0f} (@ {rate})\n"
        f"💳 *IP Due:* {current_due:,.0f} BDT\n"
        f"🆔 *ID:* `{user_id}`"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("💵 Add $", callback_data=f"add_earn_{user_id}"),
            InlineKeyboardButton("💱 Rate", callback_data=f"set_rate_{user_id}"),
            InlineKeyboardButton("💸 Pay", callback_data=f"payment_{user_id}"),
            InlineKeyboardButton("💳 Due", callback_data=f"ip_due_{user_id}")
        ],
        [
            InlineKeyboardButton("⏰ Extend", callback_data=f"sub_extend_{user_id}"),
            InlineKeyboardButton("⏬ Reduce", callback_data=f"sub_reduce_{user_id}"),
            InlineKeyboardButton("🗑️ Remove", callback_data=f"remove_{user_id}")
        ]
    ]
    
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=user_message,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        if "Message is not modified" in str(e):
            pass # Ignore if content is identical
        else:
            logger.error(f"Failed to refresh user card: {e}")

async def show_user_info(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return

    parts = query.data.split("_")
    # Handle different callback patterns (user_info_123 or back_to_user_123)
    uid = parts[-1]
    
    if uid not in users:
        await query.edit_message_text("❌ User not found.")
        return
        
    data = users[uid]
    username = data.get("username", "Unknown").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    status = data.get("status", "pending")
    expiry = data.get("expiry_date", "N/A")
    
    # Initialize earnings if not exists
    if "earnings" not in data:
        data["earnings"] = {
            "total_usd": 0.0,
            "rate": 120,
            "history": [],
            "payments": []
        }
        save_users(users)
    
    # Initialize IP Due if not exists
    if "ip_due" not in data:
        data["ip_due"] = {
            "current_due": 1400.0,
            "due_rate": 1400.0,
            "history": []
        }
        save_users(users)
    
    earnings = data["earnings"]
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    
    ip_due_data = data.get("ip_due", {"current_due": 1400.0, "due_rate": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    
    status_emoji = "✅" if status == "approved" else "⏳" if status == "pending" else "❌"
    
    # Create user card message
    user_message = (
        f"👤 *@{username}*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"{status_emoji} *Status:* {status.upper()}\n"
        f"📅 *Expires:* {expiry}\n"
        f"💰 *Earnings:* ${total_usd:.2f}\n"
        f"💴 *BDT:* {total_bdt:,.0f} (@ {rate})\n"
        f"💳 *IP Due:* {current_due:,.0f} BDT\n"
        f"🆔 *ID:* `{uid}`"
    )
    
    # Create buttons for this user
    keyboard = [
        # Row 1: Earnings buttons
        [
            InlineKeyboardButton("💵 Add $", callback_data=f"add_earn_{uid}"),
            InlineKeyboardButton("💱 Rate", callback_data=f"set_rate_{uid}"),
            InlineKeyboardButton("💸 Pay", callback_data=f"payment_{uid}"),
            InlineKeyboardButton("💳 Due", callback_data=f"ip_due_{uid}")
        ],
        # Row 2: Subscription management
        [
            InlineKeyboardButton("⏰ Extend", callback_data=f"sub_extend_{uid}"),
            InlineKeyboardButton("⏬ Reduce", callback_data=f"sub_reduce_{uid}"),
            InlineKeyboardButton("🗑️ Remove", callback_data=f"remove_{uid}")
        ]
    ]
    
    await query.edit_message_text(
        user_message,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def broadcast(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) != str(ADMIN_ID):
        await update.message.reply_text("❌ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    message = " ".join(context.args)
    count = 0
    for user_id in users:
        try:
            await context.bot.send_message(chat_id=user_id, text=f"📢 Admin Announcement:\n\n{message}")
            count += 1
        except Exception as e:
            logger.warning(f"Failed to send broadcast to {user_id}: {e}")
    await update.message.reply_text(f"✅ Broadcast sent to {count} users.")

# ========== ADMIN PROXY MANAGEMENT =============
async def addproxy(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) != str(ADMIN_ID):
        await update.message.reply_text("❌ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /addproxy <proxy>")
        return
    proxy = " ".join(context.args).strip()
    if not proxy:
        await update.message.reply_text("❌ Proxy cannot be empty.")
        return
    try:
        with open("ip.txt", "a", encoding="utf-8") as f:
            f.write(f"{proxy}\n")
        await update.message.reply_text(f"✅ Proxy added: {proxy}")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to add proxy: {e}")

async def removeproxy(update: Update, context: CallbackContext) -> None:
    if str(update.effective_user.id) != str(ADMIN_ID):
        await update.message.reply_text("❌ Not authorized.")
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /removeproxy <panel_number>")
        return
    panel_number = int(context.args[0])
    try:
        with open("ip.txt", "r", encoding="utf-8") as f:
            lines = [line for line in f if line.strip()]
        if not (1 <= panel_number <= len(lines)):
            await update.message.reply_text(f"❌ Invalid panel number. There are {len(lines)} proxies.")
            return
        removed_proxy = lines.pop(panel_number - 1).strip()
        with open("ip.txt", "w", encoding="utf-8") as f:
            f.writelines(lines)
        await update.message.reply_text(f"✅ Removed Panel Ip {panel_number}: {removed_proxy}")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to remove proxy: {e}")

# ========== ADMIN ANALYTICS =============
async def safe_edit_message(query_or_msg, text, reply_markup=None, parse_mode="Markdown"):
    try:
        # Try to edit directly
        if hasattr(query_or_msg, 'edit_message_text'):
            return await query_or_msg.edit_message_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        elif hasattr(query_or_msg, 'edit_text'):
            return await query_or_msg.edit_text(text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
             # Should not happen with Query or Message, but good safety
             raise AttributeError(f"Object {type(query_or_msg)} has no edit method")
    except Exception as e:
        # If editing fails (e.g. photo, no text, or deleted), delete and send new
        err_str = str(e)
        if any(x in err_str for x in ["There is no text", "Message is not modified", "Message to edit not found"]):
            # Get the message object to delete
            msg = query_or_msg.message if hasattr(query_or_msg, 'message') else query_or_msg
            chat_id = msg.chat_id
            
            try:
                await msg.delete()
            except:
                pass
            
            # Send new message using the bot instance from the message
            return await msg.get_bot().send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            raise e

async def admin_analytics(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    if query:
        await query.answer()
        user_id = str(query.from_user.id)
    else:
        user_id = str(update.effective_user.id)

    if not is_admin(user_id):
        if query:
            await safe_edit_message(query, "❌ Not authorized.")
        else:
            await update.message.reply_text("❌ Not authorized.")
        return

    total_users = len(users)
    active_subs = 0
    expired_subs = 0
    pending_subs = 0
    total_proxies = len(load_socks5_ips_from_file())

    total_earnings_usd = 0.0
    total_earnings_bdt = 0.0
    total_ip_due = 0.0

    for u in users.values():
        # Subscription stats
        status = u.get("status")
        if status == "approved":
            try:
                expiry = datetime.strptime(u.get("expiry_date"), "%Y-%m-%d")
                if datetime.now() <= expiry:
                    active_subs += 1
                else:
                    expired_subs += 1
            except:
                expired_subs += 1
        elif status == "pending":
            pending_subs += 1
        else:
            expired_subs += 1
            
        # Financial stats
        earnings = u.get("earnings", {})
        usd = earnings.get("total_usd", 0.0)
        rate = earnings.get("rate", 120)
        
        total_earnings_usd += usd
        total_earnings_bdt += (usd * rate)
        
        ip_due = u.get("ip_due", {}).get("current_due", 0.0)
        total_ip_due += ip_due

    net_payable = total_earnings_bdt - total_ip_due

    analytics_text = (
        "📈 *Admin Analytics*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 *Total Users:* `{total_users}`\n"
        f"✅ *Active Subscriptions:* `{active_subs}`\n"
        f"⚠️ *Expired/Inactive:* `{expired_subs}`\n"
        f"⏳ *Pending Requests:* `{pending_subs}`\n"
        f"🌐 *Total Proxies:* `{total_proxies}`\n\n"
        "💰 *Financial Overview*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"💵 *Total Earnings:* `${total_earnings_usd:,.2f}`\n"
        f"💴 *Total BDT:* `{total_earnings_bdt:,.0f} BDT`\n"
        f"💳 *Total IP Due:* `{total_ip_due:,.0f} BDT`\n"
        f"📉 *Net Payable:* `{net_payable:,.0f} BDT`\n\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        try:
            await query.edit_message_text(analytics_text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as e:
            if "no text in the message" in str(e).lower():
                await query.message.reply_text(analytics_text, parse_mode="Markdown", reply_markup=reply_markup)
            elif "Message is not modified" in str(e):
                pass  # Ignore if content is identical
            else:
                raise
    else:
        await update.message.reply_text(analytics_text, parse_mode="Markdown", reply_markup=reply_markup)

# ========== USER USAGE ANALYTICS =============
async def user_analytics(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    if not is_admin(user_id):
        await safe_edit_message(query, "❌ Not authorized.")
        return

    # Collect usage stats
    analytics_text = "📊 *User Usage Analytics*\n━━━━━━━━━━━━━━━━━━\n\n"
    
    total_requests = 0
    active_users = []
    
    for uid, data in users.items():
        requests = data.get("proxy_requests", [])
        if requests:
            total_requests += len(requests)
            last_request = requests[-1]["timestamp"] if requests else "Never"
            active_users.append({
                "username": data.get("username", "Unknown"),
                "count": len(requests),
                "last": last_request
            })
    
    # Sort by request count
    active_users.sort(key=lambda x: x["count"], reverse=True)
    
    analytics_text += f"📈 *Total Proxy Requests:* `{total_requests}`\n"
    analytics_text += f"👥 *Active Users:* `{len(active_users)}`\n\n"
    analytics_text += "🔥 *Top 5 Users:*\n"
    
    for i, user in enumerate(active_users[:5], 1):
        safe_username = user['username'].replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
        analytics_text += f"{i}. @{safe_username} - `{user['count']}` requests\n"
        analytics_text += f"   _Last: {user['last']}_\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(query, analytics_text, reply_markup=reply_markup)

# ========== TRAINING BUTTON =============
async def show_training(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass  # Ignore if query is too old
    
    training_link = "https://drive.google.com/drive/folders/1RG_C7VNh6a8WPOH-ojtnm_b-h3CVmg3Y"
    
    message = (
        "📚 *Training Materials*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Access our complete training resources:\n\n"
        f"`{training_link}`\n\n"
        "👆 _Tap to copy the link above_\n\n"
        "Or use the button below:"
    )
    
    keyboard = [
        [InlineKeyboardButton("📂 Open Training Folder", url=training_link)],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(query, message, reply_markup=reply_markup)

# ========== ADMIN PROXY CHECKER =============
async def admin_check_proxies(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    if not is_admin(user_id):
        await safe_edit_message(query, "❌ Not authorized.")
        return

    # Show checking message
    await safe_edit_message(query, "🔍 *Checking all proxies...*\n\n_This may take 15-20 seconds..._")
    
    # Load and check all proxies
    socks5_ips = load_socks5_ips_from_file()
    results = []
    
    # Clear old cache
    proxy_status_cache.clear()
    
    for panel_name, proxy_string in socks5_ips.items():
        try:
            # Use ProxyChecker.org API (works on PythonAnywhere)
            is_online, status_msg = check_socks5_proxy_via_api(proxy_string)
            
            # Cache the result
            proxy_status_cache[panel_name] = {
                "status": "online" if is_online else "offline",
                "error": status_msg,
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if is_online:
                status = "✅ Online"
                results.append(f"{status} | {panel_name}\n   {status_msg}")
            else:
                status = "🔴 Offline"
                results.append(f"{status} | {panel_name}\n   _{status_msg}_")
                
        except Exception as e:
            proxy_status_cache[panel_name] = {
                "status": "error",
                "error": str(e)[:30],
                "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(f"❌ Error | {panel_name}\n   _{str(e)[:30]}_")
    
    # Save cache to file
    save_proxy_status()
    
    # Build results message
    online_count = sum(1 for r in results if "✅" in r)
    offline_count = sum(1 for r in results if "🔴" in r)
    
    message = (
        "🔍 *Proxy Status Report*\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ *Online:* `{online_count}`\n"
        f"🔴 *Offline:* `{offline_count}`\n"
        f"📊 *Total:* `{len(socks5_ips)}`\n\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
    )
    
    message += "\n\n".join(results)
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(query, message, reply_markup=reply_markup)

# ========== BACKGROUND PROXY CHECKER =============
async def background_check_proxies(context: CallbackContext):
    """Background job to check all proxies every hour"""
    try:
        logger.info("=" * 60)
        logger.info("🔍 PROXY CHECK STARTED")
        logger.info("=" * 60)
        
        socks5_ips = load_socks5_ips_from_file()
        online_count = 0
        total_count = len(socks5_ips)
        
        # Clear old cache
        proxy_status_cache.clear()
        
        for panel_name, proxy_string in socks5_ips.items():
            try:
                # Extract host for logging
                host = proxy_string.split(':')[0]
                
                # Log which proxy is being tested
                logger.info(f"⏳ Testing {panel_name}: {host}:****")
                
                # Check the proxy using API
                is_online, status_msg = check_socks5_proxy_via_api(proxy_string)
                
                # Cache the result
                proxy_status_cache[panel_name] = {
                    "status": "online" if is_online else "offline",
                    "error": status_msg,
                    "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                if is_online:
                    online_count += 1
                    logger.info(f"✅ {panel_name}: ONLINE - {status_msg}")
                else:
                    logger.info(f"❌ {panel_name}: OFFLINE - {status_msg}")
                    
            except Exception as e:
                logger.error(f"⚠️  {panel_name}: ERROR - {str(e)[:50]}")
                proxy_status_cache[panel_name] = {
                    "status": "error",
                    "error": str(e)[:30],
                    "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
        
        # Save cache to file
        save_proxy_status()
        
        logger.info("=" * 60)
        logger.info(f"✓ Proxy check complete. Online: {online_count}/{total_count}")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"CRITICAL ERROR in background_check_proxies: {e}", exc_info=True)
        print(f"CRITICAL ERROR: {e}")


# ========== EARNINGS SYSTEM HANDLERS =============
async def add_earnings_start(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    user_id = query.data.split("_")[2]
    context.user_data['earning_user_id'] = user_id
    context.user_data['awaiting_earning_id'] = True
    context.user_data['card_message_id'] = query.message.message_id
    
    username = users[user_id].get("username", "Unknown")
    
    prompt_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"👇 *Add Earnings for @{username}*\n\nStep 1/2: Enter the ID/NAME for this earning:\n_(e.g., 'Project ABC', 'Task #123')_",
        parse_mode="Markdown"
    )
    context.user_data['prompt_message_id'] = prompt_msg.message_id

async def set_rate_start(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    user_id = query.data.split("_")[2]
    context.user_data['rate_user_id'] = user_id
    context.user_data['awaiting_rate'] = True
    context.user_data['card_message_id'] = query.message.message_id
    
    username = users[user_id].get("username", "Unknown")
    current_rate = users[user_id].get("earnings", {}).get("rate", 120)
    
    prompt_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"👇 *Set Rate for @{username}*\nCurrent: {current_rate} BDT\n\nEnter new rate (BDT per $1):",
        parse_mode="Markdown"
    )
    context.user_data['prompt_message_id'] = prompt_msg.message_id

async def process_payment(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    user_id = query.data.split("_")[1]
    
    if user_id not in users:
        await query.edit_message_text("❌ User not found.")
        return
    
    username = users[user_id].get("username", "Unknown")
    earnings = users[user_id].get("earnings", {})
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    
    if total_usd <= 0:
        await query.edit_message_text(
            f"⚠️ @{escape_markdown(username)} has no earnings to pay.\n\n"
            f"Current balance: $0.00"
        )
        return
    
    # Check IP Due
    ip_due_data = users[user_id].get("ip_due", {"current_due": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    
    if current_due > 0:
        # Ask for deduction
        keyboard = [
            [InlineKeyboardButton(f"🔴 Full Deduct (-{current_due:,.0f})", callback_data=f"pay_deduct_full_{user_id}")],
            [InlineKeyboardButton("🟡 Partial Deduct", callback_data=f"pay_deduct_partial_{user_id}")],
            [InlineKeyboardButton("🟢 No Deduct", callback_data=f"pay_deduct_none_{user_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="close_msg")]
        ]
        
        await query.edit_message_text(
            f"💸 *PAYMENT - IP DUE CHECK*\n"
            f"User: @{escape_markdown(username)}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 Earnings: {total_bdt:,.0f} BDT\n"
            f"💳 IP Due: {current_due:,.0f} BDT\n\n"
            f"Select deduction option:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        # No due, proceed normally
        keyboard = [
            [InlineKeyboardButton("✅ Confirm Payment", callback_data=f"confirm_pay_{user_id}_0")],
            [InlineKeyboardButton("❌ Cancel", callback_data="close_msg")]
        ]
        
        await query.edit_message_text(
            f"💸 *PAYMENT CONFIRMATION*\n\n"
            f"User: @{escape_markdown(username)}\n"
            f"Amount: ${total_usd:.2f}\n"
            f"BDT: {total_bdt:,.0f} (@ {rate})\n\n"
            f"Confirm payment?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def payment_deduction_step(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    action = query.data
    user_id = action.split("_")[-1]
    
    if "pay_deduct_full" in action:
        # Proceed with full deduction
        ip_due = users[user_id].get("ip_due", {}).get("current_due", 0)
        await show_final_payment_confirm(query, user_id, ip_due)
        
    elif "pay_deduct_none" in action:
        # Proceed with 0 deduction
        await show_final_payment_confirm(query, user_id, 0)
        
    elif "pay_deduct_partial" in action:
        # Ask for amount
        context.user_data['payment_user_id'] = user_id
        context.user_data['awaiting_deduct_amount'] = True
        
        await query.edit_message_text(
            f"💳 *Partial Deduction*\n\n"
            f"Enter amount to deduct from IP Due (BDT):",
            parse_mode="Markdown"
        )

async def extend_user(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return

    user_id = query.data.split("_")[2] # Format: sub_extend_ID
    context.user_data['admin_action'] = 'extend'
    context.user_data['target_user_id'] = user_id
    context.user_data['card_message_id'] = query.message.message_id
    
    username = users[user_id].get("username", "Unknown").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    
    prompt_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"👇 *Enter days to extend for @{username}:*",
        parse_mode="Markdown"
    )
    context.user_data['prompt_message_id'] = prompt_msg.message_id

async def reduce_user(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return

    user_id = query.data.split("_")[2] # Format: sub_reduce_ID
    context.user_data['admin_action'] = 'reduce'
    context.user_data['target_user_id'] = user_id
    context.user_data['card_message_id'] = query.message.message_id
    
    username = users[user_id].get("username", "Unknown").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    
    prompt_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"👇 *Enter days to reduce for @{username}:*",
        parse_mode="Markdown"
    )
    context.user_data['prompt_message_id'] = prompt_msg.message_id

async def show_final_payment_confirm(query, user_id, deduct_amount):
    username = users[user_id].get("username", "Unknown")
    earnings = users[user_id].get("earnings", {})
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    
    final_amount = total_bdt - deduct_amount
    
    keyboard = [
        [InlineKeyboardButton("✅ Confirm Payment", callback_data=f"confirm_pay_{user_id}_{int(deduct_amount)}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="close_msg")]
    ]
    
    await query.edit_message_text(
        f"💸 *FINAL PAYMENT CONFIRMATION*\n"
        f"User: @{escape_markdown(username)}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Earnings: {total_bdt:,.0f} BDT\n"
        f"💳 Deduction: -{deduct_amount:,.0f} BDT\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💵 *NET PAY: {final_amount:,.0f} BDT*\n\n"
        f"Confirm processing?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def confirm_payment(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    parts = query.data.split("_")
    user_id = parts[2]
    deduct_amount = float(parts[3]) if len(parts) > 3 else 0.0
    
    username = users[user_id].get("username", "Unknown")
    earnings = users[user_id].get("earnings", {})
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    final_pay = total_bdt - deduct_amount
    
    # Update IP Due
    if deduct_amount > 0:
        if "ip_due" not in users[user_id]:
             users[user_id]["ip_due"] = {"current_due": 1400.0, "due_rate": 1400.0}
        
        users[user_id]["ip_due"]["current_due"] = max(0, users[user_id]["ip_due"]["current_due"] - deduct_amount)
        
        # Log due history
        if "history" not in users[user_id]["ip_due"]:
            users[user_id]["ip_due"]["history"] = []
            
        users[user_id]["ip_due"]["history"].append({
            "action": "payment_deduct",
            "amount": deduct_amount,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S")
        })

    # Add to payment history
    payment_record = {
        "amount_usd": total_usd,
        "amount_bdt": total_bdt,
        "deducted": deduct_amount,
        "final_pay": final_pay,
        "rate": rate,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S")
    }
    
    if "payments" not in earnings:
        earnings["payments"] = []
    
    earnings["payments"].append(payment_record)
    earnings["total_usd"] = 0.0  # Reset total
    earnings["history"] = []  # Clear earning history after payment
    
    save_users(users)
    
    # Notify user
    await context.bot.send_message(
        user_id,
        f"💸 *PAYMENT PROCESSED!*\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Earnings:* {total_bdt:,.0f} BDT\n"
        f"💳 *IP Due Deducted:* -{deduct_amount:,.0f} BDT\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"✅ *FINAL PAY: {final_pay:,.0f} BDT*\n\n"
        f"📅 Date: {payment_record['date']}\n"
        f"Thank you for your work!",
        parse_mode="Markdown"
    )
    
    await query.edit_message_text(
        f"✅ *Payment Processed!*\n\n"
        f"User: @{username}\n"
        f"Deducted: {deduct_amount:,.0f} BDT\n"
        f"Paid: {final_pay:,.0f} BDT\n\n"
        f"User notified.",
        parse_mode="Markdown"
    )

# ========== IP DUE SYSTEM HANDLERS =============

async def manage_ip_due(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    user_id = query.data.split("_")[2]
    username = users[user_id].get("username", "Unknown").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    
    ip_due_data = users[user_id].get("ip_due", {"current_due": 1400.0, "due_rate": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    due_rate = ip_due_data.get("due_rate", 1400.0)
    
    keyboard = [
        [InlineKeyboardButton("📝 Set New Due", callback_data=f"set_due_{user_id}"),
         InlineKeyboardButton("➕ Add Due", callback_data=f"add_due_{user_id}")],
        [InlineKeyboardButton("➖ Reduce Due", callback_data=f"reduce_due_{user_id}"),
         InlineKeyboardButton("⚙️ Change Rate", callback_data=f"rate_due_{user_id}")],
        [InlineKeyboardButton("🔙 Back to User", callback_data=f"user_info_{user_id}")]
    ]
    
    try:
        await query.edit_message_text(
            f"💳 *IP DUE MANAGEMENT*\n"
            f"User: @{username}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"💰 *Current Due:* {current_due:,.0f} BDT\n"
            f"🔄 *Due Rate:* {due_rate:,.0f} BDT/renewal\n\n"
            f"Select an action:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        if "Message is not modified" in str(e):
            pass  # Ignore if content is identical
        else:
            raise

async def ip_due_action_start(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("❌ Not authorized.")
        return
    
    action, _, user_id = query.data.partition('_due_')
    context.user_data['ip_due_user_id'] = user_id
    context.user_data['ip_due_action'] = action # set, add, reduce, rate
    context.user_data['awaiting_ip_due_input'] = True
    context.user_data['card_message_id'] = query.message.message_id
    
    username = users[user_id].get("username", "Unknown").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")
    
    prompts = {
        "set": "Enter the NEW total IP Due amount (BDT):",
        "add": "Enter amount to ADD to IP Due (BDT):",
        "reduce": "Enter amount to REDUCE from IP Due (BDT):",
        "rate": "Enter NEW IP Due Rate per renewal (BDT):"
    }
    
    prompt_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f"👇 *IP Due - {action.title()}*\n@{username}\n\n{prompts.get(action, 'Enter amount:')}",
        parse_mode="Markdown"
    )
    context.user_data['prompt_message_id'] = prompt_msg.message_id
async def view_my_ip_due(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    if user_id not in users:
        await query.edit_message_text("❌ Not registered.")
        return
    username = users[user_id].get("username", "Unknown")
    
    ip_due_data = users[user_id].get("ip_due", {"current_due": 1400.0, "due_rate": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    
    message = (
        f"💳 *YOUR IP DUE*\n"
        f"User: @{escape_markdown(username)}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 *Current Due:* {current_due:,.0f} BDT\n\n"
        f"⚠️ Please clear your due to ensure uninterrupted service."
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    await safe_edit_message(query, message, reply_markup=InlineKeyboardMarkup(keyboard))

async def view_my_earnings(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    user_id = str(query.from_user.id)
    
    if user_id not in users:
        await query.edit_message_text("❌ Not registered.")
        return
    
    earnings = users[user_id].get("earnings", {})
    total_usd = earnings.get("total_usd", 0.0)
    rate = earnings.get("rate", 120)
    total_bdt = total_usd * rate
    
    ip_due_data = users[user_id].get("ip_due", {"current_due": 1400.0})
    current_due = ip_due_data.get("current_due", 1400.0)
    
    message = "💰 *MY EARNINGS*\n━━━━━━━━━━━━━━━━━━\n\n"
    message += f"💵 *Total:* ${total_usd:.2f}\n"
    message += f"💴 *BDT:* {total_bdt:,.0f} (@ {rate})\n"
    message += f"💳 *IP Due:* {current_due:,.0f} BDT\n\n"
    
    history = earnings.get("history", [])
    payments = earnings.get("payments", [])
    
    if history:
        message += "📋 *EARNING HISTORY:*\n━━━━━━━━━━━━━━━━━━\n\n"
        for i, item in enumerate(reversed(history[-10:]), 1):
            message += f"{i}. {item['id_name']}\n"
            amount_bdt = item['amount_usd'] * rate
            message += f"   💵 ${item['amount_usd']:.2f} | 💴 {amount_bdt:,.0f} BDT\n"
            message += f"   📅 {item['date']} {item['time']}\n\n"
    
    if payments:
        message += "💸 *PAYMENT HISTORY:*\n━━━━━━━━━━━━━━━━━━\n\n"
        for payment in reversed(payments[-5:]):
            message += f"▸ ${payment['amount_usd']:.2f} ({payment['amount_bdt']:,.0f} BDT) - {payment['date']}\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_menu")]]
    
    await safe_edit_message(query, message, reply_markup=InlineKeyboardMarkup(keyboard))

async def close_message(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except:
        pass

# Flask app setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_key_replace_in_production')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Premium SOCKS5 Service</title>
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #1a1a2e, #16213e);
                color: #e6e6e6;
                margin: 0;
                padding: 0;
                display: flex;
                justify-content: center;
                align-items: center;
                min-height: 100vh;
            }
            .container {
                background-color: rgba(30, 41, 59, 0.8);
                border-radius: 10px;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                padding: 30px;
                text-align: center;
                max-width: 600px;
                width: 90%;
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255, 255, 255, 0.1);
            }
            h1 {
                color: #4cc9f0;
                margin-bottom: 20px;
                font-size: 2.5rem;
            }
            p {
                margin-bottom: 20px;
                line-height: 1.6;
                font-size: 1.1rem;
            }
            .status {
                background-color: #2d3748;
                padding: 15px;
                border-radius: 8px;
                margin-top: 20px;
                border-left: 4px solid #4cc9f0;
            }
            .btn {
                display: inline-block;
                background-color: #4361ee;
                color: white;
                padding: 12px 24px;
                border-radius: 30px;
                text-decoration: none;
                font-weight: bold;
                margin-top: 20px;
                transition: all 0.3s ease;
                border: none;
                cursor: pointer;
            }
            .btn:hover {
                background-color: #3a0ca3;
                transform: translateY(-2px);
                box-shadow: 0 10px 20px rgba(0, 0, 0, 0.2);
            }
            .icon {
                font-size: 3rem;
                margin-bottom: 20px;
                color: #4cc9f0;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="icon">🌐</div>
            <h1>Premium SOCKS5 Service</h1>
            <p>Welcome to our premium SOCKS5 proxy service. Access high-speed, secure connections from multiple locations across the United States.</p>
            <p>To get started, please contact our Telegram bot for registration and access to proxy credentials.</p>
            <div class="status">
                <p>Server Status: <strong style="color: #4ade80;">Online</strong></p>
                <p>Available Locations: <strong>8</strong> (California, Texas, New York, Florida, Georgia)</p>
            </div>
            <a href="https://t.me/your_bot_username" class="btn">Connect via Telegram</a>
        </div>
    </body>
    </html>
    """

def run_flask():
    logging.info("Starting Waitress WSGI server on port 5051")
    serve(app, host='0.0.0.0', port=5051, threads=20, connection_limit=200)

async def request_renewal(update: Update, context: CallbackContext) -> None:
    query = update.callback_query if hasattr(update, 'callback_query') else None
    user_id = str(query.from_user.id) if query else str(update.effective_user.id)
    username = query.from_user.username if query else update.effective_user.username

    if user_id not in users:
        message = "❌ You are not registered. Please register first."
        if query:
            await query.edit_message_text(message)
        else:
            await update.message.reply_text(message)
        return

    users[user_id]["status"] = "renewal_requested"
    save_users(users)

    message = "🔄 Your renewal request has been submitted. Please wait for admin approval."
    if query:
        try:
            await query.edit_message_text(message)
        except Exception as e:
            if "no text in the message" in str(e).lower():
                await query.message.reply_text(message)
            else:
                raise
    else:
        await update.message.reply_text(message)

    keyboard = [
        [InlineKeyboardButton("✅ Approve Renewal", callback_data=f"approve_renewal_{user_id}"),
         InlineKeyboardButton("❌ Decline Renewal", callback_data=f"decline_renewal_{user_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
        ADMIN_ID,
        f"🔄 *Renewal Request*\n\nUser: @{escape_markdown(username)}\nID: {user_id}\nPrevious Expiry: {users[user_id].get('expiry_date', 'N/A')}",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

async def show_dashboard(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except:
        pass
    
    user_id = str(query.from_user.id)
    username = query.from_user.username or f"User_{user_id[:8]}"
    
    if user_id not in users:
        await safe_edit_message(
            query,
            "❌ *You are not registered!*\n\nPlease use /start to register.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='back_to_menu')]])
        )
        return
    
    user_data = users[user_id]
    status = user_data.get("status", "pending")
    expiry = user_data.get("expiry_date", "N/A")
    
    # Build dashboard message
    dashboard_text = "📊 *MY DASHBOARD*\n"
    dashboard_text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # User Info
    dashboard_text += f"👤 *Username:* @{escape_markdown(username)}\n"
    dashboard_text += f"🆔 *User ID:* `{user_id}`\n\n"
    
    # Status with emoji
    if status == "approved":
        status_emoji = "✅"
        status_text = "ACTIVE"
    elif status == "pending":
        status_emoji = "⏳"
        status_text = "PENDING APPROVAL"
    elif status == "renewal_requested":
        status_emoji = "🔄"
        status_text = "RENEWAL REQUESTED"
    else:
        status_emoji = "❌"
        status_text = "EXPIRED"
    
    
    dashboard_text += f"📊 *Status:* {status_emoji} {status_text}\n"
    
    # Subscription Info
    if status == "approved" and expiry != "N/A":
        try:
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            current_date = datetime.now()
            days_remaining = (expiry_date - current_date).days
            
            dashboard_text += f"📅 *Expires:* {expiry}\n"
            dashboard_text += f"⏰ *Days Left:* {max(0, days_remaining)} days\n\n"
            
            # Progress bar (30 days total)
            total_days = 30
            days_used = total_days - days_remaining
            progress_percent = min(100, max(0, (days_used / total_days) * 100))
            
            filled = int(progress_percent / 10)
            empty = 10 - filled
            progress_bar = "█" * filled + "░" * empty
            
            dashboard_text += f"📈 *Subscription Progress:*\n"
            dashboard_text += f"[{progress_bar}] {int(progress_percent)}%\n\n"
            
            if days_remaining <= 3:
                dashboard_text += "⚠️ *Warning:* Subscription expiring soon!\n\n"
        except:
            dashboard_text += f"📅 *Expires:* {expiry}\n\n"
    else:
        dashboard_text += f"� *Expires:* {expiry}\n\n"
    
    # Favorite proxies count
    favorites = user_data.get("favorites", [])
    dashboard_text += f"⭐ *Favorite Proxies:* {len(favorites)}\n"
    
    dashboard_text += "\n━━━━━━━━━━━━━━━━━━━━━━"
    
    # Buttons
    keyboard = []
    
    if status == "approved":
        try:
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            if datetime.now() > expiry_date:
                keyboard.append([InlineKeyboardButton("�  Request Renewal  ", callback_data='request_renewal')])
            else:
                keyboard.append([InlineKeyboardButton("🌐  Get Proxy IPs  ", callback_data='getip')])
                keyboard.append([InlineKeyboardButton("⭐  Manage Favorites  ", callback_data='manage_favorites')])
        except:
            pass
    
    keyboard.append([InlineKeyboardButton("🔙  Back to Menu  ", callback_data='back_to_menu')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(query, dashboard_text, reply_markup=reply_markup)

async def manage_favorites(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    if user_id not in users or users[user_id]["status"] != "approved":
        await query.edit_message_text("❌ Not approved. Please register first.")
        return

    # Removed artificial delay
    # await safe_edit_message(query, "⭐ *Loading your favorites...*")
    # time.sleep(0.5)

    if "favorites" not in users[user_id] or not users[user_id]["favorites"]:
        message = "⭐ *Your Favorite IPs*\n\n_You don't have any favorite IPs yet. Browse IPs and add them to favorites._"
        keyboard = [
            [InlineKeyboardButton("🌐 Browse IPs", callback_data='getip')],
            [InlineKeyboardButton("🔙 Back to Dashboard", callback_data='dashboard')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message(query, message, reply_markup=reply_markup)
        return

    message = "⭐ *Your Favorite IPs*\n\n"
    keyboard = []

    for state in users[user_id]["favorites"]:
        if state in load_socks5_ips_from_file(): # Use the new function here
            keyboard.append([InlineKeyboardButton(f"🌐 {state}", callback_data=f"state_{state}")])

    keyboard.append([InlineKeyboardButton("🗑 Clear All Favorites", callback_data="clear_favorites")])
    keyboard.append([InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dashboard")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        message + "_Select a favorite IP to use:_",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

async def handle_favorites(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    if user_id not in users or users[user_id]["status"] != "approved":
        await query.edit_message_text("❌ Not approved. Please register first.")
        return

    action, state = query.data.split("_", 1)

    if "favorites" not in users[user_id]:
        users[user_id]["favorites"] = []

    if action == "fav":
        if state not in users[user_id]["favorites"]:
            await query.edit_message_text(
                f"🛜 *SOCKS5 IP for {state}*\n\n⭐ _Adding to favorites..._\n\n`{load_socks5_ips_from_file()[state]}`", # Use the new function here
                parse_mode="Markdown"
            )
            # time.sleep(0.5)

            users[user_id]["favorites"].append(state)
            save_users(users)

            keyboard = [
                [InlineKeyboardButton("⭕ Remove from Favorites", callback_data=f"unfav_{state}")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]
            ]
            await query.edit_message_text(
                f"🛜 *SOCKS5 IP for {state}*\n\n✨ _Added to favorites!_\n\n`{load_socks5_ips_from_file()[state]}`\n\n_Copy the above credentials to configure your SOCKS5 proxy._", # Use the new function here
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    elif action == "unfav":
        if state in users[user_id]["favorites"]:
            await query.edit_message_text(
                f"🛜 *SOCKS5 IP for {state}*\n\n⭕ _Removing from favorites..._\n\n`{load_socks5_ips_from_file()[state]}`", # Use the new function here
                parse_mode="Markdown"
            )
            # time.sleep(0.5)

            users[user_id]["favorites"].remove(state)
            save_users(users)

            keyboard = [
                [InlineKeyboardButton("⭐ Add to Favorites", callback_data=f"fav_{state}")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data='back_to_menu')]
            ]
            await query.edit_message_text(
                f"🛜 *SOCKS5 IP for {state}*\n\n✨ _Removed from favorites!_\n\n`{load_socks5_ips_from_file()[state]}`\n\n_Copy the above credentials to configure your SOCKS5 proxy._", # Use the new function here
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

async def clear_all_favorites(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)

    if user_id not in users or users[user_id]["status"] != "approved":
        await query.edit_message_text("❌ Not approved. Please register first.")
        return

    await query.edit_message_text("⭐ *Clearing your favorites...*", parse_mode="Markdown")
    # time.sleep(0.5)

    users[user_id]["favorites"] = []
    save_users(users)

    message = "⭐ *Your Favorite IPs*\n\n_All favorites have been cleared successfully._"
    keyboard = [
        [InlineKeyboardButton("🌐 Browse IPs", callback_data='getip')],
        [InlineKeyboardButton("🔙 Back to Dashboard", callback_data='dashboard')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, parse_mode="Markdown", reply_markup=reply_markup)

async def back_to_menu(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Error answering callback query: {e}")
    await send_menu(update, context)

async def check_expiring_subscriptions(context: CallbackContext) -> None:
    current_date = datetime.now()
    reminder_sent_count = 0

    for user_id, user_data in users.items():
        if user_data["status"] == "approved":
            try:
                expiry_date = datetime.strptime(user_data["expiry_date"], "%Y-%m-%d")
                days_remaining = (expiry_date - current_date).days

                if days_remaining <= 3 and days_remaining >= 0:
                    last_notification = user_data.get("last_notification")
                    if not last_notification or (current_date - datetime.strptime(last_notification, "%Y-%m-%d")).days >= 1:
                        message = "⚠️ *SUBSCRIPTION EXPIRING SOON* ⚠️\n\n"
                        message += f"Your subscription will expire in *{days_remaining} days*.\n\n"

                        total_days = 30
                        days_passed = (current_date - (expiry_date - timedelta(days=30))).days
                        progress_percent = min(100, max(0, (days_passed / total_days) * 100))
                        filled_blocks = int(progress_percent / 10)
                        empty_blocks = 10 - filled_blocks
                        progress_bar = "█" * filled_blocks + "░" * empty_blocks

                        message += f"Subscription progress: [{progress_bar}] {int(progress_percent)}%\n\n"
                        message += "_Your subscription will expire soon. Please renew after it expires._"

                        reply_markup = None

                        await context.bot.send_message(
                            chat_id=user_id,
                            text="⏳ *Checking your subscription status...*",
                            parse_mode="Markdown"
                        )
                        time.sleep(0.8)

                        await context.bot.send_message(
                            chat_id=user_id,
                            text=message,
                            parse_mode="Markdown",
                            reply_markup=reply_markup
                        )

                        users[user_id]["last_notification"] = current_date.strftime("%Y-%m-%d")
                        save_users(users)
                        reminder_sent_count += 1
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing user {user_id}: {e}")
                continue

    if reminder_sent_count > 0:
        logger.info(f"Sent {reminder_sent_count} subscription expiry reminders")

async def error_callback(update: object, context: CallbackContext) -> None:
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

    if isinstance(update, Update) and update.callback_query:
        try:
            await update.callback_query.answer("An error occurred. Please try again.")
        except Exception:
            pass

        try:
            await update.callback_query.edit_message_text(
                "Sorry, an error occurred. Please use /start to restart the bot."
            )
        except Exception:
            pass
    elif isinstance(update, Update) and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Sorry, an error occurred. Please use /start to restart the bot."
        )

def generate_subscription_progress_bar(days_left: int, total_days: int = 30) -> tuple[str, int]:
    days_passed = total_days - max(0, days_left)
    progress_percent = min(100, max(0, int((days_passed / total_days) * 100)))

    bar = ''
    for i in range(PROGRESS_BAR_LENGTH):
        if i < int(PROGRESS_BAR_LENGTH * progress_percent / 100):
            bar += ANIMATION_FRAMES[1] if i % 2 == 0 else ANIMATION_FRAMES[1]
        else:
            bar += ANIMATION_FRAMES[0]

    return bar, progress_percent

def main() -> None:
    """Run the bot with guaranteed Python 3.13 compatibility."""
    print("\n" + "="*50)
    print("Premium SOCKS5 Telegram Bot Starting...")
    print(f"Python {sys.version_info.major}.{sys.version_info.minor} compatible version")
    print("="*50 + "\n")

    async def post_init(app):
        logging.info("Application initialized")
        print("Bot initialization completed successfully")

    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    try:
        # Build application with our monkey-patched weakref handling
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .post_init(post_init)
            .build()
        )

        # Register handlers in proper order
        handlers = [
            CommandHandler("start", start),
            CommandHandler("register", register),
            CommandHandler("getip", get_ip),
            CommandHandler("remove", remove_user),
            CommandHandler("listusers", list_users),
            CommandHandler("renew", request_renewal),
            CommandHandler("broadcast", broadcast), # admin broadcast
            CommandHandler("addproxy", addproxy),   # admin add proxy
            CommandHandler("removeproxy", removeproxy), # admin remove proxy

            # Admin action handlers
            CallbackQueryHandler(extend_user, pattern='^sub_extend_'),
            CallbackQueryHandler(reduce_user, pattern='^sub_reduce_'),
            CallbackQueryHandler(handle_remove_user_button, pattern='^remove_'),
            CallbackQueryHandler(cancel_admin_action, pattern='^cancel_admin_action$'),
            
            # IP Due Actions
            CallbackQueryHandler(ip_due_action_start, pattern='^(set|add|reduce|rate)_due_'),

            # Panel selection handler (new)
            CallbackQueryHandler(handle_panel_selection, pattern='^panel_\\d+$'),

            # State selection handler (legacy, can be kept for favorites)
            # CallbackQueryHandler(handle_state_selection, pattern='^state_'), # This line is removed

            # Other specific handlers
            CallbackQueryHandler(handle_approval, pattern='^(approve|decline)'),
            CallbackQueryHandler(handle_favorites, pattern='^(fav|unfav)'),
            CallbackQueryHandler(back_to_menu, pattern='^back_to_menu$'),
            CallbackQueryHandler(manage_favorites, pattern='^manage_favorites$'),
            CallbackQueryHandler(clear_all_favorites, pattern='^clear_favorites$'),
            CallbackQueryHandler(request_renewal, pattern='^request_renewal$'),
            CallbackQueryHandler(show_dashboard, pattern='^dashboard$'),
            CallbackQueryHandler(admin_broadcast_button, pattern='^admin_broadcast$'),
            CallbackQueryHandler(admin_analytics, pattern='^admin_analytics$'),
            CallbackQueryHandler(user_analytics, pattern='^user_analytics$'),
            CallbackQueryHandler(show_training, pattern='^training$'),
            CallbackQueryHandler(admin_check_proxies, pattern='^admin_check_proxies$'),
            CallbackQueryHandler(show_user_info, pattern='^user_info_'),
            CallbackQueryHandler(show_user_info, pattern='^userinfo_'), # For list_users compatibility
            CallbackQueryHandler(add_earnings_start, pattern='^add_earn_'),
            CallbackQueryHandler(set_rate_start, pattern='^set_rate_'),
            CallbackQueryHandler(process_payment, pattern='^payment_'),
            CallbackQueryHandler(payment_deduction_step, pattern='^pay_deduct_'),
            CallbackQueryHandler(confirm_payment, pattern='^confirm_pay_'),
            CallbackQueryHandler(view_my_earnings, pattern='^my_earnings$'),
            CallbackQueryHandler(view_my_ip_due, pattern='^my_ip_due$'),
            CallbackQueryHandler(manage_ip_due, pattern='^ip_due_'),
            CallbackQueryHandler(ip_due_action_start, pattern='^.*_due_'),
            CallbackQueryHandler(close_message, pattern='^close_msg$'),
            CallbackQueryHandler(admin_edit_ips, pattern='^admin_edit_ips$'),
            CallbackQueryHandler(admin_del_ip, pattern='^admin_del_ip_\\d+$'),
            CallbackQueryHandler(admin_add_ip, pattern='^admin_add_ip$'),
            CallbackQueryHandler(admin_del_all_ips, pattern='^admin_del_all_ips$'),
            CallbackQueryHandler(admin_confirm_del_all_ips, pattern='^admin_confirm_del_all_ips$'),

            # Generic button handler should be last
            CallbackQueryHandler(button_click),

            # Unified admin text input handler should be last
            MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, handle_admin_text_input),
        ]

        for handler in handlers:
            application.add_handler(handler)

        application.add_error_handler(error_callback)

        # Schedule subscription expiry check (daily at midnight)
        job_queue = application.job_queue
        job_queue.run_daily(check_expiring_subscriptions, time=datetime.strptime("00:00", "%H:%M").time())
        
        # Schedule hourly proxy status check (first check after 5 seconds, then every hour)
        job_queue.run_repeating(background_check_proxies, interval=3600, first=5)
        
        logger.info("Bot started successfully!")
        logger.info("Proxy status check will run in 1 second, then every hour")

        # Start the bot with more verbose logging
        print("Starting bot polling...")
        logging.info("Starting bot polling with verbose logging")
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False,
            stop_signals=None,
            poll_interval=0.5
        )

    except Exception as e:
        print(f"FATAL ERROR: {str(e)}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        time.sleep(10)
    finally:
        logging.info("Bot shutdown complete")
        print("Bot has stopped")

if __name__ == "__main__":
    main()