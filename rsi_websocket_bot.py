import asyncio
import json
import logging
import os
from collections import defaultdict
import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# =========================
# الإعدادات المتغيرات العامة
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAMES = {
    "1m": "1 دقيقة",
    "5m": "5 دقائق",
    "15m": "15 دقيقة",
    "1h": "1 ساعة",
    "4h": "4 ساعات",
    "1d": "يومي",
    "1w": "أسبوعي",
}
RSI_LEVELS = [40, 50, 60]

klines_store = defaultdict(lambda: defaultdict(list))
alert_states = defaultdict(lambda: defaultdict(dict))

user_config = {
    "mode": "SELECTED",
    "watchlist": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
    "active_timeframes": ["1m", "5m", "15m", "1h", "4h", "1d", "1w"],
    "muted": False,
}

logging.basicConfig(level=logging.INFO)

# =========================
# حساب RSI
# =========================
def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    series = pd.Series(closes)
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

# =========================
# معالجة بيانات WebSocket
# =========================
async def process_kline(data, app):
    kline = data.get("k", {})
    is_closed = kline.get("x", False)
    symbol = kline.get("s")
    interval = kline.get("i")
    close_price = float(kline.get("c", 0))

    if not symbol or interval not in user_config["active_timeframes"]:
        return

    if user_config["mode"] == "SELECTED" and symbol not in user_config["watchlist"]:
        return

    closes = klines_store[symbol][interval]
    if is_closed:
        closes.append(close_price)
        if len(closes) > 100:
            closes.pop(0)

    if len(closes) < 16:
        return

    current_rsi = calculate_rsi(closes)
    if current_rsi is None:
        return

    previous_rsi = calculate_rsi(closes[:-1]) if len(closes) > 16 else current_rsi

    for level in RSI_LEVELS:
        last_state = alert_states[symbol][interval].get(level)

        if previous_rsi < level <= current_rsi and last_state != "UP":
            alert_states[symbol][interval][level] = "UP"
            await trigger_alert(app, symbol, interval, current_rsi, previous_rsi, level, "UP")

        elif previous_rsi > level >= current_rsi and last_state != "DOWN":
            alert_states[symbol][interval][level] = "DOWN"
            await trigger_alert(app, symbol, interval, current_rsi, previous_rsi, level, "DOWN")

async def trigger_alert(app, symbol, interval, current_rsi, previous_rsi, level, direction):
    if user_config["muted"] or not TELEGRAM_CHAT_ID:
        return

    timeframe_name = TIMEFRAMES.get(interval, interval)
    if direction == "UP":
        emoji = "🟢"
        action = "اخترق صعوداً ⬆️"
    else:
        emoji = "🔴"
        action = "كسر هبوطاً ⬇️"

    message = (
        f"🚨 **RSI ALERT**\n\n"
        f"💰 **العملة:** `{symbol}`\n"
        f"⏱ **الفريم:** {timeframe_name}\n"
        f"📊 **المؤشر:** RSI(14)\n"
        f"📉 **RSI السابق:** {previous_rsi:.2f}\n"
        f"📈 **RSI الحالي:** {current_rsi:.2f}\n"
        f"🎯 **المستوى:** {level}\n\n"
        f"{emoji} **الحالة:** {action} مستوى {level}"
    )

    try:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Telegram Alert Error: {e}")

# =========================
# اتصال WebSocket
# =========================
async def start_binance_websocket(app):
    import websockets

    streams = []
    symbols = user_config["watchlist"] if user_config["mode"] == "SELECTED" else ["btcusdt", "ethusdt"]

    for s in symbols:
        for tf in user_config["active_timeframes"]:
            streams.append(f"{s.lower()}@kline_{tf}")

    stream_url = f"wss://stream.binance.com:9443/ws/{'/'.join(streams)}"

    while True:
        try:
            async with websockets.connect(stream_url) as ws:
                logging.info("⚡ تم الاتصال بـ Binance WebSockets بنجاح!")
                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    await process_kline(data, app)
        except Exception as e:
            logging.error(f"WS Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# =========================
# أوامر تلجرام التحكمية
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 **مرحباً بك في بوت تنبيهات RSI الاحترافي (WebSocket)**\n\n"
        "الأوامر المتاحة:\n"
        "🔹 `/status` - عرض حالة البوت والإعدادات الحالية\n"
        "🔹 `/coins` - عرض العملات المراقبة\n"
        "🔹 `/add <SYMBOL>` - إضافة عملة (مثال: `/add SOLUSDT`)\n"
        "🔹 `/remove <SYMBOL>` - إزالة عملة\n"
        "🔹 `/all` - التبديل بين مراقبة كل العملات أو القائمة المحددة\n"
        "🔹 `/mute` - كتم/تفعيل التنبيهات\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_msg = (
        f"⚙️ **حالة البوت الحالية:**\n\n"
        f"▪️ **الوضع:** {user_config['mode']}\n"
        f"▪️ **عدد العملات:** {len(user_config['watchlist'])}\n"
        f"▪️ **الفريمات النشطة:** {', '.join(user_config['active_timeframes'])}\n"
        f"▪️ **حالة الصوت:** {'🔕 مكتوم' if user_config['muted'] else '🔔 مفعّل'}\n"
    )
    await update.message.reply_text(status_msg, parse_mode="Markdown")

async def cmd_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    coins_list = "\n".join([f"• `{c}`" for c in user_config["watchlist"]])
    await update.message.reply_text(f"📋 **العملات المراقبة حالياً:**\n\n{coins_list}", parse_mode="Markdown")

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ اكتب رمز العملة، مثال: `/add ADAUSDT`", parse_mode="Markdown")
        return
    coin = context.args[0].upper()
    if coin not in user_config["watchlist"]:
        user_config["watchlist"].append(coin)
        await update.message.reply_text(f"✅ تم إضافة `{coin}` بنجاح.")
    else:
        await update.message.reply_text(f"ℹ️ العملة `{coin}` موجودة بالفعل.")

async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("⚠️ اكتب رمز العملة المراد حذفها.")
        return
    coin = context.args[0].upper()
    if coin in user_config["watchlist"]:
        user_config["watchlist"].remove(coin)
        await update.message.reply_text(f"🗑 تم حذف `{coin}` من القائمة.")
    else:
        await update.message.reply_text("⚠️ العملة غير موجودة في القائمة.")

async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_config["muted"] = not user_config["muted"]
    state = "تم كتم التنبيهات 🔕" if user_config["muted"] else "تم تفعيل التنبيهات 🔔"
    await update.message.reply_text(f"إعدادات الصوت: {state}")

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is missing!")
        return

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("coins", cmd_coins))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("mute", cmd_mute))

    loop = asyncio.get_event_loop()
    loop.create_task(start_binance_websocket(app))

    print("🚀 Bot v2.0 (WebSocket) is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
