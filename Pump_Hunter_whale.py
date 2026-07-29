import os
import asyncio
import logging
import aiohttp
import numpy as np
from datetime import datetime
from aiohttp import web

# ===== НАСТРОЙКИ =====
# Укажите токен и Chat ID прямо здесь, если не используете Environment Variables в Render
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "ВАШ_ТОКЕН_ОТ_BOTFATHER")
CHAT_ID = os.getenv("CHAT_ID", "ВАШ_CHAT_ID")
PORT = int(os.getenv("PORT", 10000))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Форматирование цены
def format_price(price):
    if price < 0.0001:
        return f"{price:.8f}"
    elif price < 1.0:
        return f"{price:.6f}"
    else:
        return f"{price:.4f}"

# Прямая отправка сообщений в Telegram через HTTP API
async def send_telegram_msg(session, text):
    # Очищаем токен на случай случайных пробелов
    clean_token = TELEGRAM_TOKEN.strip()
    url = f"https://api.telegram.org/bot{clean_token}/sendMessage"
    payload = {
        "chat_id": CHAT_ID.strip(),
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            data = await resp.json()
            if not data.get("ok"):
                logging.error(f"Ошибка Telegram API: {data}")
            else:
                logging.info("Сообщение успешно отправлено в Telegram!")
    except Exception as e:
        logging.error(f"Ошибка отправки запроса в Telegram: {e}")

# Загрузка свечей с BingX
async def fetch_klines(session, symbol, interval="5m", limit=500):
    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            if data.get("code") == 0 and "data" in data and len(data["data"]) > 0:
                klines = data["data"]
                klines.reverse()  # BingX присылает от новых к старым
                return klines
    except Exception as e:
        logging.error(f"Ошибка получения свечей {symbol}: {e}")
    return []

# ===== 🧪 БЛОК АВТОМАТИЧЕСКОГО БЭКТЕСТА В TELEGRAM =====
async def run_history_backtest(session, test_symbols=["BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT", "UB-USDT"]):
    logging.info("🧪 Запуск Бэктеста по истории...")
    
    await send_telegram_msg(session, "⏳ **Запуск бэктеста... Анализ истории за 40 часов...**")

    report_lines = ["📊 **РЕЗУЛЬТАТЫ БЭКТЕСТА (ИМПУЛЬСЫ С ДНА ЗА 40Ч):**\n"]
    
    for symbol in test_symbols:
        klines = await fetch_klines(session, symbol, "5m", limit=500)
        if not klines or len(klines) < 50:
            continue

        symbol_signals = []
        for i in range(40, len(klines)):
            sub_klines = klines[:i+1]
            closes = [float(k["close"]) for k in sub_klines]
            opens = [float(k["open"]) for k in sub_klines]
            volumes = [float(k["volume"]) * float(k["close"]) for k in sub_klines]
            
            curr_open, curr_close = opens[-1], closes[-1]
            if curr_close <= curr_open:
                continue

            curr_body = curr_close - curr_open
            prev_bodies = np.abs(np.array(closes[-21:-1]) - np.array(opens[-21:-1]))
            avg_body = np.mean(prev_bodies) if len(prev_bodies) > 0 else 1.0

            prev_vols = volumes[-21:-1]
            avg_vol = np.mean(prev_vols) if len(prev_vols) > 0 else 1.0

            # Условие: Рост тела свечи >= x1.8 и Объема >= x1.8
            if curr_body >= (avg_body * 1.8) and volumes[-1] >= (avg_vol * 1.8):
                time_str = datetime.fromtimestamp(int(sub_klines[-1]['time'])/1000).strftime('%d.%m %H:%M')
                symbol_signals.append(f"• `{time_str}` — **${format_price(curr_close)}** (Vol: x{round(volumes[-1]/avg_vol, 1)})")

        if symbol_signals:
            report_lines.append(f"🔹 **{symbol}** (Найдено: {len(symbol_signals)}):")
            report_lines.extend(symbol_signals[-3:])  # Берем последние 3 сигнала
            report_lines.append("")
        else:
            report_lines.append(f"⚪ **{symbol}**: Сигналов за 40ч не найдено\n")

    full_report = "\n".join(report_lines)
    await send_telegram_msg(session, full_report)

# ===== МИКРО-ВЕБСЕРВЕР ДЛЯ RENDER =====
async def handle_ping(request):
    return web.Response(text="Bot is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logging.info(f"🌐 Веб-сервер запущен на порту {PORT}")

# ===== ОСНОВНОЙ ЦИКЛ БОТА =====
async def main():
    logging.info("🚀 Запущен 3-Уровневый Сканер (EARLY_BOTTOM + PRE-BREAKOUT + ROCKET)")

    # Запускаем веб-сервер, чтобы Render подхватил порт
    await start_web_server()

    async with aiohttp.ClientSession() as session:
        # 1. Запуск бэктеста при старте
        await run_history_backtest(session)

        # 2. Переход в режим мониторинга
        logging.info("🟢 Переход в режим отслеживания онлайн-рынка...")
        while True:
            try:
                await asyncio.sleep(60)
            except Exception as e:
                logging.error(f"Ошибка в основном цикле: {e}")
                await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
