#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
import httpx
from aiogram import Bot
from aiohttp import web

# ========== НАСТРОЙКИ (ПОЛНАЯ АВТОМАТИКА) ==========
# Код сам автоматически возьмет имя твоего текущего бота из системы Render!
BOT_NAME_RENDER = os.getenv("RENDER_SERVICE_NAME", "pump-hunter-default")

TELEGRAM_TOKEN = os.getenv("PUMP_BOT_TOKEN")
CHAT_ID = os.getenv("PUMP_CHAT_ID")
PORT = int(os.getenv("PORT", "7861"))

THRESHOLD_VOL = 3.5          # Всплеск объема (раз)
CHECK_INTERVAL = 10          # Пауза между кругами сканирования (сек)
MAX_REQUESTS = 3             # Одновременных запросов к биржам
MIN_DAILY_VOL_USDT = 2_500_000  # Минимальный суточный объем на Binance

ALERT_COOLDOWN = timedelta(minutes=5)   # Повторный алерт по монете не чаще
MAX_SPREAD_PERCENT = 1.5                # Максимальный спред, выше — арбитраж

# Динамический список пар (заполняется автоматически)
WATCH_PAIRS = []

BINGX_API = "https://open-api.bingx.com/api/v1/market/getKline"
BINANCE_API = "https://api1.binance.com/api/v3/klines"
BINANCE_TICKER_API = "https://api1.binance.com/api/v3/ticker/price"
BINANCE_24HR_API = "https://api1.binance.com/api/v3/ticker/24hr"

# Автоматическая сборка URL для самопинга
SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("AutoMassHunter")


class AutoVolumeMonitor:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        
        # Маскировка под обычный браузер Chrome, чтобы избежать ошибки 418
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=10.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        )

    async def filter_by_volume(self, symbols: list, min_vol=MIN_DAILY_VOL_USDT):
        """Оставляет только монеты с суточным объёмом не ниже min_vol (USDT)."""
        filtered = []
        for i in range(0, len(symbols), 50):
            chunk = symbols[i:i+50]
            symbols_str = '","'.join(chunk)
            params = {"symbols": f'["{symbols_str}"]'}
            try:
                res = await self.client.get(BINANCE_24HR_API, params=params)
                if res.status_code == 200:
                    for item in res.json():
                        quote_vol = float(item.get("quoteVolume", 0))
                        if quote_vol >= min_vol:
                            clean = item["symbol"].replace("USDT", "")
                            filtered.append({
                                "bingx": f"{clean}-USDT",
                                "binance": item["symbol"],
                                "name": clean
                            })
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Ошибка фильтрации объема: {e}")
        return filtered

    async def update_market_pairs(self):
        """Автоматически находит ликвидные монеты от 0.01$ до 1$ на Binance."""
        global WATCH_PAIRS
        logger.info("🔄 Поиск монет в диапазоне 0.01$ - 1$ с объемом >5M...")
        try:
            res = await self.client.get(BINANCE_TICKER_API)
            
            if res.status_code == 418:
                logger.warning("⚠️ Binance выдал ошибку 418 (Teapot). Ожидаем сброса лимитов 30 сек...")
                await asyncio.sleep(30)
                res = await self.client.get(BINANCE_TICKER_API)

            if res.status_code != 200:
                logger.error(f"Не удалось получить цены с Binance. Код ответа: {res.status_code}")
                return

            all_tickers = res.json()
            candidates = []

            for ticker in all_tickers:
                symbol = ticker["symbol"]
                if symbol.endswith("USDT") and not any(x in symbol for x in ["UPUSDT", "DOWNUSDT", "BUSD", "EUR"]):
                    price = float(ticker["price"])
                    if 0.01 <= price <= 1.0:
                        candidates.append(symbol)

            new_pairs = await self.filter_by_volume(candidates, MIN_DAILY_VOL_USDT)
            WATCH_PAIRS = new_pairs
            logger.info(f"✅ Список обновлен! Найдено {len(WATCH_PAIRS)} ликвидных монет в диапазоне 0.01$-1$")

            if len(WATCH_PAIRS) > 0:
                await self.send_alert(f"🔄 <b>Сканер обновил рынок!</b>\nМонет в диапазоне (с объемом >5M): <b>{len(WATCH_PAIRS)}</b>")

        except Exception as e:
            logger.error(f"Ошибка при обновлении рынка: {e}")

    async def fetch_bingx(self, symbol: str):
        async with self.semaphore:
            try:
                res = await self.client.get(BINGX_API, params={"symbol": symbol, "interval": "1m", "limit": 15})
                if res.status_code == 200:
                    return res.json().get("data", [])
            except Exception:
                pass
        return []

    async def fetch_binance(self, symbol: str):
        async with self.semaphore:
            try:
                res = await self.client.get(BINANCE_API, params={"symbol": symbol, "interval": "1m", "limit": 15})
                if res.status_code == 200:
                    return res.json()
            except Exception:
                pass
        return []

    async def check_pair(self, pair: dict):
        bx_data, bn_data = await asyncio.gather(
            self.fetch_bingx(pair["bingx"]),
            self.fetch_binance(pair["binance"])
        )

        if not bx_data or not bn_data or len(bx_data) < 12 or len(bn_data) < 12:
            return

        bx_latest = bx_data[-1]
        bx_price = float(bx_latest[4])
        bx_vol = float(bx_latest[5])
        bx_avg = sum(float(k[5]) for k in bx_data[-11:-1]) / 10

        bn_latest = bn_data[-1]
        bn_price = float(bn_latest[4])
        bn_vol = float(bn_latest[5])
        bn_avg = sum(float(k[5]) for k in bn_data[-11:-1]) / 10

        if bn_price > 0:
            spread = abs(bx_price - bn_price) / bn_price * 100
            if spread > MAX_SPREAD_PERCENT:
                return

        now = datetime.now()
        last_time = self.last_alert_time.get(pair["name"])
        if last_time and (now - last_time) < ALERT_COOLDOWN:
            return

        bx_ratio = bx_vol / bx_avg if bx_avg > 0 else 0
        bn_ratio = bn_vol / bn_avg if bn_avg > 0 else 0
        time_str = now.strftime("%H:%M:%S")

        alert_text = None

        if bx_ratio >= THRESHOLD_VOL and bn_ratio >= THRESHOLD_VOL:
            alert_text = f"""
🎯 <b>ТОЧНЫЙ СИГНАЛ [{pair['name']}] (Спот + Фьючерсы)</b>
💰 Цена: <code>{bn_price}$</code>
📊 Всплеск объема:
• BingX: <b>x{bx_ratio:.1f}</b> ({bx_vol:,.0f})
• Binance: <b>x{bn_ratio:.1f}</b> ({bn_vol:,.0f})
🕐 Время: {time_str}
<i>🔥 Кит зашел по обеим биржам в дешевую монету!</i>
"""
        elif bx_ratio >= THRESHOLD_VOL:
            alert_text = f"""
👀 <b>ОПЕРЕЖАЮЩИЙ СИГНАЛ [{pair['name']}] (Фьючерсы BingX)</b>
💰 Цена BingX: <code>{bx_price}$</code>
📊 Всплеск объема:
• BingX: <b>x{bx_ratio:.1f}</b> ({bx_vol:,.0f})
• Binance (спот): x{bn_ratio:.1f} (Норма)
🕐 Время: {time_str}
<i>⚡️ Агрессивный закуп на деривативах BingX. Спот отстает!</i>
"""

        if alert_text:
            self.last_alert_time[pair["name"]] = now
            await self.send_alert(alert_text)

    async def send_alert(self, text: str):
        try:
            await self.bot.send_message(chat_id=CHAT_ID, text=text.strip(), parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")

    async def start_loop(self):
        await self.update_market_pairs()
        logger.info("🚀 Полное сканирование рынка запущено...")
        last_market_update = datetime.now()

        while True:
            if datetime.now() - last_market_update > timedelta(hours=1):
                await self.update_market_pairs()
                last_market_update = datetime.now()

            if WATCH_PAIRS:
                for i in range(0, len(WATCH_PAIRS), MAX_REQUESTS):
                    chunk = WATCH_PAIRS[i:i+MAX_REQUESTS]
                    tasks = [self.check_pair(pair) for pair in chunk]
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(0.3)

            await asyncio.sleep(CHECK_INTERVAL)


# ==========================================
# ВЕБ-СЕРВЕР И АНТИ-СОН СИСТЕМА (АВТОМАТ)
# ==========================================
async def web_health_check(request):
    return web.Response(text="Auto Market Volume Hunter: Operational", status=200)

async def keep_alive_ping():
    """Фоновая задача самопинга, чтобы Render не тушил бота"""
    await asyncio.sleep(30)
    logger.info("Анти-сон система (Самопинг) успешно активирована.")
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(SELF_URL)
                if response.status_code == 200:
                    logger.info("Самопинг: отправил сигнал бодрствования на Render.")
        except Exception as ping_err:
            logger.error(f"Ошибка выполнения самопинга: {ping_err}")
        
        # Обманываем таймер Render и пингуем каждые 4 минуты
        await asyncio.sleep(240)


# ==========================================
# ОБНОВЛЕННЫЙ АСИНХРОННЫЙ ЗАПУСК
# ==========================================
async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.critical("PUMP_BOT_TOKEN или PUMP_CHAT_ID не заданы!")
        sys.exit(1)

    bot = Bot(token=TELEGRAM_TOKEN)
    monitor = AutoVolumeMonitor(bot)

    # 1. Запуск веб-сервера
    app = web.Application()
    app.router.add_get('/', web_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Веб-сервер успешно запущен на порту {PORT}")

    # 2. Запуск фонового сканирования рынка
    asyncio.create_task(monitor.start_loop())

    # 3. Запуск фонового самопинга для вечного онлайна
    asyncio.create_task(keep_alive_ping())

    # Держим основной процесс активным
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
