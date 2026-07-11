#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
import httpx
from aiogram import Bot
from aiohttp import web

# ========== НАСТРОЙКИ ==========
BOT_NAME_RENDER = os.getenv("RENDER_SERVICE_NAME", "pump-hunter-default")
TELEGRAM_TOKEN = os.getenv("PUMP_BOT_TOKEN")
CHAT_ID = os.getenv("PUMP_CHAT_ID")
PORT = int(os.getenv("PORT", "7861"))

THRESHOLD_VOL = 5.0
CHECK_INTERVAL = 30
MAX_REQUESTS = 3

# РАСШИРЕННЫЕ ФИЛЬТРЫ ДЛЯ МАКСИМАЛЬНОГО ОХВАТА МАРКЕТА БИНАНСА
MIN_DAILY_VOL_USDT = 1_000_000  # Снизили до 1 млн $, чтобы зацепить живые монеты
MIN_PRICE = 0.0001              # Захватываем дешевые мемкоины
MAX_PRICE = 1.0                # Подняли планку до 10$, зайдет вся основная альта

ALERT_COOLDOWN = timedelta(minutes=30)

# Список отслеживаемых пар (заполняется динамически)
WATCH_PAIRS = []

# Binance API
BINANCE_API = "https://api1.binance.com/api/v3/klines"
BINANCE_TICKER_API = "https://api1.binance.com/api/v3/ticker/price"
BINANCE_24HR_API = "https://api1.binance.com/api/v3/ticker/24hr"

SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("BinanceMassHunter")


class AutoVolumeMonitor:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        
        # Имитируем реальный чистый браузер, чтобы обходить блок 418
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Content-Type": "application/json"
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=20.0,
            limits=httpx.Limits(max_connections=15, max_keepalive_connections=5)
        )

    async def filter_by_volume(self, symbols: list, min_vol=MIN_DAILY_VOL_USDT):
        """Оставляет только монеты с объёмом ≥ min_vol на Binance."""
        filtered = []
        # Бьем на пачки по 50 штук — это абсолютно законно для API Binance
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
                                "binance": item["symbol"],
                                "name": clean
                            })
                # Мягкая задержка между батчами, чтобы биржа спала спокойно
                await asyncio.sleep(1.2)
            except Exception as e:
                logger.error(f"Ошибка фильтрации по объему: {e}")
                await asyncio.sleep(2.0)
        return filtered

    async def update_market_pairs(self):
        global WATCH_PAIRS
        logger.info("🔄 Поиск активных монет на Binance...")
        try:
            await asyncio.sleep(2.0)
            
            res = await self.client.get(BINANCE_TICKER_API)
            if res.status_code != 200:
                logger.error(f"⚠️ Binance ticker error: {res.status_code}. Пробуем еще раз через минуту.")
                return
                
            all_tickers = res.json()
            candidates = [
                t["symbol"] for t in all_tickers
                if t["symbol"].endswith("USDT")
                and MIN_PRICE <= float(t["price"]) <= MAX_PRICE
                and not any(x in t["symbol"] for x in ["UP", "DOWN", "BUSD", "EUR"])
            ]
            
            await asyncio.sleep(1.0)
            WATCH_PAIRS = await self.filter_by_volume(candidates, MIN_DAILY_VOL_USDT)
            logger.info(f"✅ Список обновлен! Найдено {len(WATCH_PAIRS)} монет на Binance.")
            
            await self.send_alert(f"🔄 <b>Сканер рынка Binance запущен!</b>\nМониторинг монет в диапазоне: <b>{len(WATCH_PAIRS)}</b>")
        except Exception as e:
            logger.error(f"Ошибка обновления пар: {e}")

    async def fetch_binance(self, symbol: str):
        async with self.semaphore:
            try:
                res = await self.client.get(BINANCE_API, params={
                    "symbol": symbol,
                    "interval": "1m",
                    "limit": 15
                })
                if res.status_code == 200:
                    return res.json()
            except Exception:
                pass
        return []

    async def check_pair(self, pair: dict):
        bn_data = await self.fetch_binance(pair["binance"])
        
        if not bn_data or len(bn_data) < 12:
            return

        try:
            bn_latest = bn_data[-1]
            bn_avg = sum(float(k[5]) for k in bn_data[-11:-1]) / 10
            bn_ratio = float(bn_latest[5]) / bn_avg if bn_avg > 0 else 0

            if bn_ratio >= THRESHOLD_VOL:
                now = datetime.now()
                if self.last_alert_time.get(pair["name"]) and \
                   (now - self.last_alert_time[pair["name"]]) < ALERT_COOLDOWN:
                    return

                self.last_alert_time[pair["name"]] = now
                msg = (
                    f"🎯 <b>ВСПЛЕСК ОБЪЕМА [{pair['name']}]</b>\n"
                    f"Биржа: <b>Binance Spot</b>\n"
                    f"Увеличение: <b>x{bn_ratio:.2f}</b>\n"
                    f"🕒 {now.strftime('%H:%M:%S')}"
                )
                await self.send_alert(msg)
        except Exception as err:
            logger.error(f"Ошибка расчета {pair['name']}: {err}")

    async def send_alert(self, text: str):
        try:
            await self.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")

    async def start_loop(self):
        await asyncio.sleep(5.0)
        await self.update_market_pairs()
        logger.info("🚀 Мониторинг Binance запущен")
        last_market_update = datetime.now()
        while True:
            if datetime.now() - last_market_update > timedelta(hours=1):
                await self.update_market_pairs()
                last_market_update = datetime.now()

            if WATCH_PAIRS:
                for i in range(0, len(WATCH_PAIRS), MAX_REQUESTS):
                    chunk = WATCH_PAIRS[i:i+MAX_REQUESTS]
                    await asyncio.gather(*[self.check_pair(p) for p in chunk])
                    await asyncio.sleep(0.4)

            await asyncio.sleep(CHECK_INTERVAL)


# ========== ВЕБ-СЕРВЕР И АНТИ-СОН ==========
async def web_health_check(request):
    return web.Response(text="OK")

async def keep_alive_ping():
    await asyncio.sleep(30)
    logger.info("Запущен самопинг каждые 4 минуты")
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(SELF_URL)
                if resp.status_code == 200:
                    logger.debug("Самопинг: OK")
        except Exception as e:
            logger.error(f"Ошибка самопинга: {e}")
        await asyncio.sleep(240)


async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.critical("Не заданы PUMP_BOT_TOKEN или PUMP_CHAT_ID")
        sys.exit(1)

    bot = Bot(token=TELEGRAM_TOKEN)
    monitor = AutoVolumeMonitor(bot)

    app = web.Application()
    app.router.add_get('/', web_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Веб-сервер на порту {PORT}")

    asyncio.create_task(monitor.start_loop())
    asyncio.create_task(keep_alive_ping())

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
