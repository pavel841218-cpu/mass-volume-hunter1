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

THRESHOLD_VOL = 1.3
CHECK_INTERVAL = 10
MAX_REQUESTS = 3
MIN_DAILY_VOL_USDT = 2_500_000

ALERT_COOLDOWN = timedelta(minutes=5)
MAX_SPREAD_PERCENT = 1.5

# BingX API URL
BINGX_API = "https://open-api.bingx.com/api/v1/market/getKline"
BINGX_CONTRACTS_API = "https://open-api.bingx.com/api/v1/market/getContracts"

# Binance API (используем api1 для стабильности)
BINANCE_API = "https://api1.binance.com/api/v3/klines"
BINANCE_TICKER_API = "https://api1.binance.com/api/v3/ticker/price"
BINANCE_24HR_API = "https://api1.binance.com/api/v3/ticker/24hr"

SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("AutoMassHunter")


class AutoVolumeMonitor:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        self.bingx_symbols: set = set()  # кеш доступных пар BingX
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=15.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        )

    async def load_bingx_symbols(self):
        """Загружает список всех доступных USDT-пар на BingX."""
        try:
            res = await self.client.get(BINGX_CONTRACTS_API)
            if res.status_code == 200:
                contracts = res.json().get("data", [])
                self.bingx_symbols = {
                    c["symbol"] for c in contracts
                    if c.get("symbol", "").endswith("USDT") and c.get("status") == 1
                }
                logger.info(f"Загружено {len(self.bingx_symbols)} пар BingX")
            else:
                logger.error(f"Не удалось загрузить контракты BingX: {res.status_code}")
        except Exception as e:
            logger.error(f"Ошибка загрузки контрактов BingX: {e}")

    async def filter_by_volume(self, symbols: list, min_vol=MIN_DAILY_VOL_USDT):
        """Оставляет только монеты с объёмом ≥ min_vol и присутствующие на BingX."""
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
                            bingx_sym = f"{clean}USDT"   # без дефиса, как в API BingX
                            # Проверяем, есть ли такая пара на BingX
                            if bingx_sym in self.bingx_symbols:
                                filtered.append({
                                    "bingx": bingx_sym,
                                    "binance": item["symbol"],
                                    "name": clean
                                })
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Ошибка фильтрации: {e}")
        return filtered

    async def update_market_pairs(self):
        global WATCH_PAIRS
        logger.info("🔄 Поиск монет...")
        try:
            # Обновляем список BingX-пар перед фильтрацией
            await self.load_bingx_symbols()
            res = await self.client.get(BINANCE_TICKER_API)
            if res.status_code != 200:
                logger.error(f"Binance ticker error: {res.status_code}")
                return
            all_tickers = res.json()
            candidates = [
                t["symbol"] for t in all_tickers
                if t["symbol"].endswith("USDT")
                and 0.01 <= float(t["price"]) <= 1.0
                and not any(x in t["symbol"] for x in ["UP", "DOWN", "BUSD", "EUR"])
            ]
            WATCH_PAIRS = await self.filter_by_volume(candidates, MIN_DAILY_VOL_USDT)
            logger.info(f"✅ Список обновлен! Найдено {len(WATCH_PAIRS)} монет (есть на обеих биржах).")
        except Exception as e:
            logger.error(f"Ошибка обновления: {e}")

    async def fetch_bingx(self, symbol: str):
        """symbol уже в формате XPLUSDT (без дефиса)."""
        async with self.semaphore:
            try:
                res = await self.client.get(BINGX_API, params={
                    "symbol": symbol,
                    "interval": "1m",
                    "limit": 15
                })
                if res.status_code == 200:
                    return res.json().get("data", [])
                elif res.status_code != 200:
                    logger.warning(f"BingX {symbol} status {res.status_code}")
            except Exception as e:
                logger.debug(f"BingX {symbol} exception: {e}")
        return []

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
        bx_data, bn_data = await asyncio.gather(
            self.fetch_bingx(pair["bingx"]),
            self.fetch_binance(pair["binance"])
        )

        logger.info(f"🔍 {pair['name']} | BingX: {len(bx_data)} свечей, Binance: {len(bn_data)} свечей")
        if not bx_data or not bn_data or len(bx_data) < 12 or len(bn_data) < 12:
            return

        try:
            bx_latest, bn_latest = bx_data[-1], bn_data[-1]
            bx_avg = sum(float(k[5]) for k in bx_data[-11:-1]) / 10
            bn_avg = sum(float(k[5]) for k in bn_data[-11:-1]) / 10

            bx_ratio = float(bx_latest[5]) / bx_avg if bx_avg > 0 else 0
            bn_ratio = float(bn_latest[5]) / bn_avg if bn_avg > 0 else 0

            logger.info(f"📊 {pair['name']} | BingX: x{bx_ratio:.2f}, Binance: x{bn_ratio:.2f}")

            if bx_ratio >= THRESHOLD_VOL or bn_ratio >= THRESHOLD_VOL:
                now = datetime.now()
                if self.last_alert_time.get(pair["name"]) and \
                   (now - self.last_alert_time[pair["name"]]) < ALERT_COOLDOWN:
                    return

                self.last_alert_time[pair["name"]] = now
                msg = (
                    f"🎯 <b>ВСПЛЕСК [{pair['name']}]</b>\n"
                    f"BingX: x{bx_ratio:.2f} | Binance: x{bn_ratio:.2f}\n"
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
        await self.update_market_pairs()
        logger.info("🚀 Мониторинг запущен")
        last_market_update = datetime.now()
        while True:
            # Обновляем рынок раз в час
            if datetime.now() - last_market_update > timedelta(hours=1):
                await self.update_market_pairs()
                last_market_update = datetime.now()

            if WATCH_PAIRS:
                for i in range(0, len(WATCH_PAIRS), MAX_REQUESTS):
                    chunk = WATCH_PAIRS[i:i+MAX_REQUESTS]
                    await asyncio.gather(*[self.check_pair(p) for p in chunk])
                    await asyncio.sleep(0.3)

            await asyncio.sleep(CHECK_INTERVAL)


# ========== ВЕБ-СЕРВЕР И АНТИ-СОН ==========
async def web_health_check(request):
    return web.Response(text="OK")

async def keep_alive_ping():
    """Фоновый самопинг, чтобы Render не усыпил сервис."""
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

    # Веб-сервер
    app = web.Application()
    app.router.add_get('/', web_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Веб-сервер на порту {PORT}")

    # Запускаем мониторинг
    asyncio.create_task(monitor.start_loop())
    # Запускаем самопинг
    asyncio.create_task(keep_alive_ping())

    # Бесконечное ожидание
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
