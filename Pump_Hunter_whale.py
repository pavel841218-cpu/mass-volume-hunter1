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

WATCH_PAIRS = []

BINGX_API = "https://open-api.bingx.com/api/v1/market/getKline"
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
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        self.client = httpx.AsyncClient(headers=headers, timeout=10.0, limits=httpx.Limits(max_connections=20))

    async def filter_by_volume(self, symbols: list, min_vol=MIN_DAILY_VOL_USDT):
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
                            filtered.append({"bingx": f"{clean}-USDT", "binance": item["symbol"], "name": clean})
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Ошибка фильтрации: {e}")
        return filtered

    async def update_market_pairs(self):
        global WATCH_PAIRS
        logger.info("🔄 Поиск монет...")
        try:
            res = await self.client.get(BINANCE_TICKER_API)
            if res.status_code != 200: return
            all_tickers = res.json()
            candidates = [t["symbol"] for t in all_tickers if t["symbol"].endswith("USDT") and 0.01 <= float(t["price"]) <= 1.0 and not any(x in t["symbol"] for x in ["UP", "DOWN", "BUSD", "EUR"])]
            WATCH_PAIRS = await self.filter_by_volume(candidates, MIN_DAILY_VOL_USDT)
            logger.info(f"✅ Список обновлен! Найдено {len(WATCH_PAIRS)} монет.")
        except Exception as e:
            logger.error(f"Ошибка обновления: {e}")

    async def fetch_bingx(self, symbol: str):
        async with self.semaphore:
            try:
                # ФИКС: Убираем дефис для API BingX
                bingx_symbol = symbol.replace("-", "")
                res = await self.client.get(BINGX_API, params={"symbol": bingx_symbol, "interval": "1m", "limit": 15})
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
        bx_data, bn_data = await asyncio.gather(self.fetch_bingx(pair["bingx"]), self.fetch_binance(pair["binance"]))
        
        logger.info(f"🔍 Проверка {pair['name']} -> Получено свечей: BingX={len(bx_data)}, Binance={len(bn_data)}")
        if not bx_data or not bn_data or len(bx_data) < 12 or len(bn_data) < 12:
            return

        try:
            bx_latest, bn_latest = bx_data[-1], bn_data[-1]
            bx_avg = sum(float(k[5]) for k in bx_data[-11:-1]) / 10
            bn_avg = sum(float(k[5]) for k in bn_data[-11:-1]) / 10
            
            bx_ratio = float(bx_latest[5]) / bx_avg if bx_avg > 0 else 0
            bn_ratio = float(bn_latest[5]) / bn_avg if bn_avg > 0 else 0
            
            logger.info(f"📊 Расчет {pair['name']} -> BingX: x{bx_ratio:.2f}, Binance: x{bn_ratio:.2f}")

            if (bx_ratio >= THRESHOLD_VOL or bn_ratio >= THRESHOLD_VOL):
                now = datetime.now()
                if self.last_alert_time.get(pair["name"]) and (now - self.last_alert_time[pair["name"]]) < ALERT_COOLDOWN:
                    return
                
                self.last_alert_time[pair["name"]] = now
                msg = f"🎯 <b>ТЕСТ [{pair['name']}]</b>\nBingX: x{bx_ratio:.2f}\nBinance: x{bn_ratio:.2f}\n{now.strftime('%H:%M:%S')}"
                await self.send_alert(msg)
        except Exception as err:
            logger.error(f"Ошибка расчета {pair['name']}: {err}")

    async def send_alert(self, text: str):
        try: await self.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
        except: pass

    async def start_loop(self):
        await self.update_market_pairs()
        while True:
            if WATCH_PAIRS:
                for i in range(0, len(WATCH_PAIRS), MAX_REQUESTS):
                    await asyncio.gather(*[self.check_pair(p) for p in WATCH_PAIRS[i:i+MAX_REQUESTS]])
                    await asyncio.sleep(0.3)
            await asyncio.sleep(CHECK_INTERVAL)

async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    monitor = AutoVolumeMonitor(bot)
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', PORT).start()
    asyncio.create_task(monitor.start_loop())
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
