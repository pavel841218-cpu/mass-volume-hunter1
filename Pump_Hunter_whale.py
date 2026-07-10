#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from aiohttp import web
import httpx
from aiogram import Bot

# ========== НАСТРОЙКИ ==========
TELEGRAM_TOKEN = os.getenv("PUMP_BOT_TOKEN")
CHAT_ID = os.getenv("PUMP_CHAT_ID")
PORT = int(os.getenv("PORT", "7861"))

THRESHOLD_VOL = 3.5
CHECK_INTERVAL = 10  # Пауза между кругами сканирования
MAX_REQUESTS = 3     # Максимум одновременных запросов к биржам

ALERT_COOLDOWN = timedelta(minutes=5)
MAX_SPREAD_PERCENT = 1.5

# Динамический список пар, который бот соберет сам
WATCH_PAIRS = []

BINGX_API = "https://open-api.bingx.com/api/v1/market/getKline"
BINANCE_API = "https://api.binance.com/api/v3/klines"
BINANCE_TICKER_API = "https://api.binance.com/api/v3/ticker/price"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("AutoMassHunter")

class AutoVolumeMonitor:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        self.client = httpx.AsyncClient(
            timeout=7.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        )

    async def update_market_pairs(self):
        """Автоматически находит ВСЕ монеты от 0.01$ до 1$ на Binance"""
        global WATCH_PAIRS
        logger.info("🔄 Обновление списка монет в диапазоне 0.01$ - 1$...")
        try:
            res = await self.client.get(BINANCE_TICKER_API)
            if res.status_code != 200:
                logger.error("Не удалось получить цены с Binance")
                return

            all_tickers = res.json()
            new_pairs = []

            for ticker in all_tickers:
                symbol = ticker["symbol"]
                # Фильтруем только пары к USDT, убирая стейблкоины и кредитные токены (UP/DOWN)
                if symbol.endswith("USDT") and not any(x in symbol for x in ["UPUSDT", "DOWNUSDT", "BUSD", "EUR"]):
                    price = float(ticker["price"])
                    
                    # НАШ ДИАПАЗОН: от 0.01$ до 1$
                    if 0.01 <= price <= 1.0:
                        clean_name = symbol.replace("USDT", "")
                        new_pairs.append({
                            "bingx": f"{clean_name}-USDT",
                            "binance": symbol,
                            "name": clean_name
                        })

            WATCH_PAIRS = new_pairs
            logger.info(f"✅ Список обновлен! Найдено {len(WATCH_PAIRS)} монет в диапазоне от 0.01$ до 1$")
            
            # Отправим отчет в телеграм, чтобы ты видел, что бот пересобрал рынок
            await self.send_alert(f"🔄 <b>Сканер обновил рынок!</b>\nНайдено монет в диапазоне 0.01$-1$: <b>{len(WATCH_PAIRS)}</b>")

        except Exception as e:
            logger.error(f"Ошибка при автоматическом поиске монет: {e}")

    async def fetch_bingx(self, symbol: str):
        async with self.semaphore:
            try:
                res = await self.client.get(BINGX_API, params={"symbol": symbol, "interval": "1m", "limit": 15})
                if res.status_code == 200:
                    return res.json().get("data", [])
            except Exception as e:
                pass # Игнорируем редкие ошибки отдельных пар на BingX
        return []

    async def fetch_binance(self, symbol: str):
        async with self.semaphore:
            try:
                res = await self.client.get(BINANCE_API, params={"symbol": symbol, "interval": "1m", "limit": 15})
                if res.status_code == 200:
                    return res.json()
            except Exception as e:
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
        # Первый сбор монет при запуске
        await self.update_market_pairs()
        
        logger.info("🚀 Полное сканирование рынка запущено...")
        last_market_update = datetime.now()

        while True:
            # Раз в час полностью обновляем список монет (если кто-то вырос или упал из диапазона)
            if datetime.now() - last_market_update > timedelta(hours=1):
                await self.update_market_pairs()
                last_market_update = datetime.now()

            if WATCH_PAIRS:
                # Проверяем все найденные монеты
                tasks = [self.check_pair(pair) for pair in WATCH_PAIRS]
                await asyncio.gather(*tasks)
            
            await asyncio.sleep(CHECK_INTERVAL)

async def web_health_check(request):
    return web.Response(text="Auto Market Volume Hunter: Operational", status=200)

async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.critical("PUMP_BOT_TOKEN или PUMP_CHAT_ID не заданы!")
        sys.exit(1)

    bot = Bot(token=TELEGRAM_TOKEN)
    monitor = AutoVolumeMonitor(bot)

    app = web.Application()
    app.router.add_get('/', web_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

    await monitor.start_loop()

if __name__ == "__main__":
    asyncio.run(main())
