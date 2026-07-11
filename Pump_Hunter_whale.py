#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
import httpx
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

# ========== НАСТРОЙКИ ==========
BOT_NAME_RENDER = os.getenv("RENDER_SERVICE_NAME", "pump-hunter-default")
TELEGRAM_TOKEN = os.getenv("PUMP_BOT_TOKEN")
CHAT_ID = os.getenv("PUMP_CHAT_ID")
PORT = int(os.getenv("PORT", "7861"))

# Настройки чувствительности для качественных сигналов
THRESHOLD_VOL = 4.0             # Триггер на импульс (объём выше среднего в 4 раза)
CHECK_INTERVAL = 25             # Проверка рынка каждые 15 секунд
MAX_REQUESTS = 2                # Количество одновременных запросов к API

# Расширенные фильтры спотового рынка Binance
MIN_DAILY_VOL_USDT = 1_500_000  # Фильтр суточного объема (от 1 млн $)
MIN_PRICE = 0.0001              # Минимальная цена монеты
MAX_PRICE = 1.0                # Максимальная цена монеты (строго до 1$)

ALERT_COOLDOWN = timedelta(minutes=20) # Пауза между алертами по одной монете

# Динамический черный список (монеты из этой зоны бот игнорирует)
BLACKLIST = {"IRISUSDT", "LUNCUSDT", "USTCUSDT"}
WATCH_PAIRS = []

# Binance API
BINANCE_API = "https://api3.binance.com/api/v3/klines"
BINANCE_TICKER_API = "https://api3.binance.com/api/v3/ticker/price"
BINANCE_24HR_API = "https://api3.binance.com/api/v3/ticker/24hr"

SELF_URL = f"https://{BOT_NAME_RENDER}.onrender.com"

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("BinanceMassHunter")
# Глушим спам сетевых запросов httpx в консоли Render
logging.getLogger("httpx").setLevel(logging.WARNING)


class AutoVolumeMonitor:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_REQUESTS)
        self.last_alert_time: dict[str, datetime] = {}
        
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
        """Формирует список монет на основе объемов и убирает заблокированные."""
        filtered = []
        for i in range(0, len(symbols), 50):
            chunk = symbols[i:i+50]
            symbols_str = '","'.join(chunk)
            params = {"symbols": f'["{symbols_str}"]'}
            try:
                res = await self.client.get(BINANCE_24HR_API, params=params)
                if res.status_code == 200:
                    for item in res.json():
                        sym = item["symbol"]
                        if sym in BLACKLIST:
                            continue
                        
                        quote_vol = float(item.get("quoteVolume", 0))
                        if quote_vol >= min_vol:
                            clean = sym.replace("USDT", "")
                            filtered.append({
                                "binance": sym,
                                "name": clean
                            })
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Ошибка фильтрации рынка: {e}")
                await asyncio.sleep(2.0)
        return filtered

    async def update_market_pairs(self):
        global WATCH_PAIRS
        logger.info("🔄 Обновление списка доступных монет Binance...")
        try:
            await asyncio.sleep(1.5)
            res = await self.client.get(BINANCE_TICKER_API)
            if res.status_code != 200:
                logger.error(f"⚠️ Ошибка получения тикеров Binance: {res.status_code}")
                return
                
            all_tickers = res.json()
            candidates = [
                t["symbol"] for t in all_tickers
                if t["symbol"].endswith("USDT")
                and MIN_PRICE <= float(t["price"]) <= MAX_PRICE
                and t["symbol"] not in BLACKLIST
                and not any(x in t["symbol"] for x in ["UP", "DOWN", "BUSD", "EUR"])
            ]
            
            await asyncio.sleep(1.0)
            WATCH_PAIRS = await self.filter_by_volume(candidates, MIN_DAILY_VOL_USDT)
            logger.info(f"✅ Список синхронизирован! Мониторинг {len(WATCH_PAIRS)} пар.")
            
            await self.send_alert(f"🔄 <b>Сканер Binance перезапущен!</b>\nВ работе чистых пар (до 10$): <b>{len(WATCH_PAIRS)}</b>", reply_markup=None)
        except Exception as e:
            logger.error(f"Критическая ошибка обновления пар: {e}")

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
        if pair["binance"] in BLACKLIST:
            return

        bn_data = await self.fetch_binance(pair["binance"])
        if not bn_data or len(bn_data) < 12:
            return

        try:
            bn_latest = bn_data[-1]  # Текущая свеча
            
            open_price = float(bn_latest[1])
            high_price = float(bn_latest[2])
            close_price = float(bn_latest[4])
            current_volume = float(bn_latest[5]) # Минутный объем в монетах

            # 1. ЗАЩИТА ОТ ВЕРТОЛЕТОВ И СЛИВОВ: Свеча обязана быть строго зеленой
            if close_price <= open_price:
                return

            # Считаем средний минутный объем за прошлые 10 минут
            bn_avg = sum(float(k[5]) for k in bn_data[-11:-1]) / 10
            bn_ratio = current_volume / bn_avg if bn_avg > 0 else 0

            # 2. ЗАЩИТА ОТ ЛОЖНЫХ ПРОСТРЕЛОВ: Проверяем пробой локального хая за 10 минут
            prev_highs = [float(k[2]) for k in bn_data[-11:-1]]
            max_prev_high = max(prev_highs)

            # Триггер срабатывает, если объем повышен и идет истинный ценовой импульс вверх
            if bn_ratio >= THRESHOLD_VOL and close_price > max_prev_high:
                now = datetime.now()
                if self.last_alert_time.get(pair["name"]) and \
                   (now - self.last_alert_time[pair["name"]]) < ALERT_COOLDOWN:
                    return

                self.last_alert_time[pair["name"]] = now
                
                # Создаем кнопку удаления монеты в ЧС, чтобы не спамила в будущем
                inline_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ В Чёрный список", callback_data=f"block_{pair['binance']}")]
                ])

                msg = (
                    f"🚀 <b>ИМПУЛЬС ОБЪЕМА И ЦЕНЫ [{pair['name']}]</b>\n"
                    f"Биржа: <b>Binance Spot</b>\n"
                    f"Всплеск объема: <b>x{bn_ratio:.2f}</b> 🔥\n"
                    f"Цена пробоя: <b>{close_price} USDT</b> 📈\n"
                    f"🕒 {now.strftime('%H:%M:%S')}"
                )
                await self.send_alert(msg, reply_markup=inline_kb)
        except Exception as err:
            logger.error(f"Ошибка парсинга пары {pair['name']}: {err}")

    async def send_alert(self, text: str, reply_markup=None):
        try:
            await self.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")

    async def start_loop(self):
        await asyncio.sleep(5.0)
        await self.update_market_pairs()
        logger.info("🚀 Мониторинг спотового рынка запущен")
        last_market_update = datetime.now()
        while True:
            # Раз в час пересканируем весь рынок, удаляя старое и добавляя новые монеты
            if datetime.now() - last_market_update > timedelta(hours=1):
                await self.update_market_pairs()
                last_market_update = datetime.now()

            # Фильтруем WATCH_PAIRS "на лету" от свежезаблокированных монет
            active_pairs = [p for p in WATCH_PAIRS if p["binance"] not in BLACKLIST]

            if active_pairs:
                for i in range(0, len(active_pairs), MAX_REQUESTS):
                    chunk = active_pairs[i:i+MAX_REQUESTS]
                    await asyncio.gather(*[self.check_pair(p) for p in chunk])
                    await asyncio.sleep(0.8)

            await asyncio.sleep(CHECK_INTERVAL)


# ========== ОБРАБОТКА ИНЛАЙН-КНОПОК ==========
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

@dp.callback_query(lambda c: c.data and c.data.startswith('block_'))
async def process_callback_block(callback_query: types.CallbackQuery):
    target_symbol = callback_query.data.split('_')[1]
    
    # Добавляем в глобальный ЧС
    BLACKLIST.add(target_symbol)
    
    # Отправляем ответ пользователю, чтобы кнопка не зависала «часиками»
    await bot.answer_callback_query(callback_query.id, text=f"{target_symbol} заблокирован!")
    
    # Редактируем сообщение, подтверждая удаление
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"❌ Монета <b>{target_symbol.replace('USDT','')}</b> успешно удалена из мониторинга и добавлена в Чёрный список.",
        parse_mode="HTML"
    )

# ========== ВЕБ-СЕРВЕР И АНТИ-СОН ==========
async def web_health_check(request):
    return web.Response(text="OK")

async def keep_alive_ping():
    await asyncio.sleep(30)
    while True:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(SELF_URL)
        except Exception:
            pass
        await asyncio.sleep(240)


async def main():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.critical("Не заданы токены в Переменных Окружения Render!")
        sys.exit(1)

    monitor = AutoVolumeMonitor(bot)

    # Веб-сервер под Render
    app = web.Application()
    app.router.add_get('/', web_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Веб-сервер активен на порту {PORT}")

    # Запуск фоновых задач бота
    asyncio.create_task(monitor.start_loop())
    asyncio.create_task(keep_alive_ping())
    
    # Запуск поллинга aiogram для обработки нажатий кнопок
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен")
