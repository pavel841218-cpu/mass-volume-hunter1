import asyncio
import os
import logging
import time
import aiohttp
from aiogram import Bot
from datetime import datetime
from aiohttp import web

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Telegram переменные
BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("Не установлены PUMP_BOT_TOKEN или PUMP_CHAT_ID")

bot = Bot(token=BOT_TOKEN)

# Настройки BingX Futures API
BINGX_BASE_URL = "https://open-api.bingx.com"
TIMEFRAME = "1h"

# ===== 1. ПАРАМЕТРЫ ДЛЯ ПРОБОЕВ ИЗ НАКОПЛЕНИЯ =====
ACCUMULATION_CANDLES = 6          
MAX_ACCUMULATION_RANGE_PCT = 4.0  
MAX_ACCUMULATION_VOLUME_SPIKE = 2.5  

MIN_IMPULSE_CHANGE_PCT_LONG = 3.0     
MIN_IMPULSE_VOL_MULTIPLIER_LONG = 3.0  
MIN_BODY_QUALITY_LONG = 70.0          
MIN_IMPULSE_USDT_VOL_LONG = 50000     
MIN_BREAKOUT_STRENGTH_LONG = 1.0      

DISTRIBUTION_CANDLES = 6          
MAX_DISTRIBUTION_RANGE_PCT = 4.0  
MAX_DISTRIBUTION_VOLUME_SPIKE = 2.5  

MIN_IMPULSE_CHANGE_PCT_SHORT = 3.0     
MIN_IMPULSE_VOL_MULTIPLIER_SHORT = 3.0  
MIN_BODY_QUALITY_SHORT = 70.0          
MIN_IMPULSE_USDT_VOL_SHORT = 50000     
MIN_BREAKDOWN_STRENGTH_SHORT = 1.0     

# ===== 2. ПАРАМЕТРЫ ДЛЯ ЛОВЛИ АГРЕССИВНЫХ ПАМПОВ/ДАМПОВ (РАКЕТЫ) =====
PUMP_MIN_CHANGE_PCT = 4.0         # Минимальный рост свечи для ПАМПА (%)
PUMP_VOL_MULTIPLIER = 3.5         # Превышение объёма над средним за 12 часов
PUMP_MIN_BODY_QUALITY = 65.0      # Минимальное качество тела свечи (%)
PUMP_MIN_USDT_VOLUME = 100000     # Минимальный объём свечи в USDT ($100k)

# Хранилище сигналов
last_signals = {}
SIGNAL_COOLDOWN = 3600  # 1 час между сигналами по одной монете


# ===== ВЕБ-СЕРВЕР ДЛЯ РЕНДЕРА =====
async def health_check(request):
    return web.Response(text="BingX Scanner Bot is Live!")

async def start_dummy_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Сервер-заглушка запущен на порту {port}")


async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if data.get("code") == 0 and "data" in data:
                return [
                    item["symbol"] for item in data["data"] 
                    if item.get("symbol", "").endswith("-USDT")
                    and item.get("status") == 1
                ]
    except Exception as e:
        logging.error(f"Ошибка получения списка пар: {e}")
    return []


async def fetch_klines(session, symbol, limit=30, retries=2):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": limit}
    
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    await asyncio.sleep(2 ** attempt)
                    continue
                res = await resp.json()
                if res.get("code") == 0 and "data" in res:
                    return res["data"]
                break
        except Exception:
            await asyncio.sleep(1)
    return None


# ===== АЛГОРИТМ 1: НАКОПЛЕНИЕ + ПРОБОЙ =====
def detect_accumulation_and_breakout(klines):
    if not klines or len(klines) < ACCUMULATION_CANDLES + 3:
        return None

    try:
        impulse_candle = klines[-2]
        accumulation_candles = klines[-(ACCUMULATION_CANDLES + 2):-2]
        
        imp_open, imp_close = float(impulse_candle["open"]), float(impulse_candle["close"])
        imp_high, imp_low = float(impulse_candle["high"]), float(impulse_candle["low"])
        imp_volume = float(impulse_candle["volume"])
        
        if imp_close <= imp_open:
            return None
        
        imp_change_pct = ((imp_close - imp_open) / imp_open) * 100
        if imp_change_pct < MIN_IMPULSE_CHANGE_PCT_LONG:
            return None
        
        imp_range = imp_high - imp_low
        if imp_range <= 0 or ((imp_close - imp_open) / imp_range) * 100 < MIN_BODY_QUALITY_LONG:
            return None
        
        imp_usdt_volume = imp_volume * imp_close
        if imp_usdt_volume < MIN_IMPULSE_USDT_VOL_LONG:
            return None
        
        acc_highs = [float(c["high"]) for c in accumulation_candles]
        acc_lows = [float(c["low"]) for c in accumulation_candles]
        acc_vols = [float(c["volume"]) * float(c["close"]) for c in accumulation_candles]
        
        acc_high, acc_low = max(acc_highs), min(acc_lows)
        if ((acc_high - acc_low) / acc_low) * 100 > MAX_ACCUMULATION_RANGE_PCT:
            return None
        
        acc_avg_vol = sum(acc_vols) / len(acc_vols)
        if acc_avg_vol <= 0 or max(acc_vols) > acc_avg_vol * MAX_ACCUMULATION_VOLUME_SPIKE:
            return None
        
        vol_mult = imp_usdt_volume / acc_avg_vol
        if vol_mult < MIN_IMPULSE_VOL_MULTIPLIER_LONG or imp_close <= acc_high:
            return None
        
        breakout_str = ((imp_close - acc_high) / acc_high) * 100
        if breakout_str < MIN_BREAKOUT_STRENGTH_LONG:
            return None
        
        return {
            "type": "ACCUMULATION_LONG",
            "price_change": round(imp_change_pct, 2),
            "vol_multiplier": round(vol_mult, 2),
            "body_quality": round(((imp_close - imp_open) / imp_range) * 100, 1),
            "close_price": imp_close,
            "usdt_volume": int(imp_usdt_volume),
            "acc_range": round(((acc_high - acc_low) / acc_low) * 100, 2),
            "acc_low": acc_low,
            "acc_high": acc_high
        }
    except Exception:
        return None


# ===== АЛГОРИТМ 2: ЛОВЕЦ РАКЕТ / ПАМПОВ (БЕЗ БОКОВИКА) =====
def detect_direct_pump_or_dump(klines):
    if not klines or len(klines) < 14:
        return None

    try:
        current_candle = klines[-2]
        history_candles = klines[-14:-2]  # Предыдущие 12 часов
        
        open_p, close_p = float(current_candle["open"]), float(current_candle["close"])
        high_p, low_p = float(current_candle["high"]), float(current_candle["low"])
        vol = float(current_candle["volume"])
        
        usdt_vol = vol * close_p
        if usdt_vol < PUMP_MIN_USDT_VOLUME:
            return None
            
        c_range = high_p - low_p
        if c_range <= 0:
            return None
            
        body = abs(close_p - open_p)
        body_quality = (body / c_range) * 100
        if body_quality < PUMP_MIN_BODY_QUALITY:
            return None
            
        # Средний объем за 12 часов
        avg_vol = sum([float(c["volume"]) * float(c["close"]) for c in history_candles]) / len(history_candles)
        if avg_vol <= 0:
            return None
            
        vol_multiplier = usdt_vol / avg_vol
        if vol_multiplier < PUMP_VOL_MULTIPLIER:
            return None
            
        # Проверяем процент изменения цены
        change_pct = ((close_p - open_p) / open_p) * 100
        
        # ЛОНГ РАКЕТА
        if change_pct >= PUMP_MIN_CHANGE_PCT:
            return {
                "type": "PUMP_ROCKET",
                "direction": "LONG",
                "price_change": round(change_pct, 2),
                "vol_multiplier": round(vol_multiplier, 2),
                "body_quality": round(body_quality, 1),
                "close_price": close_p,
                "usdt_volume": int(usdt_vol)
            }
        # ШОРТ ДАМП
        elif change_pct <= -PUMP_MIN_CHANGE_PCT:
            return {
                "type": "PUMP_ROCKET",
                "direction": "SHORT",
                "price_change": round(abs(change_pct), 2),
                "vol_multiplier": round(vol_multiplier, 2),
                "body_quality": round(body_quality, 1),
                "close_price": close_p,
                "usdt_volume": int(usdt_vol)
            }
            
    except Exception:
        return None
    return None


def cleanup_old_signals():
    current_time = time.time()
    expired = [s for s, t in last_signals.items() if current_time - t > SIGNAL_COOLDOWN * 2]
    for s in expired:
        del last_signals[s]


async def send_signal_msg(symbol, data):
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        
        if data["type"] == "ACCUMULATION_LONG":
            message = (
                f"🟢 <b>LONG: ПРОБОЙ НАКОПЛЕНИЯ</b>\n"
                f"📊 <b>{clean_symbol}</b> | BingX 1H\n\n"
                f"📈 Рост: <b>+{data['price_change']}%</b>\n"
                f"🔥 Объём: <b>x{data['vol_multiplier']}</b> от накопления\n"
                f"💵 Объём USDT: <b>${data['usdt_volume']:,}</b>\n"
                f"📦 Диапазон базы: <b>{data['acc_range']}%</b>\n"
                f"💥 Цена пробоя: <b>${data['close_price']:.4f}</b>\n"
                f"🛑 Стоп: <b>${data['acc_low']:.4f}</b>\n"
                f"🕒 {datetime.now().strftime('%H:%M:%S')}"
            )
        elif data["type"] == "PUMP_ROCKET":
            icon = "🚀" if data["direction"] == "LONG" else "💥"
            side = "PUMP (ЛОНГ)" if data["direction"] == "LONG" else "DUMP (ШОРТ)"
            message = (
                f"{icon} <b>ИМПУЛЬСНАЯ РАКЕТА: {side}</b>\n"
                f"📊 <b>{clean_symbol}</b> | BingX 1H\n\n"
                f"⚡ Движение: <b>{'+' if data['direction']=='LONG' else '-'}{data['price_change']}%</b> за час!\n"
                f"📊 Всплеск объёма: <b>x{data['vol_multiplier']}</b> к 12ч среднему\n"
                f"💵 Объём свечи: <b>${data['usdt_volume']:,}</b>\n"
                f"🎯 Текущая цена: <b>${data['close_price']:.4f}</b>\n"
                f"🕒 {datetime.now().strftime('%H:%M:%S')}"
            )

        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="HTML")
        logging.info(f"Сигнал отправлен: {clean_symbol} ({data['type']})")
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения: {e}")


async def scanner_loop():
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text="🔄 <b>Сканер BingX 2in1 Запущен!</b>\n\n"
                     "🟢 <b>Режим 1:</b> Пробой из накопления (4%)\n"
                     "🚀 <b>Режим 2:</b> Ловец Ракет (+4% роста, объем x3.5+)",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка старта Telegram: {e}")

        scan_count = 0
        while True:
            try:
                scan_count += 1
                logging.info(f"--- Скан #{scan_count} ---")
                
                symbols = await fetch_bingx_symbols(session)
                if not symbols:
                    await asyncio.sleep(30)
                    continue

                current_time = time.time()
                if scan_count % 10 == 0:
                    cleanup_old_signals()

                for i, symbol in enumerate(symbols):
                    if symbol in last_signals and current_time - last_signals[symbol] < SIGNAL_COOLDOWN:
                        continue

                    klines = await fetch_klines(session, symbol, limit=20)
                    if klines:
                        # 1. Проверяем классический пробой из аккумуляции
                        acc_res = detect_accumulation_and_breakout(klines)
                        if acc_res:
                            await send_signal_msg(symbol, acc_res)
                            last_signals[symbol] = current_time
                            continue
                        
                        # 2. Проверяем резкую ракету / памп
                        pump_res = detect_direct_pump_or_dump(klines)
                        if pump_res:
                            await send_signal_msg(symbol, pump_res)
                            last_signals[symbol] = current_time

                    if i % 10 == 0:
                        await asyncio.sleep(0.1)

                await asyncio.sleep(60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Ошибка цикла: {e}")
                await asyncio.sleep(30)


async def main():
    await start_dummy_server()
    await scanner_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
