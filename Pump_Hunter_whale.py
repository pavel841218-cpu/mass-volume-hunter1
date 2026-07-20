import asyncio
import os
import logging
import time
import aiohttp
from aiogram import Bot
from datetime import datetime, timedelta

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

# Параметры фильтрации для 1H
TIMEFRAME = "1h"

# ===== ПАРАМЕТРЫ ДЛЯ ЛОНГОВ (АККУМУЛЯЦИЯ + ПРОБОЙ ВВЕРХ) =====
ACCUMULATION_CANDLES = 6          # Сколько свечей анализировать для поиска накопления
MAX_ACCUMULATION_RANGE_PCT = 4.0  # Максимальный диапазон колебаний в фазе накопления (%)
MAX_ACCUMULATION_VOLUME_SPIKE = 2.5  # Объем в накоплении не должен превышать средний в x1.5

MIN_IMPULSE_CHANGE_PCT_LONG = 3.0     # Минимальный рост свечи пробоя (%)
MIN_IMPULSE_VOL_MULTIPLIER_LONG = 3.0  # Превышение объёма над средним в фазе накопления
MIN_BODY_QUALITY_LONG = 70.0          # Качество тела свечи пробоя (%)
MIN_IMPULSE_USDT_VOL_LONG = 50000     # Минимальный объём свечи пробоя в USDT
MIN_BREAKOUT_STRENGTH_LONG = 1.0      # Минимальный % пробоя над максимумом аккумуляции

# ===== ПАРАМЕТРЫ ДЛЯ ШОРТОВ (ДИСТРИБУЦИЯ + ПРОБОЙ ВНИЗ) =====
DISTRIBUTION_CANDLES = 6          # Сколько свечей анализировать для поиска дистрибуции
MAX_DISTRIBUTION_RANGE_PCT = 4.0  # Максимальный диапазон колебаний в фазе дистрибуции (%)
MAX_DISTRIBUTION_VOLUME_SPIKE = 2.5  # Объем в дистрибуции не должен превышать средний в x1.5

MIN_IMPULSE_CHANGE_PCT_SHORT = 3.0     # Минимальное падение свечи пробоя (%)
MIN_IMPULSE_VOL_MULTIPLIER_SHORT = 3.0  # Превышение объёма над средним в фазе дистрибуции
MIN_BODY_QUALITY_SHORT = 70.0          # Качество тела свечи пробоя (%)
MIN_IMPULSE_USDT_VOL_SHORT = 50000     # Минимальный объём свечи пробоя в USDT
MIN_BREAKDOWN_STRENGTH_SHORT = 1.0     # Минимальный % пробоя под минимум дистрибуции

# Хранилище сигналов с временем
last_signals = {}
SIGNAL_COOLDOWN = 7200  # 2 часа


async def fetch_bingx_symbols(session):
    """Получаем список всех активных USDT-M фьючерсных пар"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logging.error(f"HTTP {resp.status} при получении контрактов")
                return []
            
            data = await resp.json()
            if data.get("code") == 0 and "data" in data:
                symbols = [
                    item["symbol"] for item in data["data"] 
                    if item.get("symbol", "").endswith("-USDT")
                    and item.get("status") == 1
                ]
                logging.info(f"Получено {len(symbols)} активных пар")
                return symbols
            else:
                logging.error(f"API error: {data.get('msg', 'Unknown')}")
    except asyncio.TimeoutError:
        logging.error("Таймаут при получении списка пар")
    except Exception as e:
        logging.error(f"Ошибка при получении списка пар: {e}")
    return []


async def fetch_klines(session, symbol, limit=30, retries=2):
    """Запрашиваем свечи с повторными попытками"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": limit
    }
    
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, 
                                 timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:  # Rate limit
                    await asyncio.sleep(2 ** attempt)
                    continue
                    
                res = await resp.json()
                if res.get("code") == 0 and "data" in res:
                    return res["data"]
                break
        except Exception as e:
            if attempt == retries - 1:
                logging.debug(f"Ошибка получения свечей для {symbol}: {e}")
            await asyncio.sleep(1)
    return None


def detect_accumulation_and_breakout(klines):
    """
    Ищет паттерн для ЛОНГА: аккумуляция -> импульсный пробой ВВЕРХ
    Возвращает словарь с параметрами или None
    """
    if not klines or len(klines) < ACCUMULATION_CANDLES + 3:
        return None

    try:
        # Последняя закрытая свеча (импульс)
        impulse_candle = klines[-2]
        # Предыдущие свечи для анализа аккумуляции
        accumulation_candles = klines[-(ACCUMULATION_CANDLES + 2):-2]
        
        # Параметры свечи импульса
        imp_open = float(impulse_candle["open"])
        imp_close = float(impulse_candle["close"])
        imp_high = float(impulse_candle["high"])
        imp_low = float(impulse_candle["low"])
        imp_volume = float(impulse_candle["volume"])
        
        # Проверка на ЗЕЛЕНУЮ свечу импульса (рост)
        if imp_close <= imp_open:
            return None
        
        # Рост импульсной свечи
        imp_change_pct = ((imp_close - imp_open) / imp_open) * 100
        if imp_change_pct < MIN_IMPULSE_CHANGE_PCT_LONG:
            return None
        
        # Качество тела импульсной свечи
        imp_range = imp_high - imp_low
        if imp_range <= 0:
            return None
        
        imp_body = imp_close - imp_open
        imp_body_quality = (imp_body / imp_range) * 100
        if imp_body_quality < MIN_BODY_QUALITY_LONG:
            return None
        
        # Объем импульса в USDT
        imp_usdt_volume = imp_volume * imp_close
        if imp_usdt_volume < MIN_IMPULSE_USDT_VOL_LONG:
            return None
        
        # АНАЛИЗ ФАЗЫ АККУМУЛЯЦИИ
        acc_highs = []
        acc_lows = []
        acc_volumes = []
        acc_closes = []
        
        for candle in accumulation_candles:
            acc_highs.append(float(candle["high"]))
            acc_lows.append(float(candle["low"]))
            acc_volumes.append(float(candle["volume"]) * float(candle["close"]))
            acc_closes.append(float(candle["close"]))
        
        if not acc_highs:
            return None
        
        # Диапазон аккумуляции
        acc_high = max(acc_highs)
        acc_low = min(acc_lows)
        acc_range_pct = ((acc_high - acc_low) / acc_low) * 100
        
        # Аккумуляция должна быть узкой (боковик)
        if acc_range_pct > MAX_ACCUMULATION_RANGE_PCT:
            return None
        
        # Средний объем в фазе аккумуляции
        acc_avg_volume = sum(acc_volumes) / len(acc_volumes)
        if acc_avg_volume <= 0:
            return None
        
        # Проверяем, что в аккумуляции не было всплесков объема
        max_acc_volume = max(acc_volumes)
        if max_acc_volume > acc_avg_volume * MAX_ACCUMULATION_VOLUME_SPIKE:
            return None
        
        # ОБЪЕМ ИМПУЛЬСА ОТНОСИТЕЛЬНО АККУМУЛЯЦИИ
        vol_multiplier = imp_usdt_volume / acc_avg_volume
        if vol_multiplier < MIN_IMPULSE_VOL_MULTIPLIER_LONG:
            return None
        
        # Проверка, что импульс пробивает верхнюю границу аккумуляции
        if imp_close <= acc_high:
            return None
        
        # Сила пробоя над максимумом аккумуляции
        breakout_strength = ((imp_close - acc_high) / acc_high) * 100
        if breakout_strength < MIN_BREAKOUT_STRENGTH_LONG:
            return None
        
        # Средняя цена в аккумуляции
        acc_avg_price = sum(acc_closes) / len(acc_closes)
        
        return {
            "type": "LONG",
            "price_change": round(imp_change_pct, 2),
            "vol_multiplier": round(vol_multiplier, 2),
            "body_quality": round(imp_body_quality, 1),
            "close_price": imp_close,
            "usdt_volume": int(imp_usdt_volume),
            "accumulation_range": round(acc_range_pct, 2),
            "accumulation_duration": ACCUMULATION_CANDLES,
            "breakout_strength": round(breakout_strength, 2),
            "accumulation_high": acc_high,
            "accumulation_low": acc_low,
            "acc_avg_volume": int(acc_avg_volume)
        }
        
    except (KeyError, ValueError, ZeroDivisionError) as e:
        logging.debug(f"Ошибка анализа лонга: {e}")
        return None


def detect_distribution_and_breakdown(klines):
    """
    Ищет паттерн для ШОРТА: дистрибуция -> импульсный пробой ВНИЗ
    Возвращает словарь с параметрами или None
    """
    if not klines or len(klines) < DISTRIBUTION_CANDLES + 3:
        return None

    try:
        # Последняя закрытая свеча (импульс)
        impulse_candle = klines[-2]
        # Предыдущие свечи для анализа дистрибуции
        distribution_candles = klines[-(DISTRIBUTION_CANDLES + 2):-2]
        
        # Параметры свечи импульса
        imp_open = float(impulse_candle["open"])
        imp_close = float(impulse_candle["close"])
        imp_high = float(impulse_candle["high"])
        imp_low = float(impulse_candle["low"])
        imp_volume = float(impulse_candle["volume"])
        
        # Проверка на КРАСНУЮ свечу импульса (падение)
        if imp_close >= imp_open:
            return None
        
        # Падение импульсной свечи
        imp_change_pct = abs(((imp_close - imp_open) / imp_open) * 100)
        if imp_change_pct < MIN_IMPULSE_CHANGE_PCT_SHORT:
            return None
        
        # Качество тела импульсной свечи
        imp_range = imp_high - imp_low
        if imp_range <= 0:
            return None
        
        imp_body = abs(imp_close - imp_open)
        imp_body_quality = (imp_body / imp_range) * 100
        if imp_body_quality < MIN_BODY_QUALITY_SHORT:
            return None
        
        # Объем импульса в USDT
        imp_usdt_volume = imp_volume * imp_close
        if imp_usdt_volume < MIN_IMPULSE_USDT_VOL_SHORT:
            return None
        
        # АНАЛИЗ ФАЗЫ ДИСТРИБУЦИИ
        dist_highs = []
        dist_lows = []
        dist_volumes = []
        dist_closes = []
        
        for candle in distribution_candles:
            dist_highs.append(float(candle["high"]))
            dist_lows.append(float(candle["low"]))
            dist_volumes.append(float(candle["volume"]) * float(candle["close"]))
            dist_closes.append(float(candle["close"]))
        
        if not dist_highs:
            return None
        
        # Диапазон дистрибуции
        dist_high = max(dist_highs)
        dist_low = min(dist_lows)
        dist_range_pct = ((dist_high - dist_low) / dist_low) * 100
        
        # Дистрибуция должна быть узкой (боковик)
        if dist_range_pct > MAX_DISTRIBUTION_RANGE_PCT:
            return None
        
        # Средний объем в фазе дистрибуции
        dist_avg_volume = sum(dist_volumes) / len(dist_volumes)
        if dist_avg_volume <= 0:
            return None
        
        # Проверяем, что в дистрибуции не было всплесков объема
        max_dist_volume = max(dist_volumes)
        if max_dist_volume > dist_avg_volume * MAX_DISTRIBUTION_VOLUME_SPIKE:
            return None
        
        # ОБЪЕМ ИМПУЛЬСА ОТНОСИТЕЛЬНО ДИСТРИБУЦИИ
        vol_multiplier = imp_usdt_volume / dist_avg_volume
        if vol_multiplier < MIN_IMPULSE_VOL_MULTIPLIER_SHORT:
            return None
        
        # Проверка, что импульс пробивает нижнюю границу дистрибуции
        if imp_close >= dist_low:
            return None
        
        # Сила пробоя под минимум дистрибуции
        breakdown_strength = ((dist_low - imp_close) / dist_low) * 100
        if breakdown_strength < MIN_BREAKDOWN_STRENGTH_SHORT:
            return None
        
        # Средняя цена в дистрибуции
        dist_avg_price = sum(dist_closes) / len(dist_closes)
        
        return {
            "type": "SHORT",
            "price_change": round(imp_change_pct, 2),
            "vol_multiplier": round(vol_multiplier, 2),
            "body_quality": round(imp_body_quality, 1),
            "close_price": imp_close,
            "usdt_volume": int(imp_usdt_volume),
            "distribution_range": round(dist_range_pct, 2),
            "distribution_duration": DISTRIBUTION_CANDLES,
            "breakdown_strength": round(breakdown_strength, 2),
            "distribution_high": dist_high,
            "distribution_low": dist_low,
            "dist_avg_volume": int(dist_avg_volume)
        }
        
    except (KeyError, ValueError, ZeroDivisionError) as e:
        logging.debug(f"Ошибка анализа шорта: {e}")
        return None


def cleanup_old_signals():
    """Удаляем устаревшие записи из last_signals"""
    current_time = time.time()
    expired = [
        symbol for symbol, timestamp in last_signals.items()
        if current_time - timestamp > SIGNAL_COOLDOWN * 2
    ]
    for symbol in expired:
        del last_signals[symbol]


async def send_long_signal(symbol, data):
    """Отправка сигнала на ЛОНГ"""
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        
        message = (
            f"🟢 **LONG СИГНАЛ: ПРОБОЙ ПОСЛЕ АККУМУЛЯЦИИ**\n"
            f"📊 **{clean_symbol}** | BingX Futures (1H)\n\n"
            f"📈 **ИМПУЛЬС ВВЕРХ:**\n"
            f"• Рост: **+{data['price_change']}%**\n"
            f"• Объем: **x{data['vol_multiplier']}** от накопления\n"
            f"• Качество тела: **{data['body_quality']}%**\n"
            f"• Объем USDT: **${data['usdt_volume']:,}**\n\n"
            f"📦 **АККУМУЛЯЦИЯ:**\n"
            f"• Диапазон: **{data['accumulation_range']}%**\n"
            f"• Длительность: **{data['accumulation_duration']} часов**\n"
            f"• Уровни: **${data['accumulation_low']:.4f} - ${data['accumulation_high']:.4f}**\n"
            f"• Средний объем: **${data['acc_avg_volume']:,}**\n\n"
            f"💥 **ПРОБОЙ:**\n"
            f"• Над максимумом: **+{data['breakout_strength']}%**\n"
            f"• Цена: **${data['close_price']:.4f}**\n\n"
            f"🎯 **Цели:** ${data['close_price']*1.02:.4f} / ${data['close_price']*1.05:.4f}\n"
            f"🛑 **Стоп:** ${data['accumulation_low']:.4f}\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        )
        await bot.send_message(
            chat_id=CHAT_ID, 
            text=message, 
            parse_mode="Markdown"
        )
        logging.info(f"🟢 LONG: {clean_symbol} +{data['price_change']}% | Объем x{data['vol_multiplier']}")
    except Exception as e:
        logging.error(f"Ошибка отправки LONG сигнала {symbol}: {e}")


async def send_short_signal(symbol, data):
    """Отправка сигнала на ШОРТ"""
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        
        message = (
            f"🔴 **SHORT СИГНАЛ: ПРОБОЙ ПОСЛЕ ДИСТРИБУЦИИ**\n"
            f"📊 **{clean_symbol}** | BingX Futures (1H)\n\n"
            f"📉 **ИМПУЛЬС ВНИЗ:**\n"
            f"• Падение: **-{data['price_change']}%**\n"
            f"• Объем: **x{data['vol_multiplier']}** от дистрибуции\n"
            f"• Качество тела: **{data['body_quality']}%**\n"
            f"• Объем USDT: **${data['usdt_volume']:,}**\n\n"
            f"📦 **ДИСТРИБУЦИЯ:**\n"
            f"• Диапазон: **{data['distribution_range']}%**\n"
            f"• Длительность: **{data['distribution_duration']} часов**\n"
            f"• Уровни: **${data['distribution_low']:.4f} - ${data['distribution_high']:.4f}**\n"
            f"• Средний объем: **${data['dist_avg_volume']:,}**\n\n"
            f"💥 **ПРОБОЙ ВНИЗ:**\n"
            f"• Под минимумом: **-{data['breakdown_strength']}%**\n"
            f"• Цена: **${data['close_price']:.4f}**\n\n"
            f"🎯 **Цели:** ${data['close_price']*0.98:.4f} / ${data['close_price']*0.95:.4f}\n"
            f"🛑 **Стоп:** ${data['distribution_high']:.4f}\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        )
        await bot.send_message(
            chat_id=CHAT_ID, 
            text=message, 
            parse_mode="Markdown"
        )
        logging.info(f"🔴 SHORT: {clean_symbol} -{data['price_change']}% | Объем x{data['vol_multiplier']}")
    except Exception as e:
        logging.error(f"Ошибка отправки SHORT сигнала {symbol}: {e}")


async def scanner_loop():
    """Основной цикл сканирования"""
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Уведомление о старте
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text="🔄 **Сканер BingX 1H запущен**\n\n"
                     "🔍 **Ищу паттерны:**\n"
                     "🟢 LONG: Аккумуляция + Пробой вверх\n"
                     "🔴 SHORT: Дистрибуция + Пробой вниз\n\n"
                     f"📊 Аккумуляция/Дистрибуция: {ACCUMULATION_CANDLES} часов\n"
                     f"📏 Диапазон: <{MAX_ACCUMULATION_RANGE_PCT}%\n"
                     f"📈 Объем пробоя: >x{MIN_IMPULSE_VOL_MULTIPLIER_LONG}\n"
                     f"💹 Мин. движение: >{MIN_IMPULSE_CHANGE_PCT_LONG}%"
            )
        except Exception as e:
            logging.error(f"Не удалось отправить стартовое сообщение: {e}")

        scan_count = 0
        while True:
            try:
                scan_count += 1
                logging.info(f"--- Скан #{scan_count} ---")
                
                symbols = await fetch_bingx_symbols(session)
                if not symbols:
                    logging.warning("Нет пар для сканирования")
                    await asyncio.sleep(30)
                    continue

                current_time = time.time()
                long_signals = 0
                short_signals = 0
                
                # Очистка старых сигналов каждые 10 сканов
                if scan_count % 10 == 0:
                    cleanup_old_signals()

                # Сканируем пары
                for i, symbol in enumerate(symbols):
                    # Проверка cooldown
                    if symbol in last_signals:
                        if current_time - last_signals[symbol] < SIGNAL_COOLDOWN:
                            continue

                    klines = await fetch_klines(session, symbol, 
                                               limit=max(ACCUMULATION_CANDLES, DISTRIBUTION_CANDLES) + 5)
                    if klines:
                        # Проверяем ЛОНГ
                        long_result = detect_accumulation_and_breakout(klines)
                        if long_result:
                            await send_long_signal(symbol, long_result)
                            last_signals[symbol] = current_time
                            long_signals += 1
                            continue  # Если нашли лонг, шорт не ищем
                        
                        # Проверяем ШОРТ
                        short_result = detect_distribution_and_breakdown(klines)
                        if short_result:
                            await send_short_signal(symbol, short_result)
                            last_signals[symbol] = current_time
                            short_signals += 1

                    # Задержка между запросами
                    if i % 10 == 0:
                        await asyncio.sleep(0.1)

                # Логирование результатов скана
                if long_signals > 0 or short_signals > 0:
                    logging.info(f"🎯 Скан #{scan_count}: 🟢{long_signals} LONG | 🔴{short_signals} SHORT")
                else:
                    logging.info(f"Скан #{scan_count} завершен. Сигналов нет.")
                
                # Пауза между полными сканами
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                logging.info("Сканер остановлен")
                break
            except Exception as e:
                logging.error(f"Критическая ошибка в цикле: {e}", exc_info=True)
                await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(scanner_loop())
    except KeyboardInterrupt:
        logging.info("Сканер остановлен пользователем")
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}", exc_info=True)
