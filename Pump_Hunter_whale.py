import asyncio
import logging
import time
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
from telegram import Bot

# ==========================
# 1. НАСТРОЙКИ И КОНФИГУРАЦИЯ
# ==========================
TELEGRAM_BOT_TOKEN = "ВАШ_ТЕЛЕГРАМ_ТОКЕН"
TELEGRAM_CHAT_ID = "ВАШ_CHAT_ID"

# Фильтры монет
MIN_PRICE = 0.0001
MAX_PRICE = 10.0
MIN_24H_VOLUME_USDT = 1_000_000
MAX_24H_CHANGE_PCT = 30.0

# Настройки скоринга и сетевой защиты
MIN_SCORE_THRESHOLD = 52
MIN_CANDLE_IMPULSE_PCT = 0.8     # ПРАВКА 4: Минимальный рост 5m свечи для исключения шума
SIGNAL_COOLDOWN = 1800           # Кулдаун сигналов 30 минут (1800 сек)
MAX_CONCURRENT_REQUESTS = 15     # Безопасный лимит потоков для BingX API

# Инициализация биржи BingX Futures (Swap)
exchange = ccxt.bingx({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
    }
})

bot = Bot(token=TELEGRAM_BOT_TOKEN)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Хранилища состояния
last_signals = {}  # {symbol: timestamp}
oi_history = {}    # {symbol: [(candle_timestamp, oi_value), ...]}
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


# ==========================
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================
def calculate_ema_pandas(prices, period):
    """Точный расчет EMA через Pandas"""
    if len(prices) < period:
        return prices[-1]
    series = pd.Series(prices)
    ema = series.ewm(span=period, adjust=False).mean()
    return float(ema.iloc[-1])


def check_dynamic_accumulation(klines_1h):
    """
    ПРАВКА 1: Градация ширины накопления (3.5% / 5.0% / 6.5%)
    """
    windows = [3, 4, 6, 8]
    for w in windows:
        if len(klines_1h) < w + 1:
            continue
        recent = klines_1h[-(w + 1):-1]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        
        range_high = max(highs)
        range_low = min(lows)
        flat_width = ((range_high - range_low) / range_low) * 100
        
        if flat_width <= 6.5:
            return True, round(flat_width, 2), range_high, range_low
            
    return False, 0.0, 0.0, 0.0


def update_and_check_oi(symbol, curr_oi, last_candle_time):
    """
    ПРАВКА 3: Привязка истории OI к метке времени последней свечи (last_candle_time)
    """
    if curr_oi == 0:
        return False, 0.0

    if symbol not in oi_history:
        oi_history[symbol] = []
    
    history = oi_history[symbol]
    
    # Обновляем OI только если появилась новая свеча или изменился timestamp
    if not history or history[-1][0] != last_candle_time:
        history.append((last_candle_time, curr_oi))
        if len(history) > 4:
            history.pop(0)
            
    if len(history) >= 2:
        prev_oi = history[-2][1]
        oi_delta_pct = ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0
        
        is_3_growth = False
        if len(history) >= 3:
            is_3_growth = (history[-1][1] > history[-2][1] > history[-3][1])
            
        if oi_delta_pct >= 0.15 or is_3_growth:
            return True, round(oi_delta_pct, 2)
            
    return False, 0.0


# ==========================
# 3. SCORE-МОДУЛЬ
# ==========================
def evaluate_accumulation_signal(klines_1h, klines_5m, is_oi_valid, oi_delta_pct):
    if len(klines_5m) < 60 or len(klines_1h) < 9:
        return None

    # Свечи 5m
    curr_5m = klines_5m[-1]
    prev_5m = klines_5m[-2]
    prev2_5m = klines_5m[-3]

    open_5m, high_5m, low_5m, close_5m = float(curr_5m[1]), float(curr_5m[2]), float(curr_5m[3]), float(curr_5m[4])
    candle_change_pct = ((close_5m - open_5m) / open_5m) * 100

    # ПРАВКА 4: Исключаем слишком маленькие / шумные свечи (< 0.8%)
    if candle_change_pct < MIN_CANDLE_IMPULSE_PCT:
        return None

    # 1. Накопление
    is_flat, flat_width, range_high_1h, range_low_1h = check_dynamic_accumulation(klines_1h)
    if not is_flat:
        return None

    vol_5m = float(curr_5m[5]) * close_5m
    vol_p1 = float(prev_5m[5]) * float(prev_5m[4])
    vol_p2 = float(prev2_5m[5]) * float(prev2_5m[4])

    candle_range = high_5m - low_5m

    # Метрики
    prev_20_5m = klines_5m[-21:-1]
    closes_5m_all = [float(k[4]) for k in klines_5m]
    avg_vol_20 = np.mean([float(k[5]) * float(k[4]) for k in prev_20_5m])
    
    rvol = vol_5m / avg_vol_20 if avg_vol_20 > 0 else 1.0

    # EMA 20/40
    ema20 = calculate_ema_pandas(closes_5m_all, 20)
    ema40 = calculate_ema_pandas(closes_5m_all, 40)

    score = 0
    reasons = []

    # A. Пробой / Поджим
    distance_to_high = ((range_high_1h - close_5m) / close_5m) * 100
    if close_5m >= range_high_1h:
        score += 25
        reasons.append("Пробой 1H флэта (+25)")
    elif 0 <= distance_to_high <= 0.6:
        score += 20
        reasons.append(f"Поджим к уровню {round(distance_to_high, 2)}% (+20)")

    # B. Динамика объема
    rvol_p2 = vol_p2 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    rvol_p1 = vol_p1 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    
    if rvol > rvol_p1 > rvol_p2 and rvol >= 1.3:
        score += 15
        reasons.append(f"Волна объема: {round(rvol_p2,1)} -> {round(rvol_p1,1)} -> {round(rvol,1)} (+15)")
    elif rvol >= 1.5:
        score += 10
        reasons.append(f"Всплеск RVOL x{round(rvol,1)} (+10)")

    # C. Open Interest
    if is_oi_valid:
        score += 15
        reasons.append(f"Рост OI (+15, {oi_delta_pct}%)")

    # D. Тренд EMA
    if ema20 > ema40 and close_5m > ema20:
        score += 10
        reasons.append("EMA20 > EMA40 (+10)")

    # E. Ускорение цены
    price_delta_now = close_5m - float(prev_5m[4])
    price_delta_prev = float(prev_5m[4]) - float(prev2_5m[4])
    if price_delta_now > price_delta_prev > 0:
        score += 10
        reasons.append("Ускорение шага цены (+10)")

    # F. Сильное закрытие
    if candle_range > 0 and close_5m >= (high_5m - (candle_range * 0.25)):
        score += 10
        reasons.append("Закрытие под High (+10)")

    # G. ПРАВКА 1: Динамическая градация за ширину базы
    if flat_width <= 3.5:
        score += 15
        reasons.append(f"Очень узкая база {flat_width}% (+15)")
    elif 3.5 < flat_width <= 5.0:
        score += 10
        reasons.append(f"Умеренная база {flat_width}% (+10)")
    elif 5.0 < flat_width <= 6.5:
        score += 5
        reasons.append(f"Широкая база {flat_width}% (+5)")

    return {
        "score": score,
        "reasons": reasons,
        "price": close_5m,
        "rvol": round(rvol, 1),
        "flat_width": flat_width,
        "change_5m": round(candle_change_pct, 2)
    }


# ==========================
# 4. АНАЛИЗАТОР (BINGX)
# ==========================
async def check_orderbook_density(symbol):
    try:
        orderbook = await exchange.fetch_order_book(symbol, limit=20)
        bids_vol = sum([b[1] for b in orderbook['bids']])
        asks_vol = sum([a[1] for a in orderbook['asks']])
        if asks_vol > 0 and (bids_vol / asks_vol) >= 1.4:
            return True, round(bids_vol / asks_vol, 2)
    except Exception as e:
        logging.debug(f"Orderbook error on {symbol}: {e}") # ПРАВКА 5: Логирование ошибок
    return False, 0.0


async def process_symbol(symbol, ticker):
    async with semaphore:
        try:
            now = time.time()
            
            if symbol in last_signals and (now - last_signals[symbol]) < SIGNAL_COOLDOWN:
                return

            change_24h = ticker.get('percentage', 0)
            if change_24h is None or change_24h >= MAX_24H_CHANGE_PCT:
                return

            klines_1h = await exchange.fetch_ohlcv(symbol, timeframe='1h', limit=15)
            klines_5m = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=60)

            if not klines_5m or not klines_1h:
                return

            # ПРАВКА 3: Берем timestamp последней закрывающейся свечи
            last_candle_time = klines_5m[-1][0]

            curr_oi = 0.0
            try:
                oi_data = await exchange.fetch_open_interest(symbol)
                curr_oi = float(oi_data.get('openInterestAmount', 0) or oi_data.get('openInterestValue', 0))
            except Exception as e:
                logging.debug(f"OI error on {symbol}: {e}") # ПРАВКА 5

            is_oi_valid, oi_delta_pct = update_and_check_oi(symbol, curr_oi, last_candle_time)

            # Предварительный скоринг
            signal = evaluate_accumulation_signal(klines_1h, klines_5m, is_oi_valid, oi_delta_pct)
            if not signal or signal['score'] < 40:
                return

            # Funding Rate
            funding_rate = 0.0
            try:
                funding_info = await exchange.fetch_funding_rate(symbol)
                funding_rate = float(funding_info.get('fundingRate', 0.0))
            except Exception as e:
                logging.debug(f"Funding error on {symbol}: {e}") # ПРАВКА 5

            if funding_rate <= 0:
                signal['score'] += 10
                signal['reasons'].append(f"Отрицательный/Нулевой Funding ({round(funding_rate*100, 3)}%) (+10)")

            # Orderbook
            is_orderbook_bullish, ratio = await check_orderbook_density(symbol)
            if is_orderbook_bullish:
                signal['score'] += 10
                signal['reasons'].append(f"Стакан: Преобладание бидов x{ratio} (+10)")

            # Отправка сигнала
            if signal['score'] >= MIN_SCORE_THRESHOLD:
                last_signals[symbol] = now

                reasons_str = "\n• " + "\n• ".join(signal['reasons'])
                msg = (
                    f"🎯 **BINGX: РАННИЙ СИГНАЛ (Score: {signal['score']}/100)**\n"
                    f"📌 **Монета:** `{symbol}`\n"
                    f"💵 **Цена:** `${signal['price']}`\n"
                    f"📊 **Импульс 5m:** `{signal['change_5m']}%` (24h: `{round(change_24h, 1)}%`)\n"
                    f"🔥 **RVOL:** `x{signal['rvol']}` | **База:** `{signal['flat_width']}%` \n\n"
                    f"🧠 **Факторы входа:**{reasons_str}"
                )
                logging.info(f"Сигнал по BingX [{symbol}]! Score: {signal['score']}")
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="Markdown")

        except Exception as e:
            logging.debug(f"Process error on {symbol}: {e}") # ПРАВКА 5


async def main():
    logging.info("Сканер запущен на бирже BingX Futures (Версия 10/10)...")
    await exchange.load_markets()

    while True:
        try:
            tickers = await exchange.fetch_tickers()
            valid_symbols = []

            for symbol, ticker in tickers.items():
                if '/USDT' in symbol:
                    price = ticker.get('last', 0)
                    vol_usdt = ticker.get('quoteVolume', 0)
                    
                    if price and MIN_PRICE <= price <= MAX_PRICE and vol_usdt and vol_usdt >= MIN_24H_VOLUME_USDT:
                        valid_symbols.append((symbol, ticker))

            tasks = [process_symbol(sym, tick) for sym, tick in valid_symbols]
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.error(f"Ошибка главного цикла: {e}")

        await asyncio.sleep(10)


if __name__ == "__main__":
    # ПРАВКА 6: Корректное закрытие асинхронной сессии CCXT при завершении работы
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Сканер остановлен пользователем.")
    finally:
        asyncio.run(exchange.close())
