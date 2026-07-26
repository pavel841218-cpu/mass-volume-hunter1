import asyncio
import os
import logging
import time
import aiohttp
from aiogram import Bot
from datetime import datetime
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("Не установлены PUMP_BOT_TOKEN или PUMP_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

# ===== НАСТРОЙКИ ФИЛЬТРОВ И НАКОПЛЕНИЯ =====
TIMEFRAME_SMALL = "5m"
TIMEFRAME_BIG = "1h"

# Фильтры фьючерсного рынка
MIN_PRICE = 0.001                     # Минимальная цена монеты
MAX_PRICE = 10.0                      # Максимальная цена монеты
MIN_24H_VOLUME_USDT = 50000.0         # Мин. суточный объем (50k USDT)

# Накопление (8-20 часов)
ACCUMULATION_HOURS = 10
MAX_ACCUMULATION_RANGE_PCT = 7.0      # Ширина флэта до 7%
MAX_ACCUMULATION_VOLUME_SPIKE = 5.0   

# Пороги Pre-Breakout
PRE_BREAKOUT_DIST_PCT = 0.8           # Не далее 0.8% от границы флэта
MIN_HARD_RVOL = 1.3                   # Относительный объем (RVOL)
MIN_SCORE_TO_SEND = 45                # Минимальный балл для отправки

# Защита от спама и память
SIGNAL_COOLDOWN = 1200                # 20 минут КД на монету
last_signals = {}
oi_history = {}                       # {symbol: [(timestamp, oi_value)]}


def format_price(price: float) -> str:
    if price is None or price == 0:
        return "0.00"
    if price >= 100:
        return f"{price:.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    elif price >= 0.001:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"


def calculate_ema(prices, period=40):
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def calculate_rsi(closes, period=6):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(0.0, change))
        losses.append(max(0.0, -change))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def cleanup_storage():
    current_time = time.time()
    expired_signals = [sym for sym, t in last_signals.items() if current_time - t > SIGNAL_COOLDOWN]
    for sym in expired_signals:
        del last_signals[sym]
        
    for sym in list(oi_history.keys()):
        oi_history[sym] = [(t, val) for t, val in oi_history[sym] if current_time - t <= 1800]
        if not oi_history[sym]:
            del oi_history[sym]


async def fetch_bingx_symbols(session):
    """
    Получает все тикеры с фьючерсного рынка BingX (Swap/Futures)
    и фильтрует по условиям:
    - Монета торгуется к USDT
    - Цена от MIN_PRICE (0.001) до MAX_PRICE (10.0)
    - Объем за 24ч >= MIN_24H_VOLUME_USDT (50 000 USDT)
    """
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == 0 and "data" in data:
                    valid_symbols = []
                    for item in data["data"]:
                        symbol = item.get("symbol", "")
                        if not symbol.endswith("-USDT"):
                            continue
                        
                        try:
                            last_price = float(item.get("lastPrice", 0))
                            quote_volume = float(item.get("quoteVolume", 0)) # Суточный объем в USDT
                            
                            # Проверка фильтров цены и объема
                            if MIN_PRICE <= last_price <= MAX_PRICE and quote_volume >= MIN_24H_VOLUME_USDT:
                                valid_symbols.append(symbol)
                        except (ValueError, TypeError):
                            continue
                            
                    return valid_symbols
    except Exception as e:
        logging.error(f"Ошибка получения списка фьючерсных пар: {e}")
    return []


async def fetch_klines(session, raw_symbol, interval, limit=60):
    clean_symbol = raw_symbol.replace("-", "")
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": clean_symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                res = await resp.json()
                if res.get("code") == 0 and "data" in res:
                    return res["data"]
    except Exception:
        pass
    return None


async def fetch_and_store_oi(session, raw_symbol):
    clean_symbol = raw_symbol.replace("-", "")
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/openInterest"
    params = {"symbol": clean_symbol}
    curr_time = time.time()
    
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status == 200:
                res = await resp.json()
                if res.get("code") == 0 and "data" in res:
                    curr_oi = float(res["data"].get("openInterest", 0))
                    if curr_oi <= 0:
                        return 0.0

                    if raw_symbol not in oi_history:
                        oi_history[raw_symbol] = []
                    
                    oi_history[raw_symbol].append((curr_time, curr_oi))

                    past_oi = None
                    for t, val in oi_history[raw_symbol]:
                        if 240 <= (curr_time - t) <= 900:  # Интервал 4-15 минут
                            past_oi = val
                            break
                    
                    if past_oi and past_oi > 0:
                        oi_change_pct = ((curr_oi - past_oi) / past_oi) * 100
                        return round(oi_change_pct, 2)
    except Exception:
        pass
    return 0.0


def check_accumulation_context(klines_1h):
    if not klines_1h or len(klines_1h) < ACCUMULATION_HOURS + 1:
        return None
    
    try:
        context_candles = klines_1h[-(ACCUMULATION_HOURS + 1):-1]
        highs = [float(c["high"]) for c in context_candles]
        lows = [float(c["low"]) for c in context_candles]
        volumes = [float(c["volume"]) * float(c["close"]) for c in context_candles]
        
        range_high, range_low = max(highs), min(lows)
        if range_low <= 0:
            return None
            
        range_pct = ((range_high - range_low) / range_low) * 100
        if range_pct > MAX_ACCUMULATION_RANGE_PCT:
            return None
        
        avg_volume = sum(volumes) / len(volumes)
        if max(volumes) > avg_volume * MAX_ACCUMULATION_VOLUME_SPIKE:
            return None
        
        return {
            "range_high": range_high,
            "range_low": range_low,
            "avg_volume": avg_volume,
            "range_pct": round(range_pct, 2)
        }
    except Exception:
        return None


def analyze_live_setup(klines_5m, context, oi_change_pct=0.0):
    if not klines_5m or len(klines_5m) < 45:
        return None
    
    try:
        closes = [float(c["close"]) for c in klines_5m]
        volumes = [float(c["volume"]) * float(c["close"]) for c in klines_5m]
        lows = [float(c["low"]) for c in klines_5m]
        highs = [float(c["high"]) for c in klines_5m]
        opens = [float(c["open"]) for c in klines_5m]
        times = [int(c["time"]) for c in klines_5m]

        # 1. Расчет Live Volume для текущей свечи
        now_ms = int(time.time() * 1000)
        candle_start_ms = times[-1]
        elapsed_seconds = max(5, (now_ms - candle_start_ms) // 1000)
        if elapsed_seconds > 300:
            elapsed_seconds = 300

        raw_curr_vol = volumes[-1]
        projected_vol = raw_curr_vol * (300 / elapsed_seconds)

        # Базовый расчет RVOL
        recent_volumes = volumes[-21:-1]
        avg_vol_20 = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 1.0
        rvol = projected_vol / avg_vol_20 if avg_vol_20 > 0 else 0.0

        current_price = closes[-1]
        range_high = context["range_high"]
        range_low = context["range_low"]

        # Дистанция до сопротивления / поддержки
        dist_to_high_pct = ((range_high - current_price) / current_price) * 100
        dist_to_low_pct = ((current_price - range_low) / current_price) * 100

        # Определение типа состояния
        is_breakout_long = current_price > range_high
        is_breakout_short = current_price < range_low
        
        is_pre_breakout_long = (0.0 <= dist_to_high_pct <= PRE_BREAKOUT_DIST_PCT) and (closes[-1] > opens[-1])
        is_pre_breakout_short = (0.0 <= dist_to_low_pct <= PRE_BREAKOUT_DIST_PCT) and (closes[-1] < opens[-1])

        if not (is_breakout_long or is_breakout_short or is_pre_breakout_long or is_pre_breakout_short):
            return None

        # Формирование типа сетапа
        if is_breakout_long:
            setup_type, direction = "BREAKOUT", "LONG"
        elif is_breakout_short:
            setup_type, direction = "BREAKOUT", "SHORT"
        elif is_pre_breakout_long:
            setup_type, direction = "PRE-BREAKOUT", "LONG"
        else:
            setup_type, direction = "PRE-BREAKOUT", "SHORT"

        # 2. Скоринг (0 - 100 баллов)
        score = 0
        score_details = []

        # Качество поджима к уровню (+25 баллов)
        if setup_type == "PRE-BREAKOUT":
            score += 25
            dist_val = dist_to_high_pct if direction == "LONG" else dist_to_low_pct
            score_details.append(f"Поджим к уровню: **{dist_val:.2f}%** (+25)")
        else:
            score += 20
            score_details.append("Подтвержденный пробой уровня (+20)")

        # Рост Открытого Интереса (+25 баллов)
        if oi_change_pct >= 1.0:
            score += 25
            score_details.append(f"Сильный приток OI: **+{oi_change_pct}%** (+25)")
        elif oi_change_pct >= 0.4:
            score += 15
            score_details.append(f"Рост OI: **+{oi_change_pct}%** (+15)")

        # Live RVOL (+25 баллов)
        if rvol >= 2.5:
            score += 25
            score_details.append(f"Прогноз RVOL: **x{round(rvol, 1)}** (+25)")
        elif rvol >= MIN_HARD_RVOL:
            score += 15
            score_details.append(f"Прогноз RVOL: **x{round(rvol, 1)}** (+15)")

        # EMA40 (+15 баллов)
        ema40 = calculate_ema(closes, 40)
        if ema40:
            if direction == "LONG" and current_price > ema40:
                score += 15
                score_details.append("Тренд выше EMA40 (+15)")
            elif direction == "SHORT" and current_price < ema40:
                score += 15
                score_details.append("Тренд ниже EMA40 (+15)")

        # RSI (+10 баллов)
        rsi6 = calculate_rsi(closes, 6)
        rsi14 = calculate_rsi(closes, 14)
        if direction == "LONG" and rsi6 > rsi14:
            score += 10
            score_details.append(f"Бычий RSI({round(rsi6,1)}) (+10)")
        elif direction == "SHORT" and rsi6 < rsi14:
            score += 10
            score_details.append(f"Медвежий RSI({round(rsi6,1)}) (+10)")

        if score < MIN_SCORE_TO_SEND:
            return None

        # Торговый план
        if direction == "LONG":
            stop_loss = range_low if setup_type == "PRE-BREAKOUT" else lows[-1] * 0.998
            target1 = current_price + (current_price - stop_loss) * 2.0
            target2 = current_price + (current_price - stop_loss) * 4.0
        else:
            stop_loss = range_high if setup_type == "PRE-BREAKOUT" else highs[-1] * 1.002
            target1 = current_price - (stop_loss - current_price) * 2.0
            target2 = current_price - (stop_loss - current_price) * 4.0

        return {
            "setup_type": setup_type,
            "direction": direction,
            "current_price": current_price,
            "impulse_volume": int(projected_vol),
            "rvol": round(rvol, 1),
            "score": score,
            "score_details": score_details,
            "stop_loss": stop_loss,
            "target1": target1,
            "target2": target2,
            "elapsed_seconds": elapsed_seconds,
            "oi_change_pct": oi_change_pct,
            "context_range_pct": context["range_pct"],
            "context_range_high": context["range_high"],
            "context_range_low": context["range_low"]
        }
    except Exception as e:
        logging.debug(f"Ошибка анализа: {e}")
        return None


async def send_signal_message(bot, symbol, data):
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        clean_ticker = symbol.replace("-", "")  # Чистый тикер для копирования (BTCUSDT)
        curr_p = data["current_price"]
        stop_p = data["stop_loss"]
        tp1 = data["target1"]
        tp2 = data["target2"]

        is_long = data["direction"] == "LONG"
        emoji = "🔥" if data["setup_type"] == "PRE-BREAKOUT" else "⚡"
        dir_emoji = "🟢" if is_long else "🔴"
        
        risk = abs(((curr_p - stop_p) / curr_p) * 100) if curr_p > 0 else 0
        tp1_pct = abs(((tp1 - curr_p) / curr_p) * 100) if curr_p > 0 else 0
        rr_ratio = tp1_pct / risk if risk > 0 else 0

        details_str = "\n• ".join(data["score_details"])

        message = (
            f"{emoji}{dir_emoji} **СИГНАЛ: {data['setup_type']} {data['direction']}**\n"
            f"📊 **{clean_symbol}** | BingX Futures\n"
            f"⏱ Тайминг: **{data['elapsed_seconds']} сек в текущей свече**\n"
            f"⭐ Рейтинг вероятности: **{data['score']}/100**\n\n"
            f"🎯 **МЕТРИКИ И ДРАЙВЕРЫ:**\n"
            f"• {details_str}\n"
            f"• Est. Vol (5M): **${data['impulse_volume']:,}**\n\n"
            f"📦 **ФЛЭТ ({ACCUMULATION_HOURS}H):**\n"
            f"• Диапазон: **{data['context_range_pct']}%**\n"
            f"• Уровни: **${format_price(data['context_range_low'])} - ${format_price(data['context_range_high'])}**\n\n"
            f"💰 **ТОРГОВЫЙ ПЛАН:**\n"
            f"• Вход: **${format_price(curr_p)}**\n"
            f"• 🎯 TP1: **${format_price(tp1)}** (+{tp1_pct:.1f}%)\n"
            f"• 🎯 TP2: **${format_price(tp2)}**\n"
            f"• 🛑 Стоп: **${format_price(stop_p)}** (~{risk:.2f}%)\n"
            f"• ⚖️ Risk/Reward: **1:{rr_ratio:.1f}**\n\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S')}\n"
            f"📋 `{clean_ticker}`"
        )

        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения {symbol}: {e}")
        return False


async def check_symbol(session, bot, symbol, semaphore):
    try:
        async with semaphore:
            current_time = time.time()
            if symbol in last_signals and (current_time - last_signals[symbol] < SIGNAL_COOLDOWN):
                return False

            oi_change_pct = await fetch_and_store_oi(session, symbol)

            klines_1h = await fetch_klines(session, symbol, TIMEFRAME_BIG, limit=ACCUMULATION_HOURS + 3)
            if not klines_1h:
                return False
                
            context = check_accumulation_context(klines_1h)
            if not context:
                return False

            klines_5m = await fetch_klines(session, symbol, TIMEFRAME_SMALL, limit=50)
            if not klines_5m:
                return False
                
            result = analyze_live_setup(klines_5m, context, oi_change_pct)
            if result:
                last_signals[symbol] = current_time
                return await send_signal_message(bot, symbol, result)
            return False
    except Exception as e:
        logging.error(f"Ошибка обработки {symbol}: {e}")
        return False


async def main():
    bot = Bot(token=BOT_TOKEN)
    semaphore = asyncio.Semaphore(15)
    logging.info("🚀 Запущен Pre-Breakout & Live Scanner Engine (Futures Only)")

    connector = aiohttp.TCPConnector(limit=25, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
        while True:
            try:
                cleanup_storage()
                symbols = await fetch_bingx_symbols(session)
                if not symbols:
                    await asyncio.sleep(10)
                    continue
                
                tasks = [check_symbol(session, bot, sym, semaphore) for sym in symbols]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(10)
            except Exception as e:
                logging.error(f"Ошибка в главном цикле: {e}")
                await asyncio.sleep(10)


async def handle(request):
    return web.Response(text="Pre-Breakout Engine is running!")

async def start_dummy_server():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/health', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def run_all():
    await asyncio.gather(
        start_dummy_server(),
        main()
    )

if __name__ == "__main__":
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logging.info("Сканер остановлен")
