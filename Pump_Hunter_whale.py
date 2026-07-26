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

# ===== НАСТРОЙКИ =====
TIMEFRAME_SMALL = "5m"
TIMEFRAME_BIG = "1h"

# Контекст (1H)
ACCUMULATION_HOURS = 6
MAX_ACCUMULATION_RANGE_PCT = 5.0      
MAX_ACCUMULATION_VOLUME_SPIKE = 2.5   

# Детектор импульса (5M + RVOL + EMA + RSI Cross)
MIN_FIRST_CANDLE_CHANGE_PCT = 0.5    # Ловим старт от +0.5%
MIN_RVOL_MULT = 2.5                  # RVOL: x2.5 к средней норме
EMA_PERIOD = 40

# RSI настройки
RSI_FAST_PERIOD = 6
RSI_SLOW_PERIOD = 14
RSI_MIN_LONG = 48.0                  # Чуть снизили, чтобы ловить прямо момент пересечения снизу
RSI_MAX_LONG = 75.0                  # Защита от покупок на самом пике
RSI_MIN_SHORT = 25.0
RSI_MAX_SHORT = 52.0

# Лимиты объемов
MIN_USDT_VOLUME_5M = 10000           
MIN_USDT_VOLUME_1H = 30000

# Лимиты и защита
SIGNAL_COOLDOWN = 3600
MAX_SIGNALS_PER_SCAN = 4
CONTEXT_FRESHNESS = 600   
WATCHLIST_MAX_AGE = 2400   

# Хранилища
last_signals = {}
consolidation_watchlist = {}

scan_counter = 0
error_counter = {"api": 0, "timeout": 0, "parse": 0}


def format_price(price: float) -> str:
    """Умное форматирование цены под любой щиткоин"""
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


# ===== ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ =====
def calculate_ema(prices, period=40):
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def calculate_rsi_series(closes, period):
    """Считает последовательность RSI для проверки пересечений"""
    if len(closes) < period + 2:
        return []
    
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(0.0, change))
        losses.append(max(0.0, -change))

    rsi_series = []
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        rsi_series.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi_series.append(100.0 - (100.0 / (1.0 + rs)))

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            rsi_series.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_series.append(100.0 - (100.0 / (1.0 + rs)))

    return rsi_series


def cleanup_storage():
    current_time = time.time()
    expired_signals = [sym for sym, t in last_signals.items() if current_time - t > SIGNAL_COOLDOWN]
    for sym in expired_signals:
        del last_signals[sym]


async def fetch_bingx_symbols(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == 0 and "data" in data:
                    return [
                        item["symbol"] for item in data["data"] 
                        if item.get("symbol", "").endswith("-USDT") and item.get("status") == 1
                    ]
    except Exception as e:
        error_counter["api"] += 1
        logging.error(f"Ошибка получения списка пар: {e}")
    return []


async def fetch_klines(session, symbol, interval, limit=60):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                res = await resp.json()
                if res.get("code") == 0 and "data" in res:
                    return res["data"]
    except Exception:
        error_counter["api"] += 1
    return None


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
        if avg_volume < MIN_USDT_VOLUME_1H or max(volumes) > avg_volume * MAX_ACCUMULATION_VOLUME_SPIKE:
            return None
        
        return {
            "range_high": range_high,
            "range_low": range_low,
            "avg_volume": avg_volume,
            "range_pct": round(range_pct, 2)
        }
    except Exception:
        return None


def detect_early_momentum(klines_5m, context):
    if not klines_5m or len(klines_5m) < 50:
        return None
    
    try:
        closes = [float(c["close"]) for c in klines_5m]
        volumes = [float(c["volume"]) * float(c["close"]) for c in klines_5m]
        lows = [float(c["low"]) for c in klines_5m]
        highs = [float(c["high"]) for c in klines_5m]
        opens = [float(c["open"]) for c in klines_5m]
        
        ema40 = calculate_ema(closes, EMA_PERIOD)
        if ema40 is None:
            return None

        # Расчёт серий RSI 6 и RSI 14
        rsi6_series = calculate_rsi_series(closes, RSI_FAST_PERIOD)
        rsi14_series = calculate_rsi_series(closes, RSI_SLOW_PERIOD)

        if len(rsi6_series) < 2 or len(rsi14_series) < 2:
            return None

        # Текущее и предыдущее значения RSI
        rsi6_curr, rsi6_prev = rsi6_series[-1], rsi6_series[-2]
        rsi14_curr, rsi14_prev = rsi14_series[-1], rsi14_series[-2]

        # ПРОВЕРКА ПЕРЕСЕЧЕНИЯ RSI 6 x RSI 14
        rsi_bullish_cross = (rsi6_prev <= rsi14_prev) and (rsi6_curr > rsi14_curr)
        rsi_bearish_cross = (rsi6_prev >= rsi14_prev) and (rsi6_curr < rsi14_curr)

        # Расчет RVOL (SMA20 по объемам)
        recent_volumes = volumes[-21:-1]
        avg_vol_20 = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
        if avg_vol_20 <= 0:
            return None
            
        current_vol = volumes[-1]
        rvol = current_vol / avg_vol_20

        current_close = closes[-1]
        current_open = opens[-1]
        change_pct = abs(((current_close - current_open) / current_open) * 100)

        # ДЕТЕКЦИЯ СИГНАЛА
        direction = None
        if current_close > current_open:
            # Лонг: ровно бычье пересечение RSI ИЛИ сильный пробой RSI над 14
            if (change_pct >= MIN_FIRST_CANDLE_CHANGE_PCT and 
                rvol >= MIN_RVOL_MULT and 
                current_close > ema40 and 
                rsi_bullish_cross and
                RSI_MIN_LONG <= rsi6_curr <= RSI_MAX_LONG and
                current_vol >= MIN_USDT_VOLUME_5M):
                direction = "LONG"
        else:
            # Шорт
            if (change_pct >= MIN_FIRST_CANDLE_CHANGE_PCT and 
                rvol >= MIN_RVOL_MULT and 
                current_close < ema40 and 
                rsi_bearish_cross and
                RSI_MIN_SHORT <= rsi6_curr <= RSI_MAX_SHORT and
                current_vol >= MIN_USDT_VOLUME_5M):
                direction = "SHORT"

        if not direction:
            return None

        # Динамический стоп-лосс под 5M-свечу
        if direction == "LONG":
            signal_low = lows[-1]
            stop_loss = signal_low * 0.998
            target1 = current_close + (current_close - stop_loss) * 2.0
            target2 = current_close + (current_close - stop_loss) * 4.0
        else:
            signal_high = highs[-1]
            stop_loss = signal_high * 1.002
            target1 = current_close - (stop_loss - current_close) * 2.0
            target2 = current_close - (stop_loss - current_close) * 4.0

        return {
            "type": direction,
            "first_price": current_open,
            "current_price": current_close,
            "total_move_pct": round(change_pct, 2),
            "impulse_volume": int(current_vol),
            "rvol": round(rvol, 1),
            "rsi6": round(rsi6_curr, 1),
            "rsi14": round(rsi14_curr, 1),
            "ema": ema40,
            "stop_loss": stop_loss,
            "target1": target1,
            "target2": target2,
            "context_range_pct": context["range_pct"],
            "context_range_high": context["range_high"],
            "context_range_low": context["range_low"]
        }
    except Exception as e:
        logging.debug(f"Ошибка детекта: {e}")
        return None


async def send_early_signal(bot, symbol, data):
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        curr_p = data["current_price"]
        stop_p = data["stop_loss"]
        tp1 = data["target1"]
        tp2 = data["target2"]

        if data["type"] == "LONG":
            emoji = "🟢"
            direction_text = "ЛОНГ"
            risk = abs(((curr_p - stop_p) / curr_p) * 100)
            tp1_pct = abs(((tp1 - curr_p) / curr_p) * 100)
            cross_text = "Бычье пересечение RSI(6) ↗️ RSI(14)"
        else:
            emoji = "🔴"
            direction_text = "ШОРТ"
            risk = abs(((stop_p - curr_p) / curr_p) * 100)
            tp1_pct = abs(((curr_p - tp1) / curr_p) * 100)
            cross_text = "Медвежье пересечение RSI(6) ↘️ RSI(14)"

        rr_ratio = tp1_pct / risk if risk > 0 else 0

        message = (
            f"{emoji} **ЗАРОЖДЕНИЕ ИМПУЛЬСА — {direction_text}**\n"
            f"📊 **{clean_symbol}** | BingX\n\n"
            f"🚀 **КРЕСТ И МЕМЕНТУМ:**\n"
            f"• **{cross_text}** 🔥\n"
            f"• RVOL: **x{data['rvol']}** к норме (Аномалия!)\n"
            f"• RSI(6): **{data['rsi6']}** | RSI(14): **{data['rsi14']}**\n"
            f"• EMA(40): **Тренд подтверждён**\n"
            f"• Первичная 5M свеча: **{'+' if data['type'] == 'LONG' else '-'}{data['total_move_pct']}%**\n"
            f"• Объём: **${data['impulse_volume']:,}**\n\n"
            f"📦 **КОНТЕКСТ (1H):**\n"
            f"• Флэт: **{data['context_range_pct']}%**\n"
            f"• Границы: **${format_price(data['context_range_low'])} - ${format_price(data['context_range_high'])}**\n\n"
            f"💰 **ТОРГОВЫЙ ПЛАН:**\n"
            f"• Вход: **${format_price(curr_p)}**\n"
            f"• 🎯 TP1: **${format_price(tp1)}** (+{tp1_pct:.1f}%)\n"
            f"• 🎯 TP2: **${format_price(tp2)}**\n"
            f"• 🛑 Стоп: **${format_price(stop_p)}** (Риск всего ~{risk:.2f}%)\n"
            f"• ⚖️ Risk/Reward: **1:{rr_ratio:.1f}**\n\n"
            f"🕒 {datetime.now().strftime('%H:%M:%S')}"
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

            klines_1h = await fetch_klines(session, symbol, TIMEFRAME_BIG, limit=ACCUMULATION_HOURS + 3)
            if not klines_1h:
                return False
                
            context = check_accumulation_context(klines_1h)
            if not context:
                return False

            klines_5m = await fetch_klines(session, symbol, TIMEFRAME_SMALL, limit=60)
            if not klines_5m:
                return False
                
            result = detect_early_momentum(klines_5m, context)
            if result:
                last_signals[symbol] = current_time
                return await send_early_signal(bot, symbol, result)
            return False
    except Exception as e:
        logging.error(f"Ошибка обработки {symbol}: {e}")
        return False


async def main():
    bot = Bot(token=BOT_TOKEN)
    semaphore = asyncio.Semaphore(12)
    logging.info("🚀 Сканер BingX (RVOL + EMA + RSI Cross) запущен")

    try:
        while True:
            try:
                connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
                async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
                    while True:
                        cleanup_storage()
                        symbols = await fetch_bingx_symbols(session)
                        if not symbols:
                            await asyncio.sleep(20)
                            continue
                        
                        tasks = [check_symbol(session, bot, sym, semaphore) for sym in symbols]
                        await asyncio.gather(*tasks, return_exceptions=True)
                        await asyncio.sleep(15)
            except Exception as e:
                logging.error(f"Перезапуск сессии из-за ошибки: {e}")
                await asyncio.sleep(10)
    finally:
        await bot.session.close()


async def handle(request):
    return web.Response(text="Pump Hunter Bot (RVOL+EMA+RSI Cross) is running!")

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
        logging.info("Сканер остановлен пользователем")
