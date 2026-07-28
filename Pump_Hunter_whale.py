import asyncio
import os
import logging
import time
import aiohttp
import numpy as np
from aiogram import Bot
from datetime import datetime
from aiohttp import web

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("Не установлены PUMP_BOT_TOKEN или PUMP_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

# 🚫 ЧЕРНЫЙ СПИСОК: Исключаем акции США, ETF и токенизированные активы
EXCLUDED_TICKERS = {
    "SOXS", "SOXL", "SQQQ", "TQQQ", "SPY", "QQQ", "NVDA", "TSLA", 
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "AMD", "COIN", "MSTR"
}

# ===== НАСТРОЙКИ МОДУЛЯ 1: EARLY_BOTTOM 🔥 =====
EARLY_ACCUMULATION_HOURS = 3          # Накопление за 3 часа
EARLY_MIN_RVOL = 1.08                 # Мягкий порог RVOL для самого старта
EARLY_OI_CHANGE_MIN = 0.08            # Мягкий порог OI (+0.08%)
MAX_COMPRESSION_ATR_PCT = 0.008       # Узкий диапазон сжатия (до 0.8% ATR)
IMPULSE_BODY_MULT = 2.5               # Тело свечи больше среднего в 2.5 раза
IMPULSE_VOL_MULT = 2.0                # Объём больше среднего в 2.0 раза

# ===== НАСТРОЙКИ МОДУЛЯ 2: PRE-BREAKOUT ⚡ =====
TIMEFRAME_SMALL = "5m"
TIMEFRAME_BIG = "1h"
PRE_BREAKOUT_DIST_PCT = 2.5           # Расширенный поджим (до 2.5% от границы)
MIN_HARD_RVOL = 1.1                   # Мягкий RVOL
MIN_SCORE_TO_SEND = 35                # Сниженный балл для ранней отправки

# ===== НАСТРОЙКИ МОДУЛЯ 3: ROCKET 🚀 =====
ROCKET_RVOL_THRESHOLD = 2.5           # Снижен порог RVOL для ракеты
ROCKET_PRICE_CHANGE_PCT = 2.5         # Минимальный скачок цены (+2.5%)

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


def calculate_ema(closes, period=40):
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
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
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/contracts"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("code") == 0 and "data" in data:
                    symbols = []
                    for item in data["data"]:
                        sym = item.get("symbol", "")
                        if sym.endswith("-USDT") and item.get("status") == 1:
                            base_asset = sym.replace("-USDT", "").upper()
                            if base_asset not in EXCLUDED_TICKERS:
                                symbols.append(sym)
                    return symbols
    except Exception as e:
        logging.error(f"Ошибка получения списка пар: {e}")
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
                        if 240 <= (curr_time - t) <= 900:
                            past_oi = val
                            break
                    
                    if past_oi and past_oi > 0:
                        oi_change_pct = ((curr_oi - past_oi) / past_oi) * 100
                        return round(oi_change_pct, 2)
    except Exception:
        pass
    return 0.0


# ===== МОДУЛЬ 1: EARLY BOTTOM DETECTOR (САМОЕ ДНО) =====
def check_early_bottom(klines_5m, current_oi_change_pct, current_rvol, price, ema40):
    if len(klines_5m) < 30:
        return False, {}

    closes = np.array([float(k['close']) for k in klines_5m])
    highs = np.array([float(k['high']) for k in klines_5m])
    lows = np.array([float(k['low']) for k in klines_5m])
    opens = np.array([float(k['open']) for k in klines_5m])
    volumes = np.array([float(k['volume']) * float(k['close']) for k in klines_5m])

    curr_open = opens[-1]
    curr_close = closes[-1]
    curr_vol = volumes[-1]

    if curr_close <= curr_open:
        return False, {}

    curr_body = curr_close - curr_open

    prev_bodies = np.abs(closes[-21:-1] - opens[-21:-1])
    avg_body_20 = np.mean(prev_bodies)
    
    prev_vols = volumes[-21:-1]
    avg_vol_20 = np.mean(prev_vols)

    ranges_10 = (highs[-11:-1] - lows[-11:-1]) / closes[-11:-1]
    avg_range_compression = np.mean(ranges_10)

    is_compressed = avg_range_compression <= MAX_COMPRESSION_ATR_PCT
    is_impulse_body = curr_body >= (avg_body_20 * IMPULSE_BODY_MULT)
    is_impulse_vol = curr_vol >= (avg_vol_20 * IMPULSE_VOL_MULT)
    
    near_ema = (ema40 is not None) and (price >= (ema40 * 0.995))

    if (
        is_compressed and 
        is_impulse_body and 
        is_impulse_vol and 
        near_ema and 
        current_rvol >= EARLY_MIN_RVOL and 
        current_oi_change_pct >= EARLY_OI_CHANGE_MIN
    ):
        body_ratio = round(curr_body / avg_body_20, 2) if avg_body_20 > 0 else 0
        vol_ratio = round(curr_vol / avg_vol_20, 2) if avg_vol_20 > 0 else 0
        
        metrics = {
            "module": "EARLY_BOTTOM",
            "setup_type": "FIRST IMPULSE",
            "direction": "LONG",
            "current_price": price,
            "compression_pct": round(avg_range_compression * 100, 2),
            "body_mult": body_ratio,
            "vol_mult": vol_ratio,
            "rvol": round(current_rvol, 2),
            "oi_change": round(current_oi_change_pct, 3),
            "impulse_volume": int(curr_vol)
        }
        return True, metrics

    return False, {}


# ===== МОДУЛЬ 2: PRE-BREAKOUT =====
def check_accumulation_context(klines_1h):
    if not klines_1h or len(klines_1h) < EARLY_ACCUMULATION_HOURS + 1:
        return None
    
    try:
        context_candles = klines_1h[-(EARLY_ACCUMULATION_HOURS + 1):-1]
        highs = [float(c["high"]) for c in context_candles]
        lows = [float(c["low"]) for c in context_candles]
        volumes = [float(c["volume"]) * float(c["close"]) for c in context_candles]
        
        range_high, range_low = max(highs), min(lows)
        if range_low <= 0:
            return None
            
        range_pct = ((range_high - range_low) / range_low) * 100
        if range_pct > 8.0:
            return None
        
        avg_volume = sum(volumes) / len(volumes) if len(volumes) > 0 else 1.0
        
        return {
            "range_high": range_high,
            "range_low": range_low,
            "avg_volume": avg_volume,
            "range_pct": round(range_pct, 2)
        }
    except Exception:
        return None


def analyze_live_setup(klines_5m, context, oi_change_pct=0.0, rvol=1.0):
    if not klines_5m or len(klines_5m) < 30:
        return None
    
    try:
        closes = [float(c["close"]) for c in klines_5m]
        lows = [float(c["low"]) for c in klines_5m]
        highs = [float(c["high"]) for c in klines_5m]
        opens = [float(c["open"]) for c in klines_5m]

        current_price = closes[-1]
        range_high = context["range_high"]
        range_low = context["range_low"]

        dist_to_high_pct = ((range_high - current_price) / current_price) * 100
        dist_to_low_pct = ((current_price - range_low) / current_price) * 100

        is_breakout_long = current_price > range_high
        is_breakout_short = current_price < range_low
        
        is_pre_breakout_long = (0.0 <= dist_to_high_pct <= PRE_BREAKOUT_DIST_PCT) and (closes[-1] > opens[-1])
        is_pre_breakout_short = (0.0 <= dist_to_low_pct <= PRE_BREAKOUT_DIST_PCT) and (closes[-1] < opens[-1])

        if not (is_breakout_long or is_breakout_short or is_pre_breakout_long or is_pre_breakout_short):
            return None

        if is_breakout_long:
            setup_type, direction = "BREAKOUT", "LONG"
        elif is_breakout_short:
            setup_type, direction = "BREAKOUT", "SHORT"
        elif is_pre_breakout_long:
            setup_type, direction = "PRE-BREAKOUT", "LONG"
        else:
            setup_type, direction = "PRE-BREAKOUT", "SHORT"

        score = 0
        score_details = []

        if setup_type == "PRE-BREAKOUT":
            score += 25
            dist_val = dist_to_high_pct if direction == "LONG" else dist_to_low_pct
            score_details.append(f"Поджим к уровню: **{dist_val:.2f}%** (+25)")
        else:
            score += 20
            score_details.append("Подтвержденный пробой уровня (+20)")

        if oi_change_pct >= 0.30:
            score += 20
            score_details.append(f"Приток OI: **+{oi_change_pct}%** (+20)")
        elif oi_change_pct >= 0.10:
            score += 10
            score_details.append(f"Приток OI: **+{oi_change_pct}%** (+10)")

        if rvol >= 2.0:
            score += 25
            score_details.append(f"Прогноз RVOL: **x{round(rvol, 1)}** (+25)")
        elif rvol >= MIN_HARD_RVOL:
            score += 15
            score_details.append(f"Прогноз RVOL: **x{round(rvol, 1)}** (+15)")

        ema40 = calculate_ema(closes, 40)
        if ema40:
            if direction == "LONG" and current_price >= (ema40 * 0.995):
                score += 15
                score_details.append("Цена около/выше EMA40 (+15)")
            elif direction == "SHORT" and current_price <= (ema40 * 1.005):
                score += 15
                score_details.append("Цена около/ниже EMA40 (+15)")

        if score < MIN_SCORE_TO_SEND:
            return None

        if direction == "LONG":
            stop_loss = range_low if setup_type == "PRE-BREAKOUT" else lows[-1] * 0.998
            target1 = current_price + (current_price - stop_loss) * 2.0
            target2 = current_price + (current_price - stop_loss) * 4.0
        else:
            stop_loss = range_high if setup_type == "PRE-BREAKOUT" else highs[-1] * 1.002
            target1 = current_price - (stop_loss - current_price) * 2.0
            target2 = current_price - (stop_loss - current_price) * 4.0

        return {
            "module": "PRE_BREAKOUT",
            "setup_type": setup_type,
            "direction": direction,
            "current_price": current_price,
            "rvol": round(rvol, 1),
            "score": score,
            "score_details": score_details,
            "stop_loss": stop_loss,
            "target1": target1,
            "target2": target2,
            "oi_change_pct": oi_change_pct,
            "context_range_pct": context["range_pct"],
            "context_range_high": context["range_high"],
            "context_range_low": context["range_low"]
        }
    except Exception as e:
        logging.debug(f"Ошибка анализа PRE-BREAKOUT: {e}")
        return None


# ===== МОДУЛЬ 3: ROCKET DETECTOR =====
def analyze_rocket_spike(klines_5m, oi_change_pct=0.0, rvol=1.0):
    if not klines_5m or len(klines_5m) < 25:
        return None
    try:
        closes = [float(c["close"]) for c in klines_5m]
        opens = [float(c["open"]) for c in klines_5m]

        curr_price = closes[-1]
        open_price = opens[-1]
        price_change_pct = ((curr_price - open_price) / open_price) * 100

        if rvol >= ROCKET_RVOL_THRESHOLD and abs(price_change_pct) >= ROCKET_PRICE_CHANGE_PCT:
            direction = "LONG" if price_change_pct > 0 else "SHORT"
            return {
                "module": "ROCKET_SPIKE",
                "setup_type": "INSTANT ROCKET 🚀",
                "direction": direction,
                "current_price": curr_price,
                "price_change_pct": round(price_change_pct, 2),
                "rvol": round(rvol, 1),
                "oi_change_pct": oi_change_pct
            }
    except Exception as e:
        logging.debug(f"Ошибка анализа ROCKET: {e}")
    return None


async def send_signal_message(bot, symbol, data):
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        curr_p = data["current_price"]
        
        if data["module"] == "EARLY_BOTTOM":
            message = (
                f"🔥🟢 **СИГНАЛ: EARLY_BOTTOM (Зарождение пампа)**\n"
                f"📊 **{clean_symbol}** | BingX Futures\n"
                f"🎯 **Потенциал:** +20% – +50%\n\n"
                f"🔥 **МЕТРИКИ ВХОДА С ДНА:**\n"
                f"• Сжатие волатильности: **{data['compression_pct']}%**\n"
                f"• Рост тела свечи: **x{data['body_mult']}** от среднего\n"
                f"• Всплеск объема: **x{data['vol_mult']}** от среднего\n"
                f"• RVOL: **x{data['rvol']}** | OI: **+{data['oi_change']}%**\n\n"
                f"💰 Вход по рынку: **${format_price(curr_p)}**\n"
                f"🕒 {datetime.now().strftime('%H:%M:%S')}"
            )

        elif data["module"] == "PRE_BREAKOUT":
            stop_p = data["stop_loss"]
            tp1 = data["target1"]
            tp2 = data["target2"]

            is_long = data["direction"] == "LONG"
            emoji = "⚡"
            dir_emoji = "🟢" if is_long else "🔴"
            
            risk = abs(((curr_p - stop_p) / curr_p) * 100) if curr_p > 0 else 0
            tp1_pct = abs(((tp1 - curr_p) / curr_p) * 100) if curr_p > 0 else 0
            rr_ratio = tp1_pct / risk if risk > 0 else 0

            details_str = "\n• ".join(data["score_details"])

            message = (
                f"{emoji}{dir_emoji} **СИГНАЛ: {data['setup_type']} {data['direction']}**\n"
                f"📊 **{clean_symbol}** | BingX Futures\n"
                f"⭐ Рейтинг вероятности: **{data['score']}/100**\n\n"
                f"🎯 **МЕТРИКИ И ДРАЙВЕРЫ:**\n"
                f"• {details_str}\n\n"
                f"📦 **ФЛЭТ ({EARLY_ACCUMULATION_HOURS}H):**\n"
                f"• Диапазон: **{data['context_range_pct']}%**\n"
                f"• Уровни: **${format_price(data['context_range_low'])} - ${format_price(data['context_range_high'])}**\n\n"
                f"💰 **ТОРГОВЫЙ ПЛАН:**\n"
                f"• Вход: **${format_price(curr_p)}**\n"
                f"• 🎯 TP1: **${format_price(tp1)}** (+{tp1_pct:.1f}%)\n"
                f"• 🎯 TP2: **${format_price(tp2)}**\n"
                f"• 🛑 Стоп: **${format_price(stop_p)}** (~{risk:.2f}%)\n"
                f"• ⚖️ Risk/Reward: **1:{rr_ratio:.1f}**\n\n"
                f"🕒 {datetime.now().strftime('%H:%M:%S')}"
            )

        else:
            dir_emoji = "🟢" if data["direction"] == "LONG" else "🔴"
            message = (
                f"🚀{dir_emoji} **ИМПУЛЬСНЫЙ СПАЙК: {data['setup_type']} {data['direction']}**\n"
                f"📊 **{clean_symbol}** | BingX Futures\n\n"
                f"🔥 **МЕТРИКИ ВСПЛЕСКА:**\n"
                f"• Скачок цены (5m): **{data['price_change_pct']:+.2f}%**\n"
                f"• Аномальный RVOL: **x{data['rvol']}**\n"
                f"• Динамика OI: **+{data['oi_change_pct']}%**\n\n"
                f"💰 Текущая цена: **${format_price(curr_p)}**\n\n"
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

            oi_change_pct = await fetch_and_store_oi(session, symbol)

            klines_5m = await fetch_klines(session, symbol, TIMEFRAME_SMALL, limit=50)
            if not klines_5m or len(klines_5m) < 30:
                return False

            closes_5m = [float(c["close"]) for c in klines_5m]
            volumes_5m = [float(c["volume"]) * float(c["close"]) for c in klines_5m]
            price = closes_5m[-1]
            
            # Расчет текущего RVOL
            recent_vols = volumes_5m[-21:-1]
            avg_vol_20 = sum(recent_vols) / len(recent_vols) if recent_vols else 1.0
            current_rvol = volumes_5m[-1] / avg_vol_20 if avg_vol_20 > 0 else 0.0

            ema40 = calculate_ema(closes_5m, 40)

            # === ПРИОРИТЕТ 1: EARLY_BOTTOM 🔥 (Ловим с самого дна) ===
            is_early, early_data = check_early_bottom(
                klines_5m=klines_5m,
                current_oi_change_pct=oi_change_pct,
                current_rvol=current_rvol,
                price=price,
                ema40=ema40
            )

            if is_early:
                last_signals[symbol] = current_time
                return await send_signal_message(bot, symbol, early_data)

            # === ПРИОРИТЕТ 2: PRE_BREAKOUT ⚡ (Поджим) ===
            klines_1h = await fetch_klines(session, symbol, TIMEFRAME_BIG, limit=EARLY_ACCUMULATION_HOURS + 3)
            if klines_1h:
                context = check_accumulation_context(klines_1h)
                if context:
                    result_pre_breakout = analyze_live_setup(klines_5m, context, oi_change_pct, current_rvol)
                    if result_pre_breakout:
                        last_signals[symbol] = current_time
                        return await send_signal_message(bot, symbol, result_pre_breakout)

            # === ПРИОРИТЕТ 3: ROCKET 🚀 (Импульсный спайк) ===
            result_rocket = analyze_rocket_spike(klines_5m, oi_change_pct, current_rvol)
            if result_rocket:
                last_signals[symbol] = current_time
                return await send_signal_message(bot, symbol, result_rocket)

            return False
    except Exception as e:
        logging.error(f"Ошибка обработки {symbol}: {e}")
        return False


async def main():
    bot = Bot(token=BOT_TOKEN)
    semaphore = asyncio.Semaphore(15)
    logging.info("🚀 Запущен 3-Уровневый Сканер (EARLY_BOTTOM + PRE-BREAKOUT + ROCKET)")

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
    return web.Response(text="Multi-Engine Bottom Scanner is running!")

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
