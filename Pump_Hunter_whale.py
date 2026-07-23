import asyncio
import os
import logging
import time
import aiohttp
from aiohttp import web
from aiogram import Bot
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BOT_TOKEN = os.environ.get("PUMP_BOT_TOKEN")
CHAT_ID = os.environ.get("PUMP_CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("Не установлены PUMP_BOT_TOKEN или PUMP_CHAT_ID")

BINGX_BASE_URL = "https://open-api.bingx.com"

# ===== НАСТРОЙКИ =====
TIMEFRAME_SMALL = "5m"
TIMEFRAME_BIG = "1h"

# Фильтр стоимости активов
MAX_COIN_PRICE = 1.0  # Искать монеты ТОЛЬКО дешевле 1.0 USDT

# Контекст (1H)
ACCUMULATION_HOURS = 6
MAX_ACCUMULATION_RANGE_PCT = 3.5
MAX_ACCUMULATION_VOLUME_SPIKE = 2.5

# Детектор (5M)
MIN_FIRST_CANDLE_CHANGE_PCT = 0.7
MIN_MOMENTUM_VOLUME_MULT = 3.0
MIN_MOMENTUM_CONSECUTIVE = 2
MAX_ENTRY_DELAY_CANDLES = 3

# Лимиты объемов
MIN_USDT_VOLUME_5M = 5000
MIN_USDT_VOLUME_1H = 5000

# Лимиты и защита
SIGNAL_COOLDOWN = 3600
MAX_SIGNALS_PER_SCAN = 4
MAX_DISTANCE_FROM_RANGE_PCT = 5.0
TYPICAL_MOVE_PCT = 8.0
CONTEXT_FRESHNESS = 600   # 10 минут
WATCHLIST_MAX_AGE = 2400   # 40 минут
SESSION_MAX_AGE = 1800     # 30 минут

# Хранилища
last_signals = {}
consolidation_watchlist = {}

scan_counter = 0
session_start_time = 0
error_counter = {"api": 0, "timeout": 0, "parse": 0}


async def health_check(request):
    """Заглушка для Render и UptimeRobot"""
    return web.Response(text="Bot is running!", status=200)


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


def cleanup_storage():
    current_time = time.time()
    
    expired_signals = [
        sym for sym, t in last_signals.items() 
        if current_time - t > SIGNAL_COOLDOWN
    ]
    for sym in expired_signals:
        del last_signals[sym]
    
    expired_watchlist = [
        sym for sym, data in consolidation_watchlist.items()
        if current_time - data.get("updated_at", 0) > WATCHLIST_MAX_AGE
    ]
    for sym in expired_watchlist:
        del consolidation_watchlist[sym]
    
    if expired_signals or expired_watchlist:
        logging.info(f"🧹 Очистка: -{len(expired_signals)} сигналов, -{len(expired_watchlist)} из вотчлиста")


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
        logging.error(f"Ошибка получения пар: {e}")
    return []


async def fetch_klines(session, symbol, interval, limit=30):
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
        if len(context_candles) < ACCUMULATION_HOURS:
            return None
        
        highs, lows, volumes, closes, opens = [], [], [], [], []
        
        for c in context_candles:
            try:
                highs.append(float(c["high"]))
                lows.append(float(c["low"]))
                volumes.append(float(c["volume"]) * float(c["close"]))
                closes.append(float(c["close"]))
                opens.append(float(c["open"]))
            except (KeyError, ValueError, TypeError):
                continue
        
        if not closes or not opens or len(highs) < ACCUMULATION_HOURS // 2:
            return None
        
        range_high = max(highs)
        range_low = min(lows)
        if range_low <= 0:
            return None
            
        range_pct = ((range_high - range_low) / range_low) * 100
        
        if range_pct > MAX_ACCUMULATION_RANGE_PCT:
            return None
        
        avg_volume = sum(volumes) / len(volumes)
        if avg_volume < MIN_USDT_VOLUME_1H:
            return None
        
        if max(volumes) > avg_volume * MAX_ACCUMULATION_VOLUME_SPIKE:
            return None
        
        last_open = opens[-1]
        last_close = closes[-1]
        if last_open > 0:
            last_change = abs(((last_close - last_open) / last_open) * 100)
            if last_change > 3.0:
                return None
        
        if last_close > range_high * 1.02 or last_close < range_low * 0.98:
            return None
        
        trend_pct = ((closes[-1] - closes[0]) / closes[0]) * 100 if closes[0] != 0 else 0
        
        return {
            "range_high": range_high,
            "range_low": range_low,
            "avg_volume": avg_volume,
            "range_pct": round(range_pct, 2),
            "trend_pct": round(trend_pct, 2),
            "mid_price": (range_high + range_low) / 2
        }
    except Exception:
        return None


def detect_early_momentum(klines_5m, context):
    if not klines_5m or len(klines_5m) < 25:
        return None
    
    try:
        # Берём окно из последних 8 свечей (40 минут), чтобы точно зацепить истинный старт
        recent_candles = klines_5m[-8:]
        
        # Считаем средний объем по истории ДО этого 40-минутного окна
        volume_history = []
        for c in klines_5m[-25:-8]:
            try:
                volume_history.append(float(c["volume"]) * float(c["close"]))
            except (KeyError, ValueError, TypeError):
                continue
        
        if len(volume_history) < 10:
            return None
        
        avg_5m_volume = sum(volume_history) / len(volume_history)
        if avg_5m_volume <= 0:
            return None

        impulse_start_idx = None
        direction = None
        
        # 🎯 Сканируем СЛЕВА НАПРАВО (от старой к свежей в окне из 8 свечей)
        for i in range(len(recent_candles)):
            try:
                candle = recent_candles[i]
                open_p = float(candle["open"])
                close_p = float(candle["close"])
                volume = float(candle["volume"]) * close_p
                
                if open_p == 0:
                    continue
                    
                change_pct = abs(((close_p - open_p) / open_p) * 100)

                # Ищем САМУЮ ПЕРВУЮ свечу, давшую всплеск объёма и изменения
                if (change_pct >= MIN_FIRST_CANDLE_CHANGE_PCT and 
                    volume >= avg_5m_volume * MIN_MOMENTUM_VOLUME_MULT and
                    volume >= MIN_USDT_VOLUME_5M):
                    
                    impulse_start_idx = i
                    direction = "LONG" if close_p > open_p else "SHORT"
                    break  # Нашли ИСТИННУЮ первую свечу начала движения!
            except (KeyError, ValueError, TypeError):
                continue
        
        if impulse_start_idx is None or direction is None:
            return None
        
        # Выделяем все свечи от истока импульса до текущей
        impulse_candles = recent_candles[impulse_start_idx:]
        candles_count = len(impulse_candles)
        
        # ⛔ ЖЁСТКИЙ ФИЛЬТР: Если с момента ИСТИННОГО старта прошло больше 3 свечей (15 минут) — игнорируем!
        if candles_count > MAX_ENTRY_DELAY_CANDLES:
            return None
        
        valid_candles = []
        total_volume = 0.0
        for c in impulse_candles:
            try:
                o, cl, v = float(c["open"]), float(c["close"]), float(c["volume"])
                if o > 0 and cl > 0:
                    valid_candles.append({"open": o, "close": cl})
                    total_volume += v * cl
            except (KeyError, ValueError, TypeError):
                continue

        if len(valid_candles) != candles_count:
            return None

        first_price = valid_candles[0]["open"]
        current_price = valid_candles[-1]["close"]

        if direction == "LONG":
            if current_price < context["mid_price"]:
                return None
            distance_from_range = ((current_price - context["range_high"]) / context["range_high"]) * 100
            if distance_from_range > MAX_DISTANCE_FROM_RANGE_PCT or distance_from_range < -1.0:
                return None
            consecutive = sum(1 for c in valid_candles if c["close"] > c["open"])
            if consecutive < MIN_MOMENTUM_CONSECUTIVE:
                return None
            total_move_pct = ((current_price - first_price) / first_price) * 100
            if total_move_pct >= TYPICAL_MOVE_PCT * 0.9:
                return None
            min_allowed = context["range_high"] * 0.995
            for c in valid_candles:
                if c["close"] < min_allowed:
                    return None
        else:
            if current_price > context["mid_price"]:
                return None
            distance_from_range = ((context["range_low"] - current_price) / context["range_low"]) * 100
            if distance_from_range > MAX_DISTANCE_FROM_RANGE_PCT or distance_from_range < -1.0:
                return None
            consecutive = sum(1 for c in valid_candles if c["close"] < c["open"])
            if consecutive < MIN_MOMENTUM_CONSECUTIVE:
                return None
            total_move_pct = ((first_price - current_price) / first_price) * 100
            if total_move_pct >= TYPICAL_MOVE_PCT * 0.9:
                return None
            max_allowed = context["range_low"] * 1.005
            for c in valid_candles:
                if c["close"] > max_allowed:
                    return None

        denom = avg_5m_volume * candles_count
        vol_mult = round(total_volume / denom, 1) if denom > 0 else 0.0

        return {
            "type": direction,
            "first_price": first_price,
            "current_price": current_price,
            "total_move_pct": round(max(0.0, total_move_pct), 2),
            "candles_count": candles_count,
            "minutes_from_start": candles_count * 5,
            "impulse_volume": int(total_volume),
            "volume_mult": vol_mult,
            "context_range_pct": context["range_pct"],
            "context_range_high": context["range_high"],
            "context_range_low": context["range_low"],
            "distance_from_range": round(distance_from_range, 2)
        }
            
    except Exception:
        return None


async def send_early_signal(bot, symbol, data):
    try:
        clean_symbol = symbol.replace("-", "").replace("USDT", "/USDT")
        curr_p = data["current_price"]
        
        if data["type"] == "LONG":
            emoji = "🟢"
            direction_text = "ЛОНГ"
            target1 = curr_p * 1.02
            stop_loss = data["context_range_low"] * 0.995
            risk = abs(((curr_p - stop_loss) / curr_p) * 100) if curr_p > 0 else 0
        else:
            emoji = "🔴"
            direction_text = "ШОРТ"
            target1 = curr_p * 0.98
            stop_loss = data["context_range_high"] * 1.005
            risk = abs(((stop_loss - curr_p) / curr_p) * 100) if curr_p > 0 else 0
        
        tp1_pct = abs(((target1 - curr_p) / curr_p) * 100) if curr_p > 0 else 0
        remaining_potential = max(0, TYPICAL_MOVE_PCT - data["total_move_pct"])
        
        if remaining_potential >= 4.0:
            potential_text = f"ещё ~{remaining_potential:.1f}% 🔥"
        elif remaining_potential >= 2.0:
            potential_text = f"ещё ~{remaining_potential:.1f}% ⚡"
        else:
            potential_text = "⚠️ Запас ограничен"
        
        rr_text = f"✅ RR 1:{tp1_pct / risk:.1f}" if risk > 0 else ""

        message = (
            f"{emoji} **НАЧАЛО ИМПУЛЬСА — {direction_text}**\n"
            f"📊 **{clean_symbol}** | BingX\n\n"
            f"⚡ **ИМПУЛЬС ТОЛЬКО НАЧАЛСЯ!**\n"
            f"• Движение: **{'+' if data['type'] == 'LONG' else '-'}{data['total_move_pct']}%**\n"
            f"• Потенциал: {potential_text}\n"
            f"• Времени прошло: **~{data['minutes_from_start']} мин** ({data['candles_count']} свеч.)\n"
            f"• Объем: **${data['impulse_volume']:,}** (x{data['volume_mult']} к норме)\n\n"
            f"📦 **КОНТЕКСТ (1H):**\n"
            f"• Флэт: **{data['context_range_pct']}%**\n"
            f"• Границы: **${format_price(data['context_range_low'])} - ${format_price(data['context_range_high'])}**\n\n"
            f"💰 **ТОЧКА ВХОДА:**\n"
            f"• Вход: **${format_price(curr_p)}**\n"
            f"• TP1: **${format_price(target1)}** (+{tp1_pct:.1f}%)\n"
            f"🛑 **Стоп:** **${format_price(stop_loss)}** (риск ~{risk:.1f}%)"
        )
        if rr_text:
            message += f"\n📊 {rr_text}"
        message += f"\n\n🕒 {datetime.now().strftime('%H:%M:%S')}"
        
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки сигнала {symbol}: {e}")
        return False


async def check_symbol(session, bot, symbol, semaphore):
    try:
        async with semaphore:
            current_time = time.time()
            
            if symbol in last_signals and (current_time - last_signals[symbol] < SIGNAL_COOLDOWN):
                return False

            context = None
            if symbol in consolidation_watchlist:
                watch_data = consolidation_watchlist[symbol]
                if current_time - watch_data.get("updated_at", 0) < CONTEXT_FRESHNESS:
                    context = watch_data["context"]

            if not context:
                klines_1h = await fetch_klines(session, symbol, TIMEFRAME_BIG, limit=ACCUMULATION_HOURS + 3)
                if not klines_1h:
                    return False
                
                try:
                    last_price = float(klines_1h[-1]["close"])
                    if last_price >= MAX_COIN_PRICE:
                        return False
                except (KeyError, ValueError, IndexError, TypeError):
                    pass

                context = check_accumulation_context(klines_1h)
                
                if context:
                    consolidation_watchlist[symbol] = {
                        "context": context,
                        "updated_at": current_time
                    }
                else:
                    consolidation_watchlist.pop(symbol, None)
                    return False

            klines_5m = await fetch_klines(session, symbol, TIMEFRAME_SMALL, limit=30)
            if not klines_5m:
                return False
                
            result = detect_early_momentum(klines_5m, context)
            if result:
                last_signals[symbol] = current_time
                success = await send_early_signal(bot, symbol, result)
                if success:
                    consolidation_watchlist.pop(symbol, None)
                return success
                
            return False
            
    except Exception as e:
        logging.error(f"Ошибка check_symbol {symbol}: {e}")
        consolidation_watchlist.pop(symbol, None)
        return False


async def scanner_loop(bot):
    global scan_counter, session_start_time
    semaphore = asyncio.Semaphore(12)
    
    while True:
        try:
            session_start_time = time.time()
            connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, force_close=False)
            
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30)) as session:
                while True:
                    scan_counter += 1
                    start_time = time.time()
                    
                    if scan_counter % 30 == 0:
                        cleanup_storage()
                    
                    if time.time() - session_start_time > SESSION_MAX_AGE:
                        break
                    
                    symbols = await fetch_bingx_symbols(session)
                    if not symbols:
                        await asyncio.sleep(30)
                        break
                    
                    total_signals = 0
                    tasks = [check_symbol(session, bot, sym, semaphore) for sym in symbols]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for r in results:
                        if r is True:
                            total_signals += 1
                    
                    elapsed = time.time() - start_time
                    logging.info(
                        f"Скан #{scan_counter} | {elapsed:.1f}с | "
                        f"Сигналов: {total_signals} | "
                        f"Вотчлист: {len(consolidation_watchlist)} | "
                        f"Cooldown: {len(last_signals)}"
                    )
                    await asyncio.sleep(15)
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Ошибка в цикле: {e}")
            await asyncio.sleep(10)


async def main():
    bot = Bot(token=BOT_TOKEN)
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    logging.info(f"🌐 Сервер запущен на порту {PORT}")
    try:
        await scanner_loop(bot)
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
