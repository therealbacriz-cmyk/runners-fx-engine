import sys
import asyncio
import json
import uvicorn
import aiohttp
from datetime import datetime
import pytz
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# ==================== CONFIGURATION ====================
TWELVE_DATA_API_KEY = "09e2bf76463e4479b4d15ca311a53d7e"
TELEGRAM_BOT_TOKEN = "8631774112:AAFy8m2EkEa6sqLmRs129tiTDWR57WfY7OE"
TELEGRAM_CHAT_ID = "1825789803"

# Session Time Filter (East Africa Time)
START_HOUR_EAT = 7   # 7:00 AM EAT
END_HOUR_EAT = 19   # 7:00 PM EAT

# Filter Settings (To avoid minor structure breaks)
PIVOT_LOOKBACK = 2       # Candle distance to confirm a major swing peak/valley
BOS_BUFFER_PIPS = 0.050  # 5 pips buffer above/below structure for valid BOS
# =======================================================

live_market_data = {
    "symbol": "GBP/JPY",
    "price": 0.0,
    "swing_high": 0.0,
    "swing_low": 0.0,
    "signal": "SCANNING...",
    "entry_price": 0.0,
    "sl": 0.0,
    "tp": 0.0,
    "macro_bias": "BULLISH_GBP"
}

last_alerted_candle = None

def is_within_trading_window() -> bool:
    """Checks if current time in Uganda/EAT is between 7:00 AM and 7:00 PM"""
    eat_tz = pytz.timezone("Africa/Kampala")
    now_eat = datetime.now(eat_tz)
    return START_HOUR_EAT <= now_eat.hour < END_HOUR_EAT

def find_major_swings(candles, lookback=2):
    """
    Identifies major fractal swing highs and lows to prevent counting minor noise.
    """
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    
    major_high = None
    major_low = None

    for i in range(lookback, len(candles) - lookback):
        if all(highs[i] > highs[i - k] for k in range(1, lookback + 1)) and \
           all(highs[i] > highs[i + k] for k in range(1, lookback + 1)):
            if major_high is None:
                major_high = highs[i]

        if all(lows[i] < lows[i - k] for k in range(1, lookback + 1)) and \
           all(lows[i] < lows[i + k] for k in range(1, lookback + 1)):
            if major_low is None:
                major_low = lows[i]

        if major_high and major_low:
            break

    if major_high is None:
        major_high = max(highs[2:])
    if major_low is None:
        major_low = min(lows[2:])

    return round(major_high, 3), round(major_low, 3)

async def send_telegram_alert(session, signal_type, entry_price, sl, tp):
    if not TELEGRAM_BOT_TOKEN:
        return

    emoji = "🟢" if signal_type == "BUY" else "🔴"
    message = (
        f"{emoji} <b>RUNNERS FX — GBPJPY 5M SIGNAL</b> {emoji}\n\n"
        f"<b>Action:</b> {signal_type}\n"
        f"<b>Entry Price:</b> {entry_price:.3f}\n"
        f"<b>Stop Loss (SL):</b> {sl:.3f}\n"
        f"<b>Take Profit (TP):</b> {tp:.3f}\n\n"
        f"<i>Structure Breakout Confirmed (Body + Buffer)</i>"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        async with session.post(url, json=payload, timeout=3) as resp:
            if resp.status == 200:
                print(f"📱 Telegram alert sent for {signal_type}!")
    except Exception as e:
        print(f"❌ Telegram Error: {e}")

async def twelve_data_listener():
    """REST polling engine with strict 7 AM - 7 PM EAT trading window enforcement"""
    global live_market_data, last_alerted_candle
    
    print("⚡ Real-time Engine Active (REST Polling Mode)")
    url = f"https://api.twelvedata.com/time_series?symbol=GBP/JPY&interval=5min&outputsize=30&apikey={TWELVE_DATA_API_KEY}"
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # 1. Check time window
                if not is_within_trading_window():
                    live_market_data.update({
                        "signal": "OUTSIDE_TRADING_HOURS",
                        "entry_price": 0.0,
                        "sl": 0.0,
                        "tp": 0.0
                    })
                    print("🌙 Outside active window (7:00 AM – 7:00 PM EAT). Pausing signals...")
                    await asyncio.sleep(60)
                    continue

                # 2. Fetch live market data during active hours
                async with session.get(url, timeout=5) as resp:
                    data = await resp.json()
                    
                    if "values" in data and len(data["values"]) >= 10:
                        candles = data["values"]
                        
                        price = round(float(candles[0]["close"]), 3)
                        closed_candle = candles[1]
                        
                        swing_high, swing_low = find_major_swings(candles, lookback=PIVOT_LOOKBACK)
                        
                        signal = "NEUTRAL"
                        entry_price = 0.0
                        sl = 0.0
                        tp = 0.0

                        c_close = float(closed_candle["close"])
                        c_time = closed_candle["datetime"]

                        # Check for valid breakout signals with buffer
                        if c_close >= (swing_high + BOS_BUFFER_PIPS):
                            signal = "BUY"
                            entry_price = price
                            sl = swing_low
                            tp = round(price + (price - swing_low) * 1.0, 3)  # 1:1 Risk-to-Reward
                        elif c_close <= (swing_low - BOS_BUFFER_PIPS):
                            signal = "SELL"
                            entry_price = price
                            sl = swing_high
                            tp = round(price - (swing_high - price) * 1.0, 3)  # 1:1 Risk-to-Reward

                        if signal != "NEUTRAL" and c_time != last_alerted_candle:
                            await send_telegram_alert(session, signal, entry_price, sl, tp)
                            last_alerted_candle = c_time

                        live_market_data.update({
                            "price": price,
                            "swing_high": swing_high,
                            "swing_low": swing_low,
                            "signal": signal,
                            "entry_price": entry_price,
                            "sl": sl,
                            "tp": tp
                        })
                        print(f"📥 [EAT Active] GBP/JPY: {price:.3f} | Signal: {signal} | Entry: {entry_price:.3f} | SL: {sl:.3f} | TP: {tp:.3f}")

                    elif "message" in data:
                        print(f"⚠️ Twelve Data API Error: {data['message']}")
                    elif "code" in data and data["code"] == 429:
                        print("⚠️ API Rate limit hit. Waiting 30s before retrying...")
                        await asyncio.sleep(30)
                        continue

            except Exception as e:
                print(f"⚠️ Engine Polling Error: {e}")
                
            await asyncio.sleep(15)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(twelve_data_listener())
    yield
    task.cancel()

app = FastAPI(title="Runners FX Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
async def serve_dashboard():
    return FileResponse("index.html")

@app.get("/health")
def health_check():
    return {"status": "Runners FX Engine active"}

@app.websocket("/ws/signals")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(live_market_data)
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    uvicorn.run(app, host="127.0.0.1", port=8000)