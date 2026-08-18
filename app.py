import sys
import asyncio
import json
import uvicorn
import aiohttp
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# ==================== CONFIGURATION ====================
TWELVE_DATA_API_KEY = "6f9bc7ddd24c4453985e471b565fcd98"
TELEGRAM_BOT_TOKEN = "8631774112:AAFy8m2EkEa6sqLmRs129tiTDWR57WfY7OE"
TELEGRAM_CHAT_ID = "1825789803"
# =======================================================

live_market_data = {
    "symbol": "GBP/JPY",
    "price": 190.500,
    "swing_high": 190.620,
    "swing_low": 190.380,
    "signal": "NEUTRAL",
    "sl": 190.380,
    "tp": 190.620,
    "macro_bias": "BULLISH_GBP"
}

last_signal_state = "NEUTRAL"
last_alerted_candle = None

async def send_telegram_alert(session, signal_type, price, sl, tp):
    """Sends formatted trade signal to Telegram asynchronously"""
    if not TELEGRAM_BOT_TOKEN:
        return

    emoji = "🟢" if signal_type == "BUY" else "🔴"
    message = (
        f"{emoji} <b>RUNNERS FX — GBPJPY 5M SIGNAL</b> {emoji}\n\n"
        f"<b>Action:</b> {signal_type}\n"
        f"<b>Entry Price:</b> {price:.3f}\n"
        f"<b>Stop Loss (SL):</b> {sl:.3f}\n"
        f"<b>Take Profit (TP):</b> {tp:.3f}\n\n"
        f"<i>Structure Breakout Confirmed (Candle Close)</i>"
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
    """Ultra-low latency async polling loop"""
    global live_market_data, last_signal_state, last_alerted_candle
    
    print("⚡ Real-time Async Engine Active (Zero-Lag Mode)")
    
    url = f"https://api.twelvedata.com/time_series?symbol=GBP/JPY&interval=5min&outputsize=10&apikey={TWELVE_DATA_API_KEY}"
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Fetch time series & price in a single async non-blocking HTTP call
                async with session.get(url, timeout=3) as resp:
                    data = await resp.json()
                    
                    if "values" in data and len(data["values"]) >= 2:
                        candles = data["values"]
                        
                        # Current live tick/price from the active candle close
                        price = round(float(candles[0]["close"]), 3)
                        
                        # Last closed candle for breakout confirmation
                        closed_candle = candles[1]
                        
                        # Swing levels from historical candles
                        past_candles = candles[2:]
                        swing_high = round(max(float(c["high"]) for c in past_candles), 3)
                        swing_low = round(min(float(c["low"]) for c in past_candles), 3)
                        
                        signal = "NEUTRAL"
                        sl = swing_low
                        tp = swing_high

                        # Candle-close breakout validation
                        c_close = float(closed_candle["close"])
                        c_time = closed_candle["datetime"]

                        if c_close > swing_high:
                            signal = "BUY"
                            sl = swing_low
                            tp = round(price + (price - swing_low) * 2, 3)
                        elif c_close < swing_low:
                            signal = "SELL"
                            sl = swing_high
                            tp = round(price - (swing_high - price) * 2, 3)

                        # Trigger Telegram notification
                        if signal != "NEUTRAL" and c_time != last_alerted_candle:
                            await send_telegram_alert(session, signal, price, sl, tp)
                            last_alerted_candle = c_time

                        live_market_data.update({
                            "price": price,
                            "swing_high": swing_high,
                            "swing_low": swing_low,
                            "signal": signal,
                            "sl": sl,
                            "tp": tp
                        })
                        print(f"📥 Live Price: {price:.3f} | High: {swing_high:.3f} | Low: {swing_low:.3f}")

                    elif "message" in data:
                        print(f"⚠️ Twelve Data API Notice: {data['message']}")

            except Exception as e:
                print(f"⚠️ Async Polling Error: {e}")
                
            # Non-blocking pause between updates
            await asyncio.sleep(2)

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
            await asyncio.sleep(0.5)  # Fast 500ms dashboard sync
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    uvicorn.run(app, host="127.0.0.1", port=8000)