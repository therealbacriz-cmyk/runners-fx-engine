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

last_alerted_candle = None

async def send_telegram_alert(session, signal_type, price, sl, tp):
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

async def fetch_5m_pivots(session):
    """Fetches high/low structure from the 5M REST candles"""
    url = f"https://api.twelvedata.com/time_series?symbol=GBP/JPY&interval=5min&outputsize=10&apikey={TWELVE_DATA_API_KEY}"
    try:
        async with session.get(url, timeout=4) as resp:
            data = await resp.json()
            if "values" in data and len(data["values"]) >= 2:
                candles = data["values"]
                closed_candle = candles[1]
                past_candles = candles[2:]
                
                high = round(max(float(c["high"]) for c in past_candles), 3)
                low = round(min(float(c["low"]) for c in past_candles), 3)
                return closed_candle, high, low
    except Exception as e:
        print(f"⚠️ Structure Fetch Error: {e}")
    return None, None, None

async def twelve_data_websocket():
    """Real-time streaming via Twelve Data WebSocket"""
    global live_market_data, last_alerted_candle
    
    ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Fetch baseline pivots first
                closed_candle, swing_high, swing_low = await fetch_5m_pivots(session)
                
                print("⚡ Connecting to Twelve Data Real-Time WebSocket Feed...")
                async with session.ws_connect(ws_url) as ws:
                    # Subscribe to GBP/JPY ticks
                    subscribe_msg = {
                        "action": "subscribe",
                        "params": {"symbols": "GBP/JPY"}
                    }
                    await ws.send_json(subscribe_msg)

                    pivot_refresh_counter = 0

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)

                            if data.get("event") == "price":
                                price = round(float(data["price"]), 3)
                                
                                # Refresh 5M swing levels every 60 seconds
                                pivot_refresh_counter += 1
                                if pivot_refresh_counter >= 30:
                                    c_candle, sh, sl_val = await fetch_5m_pivots(session)
                                    if sh and sl_val:
                                        closed_candle, swing_high, swing_low = c_candle, sh, sl_val
                                    pivot_refresh_counter = 0

                                sh_val = swing_high if swing_high else round(price + 0.120, 3)
                                sl_val = swing_low if swing_low else round(price - 0.120, 3)

                                signal = "NEUTRAL"
                                sl = sl_val
                                tp = sh_val

                                if closed_candle:
                                    c_close = float(closed_candle["close"])
                                    c_time = closed_candle["datetime"]

                                    if c_close > sh_val:
                                        signal = "BUY"
                                        sl = sl_val
                                        tp = round(price + (price - sl_val) * 2, 3)
                                    elif c_close < sl_val:
                                        signal = "SELL"
                                        sl = sh_val
                                        tp = round(price - (sh_val - price) * 2, 3)

                                    if signal != "NEUTRAL" and c_time != last_alerted_candle:
                                        await send_telegram_alert(session, signal, price, sl, tp)
                                        last_alerted_candle = c_time

                                live_market_data.update({
                                    "price": price,
                                    "swing_high": sh_val,
                                    "swing_low": sl_val,
                                    "signal": signal,
                                    "sl": sl,
                                    "tp": tp
                                })
                                print(f"🟢 WebSocket Tick GBP/JPY: {price:.3f}")

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

            except Exception as e:
                print(f"⚠️ WebSocket Connection Error: {e}")
                await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(twelve_data_websocket())
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
            await asyncio.sleep(0.2)  # Stream directly to dashboard
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    uvicorn.run(app, host="127.0.0.1", port=8000)