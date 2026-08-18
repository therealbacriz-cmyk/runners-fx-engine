import sys
import asyncio
import json
import uvicorn
import urllib.request
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

# Global market state
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
last_alerted_candle = None  # Tracks the candle timestamp to prevent multiple Telegram alerts

def fetch_real_5m_candles():
    """Fetches completed 5-minute candles from Twelve Data REST API"""
    if not TWELVE_DATA_API_KEY or TWELVE_DATA_API_KEY == "YOUR_TWELVE_DATA_API_KEY_HERE":
        return None, None, None
    try:
        url = f"https://api.twelvedata.com/time_series?symbol=GBP/JPY&interval=5min&outputsize=10&apikey={TWELVE_DATA_API_KEY}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode())
            if "values" in data and len(data["values"]) >= 2:
                candles = data["values"]
                # candles[0] is the active/open candle; candles[1] is the LAST CLOSED candle
                closed_candle = candles[1]
                
                # Historic high/low structure from older closed candles
                past_candles = candles[2:]
                highs = [float(c["high"]) for c in past_candles]
                lows = [float(c["low"]) for c in past_candles]
                
                return closed_candle, max(highs), min(lows)
    except Exception as e:
        print(f"⚠️ Failed to fetch 5M candles: {e}")
    return None, None, None

def send_telegram_alert(signal_type, price, sl, tp):
    """Sends formatted trade signal to Telegram"""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
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
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }).encode("utf-8")
    
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=3)
        print(f"📱 Telegram alert sent for {signal_type}!")
    except Exception as e:
        print(f"❌ Telegram Error: {e}")

async def twelve_data_listener():
    """Polls Twelve Data REST API continuously and confirms breakouts on 5M candle close"""
    global live_market_data, last_signal_state, last_alerted_candle
    
    loop = asyncio.get_running_loop()
    print("⚡ Real-time Structure Engine Active (Candle-Close Confirmed)")
    
    while True:
        try:
            # 1. Fetch live price
            url = f"https://api.twelvedata.com/price?symbol=GBP/JPY&apikey={TWELVE_DATA_API_KEY}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            
            def get_price():
                with urllib.request.urlopen(req, timeout=3) as resp:
                    return json.loads(resp.read().decode())
                    
            data = await loop.run_in_executor(None, get_price)
            
            if "price" in data:
                price = round(float(data["price"]), 3)
                print(f"📥 Live GBP/JPY Price: {price:.3f}")
                
                # 2. Fetch completed candle structure
                closed_candle, real_high, real_low = await loop.run_in_executor(None, fetch_real_5m_candles)
                
                swing_high = round(real_high if real_high else price + 0.120, 3)
                swing_low = round(real_low if real_low else price - 0.120, 3)
                
                signal = "NEUTRAL"
                sl = swing_low
                tp = swing_high

                # 3. Verify breakout against the LAST CLOSED CANDLE
                if closed_candle:
                    c_close = float(closed_candle["close"])
                    c_time = closed_candle["datetime"] # Timestamp of the closed 5M candle

                    if c_close > swing_high:
                        signal = "BUY"
                        sl = swing_low
                        tp = round(price + (price - swing_low) * 2, 3)
                    elif c_close < swing_low:
                        signal = "SELL"
                        sl = swing_high
                        tp = round(price - (swing_high - price) * 2, 3)

                    # 4. Fire Telegram alert ONCE per breakout candle
                    if signal != "NEUTRAL" and c_time != last_alerted_candle:
                        send_telegram_alert(signal, price, sl, tp)
                        last_alerted_candle = c_time  # Prevents repeated notifications on current candle

                live_market_data.update({
                    "price": price,
                    "swing_high": swing_high,
                    "swing_low": swing_low,
                    "signal": signal,
                    "sl": sl,
                    "tp": tp
                })

            elif "message" in data:
                print(f"⚠️ Twelve Data API Notice: {data['message']}")

        except Exception as e:
            print(f"⚠️ Polling Error: {e}")
            
        await asyncio.sleep(3)

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
    """Serves the main web dashboard interface"""
    return FileResponse("index.html")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "Runners FX Engine is active and running"}

@app.websocket("/ws/signals")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(live_market_data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    uvicorn.run(app, host="127.0.0.1", port=8000)