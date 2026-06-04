import time
import datetime
import pytz
import threading
import os
import glob
import zipfile
import io
import pandas as pd
from flask import Flask, render_template_string, send_file
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_KEY = "tcvnurel"
CLIENT_CODE = "A1070779"
PASSWORD = "5555"
TOTP_SECRET = "JJ4RKS5OISHNHXOFPZ5JHH26EY"

# We use Nifty Futures Token (NFO) for Level 2 Depth
# Token '62329' is for NIFTY30JUN26FUT. (Needs to be updated upon expiry)
TOKENS = [{"exchangeType": 2, "tokens": ["62329"]}]

BUFFER_LIMIT = 5000  # Number of rows to hold in RAM before saving
DATA_DIR = "market_data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Global variables
tick_buffer = []
total_ticks_today = 0
last_reset_date = None
buffer_lock = threading.Lock()

# Global state for real-time candle bodies
current_minute_1 = -1
current_minute_3 = -1
current_minute_5 = -1
open_1m = None
open_3m = None
open_5m = None

# ==========================================
# ⏰ MARKET TIMING LOGIC (IST)
# ==========================================
def get_market_status():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.datetime.now(ist)
    
    # Reset daily counter at midnight
    global last_reset_date, total_ticks_today
    current_date = now.date()
    if last_reset_date != current_date:
        total_ticks_today = 0
        last_reset_date = current_date

    if now.weekday() >= 5:
        return False, "😴 Weekend: Market is Closed. Not collecting data today."
    
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    if now < market_start:
        return False, "⏳ Market opens at 9:15 AM. Waiting to start collection..."
    elif now > market_end:
        return False, "🏁 Market Closed for the day. Successfully collected today's data."
    
    return True, "🟢 Market is LIVE. Actively collecting ticks."

# ==========================================
# 🌐 FLASK WEB SERVER (Dashboard & Uptime)
# ==========================================
app = Flask(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Angel One HFT Collector</title>
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #0d1117; color: #c9d1d9; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .card { background: #161b22; padding: 40px; border-radius: 12px; box-shadow: 0 8px 24px rgba(0,0,0,0.5); border: 1px solid #30363d; text-align: center; max-width: 500px; width: 100%; }
        h1 { color: #58a6ff; font-size: 24px; margin-bottom: 20px; }
        .status-box { padding: 15px; border-radius: 8px; margin-bottom: 25px; font-weight: bold; font-size: 16px; }
        .status-live { background: rgba(46, 160, 67, 0.15); color: #3fb950; border: 1px solid rgba(46, 160, 67, 0.4); }
        .status-closed { background: rgba(210, 153, 34, 0.15); color: #d29922; border: 1px solid rgba(210, 153, 34, 0.4); }
        .stats { display: flex; justify-content: space-between; text-align: left; margin-top: 20px; }
        .stat-item { background: #0d1117; padding: 15px; border-radius: 8px; border: 1px solid #30363d; flex: 1; margin: 0 5px; }
        .stat-label { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; }
        .stat-value { font-size: 24px; font-weight: bold; color: #ffffff; margin-top: 5px; }
        .pulse { display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: #3fb950; box-shadow: 0 0 10px #3fb950; animation: pulse 1.5s infinite; margin-right: 8px; }
        @keyframes pulse { 0% { transform: scale(0.95); opacity: 0.5; } 50% { transform: scale(1.1); opacity: 1; } 100% { transform: scale(0.95); opacity: 0.5; } }
    </style>
</head>
<body>
    <div class="card">
        <h1>🦅 HFT Digital Twin Collector</h1>
        <div class="status-box {{status_class}}">
            {{pulse_html | safe}} {{status_message}}
        </div>
        <div class="stats">
            <div class="stat-item">
                <div class="stat-label">Total Ticks Today</div>
                <div class="stat-value">{{total_ticks}}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">RAM Buffer</div>
                <div class="stat-value">{{ram_rows}} <span style="font-size:12px;color:#8b949e;">/ {{buffer_limit}}</span></div>
            </div>
        </div>
        <a href="/download" style="display:inline-block; margin-top:25px; padding:12px 24px; background-color:#238636; color:#ffffff; text-decoration:none; border-radius:6px; font-weight:bold; border:1px solid rgba(240,246,252,0.1); transition: background-color 0.2s;">
            📥 Download Today's Data (ZIP)
        </a>
    </div>
</body>
</html>
"""

@app.route('/')
def home():
    with buffer_lock:
        current_ram_rows = len(tick_buffer)
        current_total = total_ticks_today
        
    is_open, msg = get_market_status()
    
    status_class = "status-live" if is_open else "status-closed"
    pulse_html = '<span class="pulse"></span>' if is_open else '⏸️'
    
    html = render_template_string(HTML_TEMPLATE,
        status_class=status_class,
        pulse_html=pulse_html,
        status_message=msg,
        total_ticks=current_total,
        ram_rows=current_ram_rows,
        buffer_limit=BUFFER_LIMIT
    )
    return html

@app.route('/download')
def download_data():
    # Force flush the current RAM buffer to disk before downloading
    global tick_buffer
    with buffer_lock:
        if len(tick_buffer) > 0:
            df = pd.DataFrame(tick_buffer)
            tick_buffer = []
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{DATA_DIR}/nifty_ticks_{timestamp_str}.parquet"
            try:
                df.to_parquet(filename, engine='pyarrow', index=False)
            except:
                pass
                
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        files = glob.glob(f"{DATA_DIR}/*.parquet")
        if not files:
            return "No data collected yet! Wait for market hours."
        for file_path in files:
            zf.write(file_path, os.path.basename(file_path))
    
    memory_file.seek(0)
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'nifty_ticks_{timestamp}.zip'
    )

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ==========================================
# 💾 DATA SAVING LOGIC (Parquet)
# ==========================================
def save_parquet_chunk():
    global tick_buffer
    with buffer_lock:
        if not tick_buffer:
            return
        
        # Convert list of dicts to DataFrame
        df = pd.DataFrame(tick_buffer)
        
        # Clear the RAM buffer
        tick_buffer = []
    
    # Generate filename with current timestamp
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/nifty_ticks_{timestamp_str}.parquet"
    
    # Save to Parquet (Requires fastparquet or pyarrow installed)
    try:
        df.to_parquet(filename, engine='pyarrow', index=False)
        print(f"💾 Saved {len(df)} rows to {filename}. RAM cleared.")
    except Exception as e:
        print(f"❌ Error saving parquet: {e}")

# ==========================================
# 📈 ANGEL ONE WEBSOCKET LOGIC
# ==========================================
def on_data(wsapp, msg):
    global tick_buffer
    try:
        # Check if it's a SnapQuote message
        if 'subscription_mode' in msg and msg['subscription_mode'] == 3:
            ltp = float(msg.get("last_traded_price", 0)) / 100.0
            
            # Real-time candle body calculation
            now_time = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
            minute = now_time.minute
            minute_3 = minute // 3
            minute_5 = minute // 5
            
            global current_minute_1, current_minute_3, current_minute_5, open_1m, open_3m, open_5m
            if minute != current_minute_1:
                current_minute_1 = minute
                open_1m = ltp
            if minute_3 != current_minute_3:
                current_minute_3 = minute_3
                open_3m = ltp
            if minute_5 != current_minute_5:
                current_minute_5 = minute_5
                open_5m = ltp
                
            row = {
                "ltt": msg.get("last_traded_time", int(time.time()*1000)),
                "ltp": ltp,
                "ltq": msg.get("last_traded_quantity", 0),
                "volume": msg.get("volume_traded_for_the_day", 0),
                "total_buy_q": msg.get("total_buy_quantity", 0),
                "total_sell_q": msg.get("total_sell_quantity", 0),
                "body_1m": round(ltp - open_1m, 2) if open_1m is not None else 0.0,
                "body_3m": round(ltp - open_3m, 2) if open_3m is not None else 0.0,
                "body_5m": round(ltp - open_5m, 2) if open_5m is not None else 0.0,
            }
            
            # Extract Top 5 Best Buy and Sell
            best_buy = msg.get('best_5_buy_data', [])
            best_sell = msg.get('best_5_sell_data', [])
            
            for i in range(5):
                row[f'bid_p_{i+1}'] = float(best_buy[i].get('price', 0))/100.0 if i < len(best_buy) else 0
                row[f'bid_q_{i+1}'] = best_buy[i].get('quantity', 0) if i < len(best_buy) else 0
                row[f'ask_p_{i+1}'] = float(best_sell[i].get('price', 0))/100.0 if i < len(best_sell) else 0
                row[f'ask_q_{i+1}'] = best_sell[i].get('quantity', 0) if i < len(best_sell) else 0

            with buffer_lock:
                global total_ticks_today
                tick_buffer.append(row)
                total_ticks_today += 1
                if len(tick_buffer) >= BUFFER_LIMIT:
                    # Flush to disk on a separate thread to not block incoming ticks
                    threading.Thread(target=save_parquet_chunk).start()
                    
    except Exception as e:
        print(f"Error parsing data: {e}")

def on_open(wsapp):
    print("✅ WebSocket Connected!")
    # Mode 3 = SnapQuote
    wsapp.subscribe("nifty_stream", 3, TOKENS)

def on_error(wsapp, error):
    print(f"🛑 WebSocket Error: {error}")

def on_close(wsapp):
    print("🧊 WebSocket Closed. Flushing remaining data to Parquet...")
    save_parquet_chunk()

def start_websocket():
    while True:
        try:
            print("🔐 Logging into Angel One to generate Feed Token...")
            obj = SmartConnect(api_key=API_KEY)
            totp = pyotp.TOTP(TOTP_SECRET).now()
            data = obj.generateSession(CLIENT_CODE, PASSWORD, totp)
            
            if data['status']:
                feed_token = obj.getfeedToken()
                print("✅ Login Successful! Generating WebSocket...")
                sws = SmartWebSocketV2(data['data']['jwtToken'], API_KEY, CLIENT_CODE, feed_token)
                sws.on_open = on_open
                sws.on_data = on_data
                sws.on_error = on_error
                sws.on_close = on_close
                
                print("🦅 Starting Angel One WebSocket Connection...")
                sws.connect()
            else:
                print(f"❌ Login Failed: {data['message']}")
        except Exception as e:
            print(f"🛑 Critical Error in WebSocket process: {e}")
            
        print("🔄 Connection lost or failed. Reconnecting in 30 seconds...")
        time.sleep(30)

# ==========================================
# 🚀 MAIN EXECUTION
# ==========================================
if __name__ == '__main__':
    # 1. Start the WebSocket in a background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    
    # 2. Start the Flask Web Server on the main thread (Render needs this to bind the port)
    run_flask()
