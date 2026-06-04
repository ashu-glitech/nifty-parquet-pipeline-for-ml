import time
import datetime
import pytz
import threading
import os
import glob
import zipfile
import io
import pandas as pd
from flask import Flask, render_template_string, send_file, jsonify
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import urllib.request
import json

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
API_KEY       = "tcvnurel"
CLIENT_CODE   = "A1070779"
PASSWORD      = "5555"
TOTP_SECRET   = "JJ4RKS5OISHNHXOFPZ5JHH26EY"

BUFFER_LIMIT  = 5000   # flush to disk after this many rows
DATA_DIR      = "market_data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# ==========================================
# 🤖 AUTO-TOKEN FETCH LOGIC
# ==========================================
def get_latest_nifty_future_token():
    print("🌐 Auto-fetching nearest NIFTY Futures token...")
    try:
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())

        nifty_futs = [x for x in data
                      if x['name'] == 'NIFTY' and x['instrumenttype'] == 'FUTIDX']
        nifty_futs.sort(key=lambda x: datetime.datetime.strptime(x['expiry'], '%d%b%Y'))

        token  = nifty_futs[0]['token']
        symbol = nifty_futs[0]['symbol']
        print(f"✅ Auto-Token: {symbol} (token={token})")
        return token, symbol
    except Exception as e:
        print(f"⚠️ Auto-fetch failed, fallback token used: {e}")
        return "62329", "NIFTY30JUN26FUT"

ACTIVE_TOKEN, ACTIVE_SYMBOL = get_latest_nifty_future_token()
ACTIVE_TOKENS = [{"exchangeType": 2, "tokens": [ACTIVE_TOKEN]}]

# ==========================================
# 🌍 GLOBAL STATE
# ==========================================
tick_buffer        = []
total_ticks_today  = 0
last_reset_date    = None
buffer_lock        = threading.Lock()
ws_status          = "🔄 Connecting..."

# Candle body tracking
current_minute_1 = -1
current_minute_3 = -1
current_minute_5 = -1
open_1m = open_3m = open_5m = None

# ==========================================
# ⏰ MARKET TIMING LOGIC (IST)
# ==========================================
def get_market_status():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.datetime.now(ist)

    global last_reset_date, total_ticks_today
    today = now.date()
    if last_reset_date != today:
        total_ticks_today = 0
        last_reset_date   = today

    if now.weekday() >= 5:
        return False, "😴 Weekend — Market Closed"

    open_t  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if now < open_t:
        return False, f"⏳ Market opens at 9:15 AM IST"
    elif now > close_t:
        return False, "🏁 Market Closed — Data saved for today"
    return True, "🟢 LIVE — Actively collecting ticks"

# ==========================================
# 💾 PARQUET SAVE LOGIC
# ==========================================
def save_parquet_chunk():
    global tick_buffer
    with buffer_lock:
        if not tick_buffer:
            return
        df = pd.DataFrame(tick_buffer)
        tick_buffer = []

    ist = pytz.timezone('Asia/Kolkata')
    ts  = datetime.datetime.now(ist).strftime("%Y%m%d_%H%M%S")
    date_str = datetime.datetime.now(ist).strftime("%Y-%m-%d")
    date_dir  = os.path.join(DATA_DIR, date_str)
    os.makedirs(date_dir, exist_ok=True)
    filename = os.path.join(date_dir, f"nifty_ticks_{ts}.parquet")

    try:
        df.to_parquet(filename, engine='pyarrow', index=False)
        print(f"💾 Saved {len(df)} rows → {filename}")
    except Exception as e:
        print(f"❌ Parquet save error: {e}")

# ==========================================
# 📊 DASHBOARD HTML
# ==========================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HFT Tick Collector — Nifty</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', sans-serif;
            background: #080c14;
            color: #c9d1d9;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 860px; margin: 0 auto; }
        /* Header */
        .header {
            text-align: center;
            padding: 30px 0 20px;
        }
        .header h1 {
            font-size: 28px;
            font-weight: 700;
            background: linear-gradient(90deg, #58a6ff, #3fb950);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .header .subtitle {
            font-size: 13px;
            color: #6e7681;
            margin-top: 6px;
        }
        /* Status Bar */
        .status-bar {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 14px 20px;
            border-radius: 10px;
            margin: 16px 0;
            font-weight: 600;
            font-size: 15px;
        }
        .status-live   { background: rgba(46,160,67,0.12); color: #3fb950; border: 1px solid rgba(46,160,67,0.35); }
        .status-closed { background: rgba(210,153,34,0.12); color: #d29922; border: 1px solid rgba(210,153,34,0.35); }
        .status-error  { background: rgba(248,81,73,0.12);  color: #f85149; border: 1px solid rgba(248,81,73,0.35); }
        .dot {
            width: 10px; height: 10px; border-radius: 50%;
            flex-shrink: 0;
        }
        .dot-green { background:#3fb950; box-shadow:0 0 8px #3fb950; animation: pulse 1.5s infinite; }
        .dot-yellow { background:#d29922; }
        .dot-red    { background:#f85149; }
        @keyframes pulse {
            0%,100% { transform:scale(0.9); opacity:0.6; }
            50%      { transform:scale(1.1); opacity:1; }
        }
        /* Stat Cards */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            margin: 16px 0;
        }
        .stat-card {
            background: #0d1117;
            border: 1px solid #21262d;
            border-radius: 10px;
            padding: 18px 16px;
        }
        .stat-label {
            font-size: 11px;
            color: #6e7681;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .stat-value {
            font-size: 30px;
            font-weight: 700;
            color: #fff;
            margin-top: 6px;
        }
        .stat-sub {
            font-size: 12px;
            color: #6e7681;
            margin-top: 3px;
        }
        /* WS Status */
        .ws-bar {
            background: #0d1117;
            border: 1px solid #21262d;
            border-radius: 10px;
            padding: 12px 16px;
            font-size: 13px;
            color: #8b949e;
            margin-bottom: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        /* File list */
        .files-section {
            background: #0d1117;
            border: 1px solid #21262d;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 16px;
        }
        .files-section h3 {
            font-size: 14px;
            font-weight: 600;
            color: #8b949e;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 14px;
        }
        .date-group { margin-bottom: 14px; }
        .date-label {
            font-size: 13px;
            font-weight: 600;
            color: #58a6ff;
            margin-bottom: 6px;
        }
        .file-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 7px 10px;
            background: #161b22;
            border-radius: 6px;
            margin-bottom: 4px;
            font-size: 12px;
        }
        .file-name { color: #c9d1d9; }
        .file-size { color: #6e7681; }
        .no-files { color: #484f58; font-size: 13px; text-align: center; padding: 20px 0; }
        /* Buttons */
        .btn-row { display: flex; gap: 10px; margin-top: 4px; }
        .btn {
            flex: 1;
            display: block;
            padding: 13px;
            border-radius: 8px;
            text-align: center;
            font-size: 14px;
            font-weight: 600;
            text-decoration: none;
            cursor: pointer;
            border: none;
            transition: opacity 0.2s, transform 0.1s;
        }
        .btn:hover { opacity: 0.85; transform: translateY(-1px); }
        .btn-green  { background: #238636; color: #fff; }
        .btn-blue   { background: #1f6feb; color: #fff; }
        /* Auto-refresh */
        .refresh-note { text-align:center; font-size:11px; color:#484f58; margin-top:14px; }
    </style>
    <meta http-equiv="refresh" content="15">
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🦅 HFT Tick Collector</h1>
        <div class="subtitle">Nifty Futures Level-2 Data • Auto-refreshes every 15s</div>
    </div>

    <div class="status-bar {{ status_class }}">
        <div class="dot {{ dot_class }}"></div>
        {{ status_message }}
    </div>

    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total Ticks Today</div>
            <div class="stat-value">{{ total_ticks }}</div>
            <div class="stat-sub">rows collected</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">RAM Buffer</div>
            <div class="stat-value">{{ ram_rows }}</div>
            <div class="stat-sub">/ {{ buffer_limit }} rows</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Parquet Files</div>
            <div class="stat-value">{{ total_files }}</div>
            <div class="stat-sub">saved to disk</div>
        </div>
    </div>

    <div class="ws-bar">
        <span>🔌 WebSocket: <strong>{{ ws_status }}</strong></span>
        <span>📌 Token: <strong style="color:#58a6ff;">{{ active_symbol }}</strong></span>
    </div>

    <div class="files-section">
        <h3>📂 Saved Parquet Files (Date-wise)</h3>
        {{ files_html | safe }}
    </div>

    <div class="btn-row">
        <a href="/download" class="btn btn-green">📥 Download ALL Data (ZIP)</a>
        <a href="/download_today" class="btn btn-blue">📅 Download Today Only</a>
    </div>

    <div class="refresh-note">Page auto-refreshes every 15 seconds</div>
</div>
</body>
</html>
"""

app = Flask(__name__)

def get_files_html():
    all_dirs = sorted(glob.glob(os.path.join(DATA_DIR, "????-??-??")), reverse=True)
    if not all_dirs:
        # Also check root DATA_DIR for old files
        old_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.parquet")), reverse=True)
        if not old_files:
            return '<div class="no-files">No data collected yet. Files appear here during market hours.</div>'
        html = '<div class="date-group"><div class="date-label">Legacy Files</div>'
        for f in old_files[:20]:
            sz = round(os.path.getsize(f)/1024, 1)
            html += f'<div class="file-row"><span class="file-name">{os.path.basename(f)}</span><span class="file-size">{sz} KB</span></div>'
        html += '</div>'
        return html

    html = ""
    for d in all_dirs:
        date_label = os.path.basename(d)
        files = sorted(glob.glob(os.path.join(d, "*.parquet")), reverse=True)
        if not files:
            continue
        total_kb = round(sum(os.path.getsize(f) for f in files)/1024, 1)
        html += f'<div class="date-group">'
        html += f'<div class="date-label">📅 {date_label} — {len(files)} files ({total_kb} KB)</div>'
        for f in files[:10]:
            sz = round(os.path.getsize(f)/1024, 1)
            html += f'<div class="file-row"><span class="file-name">{os.path.basename(f)}</span><span class="file-size">{sz} KB</span></div>'
        if len(files) > 10:
            html += f'<div class="file-row"><span class="file-name" style="color:#6e7681;">... and {len(files)-10} more files</span></div>'
        html += '</div>'
    return html

@app.route('/')
def home():
    with buffer_lock:
        ram_rows      = len(tick_buffer)
        current_total = total_ticks_today

    is_open, msg = get_market_status()

    if is_open:
        status_class = "status-live"
        dot_class    = "dot-green"
    elif "Error" in ws_status or "Closed" in ws_status:
        status_class = "status-error"
        dot_class    = "dot-red"
    else:
        status_class = "status-closed"
        dot_class    = "dot-yellow"

    all_files = (glob.glob(os.path.join(DATA_DIR, "????-??-??", "*.parquet")) +
                 glob.glob(os.path.join(DATA_DIR, "*.parquet")))

    return render_template_string(
        HTML_TEMPLATE,
        status_class   = status_class,
        dot_class      = dot_class,
        status_message = msg,
        total_ticks    = current_total,
        ram_rows       = ram_rows,
        buffer_limit   = BUFFER_LIMIT,
        total_files    = len(all_files),
        ws_status      = ws_status,
        active_symbol  = ACTIVE_SYMBOL,
        files_html     = get_files_html(),
    )

def _make_zip(pattern_list):
    """Create an in-memory ZIP from list of glob patterns."""
    memory_file = io.BytesIO()
    found = []
    for pat in pattern_list:
        found += glob.glob(pat)
    if not found:
        return None
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fp in found:
            arcname = fp.replace(DATA_DIR + os.sep, "")
            zf.write(fp, arcname)
    memory_file.seek(0)
    return memory_file

@app.route('/download')
def download_all():
    # Flush RAM first
    with buffer_lock:
        if tick_buffer:
            df = pd.DataFrame(tick_buffer)
            tick_buffer.clear()
            ist = pytz.timezone('Asia/Kolkata')
            ts  = datetime.datetime.now(ist).strftime("%Y%m%d_%H%M%S")
            date_str = datetime.datetime.now(ist).strftime("%Y-%m-%d")
            date_dir = os.path.join(DATA_DIR, date_str)
            os.makedirs(date_dir, exist_ok=True)
            try: df.to_parquet(os.path.join(date_dir, f"nifty_ticks_{ts}.parquet"), index=False)
            except: pass

    patterns = [
        os.path.join(DATA_DIR, "????-??-??", "*.parquet"),
        os.path.join(DATA_DIR, "*.parquet"),
    ]
    mf = _make_zip(patterns)
    if mf is None:
        return "No data yet! Come back during market hours.", 404

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(mf, mimetype='application/zip', as_attachment=True,
                     download_name=f'nifty_ALL_{ts}.zip')

@app.route('/download_today')
def download_today():
    ist = pytz.timezone('Asia/Kolkata')
    date_str = datetime.datetime.now(ist).strftime("%Y-%m-%d")
    patterns = [os.path.join(DATA_DIR, date_str, "*.parquet")]
    mf = _make_zip(patterns)
    if mf is None:
        return "No data for today yet!", 404
    return send_file(mf, mimetype='application/zip', as_attachment=True,
                     download_name=f'nifty_{date_str}.zip')

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

# ==========================================
# 📈 WEBSOCKET CALLBACKS
# ==========================================
def on_data(wsapp, msg):
    global ws_status
    try:
        if msg.get('subscription_mode') == 3:
            ws_status = "✅ Connected & Receiving"
            ltp = float(msg.get("last_traded_price", 0)) / 100.0

            # --- Candle body tracking ---
            ist    = pytz.timezone('Asia/Kolkata')
            now_t  = datetime.datetime.now(ist)
            minute = now_t.minute
            m3     = minute // 3
            m5     = minute // 5

            global current_minute_1, current_minute_3, current_minute_5, open_1m, open_3m, open_5m
            if minute != current_minute_1:
                current_minute_1 = minute;  open_1m = ltp
            if m3 != current_minute_3:
                current_minute_3 = m3;      open_3m = ltp
            if m5 != current_minute_5:
                current_minute_5 = m5;      open_5m = ltp

            row = {
                "ltt"          : msg.get("last_traded_time", int(time.time()*1000)),
                "ltp"          : ltp,
                "ltq"          : msg.get("last_traded_quantity", 0),
                "volume"       : msg.get("volume_traded_for_the_day", 0),
                "total_buy_q"  : msg.get("total_buy_quantity", 0),
                "total_sell_q" : msg.get("total_sell_quantity", 0),
                "body_1m"      : round(ltp - open_1m, 2) if open_1m else 0.0,
                "body_3m"      : round(ltp - open_3m, 2) if open_3m else 0.0,
                "body_5m"      : round(ltp - open_5m, 2) if open_5m else 0.0,
            }

            best_buy  = msg.get('best_5_buy_data',  [])
            best_sell = msg.get('best_5_sell_data', [])
            for i in range(5):
                row[f'bid_p_{i+1}'] = float(best_buy[i].get('price',0))/100.0  if i < len(best_buy)  else 0
                row[f'bid_q_{i+1}'] = best_buy[i].get('quantity', 0)            if i < len(best_buy)  else 0
                row[f'ask_p_{i+1}'] = float(best_sell[i].get('price',0))/100.0 if i < len(best_sell) else 0
                row[f'ask_q_{i+1}'] = best_sell[i].get('quantity', 0)           if i < len(best_sell) else 0

            global tick_buffer, total_ticks_today
            with buffer_lock:
                tick_buffer.append(row)
                total_ticks_today += 1
                if len(tick_buffer) >= BUFFER_LIMIT:
                    threading.Thread(target=save_parquet_chunk, daemon=True).start()

    except Exception as e:
        print(f"❌ on_data error: {e}")

def on_open(wsapp):
    global ws_status
    ws_status = "✅ Connected — Subscribing..."
    print("✅ WebSocket Connected! Subscribing to Nifty Futures...")
    wsapp.subscribe("nifty_stream", 3, ACTIVE_TOKENS)

def on_error(wsapp, error):
    global ws_status
    ws_status = f"🛑 Error: {error}"
    print(f"🛑 WS Error: {error}")

def on_close(wsapp):
    global ws_status
    ws_status = "🔄 Disconnected — Reconnecting..."
    print("🧊 WebSocket Closed. Flushing buffer...")
    save_parquet_chunk()

# ==========================================
# 🔄 ONE LOGIN → ALL DAY NONSTOP DATA
# ==========================================
def do_login():
    """Login ONCE. Returns (sws_object) or None on failure."""
    global ws_status
    try:
        print("🔐 ONE-TIME Login to Angel One...")
        ws_status = "🔐 Logging in (one-time)..."
        obj  = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = obj.generateSession(CLIENT_CODE, PASSWORD, totp)

        if not data.get('status'):
            print(f"❌ Login Failed: {data.get('message')}")
            ws_status = f"❌ Login Failed: {data.get('message','Unknown')}"
            return None, None

        feed_token = obj.getfeedToken()
        jwt_token  = data['data']['jwtToken']
        print("✅ ONE-TIME Login Successful! JWT stored for the day.")
        return jwt_token, feed_token

    except Exception as e:
        print(f"🛑 Login exception: {e}")
        ws_status = f"🛑 Login Error: {str(e)[:60]}"
        return None, None


def start_websocket():
    """
    Strategy:
    - LOGIN only ONCE per day at startup (or after midnight reset).
    - WebSocket internally retries 9999 times without re-login.
    - Only re-login if JWT actually expires (market opens next day).
    - 9:15 AM → 3:30 PM : zero data miss. Water-like flow!
    """
    global ws_status

    login_date = None   # track which day we logged in
    jwt_token  = None
    feed_token = None

    while True:
        ist  = pytz.timezone('Asia/Kolkata')
        now  = datetime.datetime.now(ist)
        today = now.date()

        # ── Wait until market hours ──────────────────────────────────
        is_open, status_msg = get_market_status()
        if not is_open:
            ws_status = f"😴 {status_msg}"
            time.sleep(30)
            continue

        # ── Login ONCE per calendar day ──────────────────────────────
        if login_date != today or jwt_token is None:
            jwt_token, feed_token = do_login()
            if jwt_token is None:
                print("⏳ Login failed — retrying in 15s...")
                time.sleep(15)
                continue
            login_date = today
            print(f"🔑 Token valid for today ({today}). Will NOT re-login until tomorrow.")

        # ── Start WebSocket with HUGE retry (practically infinite) ───
        try:
            ws_status = "🔌 Connecting WebSocket..."
            print("🦅 Starting WebSocket with 9999 retries (nonstop data)...")

            sws = SmartWebSocketV2(
                jwt_token, API_KEY, CLIENT_CODE, feed_token,
                max_retry_attempt = 9999,   # Never give up!
                retry_delay       = 2       # 2s between auto-retries
            )
            sws.on_open  = on_open
            sws.on_data  = on_data
            sws.on_error = on_error
            sws.on_close = on_close

            sws.connect()  # ← Blocks here. Internal library retries 9999 times.

        except Exception as e:
            err = str(e).lower()
            print(f"🛑 WebSocket dropped: {e}")

            # If it's an auth / token error → force re-login
            if any(w in err for w in ['auth', 'token', 'unauthorized', '401', 'expired']):
                print("🔑 JWT expired! Will re-login on next loop.")
                jwt_token  = None
                feed_token = None
                login_date = None
                ws_status  = "🔑 JWT expired — Re-logging in..."
            else:
                ws_status = f"🔄 Reconnecting... ({str(e)[:40]})"

        # Short sleep then outer loop re-checks market hours & reconnects
        print("⏳ 10s pause then reconnect check...")
        time.sleep(10)

# ==========================================
# 🚀 MAIN
# ==========================================
if __name__ == '__main__':
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    run_flask()
