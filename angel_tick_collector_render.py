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
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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
sws                = None   # global SmartWebSocketV2 object (needed for subscribe)

# Candle body tracking
current_minute_1 = -1
current_minute_3 = -1
current_minute_5 = -1
open_1m = open_3m = open_5m = None

drive_upload_status = "Waiting for Market Close"
last_uploaded_file = None

last_total_ticks = 0
current_tps = 0

def background_monitor():
    global last_total_ticks, current_tps, tick_buffer, drive_upload_status
    has_flushed_today = False
    last_flush_date = None
    
    while True:
        try:
            ist = pytz.timezone('Asia/Kolkata')
            now = datetime.datetime.now(ist)
            
            # TPS Calculation
            current_tps = total_ticks_today - last_total_ticks
            last_total_ticks = total_ticks_today
            
            # Market Close Auto-Flush Logic (3:30 PM - 3:35 PM window)
            today_date = now.date()
            if last_flush_date != today_date:
                has_flushed_today = False
                
            if now.hour == 15 and now.minute >= 35 and not has_flushed_today:
                print("🏁 Market Closed! Auto-flushing and backing up to Drive...")
                from __main__ import flush_buffer_to_disk, create_daily_zip_file, upload_to_drive # local import just in case
                try:
                    flush_buffer_to_disk()
                    date_str = now.strftime("%Y-%m-%d")
                    zip_file = create_daily_zip_file(date_str)
                    if zip_file:
                        upload_to_drive(zip_file)
                except Exception as e:
                    print("Auto-flush/Drive error:", e)
                has_flushed_today = True
                last_flush_date = today_date
                
        except Exception as e:
            print("Monitor error:", e)
        time.sleep(1)

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
    <title>Enterprise HFT Pipeline</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap" rel="stylesheet">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Inter', sans-serif; background: #080c14; color: #c9d1d9; min-height: 100vh; padding: 20px; }
        .container { max-width: 1000px; margin: 0 auto; }
        
        /* Header */
        .header { text-align: center; padding: 20px 0; }
        .header h1 { font-size: 32px; font-weight: 800; background: linear-gradient(90deg, #58a6ff, #3fb950); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .header .subtitle { font-size: 14px; color: #6e7681; margin-top: 8px; }
        
        /* Grid Layout */
        .dashboard-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-top: 20px; }
        
        /* Cards */
        .card { background: #0d1117; border: 1px solid #21262d; border-radius: 12px; padding: 20px; }
        .card-title { font-size: 12px; font-weight: 700; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; }
        
        /* Live Metrics */
        .metric-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #21262d; }
        .metric-row:last-child { border-bottom: none; margin-bottom: 0; padding-bottom: 0; }
        .metric-value { font-size: 28px; font-weight: 800; color: #fff; }
        .metric-label { font-size: 13px; color: #6e7681; }
        
        /* Order Book Bar */
        .ob-container { width: 100%; height: 12px; background: #21262d; border-radius: 6px; overflow: hidden; display: flex; margin-top: 10px; }
        .ob-buy { height: 100%; background: #3fb950; transition: width 0.3s ease; }
        .ob-sell { height: 100%; background: #f85149; transition: width 0.3s ease; }
        .ob-labels { display: flex; justify-content: space-between; font-size: 11px; margin-top: 5px; font-weight: 600; }
        .ob-labels .buy-text { color: #3fb950; }
        .ob-labels .sell-text { color: #f85149; }
        
        /* Ticks/sec Gauge */
        .tps-gauge { text-align: center; margin: 20px 0; }
        .tps-value { font-size: 48px; font-weight: 800; color: #58a6ff; text-shadow: 0 0 20px rgba(88,166,255,0.3); }
        .tps-label { font-size: 14px; color: #6e7681; text-transform: uppercase; letter-spacing: 2px; }
        
        /* Status indicator */
        .status-dot { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; }
        .live-dot { background: #3fb950; box-shadow: 0 0 10px #3fb950; animation: pulse 1s infinite; }
        @keyframes pulse { 0% { opacity: 0.6; } 50% { opacity: 1; } 100% { opacity: 0.6; } }
        
        /* File List & Downloads */
        .file-list { max-height: 250px; overflow-y: auto; }
        .file-list::-webkit-scrollbar { width: 6px; }
        .file-list::-webkit-scrollbar-thumb { background: #21262d; border-radius: 3px; }
        .file-row { display: flex; justify-content: space-between; padding: 8px 10px; background: #161b22; border-radius: 6px; margin-bottom: 6px; font-size: 12px; }
        .file-name { color: #c9d1d9; font-family: monospace; }
        
        /* Action Buttons */
        .btn-group { display: flex; flex-direction: column; gap: 10px; margin-top: 15px; }
        .btn { padding: 12px; border-radius: 8px; text-align: center; font-size: 13px; font-weight: 700; text-decoration: none; cursor: pointer; border: none; transition: 0.2s; }
        .btn:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-primary { background: #238636; color: #fff; }
        .btn-secondary { background: #1f6feb; color: #fff; }
        .btn-outline { background: transparent; border: 1px solid #30363d; color: #c9d1d9; }
        .btn-outline:hover { background: #30363d; }
        
        /* Forms */
        .date-form { display: flex; gap: 10px; margin-top: 10px; }
        .date-input { flex: 1; padding: 10px; border-radius: 6px; background: #080c14; border: 1px solid #30363d; color: #fff; color-scheme: dark; }
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🦅 HFT Data Engineering Pipeline</h1>
        <div class="subtitle"><span class="status-dot live-dot"></span> LIVE • Nifty Futures Level-2 Order Book Stream</div>
    </div>
    
    <div class="dashboard-grid">
        <!-- LEFT COLUMN -->
        <div class="left-col">
            <div class="card" style="margin-bottom: 20px;">
                <div class="card-title">Real-Time Ingestion Engine</div>
                <div class="tps-gauge">
                    <div class="tps-value" id="tps-display">0</div>
                    <div class="tps-label">Ticks Per Second</div>
                </div>
                
                <div class="metric-row" style="margin-top: 30px;">
                    <div>
                        <div class="metric-label">Memory Buffer (RAM)</div>
                        <div class="metric-value"><span id="ram-display" style="color: #d29922;">{{ ram_rows }}</span> <span style="font-size: 16px; color:#6e7681;">/ 5000</span></div>
                    </div>
                    <div style="text-align: right;">
                        <div class="metric-label">Total Extracted Today</div>
                        <div class="metric-value">{{ total_ticks }} rows</div>
                    </div>
                </div>
                
                <div style="font-size: 11px; color: #6e7681; margin-top: 10px;">
                    ⚡ Auto-Flush at 3:30 PM | 🗜️ Parquet Compression Ratio: ~92%
                </div>
            </div>
            
            <div class="card">
                <div class="card-title">Quant Visualizer: Live Order Book Imbalance</div>
                <div class="metric-row">
                    <div>
                        <div class="metric-label">Last Traded Price (LTP)</div>
                        <div class="metric-value" id="ltp-display" style="font-family: monospace;">---</div>
                    </div>
                    <div style="text-align: right;">
                        <div class="metric-label">Imbalance Ratio</div>
                        <div class="metric-value" id="ratio-display">1.0x</div>
                    </div>
                </div>
                <div class="ob-container">
                    <div class="ob-buy" id="ob-buy-bar" style="width: 50%;"></div>
                    <div class="ob-sell" id="ob-sell-bar" style="width: 50%;"></div>
                </div>
                <div class="ob-labels">
                    <div class="buy-text">BUYERS (Bid)</div>
                    <div class="sell-text">SELLERS (Ask)</div>
                </div>
            </div>
        </div>
        
        <!-- RIGHT COLUMN -->
        <div class="right-col">
            <div class="card" style="margin-bottom: 20px;">
                <div class="card-title">Download Center (Single File Merged)</div>
                <div class="btn-group">
                    <a href="/download_today" class="btn btn-primary">📅 Download Today's Zip</a>
                    <a href="/download" class="btn btn-outline">📥 Download ALL History</a>
                </div>
                
                <div style="margin-top: 20px; border-top: 1px solid #21262d; padding-top: 15px;">
                    <div class="card-title" style="margin-bottom: 10px;">Date-Wise Download</div>
                    <form class="date-form" onsubmit="event.preventDefault(); window.location.href='/download_date/' + document.getElementById('date-picker').value;">
                        <input type="date" id="date-picker" class="date-input" required>
                        <button type="submit" class="btn btn-secondary">Get ZIP</button>
                    </form>
                </div>
                <div style="margin-top: 20px; border-top: 1px solid #21262d; padding-top: 15px;">
                    <div class="card-title" style="margin-bottom: 10px;">Cloud Backup (Google Drive)</div>
                    <div class="metric-row" style="margin-bottom:0; padding-bottom:0; border:none;">
                        <div>
                            <div class="metric-label">Auto-Sync Status (3:35 PM)</div>
                            <div class="metric-value" id="drive-status" style="font-size: 14px; color: #58a6ff;">Waiting for market close...</div>
                        </div>
                        <div style="text-align: right;">
                            <a href="/test_drive" target="_blank" class="btn btn-secondary" style="padding: 6px 12px; font-size: 11px;">Test Connection</a>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <div class="card-title">Data Storage (Render Ephemeral)</div>
                <div class="file-list">
                    {{ files_html | safe }}
                </div>
            </div>
        </div>
    </div>
</div>

<script>
    // Live AJAX polling for metrics (no page reload needed)
    setInterval(() => {
        fetch('/api/metrics')
            .then(res => res.json())
            .then(data => {
                document.getElementById('tps-display').innerText = data.tps;
                document.getElementById('ram-display').innerText = data.ram_rows;
                document.getElementById('ltp-display').innerText = data.last_ltp.toFixed(2);
                document.getElementById('ratio-display').innerText = data.ob_imbalance + 'x';
                document.getElementById('drive-status').innerText = data.drive_status;
                
                // Update Order Book Bar
                let buyPct = 50;
                if (data.ob_imbalance > 0) {
                    buyPct = (data.ob_buy_q / (data.ob_buy_q + data.ob_sell_q)) * 100;
                    if(isNaN(buyPct)) buyPct = 50;
                }
                document.getElementById('ob-buy-bar').style.width = buyPct + '%';
                document.getElementById('ob-sell-bar').style.width = (100 - buyPct) + '%';
            })
            .catch(err => console.error(err));
    }, 1000); // refresh every 1 second
</script>
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

def flush_buffer_to_disk():
    global tick_buffer
    with buffer_lock:
        if tick_buffer:
            df = pd.DataFrame(tick_buffer)
            tick_buffer.clear()
            ist = pytz.timezone('Asia/Kolkata')
            ts  = datetime.datetime.now(ist).strftime("%Y%m%d_%H%M%S")
            date_str = datetime.datetime.now(ist).strftime("%Y-%m-%d")
            date_dir = os.path.join(DATA_DIR, date_str)
            os.makedirs(date_dir, exist_ok=True)
            try: 
                df.to_parquet(os.path.join(date_dir, f"nifty_ticks_{ts}.parquet"), index=False)
            except Exception as e: 
                print(f"Flush error: {e}")

def create_daily_zip_file(date_str):
    date_dir = os.path.join(DATA_DIR, date_str)
    if not os.path.exists(date_dir):
        return None
    files = sorted(glob.glob(os.path.join(date_dir, "*.parquet")))
    chunk_files = [f for f in files if "MERGED" not in f]
    if not chunk_files:
        return None
        
    try:
        merged_df = pd.concat([pd.read_parquet(f) for f in chunk_files], ignore_index=True)
        merged_df.sort_values("ltt", inplace=True)
        merged_filename = os.path.join(date_dir, f"nifty_MERGED_{date_str}.parquet")
        merged_df.to_parquet(merged_filename, index=False)
        
        zip_path = os.path.join(date_dir, f"nifty_MERGED_{date_str}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            arcname = merged_filename.replace(DATA_DIR + os.sep, "")
            zf.write(merged_filename, arcname)
        return zip_path
    except Exception as e:
        print(f"Merge error: {e}")
        return None

def upload_to_drive(file_path):
    global drive_upload_status, last_uploaded_file
    try:
        gcp_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        folder_id = os.environ.get("GCP_DRIVE_FOLDER_ID")
        
        if not gcp_json_str or not folder_id:
            drive_upload_status = "⚠️ Keys missing (Setup Render Env)"
            return
            
        drive_upload_status = "🔄 Uploading to Drive..."
        creds_dict = json.loads(gcp_json_str)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=creds)
        
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='application/zip')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        drive_upload_status = f"✅ Uploaded (ID: {file.get('id')})"
        last_uploaded_file = os.path.basename(file_path)
        print(f"✅ Google Drive Upload Success: {file_path}")
    except Exception as e:
        drive_upload_status = f"❌ Drive Error: {str(e)[:40]}"
        print(f"❌ Google Drive Upload Error: {e}")

@app.route('/test_drive')
def test_drive():
    global drive_upload_status
    try:
        gcp_json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        folder_id = os.environ.get("GCP_DRIVE_FOLDER_ID")
        
        if not gcp_json_str or not folder_id:
            drive_upload_status = "⚠️ Keys missing (Setup Render Env)"
            return "❌ Missing Environment Variables: GCP_SERVICE_ACCOUNT_JSON or GCP_DRIVE_FOLDER_ID", 400
            
        drive_upload_status = "🔄 Testing Drive..."
        creds_dict = json.loads(gcp_json_str)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=creds)
        
        # Create a tiny test file
        test_file_path = os.path.join(DATA_DIR, "test_connection.txt")
        with open(test_file_path, "w") as f:
            f.write("Google Drive API is working perfectly from Render!")
            
        file_metadata = {
            'name': 'test_connection.txt',
            'parents': [folder_id]
        }
        media = MediaFileUpload(test_file_path, mimetype='text/plain')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        drive_upload_status = f"✅ Test Success! (ID: {file.get('id')})"
        return "✅ Success! Check your Google Drive folder, 'test_connection.txt' should be there.", 200
        
    except Exception as e:
        drive_upload_status = f"❌ Test Error: {str(e)[:40]}"
        return f"❌ Connection Error: {str(e)}", 500

@app.route('/download')
def download_all():
    flush_buffer_to_disk()

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
    flush_buffer_to_disk()
    ist = pytz.timezone('Asia/Kolkata')
    date_str = datetime.datetime.now(ist).strftime("%Y-%m-%d")
    return download_date(date_str) # Re-use the merge logic!


@app.route('/api/metrics')
def api_metrics():
    with buffer_lock:
        ram_rows = len(tick_buffer)
        
    ob_imbalance = 1.0
    last_ltp = 0
    t_buy = 0
    t_sell = 0
    
    with buffer_lock:
        if len(tick_buffer) > 0:
            last_row = tick_buffer[-1]
            t_buy = last_row.get("total_buy_q", 0)
            t_sell = last_row.get("total_sell_q", 0)
            if t_sell > 0:
                ob_imbalance = round(t_buy / t_sell, 2)
            last_ltp = last_row.get("ltp", 0)
            
    return jsonify({
        "tps": current_tps,
        "ram_rows": ram_rows,
        "ob_imbalance": ob_imbalance,
        "ob_buy_q": t_buy,
        "ob_sell_q": t_sell,
        "last_ltp": last_ltp,
        "drive_status": drive_upload_status
    })

@app.route('/download_date/<date_str>')
def download_date(date_str):
    # Auto flush if asking for today
    if date_str == datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d"):
        flush_buffer_to_disk()
        
    date_dir = os.path.join(DATA_DIR, date_str)
    files = sorted(glob.glob(os.path.join(date_dir, "*.parquet")))
    if not files:
        return f"No data found for {date_str}. Note: Render Free Tier deletes data on restarts.", 404
        
    # Exclude merged files to avoid duplication
    chunk_files = [f for f in files if "MERGED" not in f]
    if not chunk_files:
        chunk_files = files
        
    try:
        merged_df = pd.concat([pd.read_parquet(f) for f in chunk_files], ignore_index=True)
        merged_df.sort_values("ltt", inplace=True)
        
        merged_filename = os.path.join(date_dir, f"nifty_MERGED_{date_str}.parquet")
        merged_df.to_parquet(merged_filename, index=False)
        
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            arcname = merged_filename.replace(DATA_DIR + os.sep, "")
            zf.write(merged_filename, arcname)
        memory_file.seek(0)
        
        return send_file(memory_file, mimetype='application/zip', as_attachment=True,
                         download_name=f'nifty_MERGED_{date_str}.zip')
    except Exception as e:
        return f"Merge error: {e}", 500



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
    global ws_status, sws
    ws_status = "✅ Connected — Subscribing..."
    print("="*60, flush=True)
    print("✅ WebSocket on_open FIRED! Subscribing via sws object...", flush=True)
    print(f"   Token: {ACTIVE_TOKEN}  Symbol: {ACTIVE_SYMBOL}", flush=True)
    print(f"   Tokens list: {ACTIVE_TOKENS}", flush=True)
    print("="*60, flush=True)
    try:
        # sws.subscribe() — NOT wsapp.subscribe() (wsapp is raw websocket-client object)
        # Mode 3 = SnapQuote (Full Level-2 Order Book)
        sws.subscribe("nifty_stream", 3, ACTIVE_TOKENS)
        print("📡 Mode 3 (SnapQuote) subscription sent!", flush=True)
        ws_status = "📡 Subscribed Mode 3 (SnapQuote) — receiving data..."
    except Exception as e:
        print(f"❌ Subscribe ERROR: {e}", flush=True)
        ws_status = f"❌ Subscribe failed: {e}"

def on_error(wsapp, error):
    global ws_status
    ws_status = f"🛑 WS Error: {error}"
    print(f"🛑 WS ERROR DETAIL: {repr(error)}", flush=True)
    print(f"   Error type: {type(error).__name__}", flush=True)

def on_close(wsapp):
    global ws_status
    ws_status = "🔄 Disconnected — Reconnecting..."
    print("🧊 WebSocket on_close FIRED. Flushing buffer...", flush=True)
    save_parquet_chunk()

# ==========================================
# 🔄 ONE LOGIN → ALL DAY NONSTOP DATA
# ==========================================
def do_login():
    """Login ONCE. Returns (jwt_token, feed_token) or (None, None) on failure."""
    global ws_status
    try:
        print("="*60, flush=True)
        print("🔐 ONE-TIME Login to Angel One starting...", flush=True)
        ws_status = "🔐 Logging in (one-time)..."
        obj  = SmartConnect(api_key=API_KEY)
        totp_val = pyotp.TOTP(TOTP_SECRET).now()
        print(f"   TOTP generated: {totp_val}", flush=True)
        data = obj.generateSession(CLIENT_CODE, PASSWORD, totp_val)
        print(f"   Login response status: {data.get('status')}", flush=True)
        print(f"   Login response message: {data.get('message','N/A')}", flush=True)

        if not data.get('status'):
            print(f"❌ Login FAILED: {data.get('message')}", flush=True)
            ws_status = f"❌ Login Failed: {data.get('message','Unknown')}"
            return None, None

        feed_token = obj.getfeedToken()
        jwt_token  = data['data']['jwtToken']
        print(f"✅ Login SUCCESS! Feed token = {feed_token[:20]}...", flush=True)
        print("="*60, flush=True)
        return jwt_token, feed_token

    except Exception as e:
        print(f"🛑 Login EXCEPTION: {repr(e)}", flush=True)
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
            print("🦅 Starting WebSocket with 9999 retries (nonstop data)...", flush=True)

            global sws
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
    monitor_thread = threading.Thread(target=background_monitor, daemon=True)
    monitor_thread.start()
    
    ws_thread = threading.Thread(target=start_websocket, daemon=True)

    ws_thread.start()
    run_flask()
