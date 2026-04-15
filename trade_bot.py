import json
import time
import threading
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import websocket

# ================================
# 基础配置
# ================================
BASE_URL = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/stream?streams="
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ===== 通知配置（你自行填写）=====
TG_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
TG_CHAT_ID = "5408890841"

QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

# ================================
# 参数
# ================================
SURGE_MIN_PCT = 1.5
SURGE_VOL_MULT = 2.5

PULLBACK_MAX_BARS = 20
PULLBACK_MAX_PCT = 0.55
PULLBACK_MIN_PCT = 0.15
STABILIZE_VOL_MULT = 1.5
STABILIZE_CONSEC = 2

# ================================
# 全局状态
# ================================
state = {}
kline_cache = {}

# ================================
# 通知系统（只保留两次通知）
# ================================
def send_tg(msg):
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg}, timeout=10)
    except:
        pass

def send_email(msg):
    try:
        smtp = smtplib.SMTP_SSL("smtp.qq.com", 465)
        smtp.login(QQ_EMAIL, QQ_AUTH_CODE)
        m = MIMEText(msg, "plain", "utf-8")
        m["Subject"] = "Trading Bot"
        m["From"] = QQ_EMAIL
        m["To"] = EMAIL_TO
        smtp.sendmail(QQ_EMAIL, EMAIL_TO, m.as_string())
        smtp.quit()
    except:
        pass

def notify(msg):
    print(msg)
    send_tg(msg)
    send_email(msg)

# ================================
# API（带保护）
# ================================
def get_all_symbols():
    try:
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        data = requests.get(url, timeout=10).json()

        return [
            s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT"
            and s["contractType"] == "PERPETUAL"
            and s["status"] == "TRADING"
        ]
    except:
        return []

def get_klines(symbol):
    try:
        url = f"{BASE_URL}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": "3m", "limit": 50}
        data = requests.get(url, params=params, timeout=10).json()
        return data if isinstance(data, list) else None
    except:
        return None

# ================================
# 状态
# ================================
def init_state(sym):
    if sym not in state:
        state[sym] = {"phase": "idle"}
    return state[sym]

def reset(s):
    s.update({
        "phase": "idle",
        "surge_high": 0,
        "surge_low": 0,
        "pullback_low": 0,
        "pullback_bars": 0,
        "consec": 0
    })

# ================================
# 爆发检测（修复除零+均量）
# ================================
def detect_surge(s, o, h, l, c, v):
    if o[-1] <= 0:
        return

    pct = (c[-1] - o[-1]) / o[-1] * 100

    vol_slice = v[-10:-1]
    if len(vol_slice) < 5:
        return

    avg_vol = sum(vol_slice) / len(vol_slice)
    if avg_vol <= 0:
        return

    if pct >= SURGE_MIN_PCT and v[-1] >= avg_vol * SURGE_VOL_MULT:
        s["surge_high"] = max(h[-7:])
        s["surge_low"] = min(l[-7:])
        s["pullback_low"] = c[-1]
        s["pullback_bars"] = 0
        s["phase"] = "surge"

# ================================
# 回调追踪（修复 depth 作用域问题）
# ================================
def track_pullback(s, h, l, c, v):
    s["pullback_bars"] += 1
    s["pullback_low"] = min(s["pullback_low"], l[-1])

    if h[-1] > s["surge_high"]:
        s["surge_high"] = h[-1]
        s["pullback_bars"] = 0
        return

    if l[-1] < s["surge_low"]:
        reset(s)
        return

    if s["pullback_bars"] > PULLBACK_MAX_BARS:
        reset(s)
        return

    sr = s["surge_high"] - s["surge_low"]
    if sr <= 0:
        reset(s)
        return

    depth = (s["surge_high"] - s["pullback_low"]) / sr

    if depth > PULLBACK_MAX_PCT:
        reset(s)
        return

    if depth >= PULLBACK_MIN_PCT:
        s["phase"] = "pullback"

# ================================
# 企稳信号（完整修复版）
# ================================
def detect_stabilize(s, o, h, l, c, v, price, sym):

    s["pullback_bars"] += 1
    s["pullback_low"] = min(s["pullback_low"], l[-1])

    if l[-1] < s["surge_low"]:
        reset(s)
        return

    if s["pullback_bars"] > PULLBACK_MAX_BARS:
        reset(s)
        return

    sr = s["surge_high"] - s["surge_low"]
    if sr <= 0:
        reset(s)
        return

    depth = (s["surge_high"] - s["pullback_low"]) / sr

    if depth > PULLBACK_MAX_PCT:
        reset(s)
        return

    is_bull = c[-1] > o[-1]
    s["consec"] = s.get("consec", 0) + 1 if is_bull else 0

    signal = False

    if is_bull:
        pct = (c[-1] - o[-1]) / o[-1] * 100
        vol_slice = v[-10:-1]
        avg_vol = sum(vol_slice) / len(vol_slice) if len(vol_slice) >= 5 else 1

        if pct >= 0.3 and v[-1] >= avg_vol * STABILIZE_VOL_MULT:
            signal = True

    if s["consec"] >= STABILIZE_CONSEC:
        signal = True

    if not signal:
        return

    stop = s["pullback_low"]
    risk = price - stop
    if risk <= 0:
        reset(s)
        return

    tp1 = price + risk * 2
    tp2 = price + risk * 4

    msg = f"""
🔵 二买信号
币种: {sym}
价格: {price:.6f}
止损: {stop:.6f}
TP1: {tp1:.6f}
TP2: {tp2:.6f}
"""

    notify(msg)
    reset(s)

# ================================
# WS处理（加安全）
# ================================
def on_message(ws, message):
    try:
        data = json.loads(message)

        if "data" not in data:
            return

        k = data["data"].get("k")
        if not k:
            return

        sym = k["s"]
        if sym not in kline_cache:
            return

        kc = kline_cache[sym]

        o = float(k["o"])
        h = float(k["h"])
        l = float(k["l"])
        c = float(k["c"])
        v = float(k["v"])
        closed = k["x"]

        if closed:
            kc["o"].append(o)
            kc["h"].append(h)
            kc["l"].append(l)
            kc["c"].append(c)
            kc["v"].append(v)

            if len(kc["c"]) > 50:
                for key in kc:
                    kc[key].pop(0)

            if len(kc["c"]) < 15:
                return

            s = init_state(sym)

            if s["phase"] == "idle":
                detect_surge(s, kc["o"], kc["h"], kc["l"], kc["c"], kc["v"])
            elif s["phase"] == "surge":
                track_pullback(s, kc["h"], kc["l"], kc["c"], kc["v"])
            elif s["phase"] == "pullback":
                detect_stabilize(s, kc["o"], kc["h"], kc["l"], kc["c"], kc["v"], c, sym)

        else:
            kc["o"][-1] = o
            kc["h"][-1] = h
            kc["l"][-1] = l
            kc["c"][-1] = c
            kc["v"][-1] = v

    except Exception as e:
        print("WS err:", e)

# ================================
# 历史K线（限频）
# ================================
def load_history(symbols):
    print("加载历史K线...")
    for i, sym in enumerate(symbols):
        data = get_klines(sym)

        if isinstance(data, list):
            kline_cache[sym] = {
                "o": [float(k[1]) for k in data],
                "h": [float(k[2]) for k in data],
                "l": [float(k[3]) for k in data],
                "c": [float(k[4]) for k in data],
                "v": [float(k[5]) for k in data],
            }

        if (i + 1) % 50 == 0:
            print(f"{i+1}/{len(symbols)}")
            time.sleep(1)

# ================================
# WS启动
# ================================
def start_ws(symbols):
    batch = 150

    for i in range(0, len(symbols), batch):
        part = symbols[i:i + batch]
        stream = "/".join([f"{s.lower()}@kline_3m" for s in part])
        url = WS_URL + stream

        def run(u=url):
            while True:
                try:
                    ws = websocket.WebSocketApp(u, on_message=on_message)
                    ws.run_forever(ping_interval=60, ping_timeout=30)
                except:
                    time.sleep(5)

        threading.Thread(target=run, daemon=True).start()

# ================================
# MAIN（只保留2条通知）
# ================================
if __name__ == "__main__":

    print("=" * 45)
    print("🚀 BOT START")
    print("=" * 45)

    # ① 启动通知（只一次）
    notify("🚀 系统启动成功：正在初始化数据...")

    symbols = get_all_symbols()[:200]

    print("加载历史K线...")
    load_history(symbols)

    start_ws(symbols)

    # ② 就绪通知（只一次）
    notify(f"""
✅ 初始化完成
📊 币种: {len(symbols)}
🔗 WS已启动
⚡ 正在扫描合约信号
""")

    while True:
        time.sleep(300)