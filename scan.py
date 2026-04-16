import json
import time
import threading
import requests
import smtplib
import websocket
from email.mime.text import MIMEText
from datetime import datetime
from collections import defaultdict

# ========== 配置 ==========
BASE_URL = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/ws"

TG_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
TG_CHAT_ID = "5408890841"
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

MIN_VOLUME_24H = 3_000_000
WS_BATCH = 200

# 策略参数（保持不变）
DROP_7D_PCT = -10
NEAR_LOW_PCT = 8
VOL_SHRINK_DAYS = 3
VOL_SURGE_MULT = 1.8
SURGE_CHANGE_MIN = -2
SURGE_CHANGE_MAX = 8
CONSOLIDATION_DAYS = 3
CONSOLIDATION_RANGE = 0.04
OI_INCREASE_PCT = 5
FUNDING_RATE_MIN = -0.3
FUNDING_RATE_DAYS = 3

BLACKLIST = [
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "EURUSDT", "GBPUSDT", "JPYUSDT", "AUDUSDT",
]

# ========== 全局数据存储 ==========
# K线数据（每个币种保存35天日线）
kline_data = defaultdict(lambda: [])
# OI数据（每个币种保存最近5个值）
oi_data = defaultdict(lambda: [])
# 资金费率数据（每个币种保存最近10个值）
funding_data = defaultdict(lambda: [])
# 24小时行情数据
ticker_data = {}
# 信号历史
signal_history = {}
# WebSocket 连接状态
ws_connected = False

# ========== 通知函数 ==========
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
        m["Subject"] = "Building Signal"
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

# ========== 获取币种列表（一次性，用REST，后续可缓存） ==========
def get_all_symbols():
    try:
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        data = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
        symbols = []
        for s in data["symbols"]:
            if (s["contractType"] == "PERPETUAL"
                    and s["quoteAsset"] == "USDT"
                    and s["status"] == "TRADING"
                    and s["symbol"] not in BLACKLIST):
                symbols.append(s["symbol"])
        # 按成交量筛选（一次性）
        ticker_url = f"{BASE_URL}/fapi/v1/ticker/24hr"
        tickers = requests.get(ticker_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
        volume_dict = {t["symbol"]: float(t["quoteVolume"]) for t in tickers}
        symbols = [s for s in symbols if volume_dict.get(s, 0) >= MIN_VOLUME_24H]
        return symbols
    except Exception as e:
        print(f"获取币种失败: {e}")
        return []

# ========== 三阶段检测逻辑（完全保持不变） ==========
def check_stage1_decline(klines):
    if not klines or len(klines) < 30:
        return None

    closes = [k["c"] for k in klines]
    vols = [k["v"] for k in klines]

    price_now = closes[-1]
    price_7d_ago = closes[-8] if len(closes) >= 8 else closes[0]
    change_7d = (price_now - price_7d_ago) / price_7d_ago * 100

    if change_7d > DROP_7D_PCT:
        return None

    low_30d = min(closes[-30:])
    dist_to_low = (price_now - low_30d) / low_30d * 100

    if dist_to_low > NEAR_LOW_PCT:
        return None

    vol_ma20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    recent_vols = vols[-VOL_SHRINK_DAYS:]
    shrink_ok = all(v < vol_ma20 for v in recent_vols)

    if not shrink_ok:
        return None

    change_30d = (price_now - closes[-30]) / closes[-30] * 100 if len(closes) >= 30 else 0

    return {
        "change_7d": change_7d,
        "change_30d": change_30d,
        "dist_to_low": dist_to_low,
        "low_30d": low_30d,
        "vol_ma20": vol_ma20,
    }

def check_stage2_surge(klines, stage1):
    if not klines or len(klines) < 10:
        return None

    vol_ma20 = stage1["vol_ma20"]

    for i in range(len(klines) - 5, len(klines)):
        if i < 1:
            continue

        k = klines[i]
        prev = klines[i - 1]
        prev5_vols = [klines[j]["v"] for j in range(max(0, i - 5), i)]

        if not prev5_vols:
            continue

        avg5 = sum(prev5_vols) / len(prev5_vols)
        change = (k["c"] - prev["c"]) / prev["c"] * 100

        if (k["v"] >= avg5 * VOL_SURGE_MULT
                and SURGE_CHANGE_MIN <= change <= SURGE_CHANGE_MAX
                and k["c"] > prev["c"]):
            return {
                "surge_day": i,
                "surge_vol_ratio": k["v"] / avg5,
                "surge_change": change,
                "surge_low": k["l"],
                "surge_close": k["c"],
            }

    return None

def check_stage3_consolidation(klines, stage2):
    if not klines:
        return None

    surge_day = stage2["surge_day"]
    surge_low = stage2["surge_low"]

    after = klines[surge_day + 1:]
    if len(after) < 1:
        return None

    if len(after) > CONSOLIDATION_DAYS:
        after = after[:CONSOLIDATION_DAYS]

    highs = [k["h"] for k in after]
    lows = [k["l"] for k in after]

    if not highs or not lows:
        return None

    range_pct = (max(highs) - min(lows)) / min(lows)

    if range_pct > CONSOLIDATION_RANGE:
        return None

    broke_low = any(k["l"] < surge_low for k in after)
    if broke_low:
        return None

    lows_rising = True
    for i in range(1, len(after)):
        if after[i]["l"] < after[i - 1]["l"] * 0.998:
            lows_rising = False
            break

    return {
        "consolidation_days": len(after),
        "range_pct": range_pct * 100,
        "lows_rising": lows_rising,
        "held_above_surge_low": True,
    }

def check_oi_and_funding(sym):
    result = {"oi_change": 0, "oi_increasing": False, "funding_avg": 0, "funding_ok": True}

    oi_list = oi_data.get(sym, [])
    if len(oi_list) >= 3:
        oi_recent = oi_list[-1]
        oi_before = oi_list[-3]
        if oi_before > 0:
            result["oi_change"] = (oi_recent - oi_before) / oi_before * 100
            result["oi_increasing"] = result["oi_change"] > OI_INCREASE_PCT

    funding_list = funding_data.get(sym, [])
    if len(funding_list) >= FUNDING_RATE_DAYS:
        recent_funding = funding_list[-FUNDING_RATE_DAYS:]
        result["funding_avg"] = sum(recent_funding) / len(recent_funding)
        result["funding_ok"] = not all(f < FUNDING_RATE_MIN for f in recent_funding)

    return result

def classify_signal(stage1, stage2, stage3, oi_data):
    if stage1 and not stage2 and not stage3:
        return "observe"
    if stage1 and stage2 and not stage3:
        return "watch"
    if stage1 and stage2 and stage3:
        if oi_data["oi_increasing"] and stage3["lows_rising"]:
            return "entry"
        return "watch"
    return None

def format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data):
    icons = {"observe": "🟢", "watch": "🟡", "entry": "🔴"}
    labels = {"observe": "观察", "watch": "关注", "entry": "介入"}

    icon = icons.get(level, "⚪")
    label = labels.get(level, "未知")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"{icon} 建仓信号 [{label}]",
        f"币种: {sym}",
        f"时间: {now}",
        f"价格: {price:.6f}",
        "",
        "📉 下跌结构",
        f" 7天跌幅: {stage1['change_7d']:.1f}%",
        f" 30天跌幅: {stage1['change_30d']:.1f}%",
        f" 距底部: {stage1['dist_to_low']:.1f}%",
    ]

    if stage2:
        lines += [
            "",
            "📊 底部放量",
            f" 放量倍数: {stage2['surge_vol_ratio']:.1f}x",
            f" 放量当日涨幅: {stage2['surge_change']:.1f}%",
        ]

    if stage3:
        lines += [
            "",
            "📐 横盘确认",
            f" 横盘天数: {stage3['consolidation_days']}天",
            f" 波动幅度: {stage3['range_pct']:.1f}%",
            f" 低点抬高: {'是' if stage3['lows_rising'] else '否'}",
        ]

    lines += [
        "",
        "📈 OI/资金费率",
        f" OI变化: {oi_data['oi_change']:+.1f}%",
        f" OI增加: {'是' if oi_data['oi_increasing'] else '否'}",
        f" 资金费率: {oi_data['funding_avg']:.4f}%",
    ]

    return "\n".join(lines)

# ========== 处理每个币种的信号检测 ==========
def process_symbol(sym):
    klines = kline_data.get(sym, [])
    if len(klines) < 35:
        return

    price = klines[-1]["c"]

    stage1 = check_stage1_decline(klines)
    if not stage1:
        return

    stage2 = check_stage2_surge(klines, stage1)
    stage3 = None
    if stage2:
        stage3 = check_stage3_consolidation(klines, stage2)

    oi_info = check_oi_and_funding(sym)

    if not oi_info["funding_ok"]:
        return

    level = classify_signal(stage1, stage2, stage3, oi_info)
    if not level:
        return

    prev_level = signal_history.get(sym)
    if prev_level == level:
        return

    signal_history[sym] = level

    msg = format_building_signal(sym, level, price, stage1, stage2, stage3, oi_info)
    notify(msg)

# ========== WebSocket 数据处理 ==========
def on_message(ws, message):
    try:
        data = json.loads(message)
        stream = data.get("stream", "")
        result = data.get("data", {})

        # 提取币种
        if "@" in stream:
            parts = stream.split("@")
            sym = parts[0].upper() if parts else None
        else:
            return

        if not sym:
            return

        # 处理 K线数据
        if "kline" in stream:
            k = result.get("k", {})
            if k.get("x"):  # 只在收盘时处理
                bar = {
                    "o": float(k["o"]),
                    "h": float(k["h"]),
                    "l": float(k["l"]),
                    "c": float(k["c"]),
                    "v": float(k["v"]),
                    "t": k["t"],
                }
                kline_data[sym].append(bar)
                # 保留最近35根K线
                if len(kline_data[sym]) > 35:
                    kline_data[sym] = kline_data[sym][-35:]
                # 收到收盘K线后触发检测
                process_symbol(sym)

        # 处理 OI 数据
        elif "openInterest" in stream:
            oi_value = float(result.get("oi", 0))
            oi_data[sym].append(oi_value)
            if len(oi_data[sym]) > 10:
                oi_data[sym] = oi_data[sym][-10:]

        # 处理资金费率
        elif "markPrice" in stream:
            funding_rate = float(result.get("r", 0)) * 100  # 转为百分比
            funding_data[sym].append(funding_rate)
            if len(funding_data[sym]) > 15:
                funding_data[sym] = funding_data[sym][-15:]

    except Exception as e:
        print(f"处理消息错误: {e}")

def on_error(ws, error):
    print(f"WebSocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    global ws_connected
    ws_connected = False
    print("WebSocket 连接关闭，5秒后重连...")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    global ws_connected
    ws_connected = True
    print("WebSocket 连接成功！")

def start_websocket():
    symbols = get_all_symbols()
    print(f"监控币种: {len(symbols)}")

    # 构建订阅流列表
    streams = []
    for sym in symbols:
        s = sym.lower()
        streams.append(f"{s}@kline_1d")
        streams.append(f"{s}@openInterest")
        streams.append(f"{s}@markPrice")

    # 分批订阅（每批200个）
    for i in range(0, len(streams), WS_BATCH):
        batch = streams[i:i + WS_BATCH]
        stream_url = "/".join(batch)
        ws_url = f"{WS_URL}/stream?streams={stream_url}"

        def run(url):
            while True:
                try:
                    ws = websocket.WebSocketApp(
                        url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close
                    )
                    ws.run_forever(ping_interval=30, ping_timeout=10)
                except Exception as e:
                    print(f"WebSocket 连接异常: {e}")
                time.sleep(5)

        threading.Thread(target=run, args=(ws_url,), daemon=True).start()
        time.sleep(2)  # 分批间隔

    print("所有 WebSocket 连接已启动")

# ========== 主程序 ==========
if __name__ == "__main__":
    print("🔍 建仓扫描系统启动（WebSocket版）")
    notify("🔍 建仓扫描系统启动（WebSocket版）")

    # 启动 WebSocket
    start_websocket()

    # 保持主线程运行
    while True:
        time.sleep(30)
        # 打印统计信息
        print(f"[{datetime.now().strftime('%H:%M')}] K线:{len(kline_data)} OI:{len(oi_data)} 费率:{len(funding_data)}")