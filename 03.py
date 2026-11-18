# -*- coding: utf-8 -*-
"""
融合监控系统（四通道版 · 修复版 + 强提示 + 邮件限流 + 速报汇总 + 企业微信）

通道0 🟡 启动预警：1M + 3M 前置雷达
通道1 🚀 启动确认：3~6分钟延续验证，慢启动延长至15分钟
通道2 ⚡ 速报信号：3M HH ≥ 2%（入队 → 排序 → Top N 单发 + 合并汇总）
通道3 📈 趋势信号：1H K线趋势检测（未改动）
通道4 📐 建仓信号：日线下跌→放量→横盘结构

本轮改动：
[企微] 新增企业微信通道，镜像「QQ邮箱成功发出的每一条消息」
       - 速报单发 / 确认 / 趋势 / 强提示 / 建仓 / 启动通知 → 同步进企微
       - 推进 / 速报汇总 / 日报摘要 / 内部异常 → 不进企微
       - token 缓存 + 队列限速 + 失败重试
       - WECOM_MIRROR_DAILY_REPORT=True 时，日报正文也推一份

前几轮已含：
[1] 汇总每行带价格   [2] 强提示补 3M 涨幅   [3] 强提示不被追高吞
[4] 邮件优先级：强提示1000 > 速报800 > 趋势100 > 确认50 > 建仓10
[5] 邮件 9 封/分钟   [6] 修「另有 N 个」计数  [7] 强提示候选强制单发
[8] 强提示去重 48h   [9] 速报 10 分钟窗口 10 条  [10] record_channel 仅成功才记
[FIX-1..19] 见前版说明
"""

import os
import time
import sqlite3
import logging
import smtplib
import threading
import requests
import pandas as pd
from collections import deque, defaultdict
from threading import Lock
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from requests.adapters import HTTPAdapter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr

# ========================= 配置区 =========================
BINANCE_API = "https://fapi.binance.com"

TELEGRAM_TOKEN = "TG的TOKEN"
TELEGRAM_CHAT_ID = "ID"

ENABLE_EMAIL = True
QQ_EMAIL = "你得邮箱"
QQ_AUTH_CODE = "邮箱的code"
EMAIL_TO = "你得邮箱"
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

SCAN_INTERVAL = 30
SYMBOL_REFRESH_INTERVAL = 3600
REQUEST_TIMEOUT = 10

RATE_LIMIT_PER_SEC = 18.0
HTTP_RETRY_ATTEMPTS = 3
HTTP_BACKOFF_SEC = 1.0
HTTP_MAX_RETRY_AFTER = 30.0

MIN_24H_VOLUME_USDT = 8_000_000
ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]

# ================= 启动雷达 =================
EARLY_HH_MIN = 1.2
EARLY_HH_MAX = 2.5
EARLY_1M_BODY_MIN = 0.5
EARLY_1M_VOL_MULT = 2.0
EARLY_CLOSE_POSITION = 0.80
EARLY_BREAK_LOOKBACK = 5

FAST_CHECK_3M_SEC = 180
FAST_CHECK_6M_SEC = 360
SLOW_CHECK_15M_SEC = 900
FAST_MIN_PROGRESS = 0.012
SLOW_MIN_PROGRESS = 0.008

PUSH_ATR_PERIOD = 14
NORMAL_PUSH_MIN = 0.008
NORMAL_PUSH_ATR = 2.0
STRONG_PUSH_MIN = 0.015
STRONG_PUSH_ATR = 4.0
EXPLOSIVE_PUSH_MIN = 0.030
EXPLOSIVE_PUSH_ATR = 6.0

SOFT_DRAWDOWN = 0.008
EXTREME_DRAWDOWN = 0.05
SOFT_DRAWDOWN_EVENT_GAP_SEC = 600

EARLY_FAIL_COOLDOWN_SEC = 1800
LONG_FAIL_COOLDOWN_SEC = 3600

FORMAL_SCORE_MIN = 4
STRONG_SCORE_MIN = 5
MAX_PUSH = 3
MAX_DAILY_ROUND = 1

# ────────────── 速报：入队 + 排序 + 合并 ──────────────
SURGE_HH_PCT = 2.0
SURGE_TOP_PER_CYCLE = 2
SURGE_MAX_IN_WINDOW = 10
SURGE_WINDOW_SEC = 600
SURGE_MAX_PER_DAY = 2
SURGE_REPEAT_HOURS = 6
SURGE_MIN_PRICE_ADVANCE = 0.015
SURGE_MAX_1H_CHANGE = 0.05

SURGE_ENABLE_DIGEST = True
SURGE_DIGEST_MIN_TOTAL = 5
SURGE_DIGEST_MAX_LINES = 5
SURGE_DIGEST_GAP_SEC = 600
SURGE_DIGEST_MAX_PER_DAY = 12

SURGE_MIN_QUOTE_VOL = 0
SURGE_VOL_MULT_1M = 0
SURGE_CLOSE_POS_MIN = 0

# 趋势通道（未改动）
TREND_SCAN_INTERVAL = 1800
TREND_CHANGE_MIN = 5.0
TREND_BULL_MIN = 10
TREND_AT_HIGH_MIN = 90
TREND_DEDUP_HOURS = 6
TREND_MAX_COUNT = 8
TREND_WINDOW_HOURS = 48

# 强化确认层
OI_STRONG = 0.05
VOLUME_MULTIPLIER = 1.8
FUNDING_RATE_MAX = 0.001
MAX_1H_CHANGE = 0.08

CHASE_4H_HIGH_DISTANCE = 0.01
CHASE_DAY_RANGE_TOP = 0.85
CHASE_MAX_RISK = 80

BTC_PAUSE_15M = 0.01
MARKET_DOWN_RATIO = 0.60
MARKET_HARD_DOWN_RATIO = 0.70
BTC_HARD_NEGATIVE = -0.005

DEDUP_HOURS = 2
EARLY_DEDUP_MINUTES = 60

TRACKING_TIMEOUT_HOURS = 24
MOMENTUM_WEAK_MINUTES = 15
MOMENTUM_FAIL_MINUTES = 30

DAILY_REPORT_HOUR = 0
DAILY_REPORT_MINUTE = 5
REPORT_KEEP_DAYS = 30
DAILY_REPORT_MAX_ATTEMPTS = 3
DAILY_REPORT_RETRY_SEC = 600
EVENT_SHEET_MAX_ROWS = 5000

MAX_WORKERS_TRIGGER = 30
MAX_WORKERS_TREND = 10

SILENT_PERIOD_SEC = 900
startup_time = 0

# ────────────── 建仓通道 ──────────────
BUILD_SCAN_INTERVAL = 3600
MIN_VOLUME_24H_BUILD = 3_000_000
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
BUILD_BLACKLIST = [
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "EURUSDT", "GBPUSDT", "JPYUSDT", "AUDUSDT",
]

# ────────────── 强提示 ──────────────
STRONG_ENABLE = True
STRONG_WINDOW_HOURS = 48
STRONG_DEDUP_HOURS = 48
STRONG_MAX_PER_DAY = 12
STRONG_TREND_COUNT = 3
STRONG_TREND_MIN_LAST_CHG = 15.0
STRONG_TREND_MIN_RISE = 3.0
STRONG_SURGE_COUNT = 2
STRONG_SURGE_MIN_GAP_HOURS = 6
STRONG_SURGE_MIN_RISE = 1.0

# ────────────── 邮件限流 ──────────────
EMAIL_MAX_PER_MIN = 9
EMAIL_RATE_WINDOW_SEC = 60
EMAIL_BATCH_HOLD_SEC = 8
EMAIL_MAX_SEND_RETRY = 1

# 邮件通道权重
PRIO_STRONG = 1000.0
PRIO_SURGE = 800.0
PRIO_TREND = 100.0
PRIO_CONFIRM = 50.0
PRIO_BUILDING = 10.0

# ────────────── 企业微信（个人微信接收） ──────────────
WECOM_ENABLE = True
WECOM_CORP_ID = "企业ID"
WECOM_SECRET = "企业的ID密码"
WECOM_AGENT_ID = 1000002
WECOM_TOUSER = "@all"                  # 想只发自己就填你的 userid
WECOM_MIN_INTERVAL_SEC = 0.5           # 两条之间最小间隔（≈120 条/分钟）
WECOM_MAX_LEN = 2000                   # 文本上限 2048 字节，留余量
WECOM_MAX_RETRY = 3
WECOM_QUEUE_MAX = 500
WECOM_MIRROR_DAILY_REPORT = False      # True = 日报正文也推一份到企微

CHANNEL_CN = {
    "confirm": "🚀启动确认", "surge": "⚡速报", "trend": "📈趋势",
    "building": "📐建仓", "early": "🟡启动预警", "push": "🚀推进",
    "momentum_weak": "🟡动能减弱", "failure": "❌失效",
    "risk_reject": "⚠️风险过滤", "expired": "⏰超时",
    "strong": "🔴强提示",
}

LOG_FILE = "monitor.log"

# ===================== 日志 + 数据库 ======================
try:
    import sys
    sys.stderr.reconfigure(errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=20 * 1024 * 1024,
                            backupCount=5, encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

db_lock = Lock()
db_conn = sqlite3.connect("signals.db", check_same_thread=False)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())

with db_conn:
    db_conn.execute("PRAGMA journal_mode=WAL")
    db_conn.execute("PRAGMA synchronous=NORMAL")
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_log (
            symbol TEXT, direction TEXT,
            channel TEXT DEFAULT 'confirm', sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals_history (
            symbol TEXT, direction TEXT, score INTEGER, price REAL,
            oi_change REAL, price_change REAL, volume_ratio REAL,
            funding REAL, sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS trend_log (
            symbol TEXT, count INTEGER, first_at TIMESTAMP, sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS channel_log (
            symbol TEXT, channel TEXT, sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS building_state (
            symbol TEXT PRIMARY KEY, level TEXT, updated_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS early_signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trigger_time TIMESTAMP NOT NULL,
            trigger_price REAL NOT NULL,
            trigger_high REAL, trigger_low REAL,
            start_pct REAL,
            early_score INTEGER DEFAULT 0,
            volume_ratio REAL DEFAULT 0,
            stage TEXT DEFAULT 'EARLY',
            result TEXT DEFAULT 'PENDING',
            confirm_time TIMESTAMP, confirm_price REAL,
            price_3m REAL, price_6m REAL, price_15m REAL,
            max_price REAL, min_price REAL,
            max_gain REAL, max_drawdown REAL,
            progress_class TEXT,
            chase_risk INTEGER DEFAULT 0,
            formal_score INTEGER DEFAULT 0,
            fail_reason TEXT,
            cooldown_until TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS tracking_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, early_id INTEGER, event_time TIMESTAMP,
            event_type TEXT, price REAL, change_pct REAL,
            drawdown_pct REAL, progress_class TEXT, note TEXT
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_report_status (
            report_date TEXT PRIMARY KEY, generated_at TIMESTAMP,
            filepath TEXT, email_ok INTEGER DEFAULT 0,
            attempts INTEGER DEFAULT 0, last_error TEXT
        )
    """)
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_symbol_time ON early_signal_log(symbol, trigger_time)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_cooldown ON early_signal_log(cooldown_until)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_result ON early_signal_log(result)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_channel_symbol_time ON channel_log(symbol, sent_at)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_symbol ON sent_log(symbol, channel, sent_at)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON tracking_events(event_time)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol ON tracking_events(symbol, event_time)")

# ===================== 列迁移 + 强提示表 =====================
def ensure_columns(table, cols):
    with db_lock:
        have = {r[1] for r in db_conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for name, decl in cols:
            if name not in have:
                db_conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
                logger.info(f"DB迁移: {table} + {name} {decl}")
        db_conn.commit()

ensure_columns("sent_log", [("price", "REAL"), ("hh_pct", "REAL")])
ensure_columns("trend_log", [("price", "REAL"), ("change_24h", "REAL")])

with db_conn:
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS strong_alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, kind TEXT, reason TEXT,
            price REAL, sent_at TIMESTAMP
        )
    """)
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_strong_symbol ON strong_alert_log(symbol, kind, sent_at)")

# ===================== 时间辅助 =====================
def now_beijing():
    return datetime.now(timezone(timedelta(hours=8)))

def parse_ts(value):
    if isinstance(value, datetime):
        dt = value
    elif not value:
        return now_beijing()
    elif isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone(timedelta(hours=8)))
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return now_beijing()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return dt

def entry_trigger_dt(sb):
    dt = sb.get("trigger_dt")
    if isinstance(dt, datetime):
        return dt
    tt = sb.get("trigger_time")
    if isinstance(tt, (int, float)):
        return datetime.fromtimestamp(float(tt), tz=timezone(timedelta(hours=8)))
    return parse_ts(tt)

def record_channel(symbol, channel):
    with db_lock:
        db_conn.execute("INSERT INTO channel_log VALUES (?,?,?)",
                        (symbol, channel, now_beijing()))
        db_conn.commit()

def get_cross_channel_history(symbol, hours=24):
    cutoff = now_beijing() - timedelta(hours=hours)
    with db_lock:
        cur = db_conn.execute(
            "SELECT DISTINCT channel FROM channel_log WHERE symbol=? AND sent_at > ?",
            (symbol, cutoff))
        channels = set(row[0] for row in cur.fetchall())
    if not channels:
        return ""
    labels = []
    if "strong" in channels:
        labels.insert(0, "🔴强提示已发")
    if "surge" in channels:
        labels.append("⚡速报已报")
    if "confirm" in channels:
        labels.append("🚀确认已报")
    if "trend" in channels:
        labels.append("📈趋势已报")
    if "building" in channels:
        labels.append("📐建仓已报")
    if "early" in channels:
        labels.append("🟡启动预警已报")
    if "momentum_weak" in channels:
        labels.append("🟡动能减弱已报")
    if "failure" in channels:
        labels.append("❌失效已报")
    if "push" in channels:
        labels.append("🚀推进已报")
    if "risk_reject" in channels:
        labels.append("⚠️风险过滤已报")
    if "expired" in channels:
        labels.append("⏰超时已报")
    if labels:
        return f"📌 历史信号：{' | '.join(labels)}\n"
    return ""

# ===================== 数据库函数 =====================
def should_alert(symbol, direction, channel='confirm', max_count=3):
    cutoff_2h = now_beijing() - timedelta(hours=DEDUP_HOURS)
    cutoff_24h = now_beijing() - timedelta(hours=24)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff_2h))
        if cur.fetchone() is not None:
            return False
        cur = db_conn.execute(
            "SELECT COUNT(*) FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff_24h))
        return cur.fetchone()[0] < max_count

def mark_alerted(symbol, direction, channel='confirm', price=None, hh_pct=None):
    with db_lock:
        db_conn.execute(
            "INSERT INTO sent_log (symbol, direction, channel, sent_at, price, hh_pct) "
            "VALUES (?,?,?,?,?,?)",
            (symbol, direction, channel, now_beijing(), price, hh_pct))
        db_conn.commit()

def should_trend_alert(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count, first_at, sent_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
    if row is None:
        return True
    count, first_at, last_sent = row
    now = now_beijing()
    try:
        if (now - parse_ts(first_at)).total_seconds() > TREND_WINDOW_HOURS * 3600:
            return True
        if (now - parse_ts(last_sent)).total_seconds() < TREND_DEDUP_HOURS * 3600:
            return False
    except Exception:
        return True
    return (count or 0) < TREND_MAX_COUNT

def mark_trend_alerted(symbol, price=None, change_24h=None):
    now = now_beijing()
    with db_lock:
        row = db_conn.execute(
            "SELECT count, first_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,)).fetchone()
        if row is None:
            count, first_at = 1, now
        else:
            old_count, old_first = row
            if (now - parse_ts(old_first)).total_seconds() > TREND_WINDOW_HOURS * 3600:
                count, first_at = 1, now
            else:
                count, first_at = (old_count or 0) + 1, old_first
        db_conn.execute(
            "INSERT INTO trend_log (symbol, count, first_at, sent_at, price, change_24h) "
            "VALUES (?,?,?,?,?,?)",
            (symbol, count, first_at, now, price, change_24h))
        db_conn.commit()
    return count

# ===================== HTTP Session =====================
_rate_lock = Lock()
_rate_next = [0.0]

def _throttle():
    with _rate_lock:
        gap = 1.0 / RATE_LIMIT_PER_SEC
        now = time.monotonic()
        wait = _rate_next[0] - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _rate_next[0] = now + gap

session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
session.mount("https://", adapter)
session.mount("http://", adapter)
_orig = session.request

def _patched(method, url, **kw):
    kw.setdefault("timeout", REQUEST_TIMEOUT)
    attempts = HTTP_RETRY_ATTEMPTS if method.upper() == "GET" else 1
    last_exc = None
    r = None
    for i in range(attempts):
        _throttle()
        try:
            r = _orig(method, url, **kw)
        except Exception as e:
            last_exc = e
            if i + 1 < attempts:
                time.sleep(HTTP_BACKOFF_SEC * (i + 1))
                continue
            raise
        if r.status_code in (429, 418):
            try:
                wait = float(r.headers.get("Retry-After", 5) or 5)
            except Exception:
                wait = 5.0
            wait = min(HTTP_MAX_RETRY_AFTER, max(1.0, wait))
            logger.warning(f"HTTP {r.status_code} 限频，等待 {wait:.0f}s")
            time.sleep(wait)
            if i + 1 < attempts:
                continue
        return r
    if r is not None:
        return r
    if last_exc:
        raise last_exc
    raise RuntimeError("request failed")

session.request = _patched

# ===================== Binance API =====================
high_volume_symbols = set()
ticker_cache = {}

def get_symbols():
    global high_volume_symbols, ticker_cache
    try:
        r = session.get(f"{BINANCE_API}/fapi/v1/exchangeInfo")
        r.raise_for_status()
        info = r.json()
        all_syms = [
            s['symbol'] for s in info['symbols']
            if s['contractType'] == 'PERPETUAL'
            and s['quoteAsset'] == 'USDT'
            and s['status'] == 'TRADING'
            and s['symbol'] not in ALT_BLACKLIST
        ]
        r = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr")
        r.raise_for_status()
        tickers = r.json()
        hv = set()
        cache = {}
        for t in tickers:
            try:
                vol = float(t.get('quoteVolume', 0))
                sym = t['symbol']
                cache[sym] = {'volume': vol, 'change': float(t['priceChangePercent'])}
                if vol > MIN_24H_VOLUME_USDT:
                    hv.add(sym)
            except Exception:
                pass
        high_volume_symbols = hv
        ticker_cache = cache
        logger.info(f"全部永续: {len(all_syms)} → 全部扫描 | {MIN_24H_VOLUME_USDT//10000}万以上: {len(hv)}")
        return all_syms
    except Exception as e:
        logger.error(f"获取币种失败: {e}")
        return []

def get_klines(symbol, interval, limit):
    r = session.get(f"{BINANCE_API}/fapi/v1/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit})
    r.raise_for_status()
    return r.json()

def get_klines_parsed(symbol, interval="1d", limit=35):
    raw = get_klines(symbol, interval, limit)
    if not raw:
        return None
    return [{"o": float(k[1]), "h": float(k[2]), "l": float(k[3]),
             "c": float(k[4]), "v": float(k[5]), "qv": float(k[7]), "t": k[0]}
            for k in raw]

def get_oi_history(symbol, period="1h", limit=2):
    r = session.get(f"{BINANCE_API}/futures/data/openInterestHist",
                    params={"symbol": symbol, "period": period, "limit": limit})
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol):
    r = session.get(f"{BINANCE_API}/fapi/v1/premiumIndex", params={"symbol": symbol})
    r.raise_for_status()
    return float(r.json()['lastFundingRate'])

def get_funding_rate_history(symbol, limit=10):
    try:
        r = session.get(f"{BINANCE_API}/fapi/v1/fundingRate",
                        params={"symbol": symbol, "limit": limit})
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return [float(d["fundingRate"]) * 100 for d in data]
    except Exception as e:
        logger.debug(f"{symbol} 资金费率历史失败: {e}")
    return None

# ===================== 企业微信（镜像邮件） =====================
_wecom_lock = Lock()
_wecom_state = {"token": None, "expire_at": 0.0, "last_sent": 0.0}
_wecom_queue = deque()
_wecom_worker_on = [False]

def _wecom_get_token(force=False):
    with _wecom_lock:
        now = time.time()
        if (not force) and _wecom_state["token"] and now < _wecom_state["expire_at"] - 60:
            return _wecom_state["token"]
    url = ("https://qyapi.weixin.qq.com/cgi-bin/gettoken"
           f"?corpid={WECOM_CORP_ID}&corpsecret={WECOM_SECRET}")
    try:
        res = session.get(url, timeout=REQUEST_TIMEOUT).json()
    except Exception as e:
        logger.error(f"企业微信 token 请求失败: {e}")
        return None
    token = res.get("access_token")
    if not token:
        logger.error(f"企业微信 token 获取失败: errcode={res.get('errcode')} {res.get('errmsg')}")
        return None
    with _wecom_lock:
        _wecom_state["token"] = token
        _wecom_state["expire_at"] = time.time() + int(res.get("expires_in", 7200))
    return token

def _wecom_send_now(text):
    if not WECOM_ENABLE:
        return False
    token = _wecom_get_token()
    if not token:
        return False
    payload = {
        "touser": WECOM_TOUSER,
        "msgtype": "text",
        "agentid": WECOM_AGENT_ID,
        "text": {"content": text},
        "safe": 0,
    }
    for attempt in range(WECOM_MAX_RETRY):
        url = ("https://qyapi.weixin.qq.com/cgi-bin/message/send"
               f"?access_token={token}")
        try:
            res = session.post(url, json=payload, timeout=REQUEST_TIMEOUT).json()
        except Exception as e:
            logger.error(f"企业微信发送异常({attempt + 1}/{WECOM_MAX_RETRY}): {e}")
            time.sleep(1.0 + attempt)
            continue
        errcode = res.get("errcode")
        if errcode == 0:
            return True
        if errcode in (40014, 42001):
            token = _wecom_get_token(force=True)
            if not token:
                return False
            continue
        if errcode in (45009, -1):
            time.sleep(2.0 * (attempt + 1))
            continue
        logger.error(f"企业微信发送失败: errcode={errcode} errmsg={res.get('errmsg')}")
        return False
    return False

def _wecom_worker():
    while True:
        try:
            with _wecom_lock:
                text = _wecom_queue.popleft() if _wecom_queue else None
                last = _wecom_state["last_sent"]
            if text is None:
                time.sleep(0.3)
                continue
            gap = WECOM_MIN_INTERVAL_SEC - (time.time() - last)
            if gap > 0:
                time.sleep(gap)
            _wecom_send_now(text)
            with _wecom_lock:
                _wecom_state["last_sent"] = time.time()
        except Exception as e:
            logger.error(f"企业微信worker异常: {e}")
            time.sleep(1.0)

def notify_wecom(text):
    """把一条消息塞进企业微信队列（不阻塞调用方）。"""
    if not WECOM_ENABLE or not text:
        return False
    raw = text.encode("utf-8")
    if len(raw) > WECOM_MAX_LEN:
        text = raw[:WECOM_MAX_LEN].decode("utf-8", "ignore") + "\n…(已截断)"
        logger.warning("企业微信消息超长，已截断")
    with _wecom_lock:
        if not _wecom_worker_on[0]:
            threading.Thread(target=_wecom_worker, name="wecom-sender", daemon=True).start()
            _wecom_worker_on[0] = True
        if len(_wecom_queue) >= WECOM_QUEUE_MAX:
            _wecom_queue.popleft()
            logger.warning("企业微信队列已满，丢弃最旧的一条")
        _wecom_queue.append(text)
    return True

# ===================== 通知模块 =====================
def send_tg(text):
    try:
        r = session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"TG发送失败: {e}")
        return False

def _send_email_text_now(subject, content):
    if not ENABLE_EMAIL:
        return False
    server = None
    try:
        msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(QQ_EMAIL,
                        [e.strip() for e in EMAIL_TO.split(",") if e.strip()],
                        msg.as_string())
        return True
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass

_email_queue = []
_email_sent_ts = deque()
_email_lock = Lock()
_email_worker_on = [False]

def _email_quota_left():
    now = time.monotonic()
    with _email_lock:
        while _email_sent_ts and now - _email_sent_ts[0] >= EMAIL_RATE_WINDOW_SEC:
            _email_sent_ts.popleft()
        return max(0, EMAIL_MAX_PER_MIN - len(_email_sent_ts))

def _email_worker():
    while True:
        time.sleep(1.0)
        try:
            with _email_lock:
                if not _email_queue:
                    continue
                oldest = min(q["ts"] for q in _email_queue)
            if time.monotonic() - oldest < EMAIL_BATCH_HOLD_SEC:
                continue

            with _email_lock:
                batch = _email_queue[:]
                _email_queue.clear()
            if not batch:
                continue

            quota = _email_quota_left()
            batch.sort(key=lambda q: q["priority"], reverse=True)
            keep, dropped = batch[:quota], batch[quota:]

            for q in keep:
                ok = _send_email_text_now(q["subject"], q["text"])
                for _ in range(EMAIL_MAX_SEND_RETRY):
                    if ok:
                        break
                    time.sleep(1.0)
                    ok = _send_email_text_now(q["subject"], q["text"])
                if ok:
                    with _email_lock:
                        _email_sent_ts.append(time.monotonic())
                    notify_wecom(q["text"])      # 企业微信：只镜像邮件真正发出去的那条

            if dropped:
                logger.warning(
                    f"✉️ 邮件限流：本轮 {len(batch)} 封，额度 {quota}，"
                    f"只发优先级最高的 {len(keep)} 封，丢弃 {len(dropped)} 封")
                for q in sorted(dropped, key=lambda x: x["priority"], reverse=True)[:8]:
                    logger.info(f"   ✉️ 丢弃 {q['symbol'] or '-'} 优先级{q['priority']:.0f}")
        except Exception as e:
            logger.error(f"邮件worker异常: {e}")

def _ensure_email_worker():
    if _email_worker_on[0]:
        return
    with _email_lock:
        if not _email_worker_on[0]:
            threading.Thread(target=_email_worker, name="email-sender", daemon=True).start()
            _email_worker_on[0] = True

def send_email_text(subject, content, priority=0.0, symbol=None):
    if not ENABLE_EMAIL:
        return False
    _ensure_email_worker()
    with _email_lock:
        _email_queue.append({
            "subject": subject,
            "text": content,
            "priority": float(priority or 0),
            "symbol": symbol or "",
            "ts": time.monotonic(),
        })
    return True

def send_email_attach(subject, body, filepath):
    if not ENABLE_EMAIL:
        return False
    server = None
    try:
        msg = MIMEMultipart()
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(filepath))
            part["Content-Disposition"] = f'attachment; filename="{os.path.basename(filepath)}"'
            msg.attach(part)
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(QQ_EMAIL,
                        [e.strip() for e in EMAIL_TO.split(",") if e.strip()],
                        msg.as_string())
        return True
    except Exception as e:
        logger.error(f"邮件附件发送失败: {e}")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass

def notify_tg_only(text, channel=None, symbol=None):
    ok = send_tg(text)
    if ok and channel and symbol:
        record_channel(symbol, channel)
    return ok

def notify_tg_email(text, subject, channel=None, symbol=None, priority=0.0):
    tg_ok = send_tg(text)
    email_ok = send_email_text(subject, text, priority=priority, symbol=symbol)
    if (tg_ok or email_ok) and channel and symbol:
        record_channel(symbol, channel)
    return tg_ok, email_ok

def notify_all(text, subject="实时监控提示", channel=None, symbol=None, priority=0.0):
    return notify_tg_email(text, subject, channel=channel, symbol=symbol, priority=priority)

# ===================== 信号文案 =====================
def notify_signal(sig):
    history_text = get_cross_channel_history(sig["symbol"], hours=24)
    score = sig.get("score", 0)
    max_score = sig.get("max_score", 6)
    quality = sig.get("quality_label", "正式启动")
    text = (
        f"🚀 {sig['symbol']} — {quality}\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${sig['price']:.8f}\n"
        f"📈 启动后涨幅: {sig.get('post_start_pct', 0):+.2f}%\n"
        f"🔊 量能比: {sig.get('volume_ratio', 0):.2f}x\n"
        f"📊 OI 1h: {sig.get('oi_change', 0)*100:+.2f}%\n"
        f"💵 资金费率: {sig.get('funding', 0)*100:.4f}%\n"
        f"📐 推进: {sig.get('progress_class', '正常')}\n"
        f"🛡 追高风险: {sig.get('chase_risk', 0)}/100\n"
        f"⭐ 综合评分: {score}/{max_score}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(text, f"🚀 [正式启动] {sig['symbol']} 评分{score}/{max_score}",
                    channel="confirm", symbol=sig["symbol"],
                    priority=PRIO_CONFIRM + sig.get("post_start_pct", 0))

def notify_surge(symbol, price, start_pct):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"⚡ {symbol} — 快速拉升\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.8f}\n"
        f"📈 3M 启动涨幅: +{start_pct:.2f}%\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(text, f"⚡ [快速拉升] {symbol} +{start_pct:.2f}%",
                    channel="surge", symbol=symbol,
                    priority=PRIO_SURGE + start_pct)

def notify_trend(symbol, price, change_24h, bull_count, at_high_pct, trend_count):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"📈 {symbol} — 持续上涨趋势\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.8f}\n"
        f"📊 24h 涨幅: {change_24h:+.2f}%\n"
        f"📈 24h 阳线: {bull_count}/24\n"
        f"📍 距24h最高: {at_high_pct:.1f}%\n"
        f"🔄 趋势提醒: 第 {trend_count} 次\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(text, f"📈 [趋势信号] {symbol}",
                    channel="trend", symbol=symbol,
                    priority=PRIO_TREND + change_24h)

# ===================== 强提示 =====================
def fmt_price(p):
    if p is None:
        return "-"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.8f}"

def strong_recently(symbol, kind):
    cutoff = (now_beijing() - timedelta(hours=STRONG_DEDUP_HOURS)).isoformat()
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM strong_alert_log WHERE symbol=? AND kind=? AND sent_at>? LIMIT 1",
            (symbol, kind, cutoff))
        return cur.fetchone() is not None

def strong_daily_count():
    start = now_beijing().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    with db_lock:
        cur = db_conn.execute("SELECT COUNT(*) FROM strong_alert_log WHERE sent_at>?", (start,))
        return cur.fetchone()[0]

def record_strong(symbol, kind, reason, price):
    with db_lock:
        db_conn.execute(
            "INSERT INTO strong_alert_log (symbol, kind, reason, price, sent_at) VALUES (?,?,?,?,?)",
            (symbol, kind, reason, price, now_beijing()))
        db_conn.commit()

def _strong_text(kind_line, symbol, rows, footer_lines, chg_label="24h"):
    lines = [
        "🔴🔴🔴  强  提  示  🔴🔴🔴",
        "████████████████████████████",
        kind_line,
        f"🪙 {symbol}",
        "████████████████████████████",
    ]
    for i, (dt, p, chg) in enumerate(rows, 1):
        seg = f"  {i}️⃣ {dt.strftime('%m-%d %H:%M')}   ${fmt_price(p)}"
        if chg is not None:
            seg += f"   ({chg_label} {chg:+.1f}%)"
        if i > 1:
            seg += "   ↑"
        lines.append(seg)
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.extend(footer_lines)
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️ 结构共振提示，不是入场指令")
    lines.append(f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(lines)

def check_strong_trend(symbol):
    if not STRONG_ENABLE:
        return
    cutoff = (now_beijing() - timedelta(hours=STRONG_WINDOW_HOURS)).isoformat()
    with db_lock:
        rows = db_conn.execute(
            "SELECT sent_at, price, change_24h FROM trend_log "
            "WHERE symbol=? AND sent_at>? ORDER BY sent_at ASC",
            (symbol, cutoff)).fetchall()
    if len(rows) < STRONG_TREND_COUNT:
        return
    tail = rows[-STRONG_TREND_COUNT:]
    prices = [r[1] for r in tail]
    chgs = [r[2] for r in tail]
    vals = prices if all(p is not None for p in prices) else chgs
    if any(v is None for v in vals):
        return
    for i in range(1, len(vals)):
        if vals[i] <= vals[i - 1]:
            return
    rise = (vals[-1] - vals[0]) / vals[0] * 100 if vals[0] else 0
    if rise < STRONG_TREND_MIN_RISE:
        return
    if chgs[-1] is None or chgs[-1] < STRONG_TREND_MIN_LAST_CHG:
        return
    if strong_recently(symbol, "trend") or strong_daily_count() >= STRONG_MAX_PER_DAY:
        return
    if time.time() - startup_time < SILENT_PERIOD_SEC:
        logger.info(f"⏳ 静默期，跳过强提示: {symbol}")
        return
    items = [(parse_ts(t), p, c) for t, p, c in tail]
    span_h = (items[-1][0] - items[0][0]).total_seconds() / 3600
    text = _strong_text(
        f"📈 趋势{STRONG_TREND_COUNT}连升 · {STRONG_WINDOW_HOURS}h 内",
        symbol, items,
        [f"📈 首→末 {rise:+.2f}%   ⏱ 跨度 {span_h:.1f}h",
         f"💰 现价 ${fmt_price(prices[-1])}"],
        chg_label="24h")
    notify_tg_email(text, f"【🔴强提示】趋势{STRONG_TREND_COUNT}连升 {symbol}",
                    channel="strong", symbol=symbol, priority=PRIO_STRONG + rise)
    record_strong(symbol, "trend", f"{len(rows)}次趋势/{STRONG_WINDOW_HOURS}h 连升{rise:+.2f}%", prices[-1])
    logger.info(f"🔴 强提示(趋势连升): {symbol} {len(rows)}次 累计{rise:+.2f}%")

def check_strong_surge(symbol, price, hh_pct=None):
    """在「本次速报落库之前」调用：本次 = price，历史取自 sent_log。"""
    if not STRONG_ENABLE or price is None:
        return
    if strong_recently(symbol, "surge"):
        return
    cutoff = (now_beijing() - timedelta(hours=STRONG_WINDOW_HOURS)).isoformat()
    with db_lock:
        rows = db_conn.execute(
            "SELECT sent_at, price, hh_pct FROM sent_log "
            "WHERE symbol=? AND channel='surge' AND sent_at>? ORDER BY sent_at ASC",
            (symbol, cutoff)).fetchall()
    if not rows:
        return
    prev_ts_raw, prev_price, prev_hh = rows[-1][0], rows[-1][1], rows[-1][2]
    if prev_price is None:
        return
    prev_ts = parse_ts(prev_ts_raw)
    now = now_beijing()
    gap_h = (now - prev_ts).total_seconds() / 3600
    if gap_h < STRONG_SURGE_MIN_GAP_HOURS:
        return
    if price < prev_price * (1 + STRONG_SURGE_MIN_RISE / 100):
        return
    if strong_daily_count() >= STRONG_MAX_PER_DAY:
        return
    if time.time() - startup_time < SILENT_PERIOD_SEC:
        logger.info(f"⏳ 静默期，跳过强提示: {symbol}")
        return

    rise = (price - prev_price) / prev_price * 100
    count = len(rows) + 1
    footer = [f"📈 本次比上次高 {rise:+.2f}%   ⏱ 间隔 {gap_h:.1f}h"]
    if hh_pct is not None:
        footer.append(f"📊 本次 3M 涨幅 {hh_pct:+.2f}%")
    footer.append(f"💰 现价 ${fmt_price(price)}")
    text = _strong_text(
        f"⚡ 二次速报 · {STRONG_WINDOW_HOURS}h 内第 {count} 次",
        symbol, [(prev_ts, prev_price, prev_hh), (now, price, hh_pct)],
        footer, chg_label="3M")
    notify_tg_email(text, f"【🔴强提示】二次速报 {symbol}",
                    channel="strong", symbol=symbol, priority=PRIO_STRONG + rise)
    record_strong(symbol, "surge", f"{STRONG_WINDOW_HOURS}h 内第{count}次速报 {rise:+.2f}%", price)
    logger.info(f"🔴 强提示(二次速报): {symbol} 第{count}次 高于上次{rise:+.2f}%")

def _would_be_strong_surge(symbol, price):
    """本次速报若发出，是否会触发「二次速报」强提示（用于抢占单发位）。"""
    if not STRONG_ENABLE or price is None:
        return False
    if strong_recently(symbol, "surge"):
        return False
    cutoff = (now_beijing() - timedelta(hours=STRONG_WINDOW_HOURS)).isoformat()
    try:
        with db_lock:
            row = db_conn.execute(
                "SELECT sent_at, price FROM sent_log "
                "WHERE symbol=? AND channel='surge' AND sent_at>? "
                "ORDER BY sent_at DESC LIMIT 1",
                (symbol, cutoff)).fetchone()
    except Exception:
        return False
    if not row or row[1] is None:
        return False
    gap_h = (now_beijing() - parse_ts(row[0])).total_seconds() / 3600
    if gap_h < STRONG_SURGE_MIN_GAP_HOURS:
        return False
    return price >= row[1] * (1 + STRONG_SURGE_MIN_RISE / 100)

# ===================== 状态缓存 =====================
state_b = defaultdict(lambda: {
    "active": False, "last_high": None, "base_low": None, "push_count": 0,
    "day": None, "daily_round": 0, "start_pct": 0.0, "first_price": None,
    "daily_high": 0.0, "daily_low": 1e10, "push_times": 0,
    "trigger_time": None, "source": None
})

pending_early = {}
active_tracking = {}
early_lock = Lock()

surge_queue = []
surge_lock = Lock()
surge_send_ts = []
surge_last_price = {}
surge_digest_state = {"last_ts": 0.0, "day": None, "count": 0}

market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}

# ===================== 追踪辅助 =====================
def add_tracking_event(symbol, early_id, event_type, price=None,
                       change_pct=None, drawdown_pct=None,
                       progress_class=None, note=None):
    with db_lock:
        db_conn.execute("""
            INSERT INTO tracking_events
            (symbol, early_id, event_time, event_type, price,
             change_pct, drawdown_pct, progress_class, note)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (symbol, early_id, now_beijing().isoformat(), event_type,
              price, change_pct, drawdown_pct, progress_class, note))
        db_conn.commit()

def update_early_row(early_id, **kwargs):
    if not kwargs:
        return
    allowed = {
        "stage", "result", "confirm_time", "confirm_price",
        "price_3m", "price_6m", "price_15m", "max_price",
        "min_price", "max_gain", "max_drawdown",
        "progress_class", "chase_risk", "formal_score",
        "fail_reason", "cooldown_until"
    }
    kwargs = {k: v for k, v in kwargs.items() if k in allowed}
    if not kwargs:
        return
    assignments = ", ".join(f"{k}=?" for k in kwargs)
    values = [v.isoformat() if isinstance(v, datetime) else v for v in kwargs.values()]
    values.append(early_id)
    with db_lock:
        db_conn.execute(f"UPDATE early_signal_log SET {assignments} WHERE id=?", values)
        db_conn.commit()

def get_existing_early(symbol, within_minutes=60):
    cutoff = now_beijing() - timedelta(minutes=within_minutes)
    with db_lock:
        cur = db_conn.execute("""
            SELECT id, trigger_time, trigger_price, trigger_low, stage, result
            FROM early_signal_log
            WHERE symbol=? AND trigger_time>? AND result IN ('PENDING','CONFIRMED')
            ORDER BY id DESC LIMIT 1
        """, (symbol, cutoff.isoformat()))
        return cur.fetchone()

def is_symbol_in_cooldown(symbol):
    now = now_beijing()
    with db_lock:
        cur = db_conn.execute("""
            SELECT cooldown_until FROM early_signal_log
            WHERE symbol=? AND cooldown_until IS NOT NULL
            ORDER BY id DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
    if not row or not row[0]:
        return False
    try:
        return parse_ts(row[0]) > now
    except Exception:
        return False

def build_scan_blocklist():
    blocked = set()
    cutoff = (now_beijing() - timedelta(minutes=EARLY_DEDUP_MINUTES)).isoformat()
    now_iso = now_beijing().isoformat()
    try:
        with db_lock:
            rows = db_conn.execute("""
                SELECT symbol FROM early_signal_log
                WHERE trigger_time>? AND result IN ('PENDING','CONFIRMED')
            """, (cutoff,)).fetchall()
            blocked.update(r[0] for r in rows)
            rows = db_conn.execute("""
                SELECT symbol FROM early_signal_log
                WHERE cooldown_until IS NOT NULL AND cooldown_until > ?
            """, (now_iso,)).fetchall()
            blocked.update(r[0] for r in rows)
    except Exception as e:
        logger.warning(f"预加载状态失败: {e}")
    return blocked

def atr_percent_from_1m(klines, period=PUSH_ATR_PERIOD):
    if not klines or len(klines) < period + 1:
        return 0.0
    trs = []
    prev_close = None
    for k in klines[-(period+1):]:
        high, low, close = float(k[2]), float(k[3]), float(k[4])
        if prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        if close > 0:
            trs.append(tr / close)
        prev_close = close
    return (sum(trs) / len(trs)) if trs else 0.0

def classify_progress(change_pct_decimal, atr_pct_decimal):
    if atr_pct_decimal <= 0:
        normal_threshold = NORMAL_PUSH_MIN
        strong_threshold = STRONG_PUSH_MIN
        explosive_threshold = EXPLOSIVE_PUSH_MIN
    else:
        normal_threshold = max(NORMAL_PUSH_MIN, atr_pct_decimal * NORMAL_PUSH_ATR)
        strong_threshold = max(STRONG_PUSH_MIN, atr_pct_decimal * STRONG_PUSH_ATR)
        explosive_threshold = max(EXPLOSIVE_PUSH_MIN, atr_pct_decimal * EXPLOSIVE_PUSH_ATR)
    if change_pct_decimal >= explosive_threshold:
        return "爆发", explosive_threshold
    if change_pct_decimal >= strong_threshold:
        return "强推进", strong_threshold
    if change_pct_decimal >= normal_threshold:
        return "正常推进", normal_threshold
    if change_pct_decimal > 0:
        return "弱推进", normal_threshold
    return "无推进", normal_threshold

def get_short_term_vwap(symbol, limit=60):
    try:
        k = get_klines(symbol, "1m", limit)
        if not k:
            return None
        pv = 0.0
        vv = 0.0
        for x in k:
            typical = (float(x[2]) + float(x[3]) + float(x[4])) / 3.0
            vol = float(x[5])
            pv += typical * vol
            vv += vol
        return pv / vv if vv > 0 else None
    except Exception:
        return None

def get_4h_and_day_position(symbol):
    try:
        k4 = get_klines(symbol, "4h", 30)
        kd = get_klines(symbol, "1h", 24)
        if not k4 or not kd:
            return 0, 0, 0
        price = float(kd[-1][4])
        high4 = max(float(x[2]) for x in k4)
        high24 = max(float(x[2]) for x in kd)
        low24 = min(float(x[3]) for x in kd)
        day_range_pos = (price - low24) / (high24 - low24) if high24 > low24 else 0.5
        dist4 = (high4 - price) / price if price > 0 else 1.0
        change1h = ((float(kd[-1][4]) - float(kd[-2][4])) / float(kd[-2][4])
                    if len(kd) >= 2 and float(kd[-2][4]) > 0 else 0)
        return dist4, day_range_pos, change1h
    except Exception:
        return 0, 0, 0

def compute_chase_risk(symbol, price):
    risk = 0
    try:
        dist4, day_pos, change1h = get_4h_and_day_position(symbol)
        if change1h > MAX_1H_CHANGE:
            risk += 35
        elif change1h > MAX_1H_CHANGE * 0.75:
            risk += 20
        if 0 < dist4 < CHASE_4H_HIGH_DISTANCE:
            risk += 25
        if day_pos >= CHASE_DAY_RANGE_TOP:
            risk += 20
        vwap = get_short_term_vwap(symbol, 60)
        if vwap and price > vwap:
            premium = (price - vwap) / vwap
            if premium > 0.02:
                risk += 20
            elif premium > 0.01:
                risk += 10
        return min(100, risk)
    except Exception:
        return risk

def get_market_state(symbols=None):
    try:
        btc = get_klines("BTCUSDT", "1m", 20)
        if len(btc) >= 16:
            btc_change_15m = (float(btc[-1][4]) - float(btc[-16][4])) / float(btc[-16][4])
        else:
            btc_change_15m = 0.0
    except Exception:
        btc_change_15m = 0.0

    down_ratio = 0.0
    if symbols:
        sample = [s for s in symbols if s in high_volume_symbols][:100]
        if sample:
            def _one(s):
                try:
                    k = get_klines(s, "1m", 16)
                    if len(k) < 16:
                        return 0
                    return 1 if float(k[-1][4]) < float(k[-16][4]) else 0
                except Exception:
                    return 0
            with ThreadPoolExecutor(max_workers=15) as ex:
                vals = list(ex.map(_one, sample))
            down_ratio = sum(vals) / len(vals)

    return {"btc_change_15m": btc_change_15m, "market_down_ratio": down_ratio}

def insert_early_signal(symbol, trigger_price, trigger_high, trigger_low,
                        start_pct, early_score, volume_ratio):
    with db_lock:
        cur = db_conn.execute("""
            INSERT INTO early_signal_log
            (symbol, trigger_time, trigger_price, trigger_high, trigger_low,
             start_pct, early_score, volume_ratio, stage, result,
             max_price, min_price)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (symbol, now_beijing().isoformat(), trigger_price, trigger_high,
              trigger_low, start_pct, early_score, volume_ratio,
              "EARLY", "PENDING", trigger_price, trigger_price))
        early_id = cur.lastrowid
        db_conn.commit()
    return early_id

# ===================== 启动雷达 =====================
def scan_trigger(sym, blocked=None):
    try:
        if sym not in high_volume_symbols:
            return
        if sym in pending_early or sym in active_tracking:
            return
        if blocked is not None:
            if sym in blocked:
                return
        else:
            if get_existing_early(sym, EARLY_DEDUP_MINUTES) or is_symbol_in_cooldown(sym):
                return

        sb = state_b[sym]
        today = now_beijing().date()
        if sb["day"] != today:
            sb["day"] = today
            sb["daily_round"] = 0
            if sym not in active_tracking:
                sb["active"] = False
                sb["push_count"] = 0

        if sb["daily_round"] >= MAX_DAILY_ROUND:
            return

        k3 = get_klines(sym, "3m", 8)
        if len(k3) < 6:
            return
        closed3 = k3[:-1]
        highs = [float(x[2]) for x in closed3]
        lows = [float(x[3]) for x in closed3]

        hh = highs[-3] < highs[-2] < highs[-1]
        start_pct = (highs[-1] - lows[-3]) / lows[-3] * 100 if lows[-3] > 0 else 0
        if not hh:
            return

        current1 = get_klines(sym, "1m", 12)
        if len(current1) < 6:
            return
        last, prev = current1[-1], current1[-2]
        last_o, last_h, last_l, last_c, last_v = map(float, [
            last[1], last[2], last[3], last[4], last[5]])
        prev_h = float(prev[2])

        body_pct = (last_c - last_o) / last_o * 100 if last_o > 0 else 0
        avg_v = sum(float(x[5]) for x in current1[:-1]) / max(1, len(current1) - 1)
        vol_ratio_1m = last_v / avg_v if avg_v > 0 else 0
        rng = last_h - last_l
        close_pos = (last_c - last_l) / rng if rng > 0 else 0.5
        prior3 = (max(highs[-EARLY_BREAK_LOOKBACK-1:-1])
                  if len(highs) >= EARLY_BREAK_LOOKBACK + 1 else highs[-2])
        break_ok = last_c > max(prev_h, prior3)
        price_now = last_c

        if start_pct >= SURGE_HH_PCT:
            if time.time() - startup_time < SILENT_PERIOD_SEC:
                logger.info(f"⏳ 静默期，跳过速报候选: {sym}")
            elif ((SURGE_VOL_MULT_1M <= 0 or vol_ratio_1m >= SURGE_VOL_MULT_1M)
                  and (SURGE_CLOSE_POS_MIN <= 0 or close_pos >= SURGE_CLOSE_POS_MIN)
                  and (SURGE_MIN_QUOTE_VOL <= 0
                       or ticker_cache.get(sym, {}).get("volume", 0) >= SURGE_MIN_QUOTE_VOL)):
                with surge_lock:
                    surge_queue.append({
                        "symbol": sym,
                        "price": price_now,
                        "start_pct": start_pct,
                        "vol_ratio": vol_ratio_1m,
                        "ts": time.time(),
                    })
            else:
                logger.debug(f"⚡ 速报候选被可选过滤拦下: {sym} HH={start_pct:.2f}%")

        if not (EARLY_HH_MIN <= start_pct <= EARLY_HH_MAX):
            return

        score = 1
        if body_pct >= EARLY_1M_BODY_MIN:
            score += 1
        if vol_ratio_1m >= EARLY_1M_VOL_MULT:
            score += 1
        if close_pos >= EARLY_CLOSE_POSITION or break_ok:
            score += 1
        if score < 4:
            return

        early_id = insert_early_signal(sym, price_now, highs[-1], lows[-3],
                                       start_pct, score, vol_ratio_1m)
        entry = {
            "id": early_id, "symbol": sym,
            "trigger_time": time.time(), "trigger_dt": now_beijing(),
            "trigger_price": price_now, "trigger_high": highs[-1],
            "trigger_low": lows[-3], "start_pct": start_pct,
            "early_score": score, "volume_ratio": vol_ratio_1m,
            "max_price": price_now, "min_price": price_now,
            "highest_price": price_now, "last_high": highs[-1],
            "last_check_price": price_now,
            "check_3m_done": False, "check_6m_done": False, "check_15m_done": False,
            "weak_notified": False, "source": "early", "stage": "EARLY",
            "active": False, "new_high_observed": False,
            "max_gain_seen": -1.0, "max_drawdown_seen": 0.0,
            "last_recorded_high": price_now, "last_new_high_time": time.time(),
        }
        with early_lock:
            pending_early[sym] = entry

        sb["daily_round"] += 1
        sb["first_price"] = price_now
        sb["daily_high"] = price_now
        sb["daily_low"] = price_now
        sb["trigger_time"] = now_beijing()
        sb["source"] = "early"

        logger.info(f"🟡 启动预警(内部): {sym} HH={start_pct:.2f}% "
                    f"score={score}/4 vol={vol_ratio_1m:.2f}x")

    except Exception as e:
        logger.error(f"{sym} 启动雷达失败: {e}")

# ===================== 速报派发：Top N 单发 + 合并汇总 =====================
def _surge_digest_text(total, shown, rest_count):
    lines = [f"⚡ 速报汇总 · 本轮共 {total} 个", "━━━━━━━━━━━━━━━"]
    for i, c in enumerate(shown, 1):
        lines.append(f"{i}. {c['symbol']}  ${fmt_price(c['price'])}  "
                     f"+{c['start_pct']:.2f}%  量{c['vol_ratio']:.1f}x")
    lines.append("━━━━━━━━━━━━━━━")
    hidden = rest_count - len(shown)
    if hidden > 0:
        lines.append(f"另有 {hidden} 个仅入库，日报可见")
    lines.append(f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(lines)

def _send_surge_digest(rest, total, now_ts):
    if not SURGE_ENABLE_DIGEST or not rest:
        return 0
    today = now_beijing().date()
    if surge_digest_state["day"] != today:
        surge_digest_state.update({"day": today, "count": 0, "last_ts": 0.0})
    if now_ts - surge_digest_state["last_ts"] < SURGE_DIGEST_GAP_SEC:
        logger.info("⚡ 汇总冷却中，本轮候选仅入库: "
                    + ", ".join(c["symbol"] for c in rest[:5]))
        return 0
    if surge_digest_state["count"] >= SURGE_DIGEST_MAX_PER_DAY:
        logger.info("⚡ 汇总达每日上限，本轮候选仅入库")
        return 0

    eligible = [c for c in rest
                if should_alert(c["symbol"], "LONG", channel="surge",
                                max_count=SURGE_MAX_PER_DAY)]
    if not eligible:
        return 0
    eligible.sort(key=lambda x: (x["start_pct"], x["vol_ratio"]), reverse=True)
    chosen = eligible[:SURGE_DIGEST_MAX_LINES]

    if not send_tg(_surge_digest_text(total, chosen, len(rest))):
        return 0

    for c in rest:
        try:
            try:
                check_strong_surge(c["symbol"], c["price"], c["start_pct"])
            except Exception as e:
                logger.error(f"强提示判定失败 {c['symbol']}: {e}")
            mark_alerted(c["symbol"], "LONG", channel="surge",
                         price=c["price"], hh_pct=c["start_pct"])
            record_channel(c["symbol"], "surge")
            surge_last_price[c["symbol"]] = (now_ts, c["price"])
        except Exception as e:
            logger.warning(f"⚡ 汇总记账失败 {c['symbol']}: {e}")

    with surge_lock:
        surge_send_ts.append(now_ts)
    surge_digest_state["last_ts"] = now_ts
    surge_digest_state["count"] += 1
    logger.info(f"⚡ 速报汇总: 本轮 {len(rest)} 个，展示 {len(chosen)} 个")
    return 1

def flush_surge_queue():
    """去重 → 排序 → 强提示候选强制单发 → <5 全发 / ≥5 TopN+汇总。"""
    now_ts = time.time()
    with surge_lock:
        if not surge_queue:
            return 0
        batch = surge_queue[:]
        surge_queue.clear()

    best = {}
    for c in batch:
        s = c["symbol"]
        if s not in best or c["start_pct"] > best[s]["start_pct"]:
            best[s] = c
    ranked = sorted(best.values(),
                    key=lambda x: (x["start_pct"], x["vol_ratio"]), reverse=True)
    total = len(ranked)

    with surge_lock:
        surge_send_ts[:] = [t for t in surge_send_ts if now_ts - t < SURGE_WINDOW_SEC]
        room = max(0, SURGE_MAX_IN_WINDOW - len(surge_send_ts))

    strong_pick = []
    for c in ranked:
        try:
            if _would_be_strong_surge(c["symbol"], c["price"]):
                strong_pick.append(c)
        except Exception:
            continue
    strong_syms = {c["symbol"] for c in strong_pick}
    if strong_pick:
        logger.info("🔴 强提示候选强制单发: " + ", ".join(c["symbol"] for c in strong_pick))

    others = [c for c in ranked if c["symbol"] not in strong_syms]

    if total < SURGE_DIGEST_MIN_TOTAL:
        top, rest = ranked, []
    else:
        top = strong_pick + others[:SURGE_TOP_PER_CYCLE]
        rest = others[SURGE_TOP_PER_CYCLE:]
        logger.info(f"⚡ 普涨收敛：本轮候选 {total} 个，单发 {len(top)} 个，"
                    f"其余 {len(rest)} 个合并")

    sent = 0
    for c in top:
        if sent >= room:
            if rest:
                rest.append(c)
            else:
                logger.info(f"⚡ 10分钟额度已满，{c['symbol']} 未发送（仅入库）")
            continue
        sym = c["symbol"]

        prev = surge_last_price.get(sym)
        if prev:
            prev_ts, prev_price = prev
            if (now_ts - prev_ts < SURGE_REPEAT_HOURS * 3600 and
                    c["price"] < prev_price * (1 + SURGE_MIN_PRICE_ADVANCE)):
                continue

        if not should_alert(sym, "LONG", channel="surge", max_count=SURGE_MAX_PER_DAY):
            continue

        chase_skip = False
        try:
            k = get_klines(sym, "1m", 61)
            if len(k) >= 61:
                chg = (float(k[-1][4]) - float(k[0][4])) / float(k[0][4])
                if chg > SURGE_MAX_1H_CHANGE:
                    logger.info(f"⚡ {sym} 1h已涨 {chg*100:.1f}%，速报跳过（追高）")
                    chase_skip = True
        except Exception:
            pass

        if chase_skip:
            try:
                check_strong_surge(sym, c["price"], c["start_pct"])
            except Exception as e:
                logger.error(f"强提示(速报)判定失败 {sym}: {e}")
            continue

        notify_surge(sym, c["price"], c["start_pct"])
        try:
            check_strong_surge(sym, c["price"], c["start_pct"])
        except Exception as e:
            logger.error(f"强提示(速报)判定失败 {sym}: {e}")
        mark_alerted(sym, "LONG", channel="surge",
                     price=c["price"], hh_pct=c["start_pct"])
        surge_last_price[sym] = (now_ts, c["price"])
        with surge_lock:
            surge_send_ts.append(now_ts)
        sent += 1
        logger.info(f"⚡ 速报: {sym} 3M HH={c['start_pct']:.2f}% vol={c['vol_ratio']:.2f}x")

    if rest:
        room_now = max(0, SURGE_MAX_IN_WINDOW -
                       len([t for t in surge_send_ts if now_ts - t < SURGE_WINDOW_SEC]))
        if room_now > 0:
            _send_surge_digest(rest, total, now_ts)
        else:
            logger.warning(f"⚡ 10分钟额度已满，{len(rest)} 个候选未发送（仅入库）: "
                           + ", ".join(c["symbol"] for c in rest[:5]))
    elif total and sent == 0:
        logger.warning(f"⚡ 本轮 {total} 个候选全部被过滤（去重/追高/日上限），仅入库")

    return sent

# ===================== 正式确认/强化评分 =====================
def analyze_symbol(symbol, early=None, market_state=None):
    try:
        if symbol not in high_volume_symbols:
            return None
        k5 = get_klines(symbol, "5m", 24)
        k15 = get_klines(symbol, "15m", 12)
        if len(k5) < 12 or len(k15) < 6:
            return None

        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) >= 2:
            oi_prev = float(oi_hist[0]["sumOpenInterest"])
            oi_now = float(oi_hist[-1]["sumOpenInterest"])
            oi_change = (oi_now - oi_prev) / oi_prev if oi_prev > 0 else 0
        else:
            oi_change = 0

        funding = get_funding_rate(symbol)
        closes5 = [float(k[4]) for k in k5]
        highs5 = [float(k[2]) for k in k5]
        volumes5 = [float(k[5]) for k in k5]
        structure_ok = closes5[-1] > max(highs5[-11:-1])

        continued = False
        post_start_pct = 0.0
        if early:
            current = closes5[-1]
            post_start_pct = ((current - early["trigger_price"]) / early["trigger_price"]
                              if early["trigger_price"] else 0)
            continued = current > early.get("trigger_high", early["trigger_price"])

        base_vol = sum(volumes5[:-3]) / max(1, len(volumes5) - 3)
        recent_vol = sum(volumes5[-3:]) / 3
        volume_ratio = recent_vol / base_vol if base_vol > 0 else 0
        volume_ok = volume_ratio >= VOLUME_MULTIPLIER

        last3 = k15[-3:]
        bullish = sum(1 for k in last3 if float(k[4]) > float(k[1]))
        recent15_ok = bullish >= 2 and float(last3[-1][4]) > float(last3[-2][2])

        oi_ok = oi_change > 0
        oi_strong = oi_change >= OI_STRONG
        funding_ok = not (funding > FUNDING_RATE_MAX)

        if market_state is None:
            market_state = market_state_cache.copy()
        btc_dump = market_state["btc_change_15m"] <= -BTC_PAUSE_15M
        market_bad = market_state["market_down_ratio"] >= MARKET_DOWN_RATIO
        hard_market_reject = (
            market_state["market_down_ratio"] >= MARKET_HARD_DOWN_RATIO or
            market_state["btc_change_15m"] <= BTC_HARD_NEGATIVE
        )

        price = closes5[-1]
        chase_risk = compute_chase_risk(symbol, price)

        score = 0
        score += 1 if structure_ok else 0
        score += 1 if continued else 0
        score += 1 if volume_ok else 0
        score += 1 if oi_ok else 0
        score += 1 if (recent15_ok and funding_ok) else 0
        score += 1 if (not btc_dump and not market_bad) else 0

        one_hour_change = 0
        if len(closes5) >= 13:
            one_hour_change = (closes5[-1] - closes5[-13]) / closes5[-13]

        if hard_market_reject:
            return {"symbol": symbol, "direction": "LONG", "rejected": True,
                    "reject_reason": "市场状态不佳（下跌占比过高或BTC下跌）",
                    "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk}
        if one_hour_change > MAX_1H_CHANGE:
            return {"symbol": symbol, "direction": "LONG", "rejected": True,
                    "reject_reason": "1h涨幅超过追高阈值",
                    "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk}
        if chase_risk >= CHASE_MAX_RISK:
            return {"symbol": symbol, "direction": "LONG", "rejected": True,
                    "reject_reason": f"追高风险{chase_risk}/100",
                    "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk}

        quality = ("强启动" if score >= STRONG_SCORE_MIN
                   else "正式启动" if score >= FORMAL_SCORE_MIN else "弱启动")

        k1_for_progress = get_klines(symbol, "1m", PUSH_ATR_PERIOD + 2)
        atr_for_progress = atr_percent_from_1m(k1_for_progress) if k1_for_progress else 0.0

        return {
            "symbol": symbol, "direction": "LONG", "score": score, "max_score": 6,
            "quality_label": quality, "price": price,
            "oi_change": oi_change, "oi_strong": oi_strong,
            "volume_ratio": volume_ratio, "funding": funding,
            "post_start_pct": post_start_pct * 100,
            "progress_class": classify_progress(post_start_pct, atr_for_progress)[0],
            "chase_risk": chase_risk,
            "btc_change_15m": market_state["btc_change_15m"],
            "market_down_ratio": market_state["market_down_ratio"],
            "rejected": False,
        }
    except Exception as e:
        logger.error(f"{symbol} 正式确认分析失败: {e}")
        return None

# ===================== 3/6/15分钟验证 =====================
def process_early_validation():
    now_ts = time.time()
    with early_lock:
        symbols_snapshot = list(pending_early.keys())

    for sym in symbols_snapshot:
        with early_lock:
            entry = pending_early.get(sym)
        if not entry:
            continue
        try:
            elapsed = now_ts - entry["trigger_time"]
            k1 = get_klines(sym, "1m", 20)
            if not k1:
                continue

            price = float(k1[-1][4])
            lows = [float(x[3]) for x in k1]
            highs = [float(x[2]) for x in k1]

            old_highest = entry.get("highest_price", entry["trigger_price"])
            window_high = max(highs[-3:])
            entry["max_price"] = max(entry["max_price"], price, window_high)
            entry["min_price"] = min(entry["min_price"], price, min(lows[-3:]))
            new_high = price > old_highest
            entry["highest_price"] = max(old_highest, entry["max_price"])

            gain = ((price - entry["trigger_price"]) / entry["trigger_price"]
                    if entry["trigger_price"] > 0 else 0)
            drawdown = ((entry["max_price"] - price) / entry["max_price"]
                        if entry["max_price"] > 0 else 0)
            entry["max_gain_seen"] = max(entry.get("max_gain_seen", -1), gain)
            entry["max_drawdown_seen"] = max(entry.get("max_drawdown_seen", 0), drawdown)
            if new_high:
                entry["new_high_observed"] = True

            atr_pct = atr_percent_from_1m(k1)
            progress_class, _ = classify_progress(gain, atr_pct)

            update_early_row(entry["id"], max_price=entry["max_price"],
                             min_price=entry["min_price"],
                             max_gain=entry["max_gain_seen"],
                             max_drawdown=entry["max_drawdown_seen"],
                             progress_class=progress_class)

            if price < entry["trigger_low"]:
                reason = "跌破启动基准低点"
                update_early_row(entry["id"], result="FAILED", stage="FAILED",
                                 fail_reason=reason,
                                 cooldown_until=now_beijing() + timedelta(seconds=EARLY_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, entry["id"], "FAILED", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            if drawdown >= EXTREME_DRAWDOWN:
                reason = "启动后最大回撤达到5%"
                update_early_row(entry["id"], result="FAILED", stage="FAILED",
                                 fail_reason=reason,
                                 cooldown_until=now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, entry["id"], "EXTREME_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            if elapsed >= FAST_CHECK_3M_SEC and not entry["check_3m_done"]:
                entry["check_3m_done"] = True
                entry["stage"] = "VALIDATING_6M"
                update_early_row(entry["id"], stage="VALIDATING_6M", price_3m=price)
                add_tracking_event(sym, entry["id"], "3M_CHECK", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="3分钟承接检查")

            if elapsed >= FAST_CHECK_6M_SEC and not entry["check_6m_done"]:
                entry["check_6m_done"] = True

                if len(k1) >= 6:
                    recent_vol = sum(float(x[5]) for x in k1[-3:]) / 3
                    base_vol = sum(float(x[5]) for x in k1[:-3]) / max(1, len(k1) - 3)
                    vol_sustain = recent_vol > base_vol * 1.5
                else:
                    vol_sustain = False

                fast_ok = (gain >= FAST_MIN_PROGRESS and new_high and
                           price >= entry["trigger_price"] and vol_sustain)
                slow_ok = (gain >= SLOW_MIN_PROGRESS and
                           price >= entry["trigger_price"] and
                           drawdown < SOFT_DRAWDOWN * 1.5)

                if fast_ok or (slow_ok and progress_class in ("正常推进", "强推进", "爆发")):
                    entry["stage"] = "CONFIRMED"
                    entry["active"] = True
                    entry["confirm_price"] = price
                    update_early_row(entry["id"], result="CONFIRMED", stage="CONFIRMED",
                                     confirm_time=now_beijing(), confirm_price=price,
                                     price_6m=price, progress_class=progress_class)
                    add_tracking_event(sym, entry["id"], "CONFIRMED", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="6分钟启动确认")

                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        update_early_row(entry["id"], formal_score=sig.get("score", 0),
                                         chase_risk=sig.get("chase_risk", 0),
                                         fail_reason=sig.get("reject_reason", "风险过滤"))
                        logger.info(f"⚠️ {sym} 启动成立但风险过滤(内部): {sig.get('reject_reason')}")
                    elif sig and sig.get("score", 0) >= FORMAL_SCORE_MIN:
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过正式启动通知: {sym}")
                        update_early_row(entry["id"], formal_score=sig["score"],
                                         chase_risk=sig.get("chase_risk", 0))
                    else:
                        basic_sig = {
                            "symbol": sym, "direction": "LONG", "price": price,
                            "score": sig.get("score", 0) if sig else 0, "max_score": 6,
                            "quality_label": "启动确认（后级强化不足）",
                            "oi_change": sig.get("oi_change", 0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio", 0) if sig else 0,
                            "funding": sig.get("funding", 0) if sig else 0,
                            "post_start_pct": gain * 100, "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk", 0) if sig else 0,
                        }
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(basic_sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过正式启动通知: {sym}")

                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue

                entry["stage"] = "WATCH_15M"
                update_early_row(entry["id"], stage="WATCH_15M", price_6m=price,
                                 progress_class=progress_class)
                add_tracking_event(sym, entry["id"], "6M_WEAK", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="6分钟未达标，延长到15分钟")

            if elapsed >= SLOW_CHECK_15M_SEC and not entry["check_15m_done"]:
                entry["check_15m_done"] = True
                update_early_row(entry["id"], price_15m=price)

                slow_confirm = (gain >= SLOW_MIN_PROGRESS and
                                entry.get("new_high_observed", False) and
                                drawdown < SOFT_DRAWDOWN * 1.5)
                if slow_confirm:
                    entry["stage"] = "CONFIRMED"
                    entry["active"] = True
                    entry["confirm_price"] = price
                    update_early_row(entry["id"], result="CONFIRMED", stage="CONFIRMED",
                                     confirm_time=now_beijing(), confirm_price=price,
                                     progress_class=progress_class)
                    add_tracking_event(sym, entry["id"], "CONFIRMED", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="15分钟慢启动确认")
                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        update_early_row(entry["id"], formal_score=sig.get("score", 0),
                                         chase_risk=sig.get("chase_risk", 0),
                                         fail_reason=sig.get("reject_reason", "风险过滤"))
                        logger.info(f"⚠️ {sym} 慢启动成立但风险过滤(内部): {sig.get('reject_reason')}")
                    else:
                        basic_sig = {
                            "symbol": sym, "direction": "LONG", "price": price,
                            "score": sig.get("score", 0) if sig else 0, "max_score": 6,
                            "quality_label": "慢启动确认",
                            "oi_change": sig.get("oi_change", 0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio", 0) if sig else 0,
                            "funding": sig.get("funding", 0) if sig else 0,
                            "post_start_pct": gain * 100, "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk", 0) if sig else 0,
                        }
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(basic_sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过慢启动通知: {sym}")
                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue

                reason = "15分钟内未形成有效延续"
                update_early_row(entry["id"], result="FAILED", stage="FAILED",
                                 fail_reason=reason,
                                 cooldown_until=now_beijing() + timedelta(seconds=EARLY_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, entry["id"], "TIMEOUT_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                with early_lock:
                    pending_early.pop(sym, None)

        except Exception as e:
            logger.error(f"{sym} 启动验证失败: {e}")

# ===================== 动态跟踪 =====================
def track_active():
    for sym in list(active_tracking.keys()):
        sb = active_tracking[sym]
        try:
            trigger_dt = entry_trigger_dt(sb)
            elapsed_min = (now_beijing() - trigger_dt).total_seconds() / 60.0

            k1 = get_klines(sym, "1m", PUSH_ATR_PERIOD + 5)
            if not k1:
                continue

            price = float(k1[-1][4])
            high_now = max(float(x[2]) for x in k1[-3:])
            atr_pct = atr_percent_from_1m(k1)

            trigger_price = sb.get("trigger_price") or sb.get("first_price") or price
            trigger_low = sb.get("trigger_low") or sb.get("base_low") or price

            prev_highest = sb.get("highest_price") or trigger_price
            sb["highest_price"] = max(prev_highest, price, high_now)
            sb["daily_high"] = max(sb.get("daily_high") or price, price)
            sb["daily_low"] = min(sb.get("daily_low") or price, price)

            gain = (price - trigger_price) / trigger_price if trigger_price > 0 else 0.0
            highest = sb["highest_price"]
            drawdown = (highest - price) / highest if highest > 0 else 0.0
            sb["max_gain_seen"] = max(sb.get("max_gain_seen") or gain, gain)
            sb["max_drawdown_seen"] = max(sb.get("max_drawdown_seen") or drawdown, drawdown)

            progress_class, _ = classify_progress(gain, atr_pct)

            if elapsed_min >= TRACKING_TIMEOUT_HOURS * 60:
                update_early_row(sb["id"], result="EXPIRED", stage="EXPIRED",
                                 max_price=sb["highest_price"],
                                 min_price=min(sb.get("min_price") or price, price),
                                 max_gain=sb["max_gain_seen"],
                                 max_drawdown=sb["max_drawdown_seen"])
                add_tracking_event(sym, sb["id"], "EXPIRED", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="24小时超时")
                logger.info(f"⏰ {sym} 24小时跟踪结束(内部)")
                del active_tracking[sym]
                continue

            if price < trigger_low:
                update_early_row(sb["id"], result="FAILED", stage="FAILED",
                                 max_price=sb["highest_price"], min_price=price,
                                 max_gain=sb["max_gain_seen"],
                                 max_drawdown=sb["max_drawdown_seen"],
                                 fail_reason="正式确认后跌破启动基准低点",
                                 cooldown_until=now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, sb["id"], "STRUCTURE_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="跌破启动基准低点")
                logger.info(f"❌ {sym} 启动失效(内部): 跌破启动基准低点")
                del active_tracking[sym]
                continue

            if drawdown >= EXTREME_DRAWDOWN:
                update_early_row(sb["id"], result="FAILED", stage="FAILED",
                                 max_price=sb["highest_price"], min_price=price,
                                 max_gain=sb["max_gain_seen"],
                                 max_drawdown=sb["max_drawdown_seen"],
                                 fail_reason="正式确认后最大回撤达到5%",
                                 cooldown_until=now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, sb["id"], "EXTREME_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="最大回撤达到5%")
                logger.info(f"❌ {sym} 启动失效(内部): 最大回撤达到5%")
                del active_tracking[sym]
                continue

            if drawdown >= SOFT_DRAWDOWN:
                last_ts = sb.get("last_soft_dd_ts") or 0
                if time.time() - last_ts >= SOFT_DRAWDOWN_EVENT_GAP_SEC:
                    sb["last_soft_dd_ts"] = time.time()
                    add_tracking_event(sym, sb["id"], "SOFT_DRAWDOWN", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="软回撤0.8%")

            if not sb.get("last_recorded_high"):
                sb["last_recorded_high"] = prev_highest
                sb["last_new_high_time"] = time.time()

            ref = sb["last_recorded_high"]
            if high_now > ref:
                sb["last_new_high_time"] = time.time()
                push_pct = (high_now - ref) / ref if ref > 0 else 0.0
                need = max(NORMAL_PUSH_MIN, atr_pct * NORMAL_PUSH_ATR)
                if push_pct >= need and sb.get("push_times", 0) < MAX_PUSH:
                    sb["last_recorded_high"] = high_now
                    sb["push_times"] = sb.get("push_times", 0) + 1
                    sb["push_count"] = min(MAX_PUSH, sb.get("push_count", 0) + 1)
                    update_early_row(sb["id"], max_price=sb["highest_price"],
                                     max_gain=sb["max_gain_seen"],
                                     max_drawdown=sb["max_drawdown_seen"],
                                     progress_class=progress_class)
                    add_tracking_event(sym, sb["id"], "NEW_HIGH", price=price,
                                       change_pct=push_pct*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="有效新高推进")
                    if time.time() - startup_time >= SILENT_PERIOD_SEC:
                        if should_alert(sym, "LONG", channel="push", max_count=3):
                            notify_tg_only(
                                f"🚀 {sym} — {progress_class}\n"
                                f"当前价: {price:.8f}\n"
                                f"启动后: {gain*100:+.2f}%\n"
                                f"本次新高推进: {push_pct*100:+.2f}%\n"
                                f"累计推进次数: {sb.get('push_times', 0)}",
                                channel="push", symbol=sym)
                            mark_alerted(sym, "LONG", channel="push")
                    else:
                        logger.info(f"⏳ 静默期，跳过推进通知: {sym}")

            minutes_since_high = (time.time() - sb.get("last_new_high_time", time.time())) / 60.0

            if (elapsed_min >= MOMENTUM_WEAK_MINUTES and
                    not sb.get("weak_notified") and
                    minutes_since_high >= MOMENTUM_WEAK_MINUTES):
                add_tracking_event(sym, sb["id"], "MOMENTUM_WEAK", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="动能减弱")
                sb["weak_notified"] = True
                logger.info(f"🟡 {sym} 动能减弱(内部)")

            if (elapsed_min >= MOMENTUM_FAIL_MINUTES and
                    minutes_since_high >= MOMENTUM_FAIL_MINUTES and
                    gain < FAST_MIN_PROGRESS):
                reason = "30分钟无有效延续"
                update_early_row(sb["id"], result="FLAT", stage="FLAT",
                                 max_price=sb["highest_price"],
                                 min_price=min(sb.get("min_price") or price, price),
                                 max_gain=sb["max_gain_seen"],
                                 max_drawdown=sb["max_drawdown_seen"],
                                 fail_reason=reason)
                add_tracking_event(sym, sb["id"], "FLAT_EXIT", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"⏸ {sym} 30分钟无有效延续，退出跟踪(内部)")
                del active_tracking[sym]
                continue

        except Exception as e:
            logger.error(f"{sym} 动态追踪失败: {e}")

# ===================== 趋势通道（未改动） =====================
def scan_trends(symbols):
    target_syms = [s for s in symbols if s in high_volume_symbols]
    if not target_syms:
        return

    def check_trend(sym):
        try:
            k1h = get_klines(sym, "1h", 24)
            if len(k1h) < 24:
                return None
            closes = [float(k[4]) for k in k1h]
            opens = [float(k[1]) for k in k1h]
            highs = [float(k[2]) for k in k1h]
            change_24h = (closes[-1] - closes[0]) / closes[0] * 100
            bull_count = sum(1 for i in range(len(closes)) if closes[i] > opens[i])
            high_24h = max(highs)
            at_high_pct = closes[-1] / high_24h * 100 if high_24h > 0 else 0
            if (change_24h > TREND_CHANGE_MIN and
                    bull_count >= TREND_BULL_MIN and
                    at_high_pct > TREND_AT_HIGH_MIN):
                return (sym, closes[-1], change_24h, bull_count, at_high_pct)
        except Exception as e:
            logger.debug(f"{sym} 趋势检测失败: {e}")
        return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TREND) as ex:
        results = list(ex.map(check_trend, target_syms))

    hits = 0
    for r in results:
        if not r:
            continue
        sym, price, change_24h, bull_count, at_high_pct = r
        if not should_trend_alert(sym):
            continue
        if time.time() - startup_time < SILENT_PERIOD_SEC:
            logger.info(f"⏳ 静默期，跳过趋势通知: {sym}")
            continue
        trend_count = mark_trend_alerted(sym, price=price, change_24h=change_24h)
        notify_trend(sym, price, change_24h, bull_count, at_high_pct, trend_count)
        check_strong_trend(sym)
        hits += 1
        logger.info(f"📈 趋势信号: {sym} 24h涨幅{change_24h:+.2f}% (第{trend_count}次)，"
                    f"距24h高{at_high_pct:.1f}%")
    logger.info(f"📈 趋势扫描完成，发现 {hits} 个趋势信号")

# ===================== 状态恢复 =====================
def recover_state():
    cutoff = (now_beijing() - timedelta(hours=TRACKING_TIMEOUT_HOURS)).isoformat()
    try:
        with db_lock:
            rows = db_conn.execute("""
                SELECT id, symbol, trigger_time, trigger_price, trigger_high, trigger_low,
                       start_pct, early_score, volume_ratio, stage, result,
                       max_price, min_price, max_gain, max_drawdown
                FROM early_signal_log
                WHERE trigger_time > ? AND result IN ('PENDING','CONFIRMED')
                ORDER BY id ASC
            """, (cutoff,)).fetchall()
    except Exception as e:
        logger.error(f"状态恢复失败: {e}")
        return

    for row in rows:
        try:
            (eid, sym, trig, tprice, thigh, tlow, start_pct, escore, vratio,
             stage, result, max_price, min_price, max_gain, max_drawdown) = row
            if sym in pending_early or sym in active_tracking:
                continue
            tdt = parse_ts(trig)
            tprice = float(tprice)
            entry = {
                "id": eid, "symbol": sym,
                "trigger_time": tdt.timestamp(), "trigger_dt": tdt,
                "trigger_price": tprice,
                "trigger_high": float(thigh or tprice),
                "trigger_low": float(tlow or tprice),
                "start_pct": float(start_pct or 0),
                "early_score": int(escore or 0),
                "volume_ratio": float(vratio or 0),
                "max_price": float(max_price or tprice),
                "min_price": float(min_price or tprice),
                "highest_price": float(max_price or tprice),
                "last_high": float(thigh or tprice),
                "last_check_price": tprice,
                "check_3m_done": stage in ("VALIDATING_6M", "WATCH_15M"),
                "check_6m_done": stage == "WATCH_15M",
                "check_15m_done": False,
                "weak_notified": False,
                "source": "recover",
                "stage": stage or "EARLY",
                "active": result == "CONFIRMED",
                "new_high_observed": False,
                "max_gain_seen": float(max_gain) if max_gain is not None else -1.0,
                "max_drawdown_seen": float(max_drawdown or 0),
                "last_recorded_high": float(max_price or tprice),
                "last_new_high_time": time.time(),
            }
            if result == "CONFIRMED":
                active_tracking[sym] = entry
            else:
                pending_early[sym] = entry
        except Exception as e:
            logger.warning(f"恢复 {row[1]} 失败: {e}")

    logger.info(f"♻️ 状态恢复：待验证 {len(pending_early)}，跟踪中 {len(active_tracking)}"
                f"（来源{len(rows)}条历史）")

# ===================== 日报 =====================
def normalize_fail_reason(reason):
    try:
        if reason is None or pd.isna(reason):
            return "未分类"
    except Exception:
        pass
    r = str(reason).strip()
    if not r:
        return "未分类"
    rules = [
        ("跌破", "跌破启动低点"), ("回撤", "回撤超限"),
        ("15分钟", "15分钟未延续"), ("30分钟", "30分钟无延续"),
        ("24小时", "跟踪超时"), ("超时", "跟踪超时"),
        ("追高", "风险过滤"), ("市场状态", "风险过滤"),
        ("1h涨幅", "风险过滤"), ("全市场", "风险过滤"),
    ]
    for key, label in rules:
        if key in r:
            return label
    return "其他"

def generate_daily_report(report_date=None):
    if report_date is None:
        report_date = now_beijing().date()

    date_start = datetime(report_date.year, report_date.month, report_date.day,
                          tzinfo=timezone(timedelta(hours=8)))
    date_end = date_start + timedelta(days=1)

    with db_lock:
        early_rows = db_conn.execute("""
            SELECT id, symbol, trigger_time, trigger_price, start_pct,
                   early_score, volume_ratio, stage, result,
                   confirm_time, confirm_price, price_3m, price_6m, price_15m,
                   max_price, min_price, max_gain, max_drawdown,
                   progress_class, chase_risk, formal_score, fail_reason
            FROM early_signal_log
            WHERE trigger_time>=? AND trigger_time<?
            ORDER BY trigger_time ASC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

        event_rows = db_conn.execute("""
            SELECT symbol, early_id, event_time, event_type, price,
                   change_pct, drawdown_pct, progress_class, note
            FROM tracking_events
            WHERE event_time>=? AND event_time<?
            ORDER BY event_time ASC LIMIT ?
        """, (date_start.isoformat(), date_end.isoformat(),
              EVENT_SHEET_MAX_ROWS)).fetchall()

        event_stats = db_conn.execute("""
            SELECT event_type, COUNT(*), AVG(change_pct), AVG(drawdown_pct)
            FROM tracking_events
            WHERE event_time>=? AND event_time<?
            GROUP BY event_type ORDER BY COUNT(*) DESC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

        ch_rows = db_conn.execute("""
            SELECT channel, COUNT(*), COUNT(DISTINCT symbol)
            FROM channel_log
            WHERE sent_at>=? AND sent_at<?
            GROUP BY channel ORDER BY COUNT(*) DESC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

        strong_rows = db_conn.execute("""
            SELECT symbol, kind, reason, price, sent_at
            FROM strong_alert_log
            WHERE sent_at>=? AND sent_at<?
            ORDER BY sent_at ASC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

    cols = ["ID", "币种", "启动时间", "启动价", "3M启动幅度%", "预警评分",
            "初始量能比", "最终阶段", "最终结果", "确认时间", "确认价",
            "3M价格", "6M价格", "15M价格", "最高价", "最低价",
            "最大涨幅%", "最大回撤%", "推进等级", "追高风险", "正式评分", "失败原因"]
    df = pd.DataFrame(early_rows, columns=cols)

    if not df.empty:
        df["3M启动幅度%"] = pd.to_numeric(df["3M启动幅度%"], errors="coerce").fillna(0).round(2)
        for c in ("最大涨幅%", "最大回撤%"):
            df[c] = (pd.to_numeric(df[c], errors="coerce").fillna(0) * 100).round(2)
        for c in ("启动时间", "确认时间"):
            df[c] = (pd.to_datetime(df[c], errors="coerce", utc=True)
                     .dt.tz_convert("Asia/Shanghai").dt.tz_localize(None))
        df["失败原因分类"] = df["失败原因"].apply(normalize_fail_reason)

        now_naive = now_beijing().replace(tzinfo=None)
        df["成熟度"] = [
            "未知" if pd.isna(t) else
            ("已成熟" if (now_naive - t) >= timedelta(hours=TRACKING_TIMEOUT_HOURS) else "跟踪中")
            for t in df["启动时间"]
        ]

    total = len(df)
    if total:
        has_confirm = df["确认时间"].notna()
        confirmed = int(has_confirm.sum())
        confirm_then_fail = int((has_confirm & df["最终结果"].isin(
            ["FAILED", "FLAT", "EXPIRED"])).sum())
        failed = int((df["最终结果"] == "FAILED").sum())
        flat = int((df["最终结果"] == "FLAT").sum())
        pending = int((df["最终结果"] == "PENDING").sum())
        expired = int((df["最终结果"] == "EXPIRED").sum())
        p3 = int(df["3M价格"].notna().sum())
        p6 = int(df["6M价格"].notna().sum())
        p15 = int(df["15M价格"].notna().sum())
        confirm_rate = confirmed / total * 100
        mature_rows = df[df["成熟度"] == "已成熟"]
        mature_rate = (mature_rows["确认时间"].notna().sum() / len(mature_rows) * 100
                       if len(mature_rows) else 0.0)
        mfe_median = float(df["最大涨幅%"].median())
        mae_median = float(df["最大回撤%"].median())
    else:
        confirmed = confirm_then_fail = failed = flat = pending = expired = 0
        p3 = p6 = p15 = 0
        confirm_rate = mature_rate = mfe_median = mae_median = 0.0

    pre_alert = total

    stages = [("启动预警", pre_alert), ("记录到3分钟", p3), ("进入6分钟节点", p6),
              ("进入15分钟节点", p15), ("正式确认", confirmed)]
    rows = []
    prev = None
    for name, cnt in stages:
        pct = cnt / pre_alert * 100 if pre_alert else 0.0
        step = (cnt / prev * 100) if (prev and prev > 0) else (100.0 if name == "启动预警" else 0.0)
        rows.append([name, int(cnt), round(pct, 2), round(step, 2)])
        prev = cnt
    funnel = pd.DataFrame(rows, columns=["阶段", "数量", "占启动预警比例%", "相对上一阶段%"])

    result_rows = [
        ["最终确认（未被推翻）", confirmed, round(confirmed / pre_alert * 100, 2) if pre_alert else 0],
        ["确认后失效/横盘/超时", confirm_then_fail, round(confirm_then_fail / pre_alert * 100, 2) if pre_alert else 0],
        ["失败（未确认即失效）", failed, round(failed / pre_alert * 100, 2) if pre_alert else 0],
        ["横盘退出", flat, round(flat / pre_alert * 100, 2) if pre_alert else 0],
        ["跟踪超时", expired, round(expired / pre_alert * 100, 2) if pre_alert else 0],
        ["仍待处理", pending, round(pending / pre_alert * 100, 2) if pre_alert else 0],
    ]
    results_df = pd.DataFrame(result_rows, columns=["结果", "数量", "占比%"])

    strong_count = len(strong_rows)
    summary_rows = [
        ["启动预警数", pre_alert],
        ["确认率（全样本）%", round(confirm_rate, 2)],
        ["确认率（仅已成熟样本）%", round(mature_rate, 2)],
        ["已成熟样本数", int((df["成熟度"] == "已成熟").sum()) if total else 0],
        ["跟踪中（未满24h）", int((df["成熟度"] == "跟踪中").sum()) if total else 0],
        ["确认后失效率%", round(confirm_then_fail / confirmed * 100, 2) if confirmed else 0],
        ["最大涨幅 MFE 中位数%", round(mfe_median, 2)],
        ["最大回撤 MAE 中位数%", round(mae_median, 2)],
        ["强提示条数", strong_count],
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    failure_df = pd.DataFrame(columns=["失败原因分类", "数量", "占失败比例%"])
    if total:
        fails = df.loc[df["最终结果"].isin(["FAILED", "FLAT", "EXPIRED"])]
        if not fails.empty:
            vc = fails["失败原因分类"].value_counts()
            failure_df = pd.DataFrame({"失败原因分类": vc.index, "数量": vc.values})
            failure_df["占失败比例%"] = (failure_df["数量"] / max(1, len(fails)) * 100).round(2)

    best_df = pd.DataFrame()
    if total:
        best_df = df.sort_values(["最大涨幅%", "正式评分"],
                                 ascending=[False, False]).head(10).copy()
        best_df = best_df[["币种", "启动时间", "启动价", "最高价", "最大涨幅%",
                           "最大回撤%", "最终结果", "正式评分", "推进等级", "成熟度"]]

    ch_df = (pd.DataFrame([[CHANNEL_CN.get(r[0], r[0]), int(r[1]), int(r[2])] for r in ch_rows],
                          columns=["通道", "条数", "覆盖币种数"])
             if ch_rows else pd.DataFrame([["当天没有对外通知"]], columns=["说明"]))

    strong_df = pd.DataFrame(strong_rows, columns=["币种", "类型", "依据", "价格", "时间"])
    if not strong_df.empty:
        strong_df["时间"] = (pd.to_datetime(strong_df["时间"], errors="coerce", utc=True)
                             .dt.tz_convert("Asia/Shanghai").dt.tz_localize(None))
        strong_df["类型"] = (strong_df["类型"].map({"trend": "趋势连升", "surge": "二次速报"})
                             .fillna(strong_df["类型"]))

    event_cols = ["币种", "启动ID", "事件时间", "事件类型", "价格", "变化%", "回撤%", "推进等级", "说明"]
    events_df = pd.DataFrame(event_rows, columns=event_cols)
    if not events_df.empty:
        events_df["事件时间"] = (pd.to_datetime(events_df["事件时间"], errors="coerce", utc=True)
                                 .dt.tz_convert("Asia/Shanghai").dt.tz_localize(None))

    if event_stats:
        ev_stat_df = pd.DataFrame(
            [[r[0], int(r[1]), round(float(r[2] or 0), 2), round(float(r[3] or 0), 2)]
             for r in event_stats],
            columns=["事件类型", "数量", "平均变化%", "平均回撤%"])
    else:
        ev_stat_df = pd.DataFrame([["当天没有跟踪事件"]], columns=["说明"])

    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"

    try:
        with pd.ExcelWriter(fname, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="概览", index=False)
            funnel.to_excel(writer, sheet_name="启动漏斗", index=False)
            results_df.to_excel(writer, sheet_name="结果分布", index=False)
            ch_df.to_excel(writer, sheet_name="通道统计", index=False)

            if df.empty:
                pd.DataFrame([["当天没有启动预警记录"]], columns=["说明"]).to_excel(
                    writer, sheet_name="信号明细", index=False)
            else:
                df.to_excel(writer, sheet_name="信号明细", index=False)

            if failure_df.empty:
                pd.DataFrame([["当天没有失败记录"]], columns=["说明"]).to_excel(
                    writer, sheet_name="失败分析", index=False)
            else:
                failure_df.to_excel(writer, sheet_name="失败分析", index=False)

            if best_df.empty:
                pd.DataFrame([["当天没有最佳信号"]], columns=["说明"]).to_excel(
                    writer, sheet_name="最佳信号", index=False)
            else:
                best_df.to_excel(writer, sheet_name="最佳信号", index=False)

            if strong_df.empty:
                pd.DataFrame([["当天没有强提示"]], columns=["说明"]).to_excel(
                    writer, sheet_name="强提示", index=False)
            else:
                strong_df.to_excel(writer, sheet_name="强提示", index=False)

            ev_stat_df.to_excel(writer, sheet_name="事件统计", index=False)

            if events_df.empty:
                pd.DataFrame([["当天没有跟踪事件"]], columns=["说明"]).to_excel(
                    writer, sheet_name="跟踪事件", index=False)
            else:
                events_df.to_excel(writer, sheet_name="跟踪事件", index=False)
    except Exception as e:
        logger.error(f"📊 日报Excel生成失败: {e}")
        send_tg(f"❌ 日报Excel生成失败: {e}")
        return False

    top_text = "无"
    if not best_df.empty:
        top = best_df.iloc[0]
        top_text = f"{top['币种']}，启动后最大涨幅 {float(top['最大涨幅%']):+.2f}%"

    body = (
        f"📊 盘面日报 {report_date}\n\n"
        f"启动预警：{pre_alert}\n正式确认：{confirmed}\n"
        f"确认后失效：{confirm_then_fail}\n失败：{failed}\n"
        f"横盘退出：{flat}\n超时：{expired}\n仍待处理：{pending}\n"
        f"强提示：{strong_count}\n\n"
        f"确认率（全样本）：{confirm_rate:.2f}%\n"
        f"确认率（仅已成熟）：{mature_rate:.2f}%\n"
        f"确认后失效率：{(confirm_then_fail / confirmed * 100) if confirmed else 0:.2f}%\n"
        f"MFE 中位数：{mfe_median:+.2f}%\n\n"
        f"今日最佳：{top_text}\n\n"
        f"附件包含：概览、启动漏斗、结果分布、通道统计、信号明细、失败分析、"
        f"最佳信号、强提示、事件统计、跟踪事件。"
    )

    email_ok = send_email_attach(f"📊 [盘面日报] {report_date}", body, fname)
    if WECOM_MIRROR_DAILY_REPORT and email_ok:
        notify_wecom(body)

    with db_lock:
        db_conn.execute("""
            INSERT INTO daily_report_status
            (report_date, generated_at, filepath, email_ok, attempts, last_error)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(report_date) DO UPDATE SET
              generated_at=excluded.generated_at,
              filepath=excluded.filepath,
              email_ok=excluded.email_ok,
              attempts=daily_report_status.attempts+1,
              last_error=excluded.last_error
        """, (str(report_date), now_beijing().isoformat(), fname,
              1 if email_ok else 0, 1, "" if email_ok else "邮件发送失败"))
        db_conn.commit()

    logger.info(f"📊 {report_date} 日报生成完成：{fname}，预警{pre_alert}，"
                f"确认{confirmed}，强提示{strong_count}，邮件{'成功' if email_ok else '失败'}")

    send_tg(
        f"📊 {report_date} 盘面日报\n"
        f"启动预警: {pre_alert}\n正式确认: {confirmed}\n"
        f"确认后失效: {confirm_then_fail}\n失败: {failed}\n"
        f"横盘: {flat}\n超时: {expired}\n强提示: {strong_count}\n"
        f"确认率: {confirm_rate:.2f}%（成熟样本 {mature_rate:.2f}%）\n"
        f"最佳: {top_text}\n邮箱日报: {'✅' if email_ok else '❌'}"
    )
    return email_ok

def maybe_send_daily_report():
    now = now_beijing()
    if now.hour < DAILY_REPORT_HOUR:
        return
    if now.hour == DAILY_REPORT_HOUR and now.minute < DAILY_REPORT_MINUTE:
        return

    report_date = now.date() - timedelta(days=1)
    with db_lock:
        row = db_conn.execute(
            "SELECT email_ok, attempts, generated_at FROM daily_report_status WHERE report_date=?",
            (str(report_date),)).fetchone()

    if row:
        email_ok, attempts, generated_at = row
        if email_ok:
            return
        if (attempts or 0) >= DAILY_REPORT_MAX_ATTEMPTS:
            return
        if generated_at:
            try:
                if (now - parse_ts(generated_at)).total_seconds() < DAILY_REPORT_RETRY_SEC:
                    return
            except Exception:
                pass

    try:
        generate_daily_report(report_date)
    except Exception as e:
        logger.error(f"📊 日报任务异常: {e}")
        with db_lock:
            db_conn.execute("""
                INSERT INTO daily_report_status
                (report_date, generated_at, filepath, email_ok, attempts, last_error)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(report_date) DO UPDATE SET
                  generated_at=excluded.generated_at,
                  attempts=daily_report_status.attempts+1,
                  last_error=excluded.last_error
            """, (str(report_date), now_beijing().isoformat(), "", 0, 1, str(e)))
            db_conn.commit()
        send_tg(f"❌ 日报任务异常: {e}")

def cleanup_old_data():
    cutoff_iso = (now_beijing() - timedelta(days=REPORT_KEEP_DAYS)).isoformat()
    deleted = 0
    try:
        with db_lock:
            db_conn.execute("DELETE FROM sent_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM trend_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM channel_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM tracking_events WHERE event_time < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM early_signal_log WHERE trigger_time < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM strong_alert_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM daily_report_status WHERE report_date < ?",
                            (str(now_beijing().date() - timedelta(days=REPORT_KEEP_DAYS)),))
            db_conn.commit()
        limit_date = now_beijing().date() - timedelta(days=REPORT_KEEP_DAYS)
        for p in Path(".").glob("日报_*.xlsx"):
            try:
                if datetime.strptime(p.stem.replace("日报_", ""), "%Y%m%d").date() < limit_date:
                    p.unlink()
                    deleted += 1
            except Exception:
                continue
        logger.info(f"🗑️ 已清理{REPORT_KEEP_DAYS}天前的历史数据，删除旧日报 {deleted} 个")
    except Exception as e:
        logger.error(f"🗑️ 清理旧数据失败: {e}")

# ===================== 建仓通道 =====================
def check_stage1_decline(klines):
    if not klines or len(klines) < 30:
        return None
    closes = [k["c"] for k in klines]
    vols = [k["qv"] for k in klines]
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
    if not all(v < vol_ma20 for v in vols[-VOL_SHRINK_DAYS:]):
        return None
    change_30d = (price_now - closes[-30]) / closes[-30] * 100
    return {"change_7d": change_7d, "change_30d": change_30d,
            "dist_to_low": dist_to_low, "low_30d": low_30d, "vol_ma20": vol_ma20}

def check_stage2_surge(klines, stage1):
    if not klines or len(klines) < 10:
        return None
    for i in range(len(klines) - 5, len(klines) - 1):
        if i < 1:
            continue
        k, prev = klines[i], klines[i - 1]
        prev5 = [klines[j]["qv"] for j in range(max(0, i - 5), i)]
        if not prev5:
            continue
        avg5 = sum(prev5) / len(prev5)
        change = (k["c"] - prev["c"]) / prev["c"] * 100
        if (k["qv"] >= avg5 * VOL_SURGE_MULT and
                SURGE_CHANGE_MIN <= change <= SURGE_CHANGE_MAX and k["c"] > prev["c"]):
            return {"surge_day": i, "surge_vol_ratio": k["qv"] / avg5,
                    "surge_change": change, "surge_low": k["l"], "surge_close": k["c"]}
    return None

def check_stage3_consolidation(klines, stage2):
    if not klines:
        return None
    after = klines[stage2["surge_day"] + 1:]
    if not after:
        return None
    after = after[:CONSOLIDATION_DAYS]
    highs = [k["h"] for k in after]
    lows = [k["l"] for k in after]
    if not highs or not lows:
        return None
    avg_price = (max(highs) + min(lows)) / 2
    if avg_price == 0:
        return None
    range_pct = (max(highs) - min(lows)) / avg_price
    if range_pct > CONSOLIDATION_RANGE:
        return None
    if any(k["l"] < stage2["surge_low"] for k in after):
        return None
    lows_rising = all(after[i]["l"] >= after[i - 1]["l"] * 0.998 for i in range(1, len(after)))
    return {"consolidation_days": len(after), "range_pct": range_pct * 100,
            "lows_rising": lows_rising, "held_above_surge_low": True}

def check_oi_and_funding(symbol):
    oi_hist = get_oi_history(symbol, "1d", 5)
    funding = get_funding_rate_history(symbol, 10)
    result = {"oi_change": 0, "oi_increasing": True, "funding_avg": 0, "funding_ok": True}
    if oi_hist and len(oi_hist) >= 3:
        oi_recent = float(oi_hist[-1]['sumOpenInterest'])
        oi_before = float(oi_hist[-3]['sumOpenInterest'])
        if oi_before > 0:
            result["oi_change"] = (oi_recent - oi_before) / oi_before * 100
            result["oi_increasing"] = result["oi_change"] > OI_INCREASE_PCT
    if funding and len(funding) >= FUNDING_RATE_DAYS:
        recent = funding[-FUNDING_RATE_DAYS:]
        result["funding_avg"] = sum(recent) / len(recent)
        result["funding_ok"] = not all(f < FUNDING_RATE_MIN for f in recent)
    return result

def classify_signal(stage1, stage2, stage3, oi_data):
    if stage1 and not stage2 and not stage3:
        return "observe"
    if stage1 and stage2 and not stage3:
        return "watch"
    if stage1 and stage2 and stage3:
        return "entry" if (oi_data["oi_increasing"] and stage3["lows_rising"]) else "watch"
    return None

def format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data):
    icons = {"observe": "🟢", "watch": "🟡", "entry": "🔴"}
    labels = {"observe": "观察", "watch": "关注", "entry": "介入"}
    lines = [
        f"{icons.get(level, '⚪')} 建仓信号 [{labels.get(level, '未知')}]",
        f"币种: {sym}",
        f"时间: {now_beijing().strftime('%Y-%m-%d %H:%M')}",
        f"价格: {price:.6f}",
        get_cross_channel_history(sym, hours=24).rstrip(),
        "", "📉 下跌结构",
        f" 7天跌幅: {stage1['change_7d']:.1f}%",
        f" 30天跌幅: {stage1['change_30d']:.1f}%",
        f" 距底部: {stage1['dist_to_low']:.1f}%",
    ]
    if stage2:
        lines += ["", "📊 底部放量",
                  f" 放量倍数: {stage2['surge_vol_ratio']:.1f}x",
                  f" 放量当日涨幅: {stage2['surge_change']:.1f}%"]
    if stage3:
        lines += ["", "📐 横盘确认",
                  f" 横盘天数: {stage3['consolidation_days']}天",
                  f" 波动幅度: {stage3['range_pct']:.1f}%",
                  f" 低点抬高: {'是' if stage3['lows_rising'] else '否'}"]
    lines += ["", "📈 OI/资金费率",
              f" OI变化: {oi_data['oi_change']:+.1f}%",
              f" OI增加: {'是' if oi_data['oi_increasing'] else '否'}",
              f" 资金费率: {oi_data['funding_avg']:.4f}%"]
    return "\n".join(lines)

def scan_building_signals():
    logger.info("📐 建仓通道开始扫描")
    candidates = [sym for sym, data in ticker_cache.items()
                  if data['volume'] >= MIN_VOLUME_24H_BUILD and sym not in BUILD_BLACKLIST]
    if not candidates:
        logger.info("📐 无候选币种")
        return
    count = {"observe": 0, "watch": 0, "entry": 0}
    for sym in candidates:
        try:
            klines = get_klines_parsed(sym, "1d", 40)
            if not klines or len(klines) < 11:
                continue
            klines = klines[:-1]
            if len(klines) < 10:
                continue
            price = klines[-1]["c"]
            stage1 = check_stage1_decline(klines)
            if not stage1:
                continue
            stage2 = check_stage2_surge(klines, stage1)
            stage3 = check_stage3_consolidation(klines, stage2) if stage2 else None
            oi_data = check_oi_and_funding(sym)
            if not oi_data["funding_ok"]:
                continue
            level = classify_signal(stage1, stage2, stage3, oi_data)
            if not level:
                continue
            if level == "observe":
                with db_lock:
                    db_conn.execute("INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                                    (sym, "observe", now_beijing()))
                    db_conn.commit()
                count["observe"] += 1
                continue
            if time.time() - startup_time < SILENT_PERIOD_SEC:
                logger.info(f"⏳ 静默期，跳过建仓通知: {sym}")
                continue
            if not should_alert(sym, "LONG", channel="building", max_count=3):
                continue
            with db_lock:
                row = db_conn.execute("SELECT level FROM building_state WHERE symbol=?",
                                      (sym,)).fetchone()
                prev_level = row[0] if row else None
            if prev_level == level:
                continue
            notify_all(format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data),
                       priority=PRIO_BUILDING)
            mark_alerted(sym, "LONG", channel="building")
            record_channel(sym, "building")
            with db_lock:
                db_conn.execute("INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                                (sym, level, now_beijing()))
                db_conn.commit()
            count[level] += 1
            logger.info(f"📐 建仓推送: {sym} {level}")
        except Exception as e:
            logger.error(f"📐 {sym} 建仓检测失败: {e}")
    logger.info(f"📐 建仓扫描完成: 观察{count['observe']} 关注{count['watch']} 介入{count['entry']}")

# ===================== 主循环 =====================
def main_loop():
    global startup_time, market_state_cache
    startup_time = time.time()

    logger.info("🚀 融合监控系统启动（启动雷达 + 四通道 + 动态日报 · 强提示 + 速报汇总 + 企微）")

    startup_notice = (
        "✅ 融合监控系统启动\n"
        "🟡 通道0：启动雷达（1M + 3M）\n"
        "🚀 通道1：启动确认（3~6分钟，慢启动15分钟）\n"
        "⚡ 通道2：快速拉升（3M HH ≥ 2%，Top2 单发 + 合并）\n"
        "📈 通道3：趋势信号（1H）\n"
        "📐 通道4：建仓信号（日线结构）\n"
        "🔴 强提示：趋势3连升 / 二次速报（48h）\n"
        "📊 日报：北京时间00:05\n"
        "✉️ 邮件：每分钟最多9封，按 强提示>速报>趋势>确认>建仓 排序\n"
        "💬 企微：镜像邮件（速报/确认/趋势/强提示/建仓）"
    )
    notify_all(startup_notice, "监控系统启动")

    symbols = get_symbols()
    if not symbols or not high_volume_symbols:
        logger.error("无可用币种或无法获取成交额数据，退出")
        notify_all("❌ 系统启动失败：无法获取币种列表或成交额数据", "系统启动失败")
        return

    recover_state()

    last_refresh = time.time()
    last_market_state_scan = 0.0
    last_trend_submit = 0.0
    last_build_submit = 0.0
    last_cleanup_day = None
    market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}

    try:
        import openpyxl  # noqa: F401
    except ImportError:
        logger.error("❌ 缺少openpyxl库，日报功能将不可用。请执行: pip install openpyxl")
        send_tg("❌ 缺少openpyxl库，日报功能将不可用。")

    bg_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="bg")
    trend_future = None
    build_future = None
    next_cycle = time.monotonic()

    try:
        while True:
            try:
                maybe_send_daily_report()

                now_bj = now_beijing()
                today = now_bj.date()

                if last_cleanup_day != today and now_bj.hour >= DAILY_REPORT_HOUR:
                    if now_bj.hour > DAILY_REPORT_HOUR or now_bj.minute >= DAILY_REPORT_MINUTE:
                        cleanup_old_data()
                        last_cleanup_day = today

                for sym, sb in state_b.items():
                    if sb.get("day") != today:
                        sb["day"] = today
                        sb["daily_round"] = 0
                        sb["push_times"] = 0
                        sb["first_price"] = None
                        sb["daily_high"] = 0.0
                        sb["daily_low"] = 1e10
                        sb["source"] = None
                        if sym not in active_tracking:
                            sb["active"] = False
                            sb["push_count"] = 0

                if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                    new_syms = get_symbols()
                    if new_syms:
                        symbols = new_syms
                        last_refresh = time.time()
                        sym_set = set(symbols)
                        for s in [x for x in list(state_b.keys())
                                  if x not in sym_set and x not in pending_early
                                  and x not in active_tracking]:
                            state_b.pop(s, None)

                if time.time() - last_market_state_scan >= 120:
                    try:
                        market_state_cache = get_market_state(symbols)
                    except Exception as e:
                        logger.warning(f"市场状态更新失败: {e}")
                    last_market_state_scan = time.time()

                blocked = build_scan_blocklist()

                with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                    list(ex.map(lambda s: scan_trigger(s, blocked), symbols))

                flush_surge_queue()

                logger.info(f"🟡 启动雷达完成，观察池 {len(pending_early)}，跟踪池 {len(active_tracking)}")

                process_early_validation()

                if (time.time() - last_trend_submit >= TREND_SCAN_INTERVAL
                        and (trend_future is None or trend_future.done())):
                    trend_future = bg_executor.submit(scan_trends, list(symbols))
                    last_trend_submit = time.time()

                if (time.time() - last_build_submit >= BUILD_SCAN_INTERVAL
                        and (build_future is None or build_future.done())):
                    build_future = bg_executor.submit(scan_building_signals)
                    last_build_submit = time.time()

                if active_tracking:
                    track_active()

                next_cycle += SCAN_INTERVAL
                delay = next_cycle - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_cycle = time.monotonic()

            except KeyboardInterrupt:
                logger.info("🛑 手动退出")
                break
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                time.sleep(10)
                next_cycle = time.monotonic()
    finally:
        bg_executor.shutdown(wait=False)
        try:
            db_conn.commit()
            db_conn.close()
        except Exception:
            pass
        logger.info("👋 已退出")

if __name__ == "__main__":
    main_loop()