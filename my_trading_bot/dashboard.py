import json
import time
import threading
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import requests

app = Flask(__name__)
app.config['SECRET_KEY'] = 'trading_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# 共享数据（需要与主程序对接）
positions = {}  # {sym: {"side": "long"/"short", "entry": 100, "size": 0.01, "tp1": 105, "tp2": 110, "stop": 98}}
signals = []    # 历史信号列表
max_signals = 100

# 模拟数据存储（实际使用时需要与主程序共享）
class TradingData:
    def __init__(self):
        self.zone_states = {}
        self.cooldowns = {}
        self.blacklist = {}
        self.loss_counter = {"count": 0, "pause_until": 0}
        self.btc_cache = {"1h": []}
        self.last_signal = None
    
    def update_from_main(self, data):
        """从主程序更新数据（通过文件或直接调用）"""
        self.zone_states = data.get("zone_states", {})
        self.cooldowns = data.get("cooldowns", {})
        self.blacklist = data.get("blacklist", {})
        self.loss_counter = data.get("loss_counter", {"count": 0, "pause_until": 0})

trading_data = TradingData()

# 添加新信号
def add_signal(signal):
    signal["timestamp"] = datetime.now().strftime("%H:%M:%S")
    signal["datetime"] = datetime.now().isoformat()
    signals.insert(0, signal)
    if len(signals) > max_signals:
        signals.pop()
    # 通过WebSocket推送
    socketio.emit('new_signal', signal)
    return signal

# 更新币种状态
def update_symbol_status(symbol, status):
    socketio.emit('symbol_update', {"symbol": symbol, "status": status})

# 更新风控状态
def update_risk_status():
    socketio.emit('risk_update', {
        "loss_count": trading_data.loss_counter.get("count", 0),
        "pause_until": trading_data.loss_counter.get("pause_until", 0),
        "cooldowns": len(trading_data.cooldowns),
        "blacklist": len(trading_data.blacklist)
    })

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/signals')
def get_signals():
    return jsonify(signals)

@app.route('/api/positions')
def get_positions():
    return jsonify(positions)

@app.route('/api/status')
def get_status():
    now = time.time()
    # 计算活跃zone
    active_zones = sum(1 for z in trading_data.zone_states.values() 
                      if hasattr(z, 'in_zone') and z.in_zone)
    
    # 获取BTC价格
    btc_price = trading_data.btc_cache[-1]["c"] if trading_data.btc_cache else 0
    
    return jsonify({
        "active_zones": active_zones,
        "cooldowns": len(trading_data.cooldowns),
        "blacklist": len(trading_data.blacklist),
        "loss_count": trading_data.loss_counter.get("count", 0),
        "pause_until": trading_data.loss_counter.get("pause_until", 0),
        "btc_price": btc_price,
        "positions": len(positions),
        "signals_today": len([s for s in signals if s["timestamp"].startswith(datetime.now().strftime("%H"))]),
    })

@app.route('/api/positions/add', methods=['POST'])
def add_position():
    data = request.json
    sym = data.get("symbol")
    if sym not in positions:
        positions[sym] = []
    positions[sym].append({
        "side": data.get("side"),
        "entry": float(data.get("entry")),
        "size": float(data.get("size")),
        "stop": float(data.get("stop")),
        "tp1": float(data.get("tp1")),
        "tp2": float(data.get("tp2")),
        "open_time": datetime.now().isoformat(),
    })
    socketio.emit('position_update', {"symbol": sym, "positions": positions[sym]})
    return jsonify({"status": "ok"})

@app.route('/api/positions/close', methods=['POST'])
def close_position():
    data = request.json
    sym = data.get("symbol")
    idx = data.get("idx")
    exit_price = float(data.get("exit_price"))
    
    if sym in positions and idx < len(positions[sym]):
        pos = positions[sym].pop(idx)
        # 计算盈亏
        if pos["side"] == "long":
            pnl = (exit_price - pos["entry"]) * pos["size"]
        else:
            pnl = (pos["entry"] - exit_price) * pos["size"]
        
        socketio.emit('position_closed', {
            "symbol": sym, 
            "pnl": pnl,
            "pnl_pct": pnl / (pos["entry"] * pos["size"]) * 100
        })
        socketio.emit('position_update', {"symbol": sym, "positions": positions.get(sym, [])})
    
    return jsonify({"status": "ok"})

# 主程序集成 - 修改原程序的notify函数
def notify_with_dashboard(msg):
    """替换原notify函数，同时推送到面板"""
    print(msg)
    send_tg(msg)
    send_email(msg)
    
    # 解析信号并推送到面板
    if "信号" in msg and ("做多" in msg or "做空" in msg):
        # 简单解析，实际可以更精细
        signal = {
            "raw": msg,
            "type": "long" if "做多" in msg else "short",
        }
        # 提取币种
        for line in msg.split("\n"):
            if "币种:" in line:
                signal["symbol"] = line.split(":")[1].strip()
            elif "入场:" in line:
                signal["entry"] = float(line.split(":")[1].strip().split()[0])
        add_signal(signal)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)