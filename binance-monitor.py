#!/usr/bin/env python3
"""
跨所套利监控面板 (Deribit + Binance)
======================================
实时监控跨所套利机器人运行状态、双交易所持仓、利润、日志。

启动方式:
    python monitor.py
    python monitor.py --port 8080
    python monitor.py --currency ETH

浏览器访问: http://localhost:5556
"""

import os, sys, csv, json, time, hashlib, hmac, re
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import URLError
import argparse

# ======================== 配置 ========================
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))
import config

CURRENCY = config.BASE_CONFIG.get("target_currency", "BTC")
IS_TESTNET = config.BASE_CONFIG.get("test_trading", True)
API_BASE = "https://test.deribit.com" if IS_TESTNET else "https://www.deribit.com"

COIN_CONFIG = config.BTC_CONFIG if CURRENCY == "BTC" else config.ETH_CONFIG
CLIENT_ID = COIN_CONFIG["CLIENT_ID"]
CLIENT_SECRET = COIN_CONFIG["CLIENT_SECRET"]

LOG_FILE = BASE_DIR / f"{CURRENCY}-log.txt"
_ENV_SUFFIX = "testnet" if IS_TESTNET else "main"
_TRADE_DB_PATH = BASE_DIR / f"trading_{CURRENCY}_{_ENV_SUFFIX}.db"

# ======================== Binance REST API ========================
BN_API_KEY = config.BINANCE_CONFIG.get("API_KEY", "")
BN_API_SECRET = config.BINANCE_CONFIG.get("API_SECRET", "")
BN_USE_TESTNET = config.BINANCE_CONFIG.get("use_testnet", True)
BN_API_BASE = "https://demo-fapi.binance.com" if BN_USE_TESTNET else "https://fapi.binance.com"


def _binance_signed_request(endpoint, params=None):
    """发送 Binance USDT-M 期货签名请求"""
    if params is None:
        params = {}
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params)
    sig = hmac.new(BN_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BN_API_BASE}{endpoint}?{query}&signature={sig}"
    req = Request(url, headers={"X-MBX-APIKEY": BN_API_KEY})
    return json.loads(urlopen(req, timeout=5).read())


# ======================== Flask ========================
try:
    from flask import Flask, jsonify, request as flask_request, Response, session, redirect, url_for, render_template_string
except ImportError:
    print("请先安装 Flask:  pip install flask")
    sys.exit(1)

import secrets as _secrets
from functools import wraps as _wraps

app = Flask(__name__)

# ======================== 认证系统 (admin-only, 默认密码 123456) ========================
# 凭证文件: .monitor_auth.json (已加入 .gitignore)
# 首次运行自动生成: username=admin, password=123456 (建议登录后立即修改)
# 登录后 session 有效期 12 小时

AUTH_FILE = BASE_DIR / ".monitor_auth.json"
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "123456"
SESSION_HOURS = 12
MAX_LOGIN_ATTEMPTS = 5          # 连续失败上限
LOGIN_LOCKOUT_SECONDS = 300     # 锁定 5 分钟

# 内存登录尝试追踪 (IP → [fail_count, lockout_until_ts])
_login_attempts = defaultdict(lambda: [0, 0.0])


def _hash_password(password: str, salt: str) -> str:
    """PBKDF2-HMAC-SHA256, 100k 轮"""
    return hashlib.pbkdf2_hmac(
        'sha256', password.encode('utf-8'), salt.encode('utf-8'), 100_000
    ).hex()


def _load_auth() -> dict:
    """加载凭证, 首次运行时自动创建默认 admin/123456"""
    if AUTH_FILE.exists():
        try:
            with open(AUTH_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 字段校验
            if all(k in data for k in ('username', 'password_hash', 'salt', 'session_secret')):
                return data
            print(f"⚠️ {AUTH_FILE} 字段不完整, 重建默认凭证")
        except Exception as e:
            print(f"⚠️ 读取 {AUTH_FILE} 失败: {e}, 重建默认凭证")
    # 首次创建
    salt = _secrets.token_hex(16)
    data = {
        'username': DEFAULT_USERNAME,
        'password_hash': _hash_password(DEFAULT_PASSWORD, salt),
        'salt': salt,
        'session_secret': _secrets.token_hex(32),
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'password_changed_at': None,
        'is_default_password': True,
    }
    _save_auth(data)
    print(f"✅ 已创建默认凭证 → 用户名: {DEFAULT_USERNAME} 密码: {DEFAULT_PASSWORD}")
    print(f"   ⚠️ 强烈建议登录后立即通过 /change_password 修改默认密码")
    return data


def _save_auth(data: dict) -> None:
    try:
        with open(AUTH_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        try:
            os.chmod(AUTH_FILE, 0o600)   # 仅 owner 读写
        except Exception:
            pass
    except Exception as e:
        print(f"❌ 写入 {AUTH_FILE} 失败: {e}")


def _verify_password(username: str, password: str) -> bool:
    auth = _load_auth()
    if username != auth['username']:
        return False
    return hmac.compare_digest(
        _hash_password(password, auth['salt']),
        auth['password_hash']
    )


def _update_password(new_password: str) -> None:
    """修改密码, 重新生成 salt 防止 rainbow table"""
    auth = _load_auth()
    new_salt = _secrets.token_hex(16)
    auth['salt'] = new_salt
    auth['password_hash'] = _hash_password(new_password, new_salt)
    auth['password_changed_at'] = datetime.now().isoformat(timespec='seconds')
    auth['is_default_password'] = (new_password == DEFAULT_PASSWORD)
    _save_auth(auth)


# 启动时加载并设置 Flask session secret
_auth_bootstrap = _load_auth()
app.secret_key = _auth_bootstrap['session_secret']
app.permanent_session_lifetime = 60 * 60 * SESSION_HOURS


# ======================== 登录态装饰器 ========================
def login_required(f):
    """保护页面 / API; 未登录时:
       - API (/api/*): 返回 401 JSON
       - 其他 (页面): 302 跳到 /login
    """
    @_wraps(f)
    def wrapped(*args, **kwargs):
        if session.get('logged_in') is True and session.get('user') == DEFAULT_USERNAME:
            return f(*args, **kwargs)
        if flask_request.path.startswith('/api/'):
            return jsonify({'error': 'unauthorized', 'login_url': '/login'}), 401
        return redirect(url_for('login', next=flask_request.path))
    return wrapped


def _client_ip() -> str:
    # 如果未来放 nginx 反代, X-Forwarded-For 在这里纳入
    return flask_request.headers.get('X-Forwarded-For', flask_request.remote_addr or '?').split(',')[0].strip()


# ======================== 登录 / 登出 / 改密码 HTML ========================
LOGIN_HTML = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>监控面板登录</title>
<style>
  *{box-sizing:border-box}
  html,body{margin:0;padding:0}
  body{font-family:-apple-system,Arial;background:#1a1d21;color:#e6e6e6;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px;-webkit-text-size-adjust:100%}
  .box{background:#24272c;border:1px solid #343840;border-radius:8px;padding:32px 40px;width:100%;max-width:380px;box-shadow:0 8px 32px rgba(0,0,0,0.3)}
  h1{margin:0 0 20px;font-size:20px}
  .row{margin:12px 0}
  label{display:block;font-size:13px;color:#b4b8bf;margin-bottom:4px}
  input{width:100%;padding:10px 12px;background:#1a1d21;border:1px solid #343840;color:#e6e6e6;border-radius:4px;font-size:16px}
  input:focus{outline:none;border-color:#4a9eff}
  button{width:100%;padding:12px;margin-top:16px;background:#4a9eff;color:#fff;border:none;border-radius:4px;font-size:15px;cursor:pointer;-webkit-appearance:none}
  button:hover{background:#3a8eef}
  button:active{opacity:0.85}
  .err{color:#ff6b6b;font-size:13px;margin-top:12px;min-height:18px}
  .warn{background:#3a2f1a;border:1px solid #5a4a2a;color:#ffcc66;font-size:12px;padding:8px 12px;border-radius:4px;margin-top:12px}
  /* 手机端 (宽度 ≤ 480px) */
  @media (max-width: 480px) {
    .box{padding:24px 20px;border-radius:6px}
    h1{font-size:18px;margin-bottom:16px}
    input{font-size:16px;padding:12px}  /* 16px 防 iOS 自动缩放 */
    button{padding:14px;font-size:16px}
    label{font-size:14px}
  }
</style></head>
<body><div class="box">
  <h1>🔐 监控面板</h1>
  <form method="POST" action="/login">
    <input type="hidden" name="next" value="{{ next_url }}">
    <div class="row"><label>用户名</label><input type="text" name="username" required autocomplete="username" autocapitalize="off" autocorrect="off" spellcheck="false"></div>
    <div class="row"><label>密码</label><input type="password" name="password" required autofocus autocomplete="current-password"></div>
    <button type="submit">登录</button>
    {% if error %}<div class="err">{{ error }}</div>{% endif %}
    {% if is_default %}<div class="warn">⚠️ 当前为默认密码 (123456), 登录后请立即修改</div>{% endif %}
  </form>
</div></body></html>"""


CHANGE_PW_HTML = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>修改密码</title>
<style>
  body{font-family:-apple-system,Arial;background:#1a1d21;color:#e6e6e6;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
  .box{background:#24272c;border:1px solid #343840;border-radius:8px;padding:32px 40px;min-width:360px}
  h1{margin:0 0 20px;font-size:18px}
  .row{margin:12px 0}
  label{display:block;font-size:13px;color:#b4b8bf;margin-bottom:4px}
  input{width:100%;box-sizing:border-box;padding:8px 12px;background:#1a1d21;border:1px solid #343840;color:#e6e6e6;border-radius:4px;font-size:14px}
  input:focus{outline:none;border-color:#4a9eff}
  button{padding:10px 20px;background:#4a9eff;color:#fff;border:none;border-radius:4px;font-size:14px;cursor:pointer;margin-top:16px}
  button:hover{background:#3a8eef}
  .back{display:inline-block;margin-left:12px;color:#8b8f98;text-decoration:none;font-size:13px;line-height:38px}
  .back:hover{color:#4a9eff}
  .msg{margin-top:12px;font-size:13px;min-height:18px}
  .err{color:#ff6b6b}
  .ok{color:#66cc66}
  .hint{color:#8b8f98;font-size:12px;margin-top:8px}
</style></head>
<body><div class="box">
  <h1>🔑 修改密码 (admin)</h1>
  <form method="POST" action="/change_password">
    <div class="row"><label>当前密码</label><input type="password" name="current" required autofocus></div>
    <div class="row"><label>新密码 (至少 6 位)</label><input type="password" name="new" required minlength="6"></div>
    <div class="row"><label>再次确认</label><input type="password" name="confirm" required minlength="6"></div>
    <button type="submit">保存新密码</button>
    <a class="back" href="/">← 返回面板</a>
    {% if error %}<div class="msg err">{{ error }}</div>{% endif %}
    {% if success %}<div class="msg ok">{{ success }}</div>{% endif %}
  </form>
  <div class="hint">修改后当前 session 保持有效, 下次登录使用新密码。</div>
</div></body></html>"""


# ======================== 登录 / 登出 / 改密码 路由 ========================
@app.route('/login', methods=['GET', 'POST'])
def login():
    _ip = _client_ip()
    _att = _login_attempts[_ip]
    _now = time.time()

    # 锁定检查
    if _att[1] > _now:
        _remain = int(_att[1] - _now)
        return render_template_string(
            LOGIN_HTML, next_url='/',
            error=f"尝试次数过多, 请 {_remain} 秒后再试",
            is_default=_load_auth().get('is_default_password', False)
        ), 429

    if flask_request.method == 'POST':
        _username = flask_request.form.get('username', '').strip()
        _password = flask_request.form.get('password', '')
        _next = flask_request.form.get('next', '/') or '/'
        # 防 open redirect: next 只允许同源路径
        if not _next.startswith('/') or _next.startswith('//'):
            _next = '/'
        if _verify_password(_username, _password):
            session.permanent = True
            session['logged_in'] = True
            session['user'] = _username
            session['login_ts'] = _now
            _login_attempts.pop(_ip, None)   # 成功清零
            print(f"[Auth] ✅ 登录成功 user={_username} ip={_ip}")
            return redirect(_next)
        # 失败计数
        _att[0] += 1
        if _att[0] >= MAX_LOGIN_ATTEMPTS:
            _att[1] = _now + LOGIN_LOCKOUT_SECONDS
            print(f"[Auth] 🚫 IP {_ip} 连续失败 {_att[0]} 次, 锁定 {LOGIN_LOCKOUT_SECONDS}s")
            _err_msg = f"尝试次数过多, 已锁定 {LOGIN_LOCKOUT_SECONDS // 60} 分钟"
        else:
            _err_msg = f"用户名或密码错误 (剩余尝试 {MAX_LOGIN_ATTEMPTS - _att[0]} 次)"
        print(f"[Auth] ❌ 登录失败 user={_username} ip={_ip} count={_att[0]}")
        return render_template_string(
            LOGIN_HTML, next_url=_next, error=_err_msg,
            is_default=_load_auth().get('is_default_password', False)
        ), 401

    # GET
    _next = flask_request.args.get('next', '/') or '/'
    if not _next.startswith('/') or _next.startswith('//'):
        _next = '/'
    # 已登录直接跳转
    if session.get('logged_in'):
        return redirect(_next)
    return render_template_string(
        LOGIN_HTML, next_url=_next, error=None,
        is_default=_load_auth().get('is_default_password', False)
    )


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if flask_request.method == 'POST':
        _cur = flask_request.form.get('current', '')
        _new = flask_request.form.get('new', '')
        _confirm = flask_request.form.get('confirm', '')
        if not _verify_password(DEFAULT_USERNAME, _cur):
            return render_template_string(CHANGE_PW_HTML, error="当前密码错误", success=None), 401
        if _new != _confirm:
            return render_template_string(CHANGE_PW_HTML, error="两次输入的新密码不一致", success=None), 400
        if len(_new) < 6:
            return render_template_string(CHANGE_PW_HTML, error="新密码至少 6 位", success=None), 400
        if _new == _cur:
            return render_template_string(CHANGE_PW_HTML, error="新密码不能与当前密码相同", success=None), 400
        _update_password(_new)
        print(f"[Auth] 🔑 密码已修改 ip={_client_ip()}")
        return render_template_string(CHANGE_PW_HTML, error=None,
                                       success="✅ 密码已修改, 下次登录请使用新密码"), 200
    return render_template_string(CHANGE_PW_HTML, error=None, success=None)

# ======================== Deribit REST API ========================
_access_token = None
_token_expiry = 0


def _deribit_request(method, params=None, auth=False):
    """发送 Deribit REST API 请求"""
    global _access_token, _token_expiry

    if auth and (not _access_token or time.time() > _token_expiry - 60):
        auth_params = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        url = f"{API_BASE}/api/v2/public/auth?{urlencode(auth_params)}"
        req = Request(url, headers={"Content-Type": "application/json"})
        resp = json.loads(urlopen(req, timeout=5).read())
        if "result" in resp:
            _access_token = resp["result"]["access_token"]
            _token_expiry = time.time() + resp["result"].get("expires_in", 900)

    url = f"{API_BASE}/api/v2/{method}"
    if params:
        url += "?" + urlencode(params)
    headers = {"Content-Type": "application/json"}
    if auth and _access_token:
        headers["Authorization"] = f"Bearer {_access_token}"
    req = Request(url, headers=headers)
    resp = json.loads(urlopen(req, timeout=5).read())
    return resp.get("result", resp)


# ======================== API 端点 ========================

@app.route("/")
@login_required
def index():
    return HTML_TEMPLATE


_pos_cache = {"data": None, "ts": 0}

def _fetch_positions_snapshot():
    """抓取 Deribit + Binance 持仓快照（不做缓存）"""
    deribit_positions = []
    binance_positions = []
    order_result = []
    errors = []

    # ---- Deribit 持仓 + 挂单 ----
    try:
        positions = _deribit_request("private/get_positions",
                                     {"currency": CURRENCY}, auth=True)
        for p in positions:
            if p.get("size", 0) == 0:
                continue
            deribit_positions.append({
                "instrument": p.get("instrument_name", ""),
                "direction": p.get("direction", ""),
                "size": p.get("size", 0),
                "avg_price": p.get("average_price", 0),
                "mark_price": p.get("mark_price", 0),
                "index_price": p.get("index_price", 0),
                "pnl": p.get("total_profit_loss", 0),
                "floating_pnl": p.get("floating_profit_loss", 0),
                "kind": p.get("kind", ""),
            })

        orders = _deribit_request("private/get_open_orders_by_currency",
                                  {"currency": CURRENCY}, auth=True)
        for o in orders:
            order_result.append({
                "instrument": o.get("instrument_name", ""),
                "direction": o.get("direction", ""),
                "amount": o.get("amount", 0),
                "filled_amount": o.get("filled_amount", 0),
                "price": o.get("price", 0),
                "mark_price": o.get("mark_price", 0) if "mark_price" in o else None,
                "order_state": o.get("order_state", ""),
                "order_type": o.get("order_type", ""),
                "label": o.get("label", ""),
                "time_in_force": o.get("time_in_force", ""),
                "creation_timestamp": o.get("creation_timestamp", 0),
            })
    except Exception as e:
        errors.append(f"Deribit: {e}")

    # ---- Binance 期货持仓 ----
    try:
        bn_positions = _binance_signed_request("/fapi/v2/positionRisk")
        for p in bn_positions:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            pos_side = str(p.get("positionSide", "BOTH")).upper()
            if pos_side in ("LONG", "SHORT"):
                direction = pos_side
            else:
                direction = "LONG" if amt > 0 else "SHORT"
            binance_positions.append({
                "symbol": p.get("symbol", ""),
                "direction": direction,
                "position_side": pos_side,
                "amount": abs(amt),
                "entry_price": float(p.get("entryPrice", 0)),
                "mark_price": float(p.get("markPrice", 0)),
                "unrealized_pnl": float(p.get("unRealizedProfit", 0)),
                "leverage": p.get("leverage", ""),
                "margin_type": p.get("marginType", ""),
            })
    except Exception as e:
        errors.append(f"Binance: {e}")

    return {
        "ok": True,
        "deribit_positions": deribit_positions,
        "binance_positions": binance_positions,
        "orders": order_result,
        "currency": CURRENCY,
        "errors": errors,
    }

def _get_positions_snapshot(max_age_sec=5):
    """带缓存读取实时持仓快照"""
    if _pos_cache["data"] and time.time() - _pos_cache["ts"] < max_age_sec:
        return _pos_cache["data"]
    result = _fetch_positions_snapshot()
    _pos_cache.update({"data": result, "ts": time.time()})
    return result


def _to_float(value, default=0.0):
    """安全转换为 float"""
    try:
        if value is None:
            return default
        if isinstance(value, str) and value.strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _to_float_opt(value):
    """安全转换为 float（失败返回 None）"""
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _fmt_strike_display(value):
    """格式化行权价用于界面展示，避免 SQLite REAL 显示成 77500.0。"""
    try:
        f = float(value)
        if f.is_integer():
            return str(int(f))
        return f"{f:g}"
    except Exception:
        return str(value).replace(".0", "")


def _parse_trade_time(ts_str):
    """解析成交时间字符串（本地时间）"""
    try:
        if not ts_str:
            return None
        return datetime.strptime(str(ts_str), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _estimate_open_fee_from_trade(open_trade):
    """优先使用CSV结构化字段估算开仓费；缺失时按成交价重算"""
    if not open_trade:
        return 0.0

    structured = _to_float_opt(open_trade.get("开仓手续费(USD)"))
    if structured is not None:
        return max(structured, 0.0)

    amt = _to_float(open_trade.get("期权数量", 0), 0.0)
    entry_f = _to_float(open_trade.get("实际_Future均价", 0), 0.0)
    c_entry = _to_float(open_trade.get("实际_Call均价", 0), 0.0)
    p_entry = _to_float(open_trade.get("实际_Put均价", 0), 0.0)
    if amt <= 0 or entry_f <= 0:
        return 0.0

    # Deribit 期权 taker fee: min(amount*0.03%, amount*premium*12.5%)
    c_fee_btc = min(amt * 0.0003, amt * max(c_entry, 0) * 0.125)
    p_fee_btc = min(amt * 0.0003, amt * max(p_entry, 0) * 0.125)
    deribit_open_fee = (c_fee_btc + p_fee_btc) * entry_f
    binance_open_fee = amt * entry_f * 0.0004
    return max(deribit_open_fee + binance_open_fee, 0.0)


def _estimate_close_fee_from_trade(open_trade, ref_price):
    """优先使用CSV结构化字段估算平仓/结算费；缺失时按当前参考价估算"""
    if not open_trade:
        return 0.0

    structured = _to_float_opt(open_trade.get("预估结算手续费(USD)"))
    if structured is not None:
        return max(structured, 0.0)

    amt = _to_float(open_trade.get("期权数量", 0), 0.0)
    if amt <= 0 or ref_price <= 0:
        return 0.0

    # 估算口径：Deribit 交割费(2腿×0.015%) + Binance 平仓 taker(0.04%)
    return max(amt * ref_price * (0.00015 * 2 + 0.0004), 0.0)


def _extract_realized_fee_from_close_trade(close_trade):
    """提取已平仓记录的纯手续费（不含 funding 收支）。"""
    if not close_trade:
        return 0.0

    open_fee = _to_float_opt(close_trade.get("开仓手续费(USD)"))
    settle_fee = _to_float_opt(close_trade.get("预估结算手续费(USD)"))
    if open_fee is not None or settle_fee is not None:
        return max(open_fee or 0.0, 0.0) + max(settle_fee or 0.0, 0.0)

    # 兼容旧CSV：实际_手续费 可能已把 funding 抵扣进去，需还原纯手续费
    mixed_fee = _to_float(close_trade.get("实际_手续费(USD)", 0), 0.0)
    funding = _to_float(close_trade.get("已实现funding(USD)", 0), 0.0)
    return max(mixed_fee + funding, 0.0)


def _estimate_open_combo_live_mtm(open_combo_meta):
    """估算持仓中组合实时MTM，并返回参考价格映射
    与引擎 _check_exit_opportunity 的 combo_pnl_usd 同口径:
      期权PnL = (entry - mark) × amount (BTC本位) × index_price → USD
      期货PnL = (mark - entry) × amount (USD, Binance 线性)
    """
    if not open_combo_meta:
        return {}, {}
    pos_snapshot = _get_positions_snapshot(max_age_sec=8)
    if not pos_snapshot or not pos_snapshot.get("ok"):
        return {}, {}

    # 构建 Deribit 期权 mark_price + index_price 映射 (按 instrument 索引)
    deribit_option_marks = {}
    for p in pos_snapshot.get("deribit_positions", []):
        inst = p.get("instrument", "")
        if not inst:
            continue
        try:
            deribit_option_marks[inst] = {
                "mark_price": float(p.get("mark_price", 0) or 0),
                "index_price": float(p.get("index_price", 0) or 0),
            }
        except Exception:
            continue

    live_map = {}
    ref_price_map = {}

    # Binance 参考价格按方向聚合
    long_mark_num = 0.0
    long_mark_den = 0.0
    short_mark_num = 0.0
    short_mark_den = 0.0
    all_mark_num = 0.0
    all_mark_den = 0.0
    for p in pos_snapshot.get("binance_positions", []):
        try:
            mark = float(p.get("mark_price", 0) or 0)
            amt = float(p.get("amount", 0) or 0)
        except Exception:
            continue
        if mark <= 0 or amt <= 0:
            continue
        all_mark_num += mark * amt
        all_mark_den += amt
        direction = str(p.get("direction", "")).upper()
        if direction == "LONG":
            long_mark_num += mark * amt
            long_mark_den += amt
        elif direction == "SHORT":
            short_mark_num += mark * amt
            short_mark_den += amt

    long_mark = (long_mark_num / long_mark_den) if long_mark_den > 0 else 0.0
    short_mark = (short_mark_num / short_mark_den) if short_mark_den > 0 else 0.0
    all_mark = (all_mark_num / all_mark_den) if all_mark_den > 0 else 0.0

    for meta in open_combo_meta:
        key = meta.get("combo", "")
        if not key:
            continue

        try:
            amt = float(meta.get("amount", 0) or 0)
        except Exception:
            amt = 0.0
        try:
            entry_ref = float(meta.get("entry_future", 0) or 0)
        except Exception:
            entry_ref = 0.0
        entry_call = float(meta.get("entry_call", 0) or 0)
        entry_put = float(meta.get("entry_put", 0) or 0)
        strategy = str(meta.get("strategy", ""))
        option_names = meta.get("option_names", [])

        # --- 期权 PnL: 与引擎同口径 (entry vs mark, BTC 本位 → USD) ---
        opt_pnl = 0.0
        _idx_price = 0.0
        c_name = option_names[0] if len(option_names) > 0 else ""
        p_name = option_names[1] if len(option_names) > 1 else ""
        c_info = deribit_option_marks.get(c_name, {})
        p_info = deribit_option_marks.get(p_name, {})
        # 降级链与引擎一致: API mark_price → entry_price (假设期权无变动)
        c_mark = c_info.get("mark_price", 0.0) or entry_call
        p_mark = p_info.get("mark_price", 0.0) or entry_put
        _idx_price = c_info.get("index_price", 0.0) or p_info.get("index_price", 0.0)

        if amt > 0 and entry_call > 0 and entry_put > 0 and _idx_price > 0:
            if "buy_future_sell_synthetic" in strategy:
                # 卖Call + 买Put
                c_pnl_btc = (entry_call - c_mark) * amt
                p_pnl_btc = (p_mark - entry_put) * amt
            else:
                # 买Call + 卖Put
                c_pnl_btc = (c_mark - entry_call) * amt
                p_pnl_btc = (entry_put - p_mark) * amt
            opt_pnl = (c_pnl_btc + p_pnl_btc) * _idx_price

        # --- 期货 PnL: Binance 线性合约 (mark - entry) × qty ---
        mark_ref = all_mark if all_mark > 0 else entry_ref
        bn_mtm = 0.0
        if "buy_future_sell_synthetic" in strategy:
            mark_ref = long_mark if long_mark > 0 else mark_ref
            if amt > 0 and entry_ref > 0 and mark_ref > 0:
                bn_mtm = (mark_ref - entry_ref) * amt
        elif "sell_future_buy_synthetic" in strategy:
            mark_ref = short_mark if short_mark > 0 else mark_ref
            if amt > 0 and entry_ref > 0 and mark_ref > 0:
                bn_mtm = (entry_ref - mark_ref) * amt

        live_map[key] = opt_pnl + bn_mtm
        ref_price_map[key] = mark_ref if mark_ref > 0 else entry_ref

    return live_map, ref_price_map

@app.route("/api/positions")
@login_required
def api_positions():
    """从 Deribit + Binance 获取实时持仓 + Deribit 活跃挂单（5 秒缓存）"""
    return jsonify(_get_positions_snapshot(max_age_sec=5))


@app.route("/api/trades")
@login_required
def api_trades():
    """从 SQLite 读取本地成交记录"""
    try:
        import db_store as _ds
        _store = _ds.TradeStore(str(_TRADE_DB_PATH))
        _store._ensure_schema_sync()
        trades = _store.query_all_sync(limit=5000)
        if not trades:
            return jsonify({"ok": True, "trades": [], "summary": {}})

        total_sim_profit = sum(_to_float(t.get("模拟_净利润(USD)", 0)) for t in trades)
        # 手续费口径说明：
        # - total_fees_usd: 仅统计已平仓/已结算轮次的实收费用（避免与持仓估算混合）
        # - open_fee_est_usd: 持仓中的可平仓费用估算（开+平）
        total_fees_realized = 0.0
        total_slippage = sum(_to_float(t.get("滑点与偏差损失(USD)", 0)) for t in trades)

        groups = defaultdict(list)
        for t in trades:
            key = f"{t.get('到期日', '')}-{_fmt_strike_display(t.get('行权价', ''))}"
            groups[key].append(t)

        close_types = ("平仓", "紧急强平", "交割结算", "紧急清仓", "紧急清仓(部分)")
        combo_profits = []
        open_combo_meta = []
        open_combo_costs = {}
        annual_profit_sum = 0.0
        annual_capital_hours_sum = 0.0
        for key, group in groups.items():
            # 按轮次拆分：每遇到平仓/紧急强平记录就结束一轮
            rounds = []
            current_round = []
            for t in group:
                tt = t.get("交易类型", "开仓")
                current_round.append(t)
                if tt in close_types:
                    rounds.append(current_round)
                    current_round = []
            if current_round:
                rounds.append(current_round)

            for rd_idx, rd in enumerate(rounds):
                trade_types = [t.get("交易类型", "开仓") for t in rd]
                has_close = any(tt in close_types for tt in trade_types)
                status = "已平仓" if has_close else "持仓中"
                funding_realized_rd = sum(_to_float(t.get("已实现funding(USD)", 0), 0.0) for t in rd)
                # 有平仓记录时用平仓的P&L（真实往返净利），否则用开仓预估
                if has_close:
                    close_trades = [t for t in rd if t.get("交易类型", "") in close_types]
                    profit = sum(_to_float(t.get("实际_净利润(USD)", 0)) for t in close_trades)
                    total_fees_realized += sum(_extract_realized_fee_from_close_trade(t) for t in close_trades)
                    pnl_source = "平仓实绩"
                else:
                    profit = sum(_to_float(t.get("实际_净利润(USD)", 0)) for t in rd)
                    if profit == 0.0:
                        profit = sum(
                            _to_float(t.get("模拟_净利润(USD)", 0)) - _to_float(t.get("滑点与偏差损失(USD)", 0))
                            for t in rd)
                    pnl_source = "开仓估算"

                # 计算每轮已平仓组合的年化收益率（收益/资金占用 × 年化系数）
                if has_close:
                    open_trade_for_annual = next((t for t in rd if t.get("交易类型", "") == "开仓"), rd[0] if rd else {})
                    close_trade_for_annual = next((t for t in reversed(rd) if t.get("交易类型", "") in close_types), rd[-1] if rd else {})
                    annual_profit = _to_float(profit, 0.0)
                    annual_capital = _to_float(open_trade_for_annual.get("期货面值(USD)", 0), 0.0)
                    if annual_capital <= 0:
                        _entry_ref = _to_float(open_trade_for_annual.get("实际_Future均价", 0), 0.0)
                        _amt_ref = _to_float(open_trade_for_annual.get("期权数量", 0), 0.0)
                        annual_capital = _entry_ref * _amt_ref
                    if annual_capital <= 0:
                        annual_capital = _to_float(close_trade_for_annual.get("期货面值(USD)", 0), 0.0)

                    _t_open = _parse_trade_time(open_trade_for_annual.get("成交时间", ""))
                    # 🌟 2026-04-24: 优先用"实际对冲关闭时间"(TWAP 提前平仓场景) 替代"成交时间"(=delivery 写入时)
                    # 让年化率不受 TWAP 分片偏移影响 (可少算 1-3h hold_hours, 对短仓影响尤大)
                    _hedge_close_str = (close_trade_for_annual.get("实际对冲关闭时间", "") or "").strip()
                    _t_close = _parse_trade_time(_hedge_close_str) if _hedge_close_str else None
                    if not _t_close:
                        _t_close = _parse_trade_time(close_trade_for_annual.get("成交时间", ""))
                    if annual_capital > 0 and _t_open and _t_close and _t_close > _t_open:
                        hold_hours = (_t_close - _t_open).total_seconds() / 3600.0
                        hold_hours = max(hold_hours, 1.0 / 60.0)  # 最低按1分钟防止极端值炸裂
                        annual_profit_sum += annual_profit
                        annual_capital_hours_sum += annual_capital * hold_hours
                # 多轮时加序号区分，如 "29MAY26-76000 #2"
                display_name = key if len(rounds) == 1 else f"{key} #{rd_idx + 1}"
                # 计算距到期剩余时间（到期日格式如 "27MAR26"）
                days_left = None
                hours_left = None
                expiry_str = key.split("-")[0]  # e.g. "27MAR26"
                try:
                    expiry_dt = datetime.strptime(expiry_str, "%d%b%y")
                    # Deribit 交割时间为到期日 08:00 UTC
                    expiry_dt = expiry_dt.replace(hour=8, tzinfo=timezone.utc)
                    remaining_sec = (expiry_dt - datetime.now(timezone.utc)).total_seconds()
                    days_left = round(remaining_sec / 86400, 1)
                    hours_left = round(remaining_sec / 3600, 1)
                except Exception:
                    pass
                # 构建 Deribit 期权合约名 (如 BTC-10APR26-74000-C / -P)
                strike = rd[0].get("行权价", "")
                strategy = rd[0].get("策略方向", "")
                option_names = []
                try:
                    s = _fmt_strike_display(strike)
                    call_name = f"{CURRENCY}-{expiry_str.upper()}-{s}-C"
                    put_name = f"{CURRENCY}-{expiry_str.upper()}-{s}-P"
                    option_names = [call_name, put_name]
                except Exception:
                    pass
                combo_profits.append({
                    "combo": display_name,
                    "option_names": option_names,
                    "strategy": strategy,
                    "trades": len(rd),
                    "amount": _to_float(rd[0].get("期权数量", 0)),
                    "entry_future_price": _to_float(rd[0].get("实际_Future均价", 0)),
                    "profit_usd": round(profit, 4),
                    "entry_profit_usd": round(profit, 4) if not has_close else None,
                    "time": rd[-1].get("成交时间", ""),
                    "status": status,
                    "days_left": days_left,
                    "hours_left": hours_left,
                    "pnl_source": pnl_source,
                    "funding_realized_usd": round(funding_realized_rd, 4),
                })
                if not has_close:
                    open_trade = next((t for t in rd if t.get("交易类型", "") == "开仓"), rd[0] if rd else {})
                    open_combo_costs[display_name] = {
                        "open_trade": open_trade,
                        "funding_realized_usd": funding_realized_rd,
                    }
                    open_combo_meta.append({
                        "combo": display_name,
                        "strategy": strategy,
                        "amount": _to_float(rd[0].get("期权数量", 0)),
                        "entry_future": _to_float(rd[0].get("实际_Future均价", 0)),
                        "entry_call": _to_float(rd[0].get("实际_Call均价", 0)),
                        "entry_put": _to_float(rd[0].get("实际_Put均价", 0)),
                        "option_names": option_names,
                    })

        live_mtm_map, live_ref_map = _estimate_open_combo_live_mtm(open_combo_meta)
        for c in combo_profits:
            if c.get("status") == "持仓中":
                key = c.get("combo", "")
                live_val = live_mtm_map.get(key)
                if live_val is None:
                    continue
                # 现在平仓净利估算 = MTM毛浮盈 - 开仓费 - 结算/平仓费 + 已实现funding
                # 优先使用CSV结构化费用字段；缺失时才回退估算
                amt = float(c.get("amount", 0) or 0)
                entry_f = float(c.get("entry_future_price", 0) or 0)
                ref_f = float(live_ref_map.get(key, entry_f) or entry_f)
                cost_ctx = open_combo_costs.get(key, {})
                open_trade = cost_ctx.get("open_trade", {})
                funding_realized = _to_float(cost_ctx.get("funding_realized_usd", 0), 0.0)
                open_fee_est = _estimate_open_fee_from_trade(open_trade)
                close_fee_est = _estimate_close_fee_from_trade(open_trade, ref_f)
                if open_fee_est <= 0 and amt > 0 and entry_f > 0:
                    # 最后兜底（仅在缺少成交字段时启用）
                    open_fee_est = amt * entry_f * 0.001
                if close_fee_est <= 0 and amt > 0 and ref_f > 0:
                    close_fee_est = amt * ref_f * (0.00015 * 2 + 0.0004)
                close_est_net = float(live_val) - open_fee_est - close_fee_est + funding_realized
                c["mtm_gross_usd"] = round(float(live_val), 4)
                c["fee_est_open_usd"] = round(open_fee_est, 4)
                c["fee_est_close_usd"] = round(close_fee_est, 4)
                c["funding_realized_usd"] = round(funding_realized, 4)
                c["close_est_net_usd"] = round(close_est_net, 4)
                c["close_now_net_usd"] = round(close_est_net, 4)
                c["delivery_est_profit_usd"] = round(
                    _to_float_opt(c.get("entry_profit_usd")) if _to_float_opt(c.get("entry_profit_usd")) is not None
                    else _to_float(c.get("profit_usd", 0), 0.0), 4)
                c["pnl_source"] = "预估利润(到期)"

        # 排序：持仓中的排前面，已平仓按时间倒序（最新平仓在前）
        combo_profits.sort(key=lambda x: (0 if x["status"] == "持仓中" else 1,
                                           x["time"] if x["status"] == "持仓中" else ""),
                           reverse=False)
        # 已平仓部分按时间倒序（最新在前，方便截断时保留最近的）
        _open_combos = [c for c in combo_profits if c["status"] == "持仓中"]
        _closed_combos = sorted(
            [c for c in combo_profits if c["status"] != "持仓中"],
            key=lambda x: x.get("time", ""), reverse=True)

        # 🌟 网页显示数量控制（CSV 全量不受影响，仅限制前端展示）
        # 规则1: 持仓中全部显示（即使超过12个）
        # 规则2: 持仓不足12个时，补充最近平仓的，总数不超过12
        _display_limit = 12
        _open_count_display = len(_open_combos)
        if _open_count_display >= _display_limit:
            # 持仓数≥12：只显示持仓
            combo_profits_display = _open_combos
        else:
            # 持仓数<12：补充最近平仓的
            _closed_slots = _display_limit - _open_count_display
            combo_profits_display = _open_combos + _closed_combos[:_closed_slots]

        # 按组合轮次统计 (combo_profits 已按 到期日-行权价 分组并拆分轮次，天然去重)
        open_count = len(combo_profits)
        close_count = len(_closed_combos)
        holding_count = len(_open_combos)

        # 🌟 总运行时长: 从 CSV 第一行"开仓"记录的成交时间起算, 系统重启不影响
        first_open_time_str = ""
        total_runtime_days = 0.0
        for _t in trades:
            if _t.get("交易类型", "") == "开仓":
                _ts = (_t.get("成交时间") or "").strip()
                if _ts:
                    if not first_open_time_str or _ts < first_open_time_str:
                        first_open_time_str = _ts
        if first_open_time_str:
            try:
                _t0 = datetime.strptime(first_open_time_str, "%Y-%m-%d %H:%M:%S")
                _delta_sec = (datetime.now() - _t0).total_seconds()
                total_runtime_days = max(0.0, _delta_sec / 86400.0)
            except Exception:
                pass
        realized_profit = sum(_to_float(c.get("profit_usd", 0)) for c in combo_profits if c.get("status") == "已平仓")
        # 当前持仓"到期交割预估利润"之和（优先使用开仓记录净利）
        open_delivery_est = sum(
            _to_float(c.get("delivery_est_profit_usd",
                        c.get("entry_profit_usd", c.get("profit_usd", 0))))
            for c in combo_profits if c.get("status") == "持仓中")
        total_profit = realized_profit + open_delivery_est
        avg_annualized_pct = (
            (annual_profit_sum / annual_capital_hours_sum) * (365.0 * 24.0) * 100.0
            if annual_capital_hours_sum > 0 else 0.0
        )
        open_mtm_gross = sum(_to_float(c.get("mtm_gross_usd", 0)) for c in combo_profits if c.get("status") == "持仓中")
        open_fee_est_total = sum(
            _to_float(c.get("fee_est_open_usd", 0)) + _to_float(c.get("fee_est_close_usd", 0))
            for c in combo_profits if c.get("status") == "持仓中"
        )

        summary = {
            "total_trades": len(trades),
            "open_trades": open_count,
            "close_trades": close_count,
            "holding_trades": holding_count,
            "first_open_time": first_open_time_str,
            "total_runtime_days": round(total_runtime_days, 2),
            "realized_profit_usd": round(realized_profit, 4),
            "close_est_profit_usd": round(open_delivery_est, 4),
            "delivery_est_profit_usd": round(open_delivery_est, 4),
            "total_profit_usd": round(total_profit, 4),
            "avg_annualized_pct": round(avg_annualized_pct, 4),
            "open_mtm_gross_usd": round(open_mtm_gross, 4),
            "total_sim_profit_usd": round(total_sim_profit, 4),
            "total_fees_usd": round(total_fees_realized, 4),
            "open_fee_est_usd": round(open_fee_est_total, 4),
            "total_slippage_usd": round(total_slippage, 4),
        }
        # combo_profits_display 用于前端卡片展示（有数量限制）
        # combo_profits 全量用于统计计算（realized_profit 等）
        return jsonify({"ok": True, "trades": trades, "combo_profits": combo_profits_display, "summary": summary})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trades": [], "summary": {}})


@app.route("/api/logs")
@login_required
def api_logs():
    """读取最新日志（支持增量拉取）"""
    try:
        offset = int(flask_request.args.get("offset", 0))
        limit = int(flask_request.args.get("limit", 200))

        if not LOG_FILE.exists():
            return jsonify({"ok": True, "lines": [], "offset": 0, "total": 0})

        file_size = LOG_FILE.stat().st_size
        if offset == 0 or offset > file_size:
            read_start = max(0, file_size - 65536)
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                f.seek(read_start)
                if read_start > 0:
                    f.readline()
                lines = f.readlines()
            lines = lines[-limit:]
            return jsonify({"ok": True, "lines": [l.rstrip() for l in lines],
                            "offset": file_size, "total": file_size})
        elif offset < file_size:
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                f.seek(offset)
                new_content = f.read(262144)
            lines = new_content.splitlines()
            if new_content and not new_content.endswith("\n"):
                lines = lines[:-1]
            return jsonify({"ok": True, "lines": lines,
                            "offset": offset + sum(len(l) + 1 for l in lines),
                            "total": file_size})
        else:
            return jsonify({"ok": True, "lines": [], "offset": offset, "total": file_size})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "lines": [], "offset": 0})


_stats_cache = {"data": None, "file_size": 0, "mtime": 0, "ts": 0}

@app.route("/api/stats")
@login_required
def api_stats():
    """解析日志统计: 运行时长、警告数、错误数、中断次数
    增量缓存：只在文件大小/修改时间变化时重新扫描，否则返回缓存"""
    try:
        if not LOG_FILE.exists():
            return jsonify({"ok": True, "stats": {}})

        st = LOG_FILE.stat()
        # 文件未变 且 缓存不超过 5 秒 → 直接返回缓存
        if (_stats_cache["data"]
                and st.st_size == _stats_cache["file_size"]
                and st.st_mtime == _stats_cache["mtime"]
                and time.time() - _stats_cache["ts"] < 5):
            return jsonify(_stats_cache["data"])

        warnings = []
        errors = []
        first_ts = None
        last_ts = None
        warning_count = 0
        error_count = 0
        reconnect_count = 0

        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if m:
                    ts_str = m.group(1)
                    if first_ts is None:
                        first_ts = ts_str
                    last_ts = ts_str

                if " - WARNING - " in line:
                    warning_count += 1
                    ts = line[:19] if len(line) > 19 else ""
                    msg = line.split(" - WARNING - ", 1)[-1].strip()
                    warnings.append({"time": ts, "message": msg})
                elif " - ERROR - " in line:
                    error_count += 1
                    ts = line[:19] if len(line) > 19 else ""
                    msg = line.split(" - ERROR - ", 1)[-1].strip()
                    errors.append({"time": ts, "message": msg})

                if "重连" in line or "reconnect" in line.lower() or "WebSocket连接已建立" in line:
                    reconnect_count += 1

        uptime_str = ""
        uptime_seconds = 0
        if first_ts and last_ts:
            try:
                t0 = datetime.strptime(first_ts, "%Y-%m-%d %H:%M:%S")
                t1 = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
                delta = t1 - t0
                uptime_seconds = max(0, int(delta.total_seconds()))
                hours, remainder = divmod(uptime_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h {minutes}m {seconds}s"
            except ValueError:
                pass

        reconnect_count = max(0, reconnect_count - 1)

        stats = {
            "start_time": first_ts or "",
            "last_time": last_ts or "",
            "uptime": uptime_str,
            "uptime_seconds": uptime_seconds,  # 🌟 供前端按 xx天xx小时 格式化
            "warning_count": warning_count,
            "error_count": error_count,
            "reconnect_count": reconnect_count,
            "warnings": warnings,
            "errors": errors,
        }
        result = {"ok": True, "stats": stats, "currency": CURRENCY, "is_testnet": IS_TESTNET}
        _stats_cache.update({"data": result, "file_size": st.st_size, "mtime": st.st_mtime, "ts": time.time()})
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "stats": {}})


@app.route("/api/config")
@login_required
def api_config():
    """返回当前配置（脱敏）"""
    return jsonify({
        "currency": CURRENCY,
        "is_testnet": IS_TESTNET,
        "min_profit_threshold": COIN_CONFIG.get("min_profit_threshold"),
        "trade_amount": COIN_CONFIG.get("trade_amount"),
        "max_wait_time": config.BASE_CONFIG.get("max_wait_time"),
        "batch_size": config.BASE_CONFIG.get("concurrent_batch_size"),
        "scan_interval_ms": config.BASE_CONFIG.get("scan_interval_ms"),
    })


@app.route("/api/daily_drawdown")
@login_required
def api_daily_drawdown():
    """🌟 每日最大浮盈/浮亏历史 + 止损阈值
    数据源: SQLite trading_{CURRENCY}.db 的 daily_drawdown 表 (WAL 模式只读, 唯一权威)
    🌟 2026-04-18 简化: 删除 CSV fallback, SQLite 是唯一数据源
    """
    try:
        records = []
        _db_path = _TRADE_DB_PATH
        if _db_path.exists():
            try:
                import db_store as _ds
                _store = _ds.DrawdownStore(str(_db_path))
                _rows = _store.recent_days_sync(30)  # 已按日期升序
                for row in _rows:
                    records.append({
                        'date': row['date'],
                        'max_single_loss':    float(row['max_single_loss_usd']    or 0),
                        'max_total_loss':     float(row['max_total_loss_usd']     or 0),
                        'max_daily_net_loss': float(row.get('max_daily_net_loss_usd', 0) or 0),
                        'max_single_gain':    float(row['max_single_gain_usd']    or 0),
                        'max_total_gain':     float(row['max_total_gain_usd']     or 0),
                        'updated_at':         row.get('updated_at', ''),
                    })
            except Exception as _sqlite_err:
                # 🌟 "表不存在" 当作"表为空"处理 (引擎尚未启动/首次安装场景)
                #   前端会显示"暂无历史数据"占位, 而不是柱图静默不刷新
                _err_msg = str(_sqlite_err).lower()
                if 'no such table' in _err_msg:
                    records = []  # 降级为"表为空", 继续往下走返回 ok=True
                else:
                    import logging as _lg
                    _lg.getLogger(__name__).error(
                        f"api_daily_drawdown: SQLite 读取失败: {_sqlite_err}")
                    return jsonify({
                        "ok": False,
                        "error": f"SQLite 读取失败: {str(_sqlite_err)[:200]}",
                        "records": [], "thresholds": {},
                    })
        return jsonify({
            "ok": True,
            "records": records,
            "thresholds": {
                "hard_stop_loss_usd": float(COIN_CONFIG.get('hard_stop_loss_usd', 300) or 300),
                "daily_loss_limit_usd": float(config.BASE_CONFIG.get('daily_loss_limit_usd', 0) or 0),
            }
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "records": [], "thresholds": {}})


@app.route("/api/daily_account_equity")
@login_required
def api_daily_account_equity():
    """每日账户总权益历史。数据源: SQLite daily_account_equity 表。"""
    try:
        records = []
        _db_path = _TRADE_DB_PATH
        if _db_path.exists():
            try:
                import db_store as _ds
                _store = _ds.AccountEquityStore(str(_db_path))
                records = _store.all_days_sync()
            except Exception as _sqlite_err:
                _err_msg = str(_sqlite_err).lower()
                if 'no such table' in _err_msg:
                    records = []
                else:
                    import logging as _lg
                    _lg.getLogger(__name__).error(
                        f"api_daily_account_equity: SQLite 读取失败: {_sqlite_err}")
                    return jsonify({
                        "ok": False,
                        "error": f"SQLite 读取失败: {str(_sqlite_err)[:200]}",
                        "records": [],
                    })
        return jsonify({"ok": True, "records": records})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "records": []})


# ======================== HTML 前端 ========================
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>跨所套利监控</title>
<style>
:root {
    --bg: #f4f6f9; --bg2: #ffffff; --bg3: #eef1f6;
    --border: #dce0e8; --text: #1a1f36; --text2: #6b7694;
    --green: #0d9f52; --red: #d93025; --yellow: #c47a0a;
    --blue: #2563eb; --purple: #7c3aed; --accent: #1a56db;
    --card-shadow: 0 1px 4px rgba(0,0,0,0.07), 0 2px 8px rgba(0,0,0,0.04);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
       background: var(--bg); color: var(--text); font-size: 14px; }

/* 顶部标题栏 */
.header { background: linear-gradient(135deg, #1e3a5f 0%, #2563eb 100%);
           border-bottom: 1px solid var(--border); padding: 16px 28px;
           display: flex; align-items: center; justify-content: space-between; }
.header h1 { font-size: 22px; font-weight: 700; color: #fff; letter-spacing: 0.5px; }
.header h1 .logo { color: #93c5fd; margin-right: 4px; }
.header h1 span { color: #93c5fd; }
.header .env-badge { padding: 5px 14px; border-radius: 14px; font-size: 12px; font-weight: 700;
                     letter-spacing: 1px; text-transform: uppercase; }
.env-badge.testnet { background: #fef3c7; color: #92400e; border: 1px solid #fcd34d; }
.env-badge.mainnet { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
.header .meta { color: #cbd5e1; font-size: 13px; display: flex; align-items: center; gap: 10px; }

/* 统计卡片 */
.stats-bar { display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr));
              gap: 14px; padding: 20px 28px; }
.stat-card { background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
              padding: 16px 20px; box-shadow: var(--card-shadow);
              transition: border-color 0.2s, transform 0.15s; }
.stat-card:hover { border-color: var(--blue); transform: translateY(-1px); }
.stat-card .label { color: var(--text2); font-size: 11px; text-transform: uppercase;
                     letter-spacing: 0.8px; margin-bottom: 8px; font-weight: 600; }
.stat-card .value { font-size: 20px; font-weight: 700; font-variant-numeric: tabular-nums; }
.stat-card .value.green { color: var(--green); }
.stat-card .value.red { color: var(--red); }
.stat-card .value.yellow { color: var(--yellow); }
.stat-card .value.blue { color: var(--blue); }
.stat-card .sub { color: var(--text2); font-size: 11px; margin-top: 4px; }

/* 通用区块 */
.section { padding: 0 28px 20px; }
.section-title { font-size: 15px; font-weight: 700; color: var(--text2); margin-bottom: 14px;
                  padding-bottom: 10px; border-bottom: 2px solid var(--border);
                  display: flex; align-items: center; gap: 8px; letter-spacing: 0.3px; }
.section-title .icon { font-size: 16px; }

.combo-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 14px; }
.combo-card { background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
               padding: 10px; box-shadow: var(--card-shadow);
               transition: border-color 0.2s; display: flex; align-items: stretch; gap: 0; }
.combo-card:hover { border-color: var(--blue); }
.combo-card .combo-left { flex: 1; display: flex; flex-direction: column; justify-content: center; gap: 4px; min-width: 0; }
.combo-card .combo-right { display: flex; flex-direction: column; align-items: flex-end; justify-content: center;
                            padding-left: 10px; border-left: 1px solid var(--border); min-width: 100px; gap: 4px; }
.combo-card .combo-name { font-weight: 700; font-size: 15px; color: var(--accent); white-space: nowrap; }
.combo-card .combo-strategy { font-size: 12px; color: var(--text2); }
.combo-card .combo-profit { font-weight: 700; font-size: 20px; white-space: nowrap; text-align: right; line-height: 1.2; }
.combo-card .combo-profit .combo-unit { font-size: 11px; font-weight: 500; opacity: 0.6; margin-left: 2px; }
.combo-card .combo-meta { font-size: 12px; color: var(--text2); text-align: right; white-space: nowrap; }
.combo-card .combo-detail { color: var(--text2); font-size: 12px; line-height: 1.8; }

/* 持仓表格 */
.pos-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.pos-table th { background: var(--bg3); color: var(--text2); text-align: left;
                 padding: 11px 14px; font-weight: 700; font-size: 11px;
                 text-transform: uppercase; letter-spacing: 0.5px; }
.pos-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); }
.pos-table tr:hover { background: #eef2ff; }
.pos-table tbody tr { transition: background 0.15s; }

/* 挂单表格 */
.order-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 6px; }
.order-table th { background: #fef9e7; color: var(--yellow); text-align: left;
                   padding: 10px 14px; font-weight: 700; font-size: 11px;
                   text-transform: uppercase; letter-spacing: 0.5px; }
.order-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); }
.order-table tr:hover { background: #fffbeb; }

/* 日志面板 — 左宽右窄 */
.log-panel { display: grid; grid-template-columns: 3fr 2fr; gap: 18px; padding: 0 28px 28px; }
@media (max-width: 1000px) { .log-panel { grid-template-columns: 1fr; } }

.log-box { background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
            display: flex; flex-direction: column; height: 520px;
            box-shadow: var(--card-shadow); }
.log-box .log-title { padding: 14px 18px; border-bottom: 1px solid var(--border);
                       font-weight: 700; font-size: 14px; display: flex;
                       justify-content: space-between; align-items: center; flex-shrink: 0;
                       letter-spacing: 0.3px; }
.log-box .log-title .badge { font-size: 11px; padding: 4px 12px; border-radius: 12px;
                              font-weight: 600; background: var(--bg3); color: var(--text2); }
.log-box .log-title .badge.warn-badge { background: #fef3c7; color: var(--yellow); }
.log-box .log-title .badge.err-badge { background: #fee2e2; color: var(--red); }
.clean-btn { background: transparent; border: 1px solid var(--border); color: #6b7280;
             padding: 3px 12px; border-radius: 12px; font-size: 11px; cursor: pointer;
             line-height: 1.2; transition: all 0.15s; }
.clean-btn:hover { background: #f3f4f6; color: #111827; border-color: #9ca3af; }
.clean-btn:active { transform: scale(0.96); }
.log-content { flex: 1; overflow-y: auto; padding: 12px 16px;
                font-family: 'SF Mono', 'Cascadia Code', 'JetBrains Mono', Consolas, monospace;
                font-size: 12.5px; line-height: 1.65; }
.log-content::-webkit-scrollbar { width: 6px; }
.log-content::-webkit-scrollbar-track { background: var(--bg); }
.log-content::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.log-content::-webkit-scrollbar-thumb:hover { background: #b0b8c8; }

.log-line { padding: 2px 6px; white-space: pre-wrap; word-break: break-all; border-radius: 4px;
            margin: 1px 0; }
.log-line.warn { color: var(--yellow); background: #fffbeb; }
.log-line.error { color: var(--red); background: #fef2f2; font-weight: 500; }
.log-line.success { color: var(--green); }
.log-line .ts { color: var(--text2); }

/* 刷新指示器 */
.refresh-dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }
.refresh-dot.on { background: #4ade80; box-shadow: 0 0 6px rgba(74,222,128,0.6); animation: pulse 2s infinite; }
.refresh-dot.off { background: var(--red); box-shadow: 0 0 6px rgba(217,48,37,0.5); }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

.loading { color: var(--text2); text-align: center; padding: 40px; }
.empty-hint { color: var(--text2); text-align: center; padding: 18px; font-size: 13px; }

/* 底部 */
.footer { text-align: center; color: var(--text2); font-size: 11px; padding: 12px 0 20px;
          letter-spacing: 0.5px; }
</style>
<!-- 🌟 Chart.js for daily drawdown bar chart -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>

<!-- 顶部 -->
<div class="header">
    <div style="display:flex;align-items:center;gap:16px;">
        <h1 id="headerTitle"><span class="logo">&#9670;</span> 跨所套利 <span>监控</span></h1>
        <span class="env-badge" id="envBadge">--</span>
    </div>
    <div class="meta">
        <span class="refresh-dot on" id="refreshDot"></span>
        <span style="margin-left:12px;font-size:12px;">
            <a href="/change_password" style="color:#8b8f98;text-decoration:none;margin-right:8px;" title="修改密码">🔑 改密</a>
            <a href="/logout" style="color:#8b8f98;text-decoration:none;" title="退出登录">⎋ 登出</a>
        </span>
    </div>
</div>

<!-- 统计栏 -->
<div class="stats-bar" id="statsBar">
    <div class="stat-card"><div class="label">已实现/待实现利润(USD)</div><div class="value" id="sTotalProfit">--</div><div class="sub" id="sTotalProfitSub"></div></div>
    <div class="stat-card"><div class="label">开仓 / 持仓 / 平仓</div><div class="value" id="sTradesCombo">--</div></div>
    <div class="stat-card"><div class="label">总运行 / 本次运行</div><div class="value" id="sUptime">--</div><div class="sub" id="sUptimeSub"></div></div>
    <div class="stat-card"><div class="label">中断 / 警告 / 错误</div><div class="value" id="sAlerts">--</div></div>
</div>

<!-- 每日账户总权益曲线 -->
<div class="section">
    <div class="section-title"><span class="icon">&#128200;</span> 每日账户总权益曲线</div>
    <div style="padding: 12px 16px; background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; box-shadow: var(--card-shadow);">
        <div style="position: relative; height: 320px;">
            <canvas id="accountEquityChart"></canvas>
        </div>
        <div id="accountEquityInfo" style="margin-top:8px; font-size:12px; color:var(--text2);"></div>
    </div>
</div>

<!-- 🌟 每日最大浮盈/浮亏柱状图 (对比止损阈值是否合理) -->
<div class="section">
    <div class="section-title"><span class="icon">&#128201;</span> 每日最大浮盈/浮亏(扣费后)</div>
    <div style="padding: 12px 16px; background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; box-shadow: var(--card-shadow);">
        <div style="position: relative; height: 300px;">
            <canvas id="drawdownChart"></canvas>
        </div>
        <div id="drawdownInfo" style="margin-top:8px; font-size:12px; color:var(--text2);"></div>
    </div>
</div>

<!-- 组合利润 -->
<div class="section">
    <div class="section-title"><span class="icon">&#128176;</span> 本地成交组合利润（主值=预估利润）</div>
    <div class="combo-grid" id="comboGrid"><div class="loading">加载中...</div></div>
</div>

<!-- 日志面板 -->
<div class="log-panel">
    <div class="log-box">
        <div class="log-title">
            &#128196; 实时交易日志
            <span class="badge" id="logCount">0 行</span>
        </div>
        <div class="log-content" id="logContent"></div>
    </div>
    <div class="log-box">
        <div class="log-title">
            &#9888;&#65039; 警告 & 错误
            <div style="display:flex;gap:6px;align-items:center;">
                <span class="badge warn-badge" id="warnCount">0 警告</span>
                <span class="badge err-badge" id="errCount">0 错误</span>
                <button id="cleanAlertsBtn" class="clean-btn" title="清除当前显示的警告/错误; 后续新记录继续累积">Clean</button>
            </div>
        </div>
        <div class="log-content" id="alertContent"></div>
    </div>
</div>

<!-- 活跃挂单 -->
<div class="section">
    <div class="section-title"><span class="icon">&#128203;</span> 活跃挂单</div>
    <div style="overflow-x:auto;">
        <table class="order-table" id="orderTable">
            <thead><tr>
                <th>合约</th><th>方向</th><th>挂单价</th><th>数量</th>
                <th>已成交</th><th>标记价</th><th>类型</th><th>创建时间</th>
            </tr></thead>
            <tbody id="orderBody"><tr><td colspan="8" class="loading">加载中...</td></tr></tbody>
        </table>
    </div>
</div>

<!-- 套利组合持仓 -->
<div class="section">
    <div class="section-title"><span class="icon">&#128200;</span> 套利组合持仓</div>
    <div style="margin-bottom:18px;">
        <div style="font-size:13px;font-weight:700;color:var(--blue);margin-bottom:8px;padding-left:4px;">&#9670; Deribit 期权持仓</div>
        <div style="overflow-x:auto;">
            <table class="pos-table" id="posTable">
                <thead><tr>
                    <th>期权合约</th><th>方向</th><th>数量</th><th>均价</th>
                    <th>标记价</th><th>到期倒计时</th><th>浮动盈亏</th><th>总盈亏</th>
                </tr></thead>
                <tbody id="posBody"><tr><td colspan="8" class="loading">加载中...</td></tr></tbody>
            </table>
        </div>
    </div>

    <div>
        <div style="font-size:13px;font-weight:700;color:var(--yellow);margin-bottom:8px;padding-left:4px;">&#9670; Binance 对冲持仓 (USDT 永续)</div>
        <div style="overflow-x:auto;">
            <table class="pos-table" id="bnPosTable">
                <thead><tr>
                    <th>合约</th><th>方向</th><th>数量</th><th>开仓价</th>
                    <th>标记价</th><th>未实现盈亏</th><th>杠杆</th>
                </tr></thead>
                <tbody id="bnPosBody"><tr><td colspan="7" class="loading">加载中...</td></tr></tbody>
            </table>
        </div>
    </div>

</div>

<div class="footer">Powered by &middot; <a href="https://num.cc" target="_blank" rel="noopener noreferrer">num.cc</a></div>

<script>
// ======================== 状态 ========================
let logOffset = 0;
let logLines = [];
const MAX_LOG_LINES = 800;

// ======================== 工具函数 ========================
function $(id) { return document.getElementById(id); }
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function colorLine(line) {
    let cls = '';
    if (/ - WARNING - /.test(line) || /\u26a0\ufe0f/.test(line)) cls = 'warn';
    else if (/ - ERROR - /.test(line) || /\ud83d\udea8/.test(line)) cls = 'error';
    else if (/成交|成功|\u2705/.test(line)) cls = 'success';
    const m = line.match(/^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/);
    if (m) {
        line = '<span class="ts">' + esc(m[1]) + '</span>' + esc(line.slice(19));
    } else {
        line = esc(line);
    }
    return `<div class="log-line ${cls}">${line}</div>`;
}

function fmtNum(v, dec=2) {
    const n = parseFloat(v);
    if (isNaN(n)) return '--';
    return n.toFixed(dec);
}

function fmtTs(epochMs) {
    if (!epochMs) return '--';
    return new Date(epochMs).toLocaleString('zh-CN', { hour12: false });
}

// ======================== 数据拉取 ========================
async function fetchJSON(url) {
    try { return await (await fetch(url)).json(); }
    catch (e) { console.error('Fetch error:', url, e); return null; }
}

async function refreshStats() {
    const [statsData, tradeData, cfgData] = await Promise.all([
        fetchJSON('/api/stats'), fetchJSON('/api/trades'), fetchJSON('/api/config')
    ]);

    if (cfgData) {
        document.title = cfgData.currency + ' 套利监控';
        $('headerTitle').innerHTML = cfgData.currency + ' <span>套利监控</span>';
        const badge = $('envBadge');
        if (cfgData.is_testnet) {
            badge.textContent = 'TEST';
            badge.className = 'env-badge testnet';
        } else {
            badge.textContent = 'MAIN';
            badge.className = 'env-badge mainnet';
        }
    }

    if (tradeData && tradeData.ok) {
        const s = tradeData.summary;
        // 开仓 / 持仓 / 平仓: 开仓总数 / 当前持仓中 / 已平仓
        $('sTradesCombo').innerHTML = `<span style="color:var(--blue)">${s.open_trades||0}</span> / <span style="color:var(--yellow)">${s.holding_trades||0}</span> / <span style="color:var(--green)">${s.close_trades||0}</span>`;
        const realized = s.realized_profit_usd || 0;
        const deliveryEst = (s.delivery_est_profit_usd ?? s.close_est_profit_usd) || 0;
        const annualized = s.avg_annualized_pct || 0;
        const mixProfit = realized + deliveryEst;
        $('sTotalProfit').textContent = `${fmtNum(realized, 2)} / ${fmtNum(deliveryEst, 2)}`;
        $('sTotalProfit').className = 'value ' + (mixProfit >= 0 ? 'green' : 'red');
        $('sTotalProfitSub').textContent = `年化: ${fmtNum(annualized, 2)}%`;

        const grid = $('comboGrid');
        if (tradeData.combo_profits && tradeData.combo_profits.length > 0) {
            grid.innerHTML = tradeData.combo_profits.map(c => {
                const strategyShort = c.strategy.includes('sell_future') ? 'SF+BS' : 'BF+SS';
                const isOpen = c.status === '\u6301\u4ed3\u4e2d';
                const mainProfit = (isOpen && c.delivery_est_profit_usd !== null && c.delivery_est_profit_usd !== undefined)
                    ? c.delivery_est_profit_usd
                    : c.profit_usd;
                const color = (!isOpen) ? '#9ca3af' : (mainProfit >= 0 ? 'var(--green)' : 'var(--red)');
                const sign = mainProfit >= 0 ? '+' : '';
                const settledTextStyle = isOpen ? '' : 'style="color:#9ca3af;"';
                const secondaryLine = isOpen && c.close_now_net_usd !== null && c.close_now_net_usd !== undefined
                    ? `<span class="combo-meta">MTM浮盈: ${fmtNum(c.close_now_net_usd, 2)} USD</span>`
                    : '';
                // 距到期倒计时：显示 Xh Ym 格式
                let expiryCountdown = '';
                let expiryColor = '';
                if (c.hours_left != null) {
                    if (c.hours_left <= 0) {
                        expiryCountdown = '已到期';
                        expiryColor = '#9ca3af';
                    } else if (c.hours_left < 24) {
                        const h = Math.floor(c.hours_left);
                        const m = Math.round((c.hours_left - h) * 60);
                        expiryCountdown = `${h}h ${m}m`;
                        expiryColor = 'var(--red)';
                    } else if (c.hours_left < 48) {
                        const h = Math.floor(c.hours_left);
                        const m = Math.round((c.hours_left - h) * 60);
                        expiryCountdown = `${h}h ${m}m`;
                    } else {
                        expiryCountdown = `${c.days_left}d`;
                    }
                }
                const cardStyle = isOpen
                    ? 'border-left:6px solid var(--green);'
                    : 'border-left:6px solid #9ca3af;background:#f3f4f6;border-color:#d1d5db;';
                const nameStyle = isOpen ? '' : 'style="color:#9ca3af;"';
                return `<div class="combo-card" style="${cardStyle}">
                    <div class="combo-left">
                        <span class="combo-name" ${nameStyle}>${esc(c.combo)}</span>
                        <span class="combo-strategy" ${settledTextStyle}>\u7b56\u7565 ${strategyShort} | \u6570\u91cf ${c.amount}</span>
                        <span class="combo-strategy" ${settledTextStyle}>${esc(c.time)}</span>
                    </div>
                    <div class="combo-right">
                        <span class="combo-meta" style="font-size:14px;font-weight:700;${!isOpen ? 'color:#9ca3af;' : (expiryColor ? 'color:'+expiryColor+';' : '')}">${expiryCountdown}</span>
                        <span class="combo-profit" style="color:${color}">${sign}${fmtNum(mainProfit, 2)}<span class="combo-unit">USD</span></span>
                        ${secondaryLine}
                    </div>
                </div>`;
            }).join('');
        } else {
            grid.innerHTML = '<div class="empty-hint">暂无成交记录</div>';
        }
    }

    if (statsData && statsData.ok) {
        const st = statsData.stats;
        // ===== 总运行 / 本次运行 =====
        // 🌟 保护规则: 总运行 >= 本次运行 (若 CSV 无数据或新起步, 总运行显示本次值)
        // 原因: CSV 第一笔开仓时间若早于"本次进程启动时间"则正常;
        //       若 CSV 晚于或等于"本次启动时间"(例如首次部署尚无交易),
        //       "总运行" 会比 "本次运行" 小, 违反逻辑 → 取 max 兜底
        const upSec = st.uptime_seconds || 0;
        const totalDaysFloat = (tradeData && tradeData.ok && tradeData.summary)
            ? (tradeData.summary.total_runtime_days || 0) : 0;
        const totalSecFromCsv = totalDaysFloat * 86400;
        const effectiveTotalSec = Math.max(totalSecFromCsv, upSec);  // 🌟 保证 >= 本次
        // 总运行 (从 CSV 第一笔开仓算起, 跨重启累加): 格式 xxdxxh, d 仅在 ≥ 1 天时显示
        const totalDays = Math.floor(effectiveTotalSec / 86400);
        const totalHours = Math.floor((effectiveTotalSec % 86400) / 3600);
        const totalLabel = (totalDays > 0) ? `${totalDays}d${totalHours}h` : `${totalHours}h`;
        // 本次运行 (日志首行 ~ 末行): 格式 xxdxxhxxm, d 仅在 ≥ 1 天时显示
        const sessDays = Math.floor(upSec / 86400);
        const sessHours = Math.floor((upSec % 86400) / 3600);
        const sessMins = Math.floor((upSec % 3600) / 60);
        const sessLabel = (sessDays > 0)
            ? `${sessDays}d${sessHours}h${sessMins}m`
            : `${sessHours}h${sessMins}m`;
        $('sUptime').innerHTML = `<span style="color:var(--blue)">${totalLabel}</span> / <span style="color:var(--green)">${sessLabel}</span>`;
        $('sUptimeSub').textContent = st.start_time ? `本次 ${st.start_time}` : '';

        const rc = st.reconnect_count || 0, wc = st.warning_count || 0, ec = st.error_count || 0;
        $('sAlerts').innerHTML = `<span style="color:var(--blue)">${rc}</span> / <span style="color:var(--yellow)">${wc}</span> / <span style="color:var(--red)">${ec}</span>`;

        // 合并警告和错误，按时间倒序（最新在最前，与实时日志一致）
        const alerts = [];
        (st.errors || []).forEach(e => alerts.push({...e, level: 'error'}));
        (st.warnings || []).forEach(w => alerts.push({...w, level: 'warn'}));
        alerts.sort((a, b) => a.time > b.time ? 1 : -1);

        // 🌟 Clean 按钮过滤: 仅展示 "最后一次 Clean 之后" 的记录 (sessionStorage 持久化)
        const clearedAt = sessionStorage.getItem('_alertsClearedAt') || '';
        const visibleAlerts = clearedAt ? alerts.filter(a => (a.time || '') > clearedAt) : alerts;
        const wcVisible = visibleAlerts.filter(a => a.level === 'warn').length;
        const ecVisible = visibleAlerts.filter(a => a.level === 'error').length;
        $('warnCount').textContent = wcVisible + ' 警告';
        $('errCount').textContent = ecVisible + ' 错误';

        const alertEl = $('alertContent');
        alertEl.innerHTML = visibleAlerts.map(a => {
            const cls = a.level === 'error' ? 'error' : 'warn';
            const icon = a.level === 'error' ? '\ud83d\udd34' : '\ud83d\udfe1';
            return `<div class="log-line ${cls}">${icon} <span class="ts">${esc(a.time)}</span> ${esc(a.message)}</div>`;
        }).join('');
        // 自动滚动到底部显示最新日志（等DOM渲染完成后执行）
        requestAnimationFrame(() => { alertEl.scrollTop = alertEl.scrollHeight; });
    }
}

// 🌟 Clean 按钮 handler: 记录当前时间戳, 后续 refresh 只显示 > 该时间的记录
function initCleanAlertsBtn() {
    const btn = document.getElementById('cleanAlertsBtn');
    if (!btn) return;
    btn.onclick = () => {
        // 使用 "yyyy-MM-dd HH:mm:ss" 格式, 与 log 时间戳一致
        const d = new Date();
        const pad = n => String(n).padStart(2, '0');
        const ts = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} `
                 + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
        sessionStorage.setItem('_alertsClearedAt', ts);
        $('alertContent').innerHTML = '';
        $('warnCount').textContent = '0 警告';
        $('errCount').textContent = '0 错误';
    };
}
document.addEventListener('DOMContentLoaded', initCleanAlertsBtn);

async function refreshPositions() {
    const data = await fetchJSON('/api/positions');
    if (!data || !data.ok) {
        const errMsg = data && data.error ? esc(data.error) : '请求失败';
        $('posBody').innerHTML = '<tr><td colspan="8" class="empty-hint">' + errMsg + '</td></tr>';
        $('bnPosBody').innerHTML = '<tr><td colspan="7" class="empty-hint">' + errMsg + '</td></tr>';
        return;
    }

    // Deribit 期权持仓
    const body = $('posBody');
    const dPos = data.deribit_positions || [];
    if (dPos.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="empty-hint">暂无持仓</td></tr>';
    } else {
        body.innerHTML = dPos.map(p => {
            const dirColor = p.direction === 'buy' ? 'var(--green)' : 'var(--red)';
            const dirLabel = p.direction === 'buy' ? 'LONG' : 'SHORT';
            const pnlColor = p.pnl >= 0 ? 'var(--green)' : 'var(--red)';
            const fpnlColor = p.floating_pnl >= 0 ? 'var(--green)' : 'var(--red)';
            // 从期权合约名解析到期日（如 BTC-10APR26-74000-C → 10APR26）
            let expiryCountdown = '--';
            const parts = p.instrument.split('-');
            if (parts.length >= 3) {
                try {
                    const expStr = parts[1]; // e.g. "10APR26"
                    const months = {JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11};
                    const day = parseInt(expStr.substring(0, expStr.length - 5));
                    const monStr = expStr.substring(expStr.length - 5, expStr.length - 2).toUpperCase();
                    const yr = 2000 + parseInt(expStr.substring(expStr.length - 2));
                    const expDate = new Date(Date.UTC(yr, months[monStr], day, 8, 0, 0));
                    const nowMs = Date.now();
                    const diffMs = expDate.getTime() - nowMs;
                    if (diffMs <= 0) {
                        expiryCountdown = '<span style="color:var(--red);font-weight:700;">已到期</span>';
                    } else {
                        const totalH = diffMs / 3600000;
                        if (totalH < 48) {
                            const h = Math.floor(totalH);
                            const m = Math.round((totalH - h) * 60);
                            expiryCountdown = `<span style="color:var(--red);font-weight:600;">距到期 ${h}h ${m}m</span>`;
                        } else {
                            const d = (totalH / 24).toFixed(1);
                            expiryCountdown = `距到期 ${d}天`;
                        }
                    }
                } catch(e) {}
            }
            return `<tr>
                <td><b>${esc(p.instrument)}</b></td>
                <td style="color:${dirColor};font-weight:700">${dirLabel}</td>
                <td>${p.size}</td>
                <td>${fmtNum(p.avg_price, 6)}</td>
                <td>${fmtNum(p.mark_price, 6)}</td>
                <td style="font-size:12px;">${expiryCountdown}</td>
                <td style="color:${fpnlColor}">${fmtNum(p.floating_pnl, 6)}</td>
                <td style="color:${pnlColor}"><b>${fmtNum(p.pnl, 6)}</b></td>
            </tr>`;
        }).join('');
    }

    // Binance 期货持仓
    const bnBody = $('bnPosBody');
    const bPos = data.binance_positions || [];
    if (bPos.length === 0) {
        bnBody.innerHTML = '<tr><td colspan="7" class="empty-hint">暂无持仓</td></tr>';
    } else {
        bnBody.innerHTML = bPos.map(p => {
            const dirColor = p.direction === 'LONG' ? 'var(--green)' : 'var(--red)';
            const pnlColor = p.unrealized_pnl >= 0 ? 'var(--green)' : 'var(--red)';
            return `<tr>
                <td><b>${esc(p.symbol)}</b></td>
                <td style="color:${dirColor};font-weight:700">${p.direction}</td>
                <td>${p.amount}</td>
                <td>${fmtNum(p.entry_price, 2)}</td>
                <td>${fmtNum(p.mark_price, 2)}</td>
                <td style="color:${pnlColor}"><b>${fmtNum(p.unrealized_pnl, 4)} USDT</b></td>
                <td>${p.leverage}x</td>
            </tr>`;
        }).join('');
    }

    // 挂单表
    const oBody = $('orderBody');
    if (!data.orders || data.orders.length === 0) {
        oBody.innerHTML = '<tr><td colspan="8" class="empty-hint">暂无挂单</td></tr>';
    } else {
        oBody.innerHTML = data.orders.map(o => {
            const dirColor = o.direction === 'buy' ? 'var(--green)' : 'var(--red)';
            const dirLabel = o.direction === 'buy' ? 'BUY' : 'SELL';
            return `<tr>
                <td><b>${esc(o.instrument)}</b></td>
                <td style="color:${dirColor};font-weight:700">${dirLabel}</td>
                <td>${fmtNum(o.price, 6)}</td>
                <td>${o.amount}</td>
                <td>${o.filled_amount}</td>
                <td>${o.mark_price != null ? fmtNum(o.mark_price, 6) : '--'}</td>
                <td>${esc(o.order_type)}</td>
                <td style="font-size:12px;color:var(--text2)">${fmtTs(o.creation_timestamp)}</td>
            </tr>`;
        }).join('');
    }
}

async function refreshLogs() {
    const data = await fetchJSON(`/api/logs?offset=${logOffset}&limit=200`);
    if (!data || !data.ok) return;

    const lc = $('logContent');
    if (data.total === 0 && logLines.length === 0) {
        lc.innerHTML = '<div class="empty-hint">日志文件尚未生成，请先启动交易程序</div>';
        return;
    }

    if (data.lines.length > 0) {
        logLines.push(...data.lines);
        if (logLines.length > MAX_LOG_LINES) {
            logLines = logLines.slice(-MAX_LOG_LINES);
        }

        const atBottom = lc.scrollTop + lc.clientHeight >= lc.scrollHeight - 30;

        lc.innerHTML = logLines.map(l => colorLine(l)).join('');
        $('logCount').textContent = logLines.length + ' 行';

        if (atBottom) {
            lc.scrollTop = lc.scrollHeight;
        }
    }
    logOffset = data.offset;
}

// ======================== 每日账户总权益曲线 ========================
let _accountEquityChart = null;
async function refreshAccountEquity() {
    try {
        const res = await fetch('/api/daily_account_equity', { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json();
        if (!data.ok) return;
        const records = data.records || [];
        if (records.length === 0) {
            $('accountEquityInfo').innerHTML = '<em>暂无账户权益历史。引擎启动并成功查询两端账户后会生成首条记录。</em>';
            if (_accountEquityChart) { _accountEquityChart.destroy(); _accountEquityChart = null; }
            return;
        }

        const labels = records.map(r => r.date || '');
        const totalUsd = records.map(r => Number(r.total_equity_usd || 0));
        const totalBtc = records.map(r => Number(r.total_equity_btc || 0));
        const btcPrice = records.map(r => Number(r.btc_usd_price || 0));

        if (_accountEquityChart) { _accountEquityChart.destroy(); }
        const ctx = document.getElementById('accountEquityChart');
        if (!ctx) return;
        _accountEquityChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {label: '总权益 USD', data: totalUsd, yAxisID: 'yUsd',
                     borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,0.10)',
                     borderWidth: 2, pointRadius: 2, tension: 0.25},
                    {label: '总权益 BTC', data: totalBtc, yAxisID: 'yBtc',
                     borderColor: '#16a34a', backgroundColor: 'rgba(22,163,74,0.10)',
                     borderWidth: 2, pointRadius: 2, tension: 0.25},
                    {label: 'BTC/USD', data: btcPrice, yAxisID: 'yUsd',
                     borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,0.08)',
                     borderWidth: 1.8, pointRadius: 1, tension: 0.25, hidden: true},
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', labels: { boxWidth: 14, padding: 8, font: {size: 11} } },
                    tooltip: { callbacks: {
                        label: (ctx) => {
                            const label = ctx.dataset.label || '';
                            const v = Number(ctx.parsed.y || 0);
                            if (label.includes('BTC') && !label.includes('USD')) return `${label}: ${v.toFixed(6)} BTC`;
                            return `${label}: $${v.toLocaleString(undefined, {maximumFractionDigits: 2})}`;
                        },
                        afterBody: (items) => {
                            if (!items || !items.length) return '';
                            const r = records[items[0].dataIndex] || {};
                            return [
                                `Deribit equity: ${Number(r.deribit_equity_btc || 0).toFixed(6)} BTC`,
                                `Binance equity: $${Number(r.binance_equity_usdt || 0).toFixed(2)}`,
                                `BTC/USD: $${Number(r.btc_usd_price || 0).toFixed(2)}`,
                            ];
                        }
                    } },
                },
                scales: {
                    x: { ticks: { font: {size: 11}, maxRotation: 45, minRotation: 0 } },
                    yUsd: {
                        type: 'linear', position: 'left',
                        title: { display: true, text: 'USD', font: {size: 11} },
                        ticks: { callback: (v) => '$' + Number(v).toLocaleString() },
                        grid: { color: 'rgba(148,163,184,0.18)' }
                    },
                    yBtc: {
                        type: 'linear', position: 'right',
                        title: { display: true, text: 'BTC', font: {size: 11} },
                        ticks: { callback: (v) => Number(v).toFixed(4) },
                        grid: { drawOnChartArea: false }
                    },
                },
            },
        });

        const latest = records[records.length - 1];
        const first = records[0];
        const usdChange = Number(latest.total_equity_usd || 0) - Number(first.total_equity_usd || 0);
        const btcChange = Number(latest.total_equity_btc || 0) - Number(first.total_equity_btc || 0);
        const usdSign = usdChange >= 0 ? '+' : '';
        const btcSign = btcChange >= 0 ? '+' : '';
        $('accountEquityInfo').innerHTML =
            `全量历史: ${records.length} 天 | ` +
            `USD变化: ${usdSign}$${usdChange.toFixed(2)} | ` +
            `BTC变化: ${btcSign}${btcChange.toFixed(6)} BTC | ` +
            `最新更新: ${(latest.updated_at || '')} UTC`;
    } catch (e) {
        console.error('refreshAccountEquity failed:', e);
    }
}

// ======================== 🌟 每日最大浮盈/浮亏柱状图 ========================
let _drawdownChart = null;
async function refreshDrawdown() {
    try {
        const res = await fetch('/api/daily_drawdown', { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json();
        if (!data.ok) return;
        const records = data.records || [];
        const th = data.thresholds || {};
        // 空数据场景: 占位提示
        if (records.length === 0) {
            $('drawdownInfo').innerHTML = '<em>暂无历史数据。引擎需跑满一天才会生成首条记录; 跨日切换时自动落盘。</em>';
            if (_drawdownChart) { _drawdownChart.destroy(); _drawdownChart = null; }
            return;
        }
        const labels = records.map(r => (r.date || '').slice(5));  // MM-DD
        // 🌟 浮亏映射为负值(向下), 浮盈保持正值(向上), 形成 0 轴上下对称布局
        // 两根浮亏柱 (全部 combo-level 扣费后, 观察用):
        //   单组合最大浮亏 = max(abs(combo.pnl))            当日取最大
        //   当日全局最大浮亏(仅浮动) = abs(sum(combo.pnl)<0) 当日取最大
        // 1 combo 时两者恒等; 多 combo 时"当日全局最大浮亏(仅浮动)" >= "单组合最大浮亏"
        const singleLossArr   = records.map(r => -(r.max_single_loss || 0));
        const globalFloatLossArr = records.map(r => -(r.max_total_loss || 0));
        const singleGainArr   = records.map(r =>   r.max_single_gain || 0);
        // 阈值线 — 仅保留 hard_stop + daily_loss 两条, 默认隐藏, 点击图例可开
        const hardStop   = th.hard_stop_loss_usd   || 0;
        const dailyLimit = th.daily_loss_limit_usd || 0;
        const lineHard   = Array(labels.length).fill(-hardStop);
        const lineDaily  = Array(labels.length).fill(-dailyLimit);

        if (_drawdownChart) { _drawdownChart.destroy(); }
        const ctx = document.getElementById('drawdownChart');
        if (!ctx) return;
        _drawdownChart = new Chart(ctx, {
            data: {
                labels: labels,
                datasets: [
                    // 浮盈柱 (向上, 绿, 参考)
                    {type: 'bar', label: '单组合最大浮盈', data: singleGainArr,
                     backgroundColor: 'rgba(134,239,172,0.85)', borderColor: '#22c55e', borderWidth: 1, order: 2},
                    // 浮亏柱 (向下, 红/紫; 🌟 已移除"全局最大浮亏"柱, 见上方注释)
                    {type: 'bar', label: '单组合最大浮亏',
                     data: singleLossArr,
                     backgroundColor: 'rgba(252,165,165,0.85)', borderColor: '#f87171', borderWidth: 1, order: 2},
                    {type: 'bar', label: '当日全局最大浮亏',
                     data: globalFloatLossArr,
                     backgroundColor: 'rgba(196,181,253,0.85)', borderColor: '#a78bfa', borderWidth: 1, order: 2},
                    // 两条阈值虚线 (hard_stop + daily_loss), 颜色与对应柱一致, 默认隐藏
                    {type: 'line', label: `hard_stop_loss_usd`, data: lineHard,
                     borderColor: '#dc2626', borderDash: [6,4], borderWidth: 2, pointRadius: 0, fill: false, order: 1, hidden: true},
                    {type: 'line', label: `daily_loss_limit_usd`, data: lineDaily,
                     borderColor: '#7c3aed', borderDash: [6,4], borderWidth: 2, pointRadius: 0, fill: false, order: 1, hidden: true},
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', labels: { boxWidth: 14, padding: 8, font: {size: 11} } },
                    tooltip: { callbacks: { label: (ctx) => {
                        // tooltip 显示用绝对值 + 正负号, 更直观
                        const v = Number(ctx.parsed.y);
                        const sign = v >= 0 ? '+' : '-';
                        return `${ctx.dataset.label}: ${sign}$${Math.abs(v).toFixed(2)}`;
                    } } },
                },
                scales: {
                    x: { ticks: { font: {size: 11} } },
                    y: {
                        title: { display: true, text: '← 浮亏  |  浮盈 →  (USD)', font: {size: 11} },
                        ticks: { callback: (v) => (v >= 0 ? '+$' : '-$') + Math.abs(v) },
                        grid: { color: (ctx) => ctx.tick.value === 0 ? '#94a3b8' : 'rgba(148,163,184,0.2)',
                                lineWidth: (ctx) => ctx.tick.value === 0 ? 1.5 : 1 }
                    },
                },
            },
        });
        // 提示信息: 最新一天 vs 阈值对比 (🌟 已移除 global_hard_stop 相关对比)
        const latest = records[records.length - 1];
        const _fmtDT = (r) => {
            if (!r || !r.updated_at) return r ? r.date : '';
            return r.updated_at + ' UTC';
        };
        const _latestDT = _fmtDT(latest);
        const _msgs = [];
        // 单组合柱 ↔ hard_stop_loss_usd (combo 扣费后)
        if (hardStop > 0 && latest.max_single_loss > hardStop) {
            _msgs.push(`⚠️ 最新 ${_latestDT} 单组合浮亏 $${latest.max_single_loss.toFixed(2)} > hard_stop_loss_usd ($${hardStop}) → 当日曾触发硬止损`);
        }
        if (dailyLimit > 0 && latest.max_total_loss > dailyLimit) {
            _msgs.push(`⚠️ 最新 ${_latestDT} 当日全局最大浮亏(仅浮动) $${latest.max_total_loss.toFixed(2)} > daily_loss_limit_usd ($${dailyLimit})`);
        }
        // 阈值余量提示 (离阈值还有多远, 便于判断是否过宽)
        if (hardStop > 0 && latest.max_single_loss > 0) {
            const _ratio = (latest.max_single_loss / hardStop * 100).toFixed(0);
            _msgs.push(`📊 最新 ${_latestDT} 单组合浮亏 $${latest.max_single_loss.toFixed(2)} (${_ratio}% of hard_stop=$${hardStop})`);
        }
        if (dailyLimit > 0 && latest.max_total_loss > 0) {
            const _ratio = (latest.max_total_loss / dailyLimit * 100).toFixed(0);
            _msgs.push(`📊 最新 ${_latestDT} 当日全局最大浮亏(仅浮动) $${latest.max_total_loss.toFixed(2)} (${_ratio}% of daily_loss_limit=$${dailyLimit})`);
        }
        // 历史浮盈峰值摘要 (仅单组合口径)
        const maxSingleGainAll = Math.max(...records.map(r => r.max_single_gain || 0));
        const peakGainDay = records.find(r => (r.max_single_gain || 0) === maxSingleGainAll);
        if (maxSingleGainAll > 0) {
            _msgs.push(`💰 近 30 天单组合最大浮盈峰值: +$${maxSingleGainAll.toFixed(2)}${peakGainDay ? ' (' + _fmtDT(peakGainDay) + ')' : ''}`);
        }
        if (_msgs.length === 0 || !_msgs.some(m => m.startsWith('⚠️'))) {
            _msgs.unshift(`✅ 近 30 天所有浮亏峰值都在阈值之内, 止损设置合理。`);
        }
        $('drawdownInfo').innerHTML = _msgs.join('<br>');
    } catch (e) {
        console.error('refreshDrawdown failed:', e);
    }
}

// ======================== 启动 ========================
async function init() {
    await Promise.all([refreshStats(), refreshPositions(), refreshLogs(), refreshAccountEquity(), refreshDrawdown()]);

    setInterval(refreshLogs, 2000);
    setInterval(refreshPositions, 15000);
    setInterval(refreshStats, 10000);
    setInterval(refreshAccountEquity, 60000);
    setInterval(refreshDrawdown, 60000);  // 🌟 每分钟刷新浮亏柱状图 (与引擎落盘周期对齐)
}

init();
</script>
</body>
</html>
"""

# ======================== 启动 ========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="套利监控面板 (Deribit + Binance)")
    parser.add_argument("--port", type=int, default=5556, help="监听端口 (默认 5556)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="监听地址")
    parser.add_argument("--currency", type=str, default=None, help="覆盖币种 (BTC/ETH)")
    args = parser.parse_args()

    if args.currency:
        CURRENCY = args.currency.upper()
        COIN_CONFIG = config.BTC_CONFIG if CURRENCY == "BTC" else config.ETH_CONFIG
        CLIENT_ID = COIN_CONFIG["CLIENT_ID"]
        CLIENT_SECRET = COIN_CONFIG["CLIENT_SECRET"]
        LOG_FILE = BASE_DIR / f"{CURRENCY}-log.txt"
        _TRADE_DB_PATH = BASE_DIR / f"trading_{CURRENCY}_{_ENV_SUFFIX}.db"

    # 认证状态展示 (启动时提示默认密码风险)
    _auth_status = _load_auth()
    _is_default = _auth_status.get('is_default_password', False)

    print(f"\n{'='*50}")
    print(f"  套利监控面板 (Deribit + Binance)")
    print(f"  币种: {CURRENCY} | {'测试网' if IS_TESTNET else '实盘'}")
    print(f"  日志: {LOG_FILE}")
    print(f"  数据库: {_TRADE_DB_PATH}")
    print(f"  地址: http://{args.host}:{args.port}")
    print(f"  认证: admin 账户 | session {SESSION_HOURS}h")
    if _is_default:
        print(f"  ⚠️ 警告: 当前使用默认密码 ({DEFAULT_PASSWORD}), 登录后请立即 /change_password 修改")
    else:
        _changed = _auth_status.get('password_changed_at', '?')
        print(f"  ✅ 密码: 已自定义 (上次修改: {_changed})")
    print(f"{'='*50}\n")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)
