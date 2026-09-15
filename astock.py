#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股多头共振策略 + 200交易日滚动回测系统
1. 自动回测：当数据库无历史记录时，自动拉取行情并回测近 200 个交易日。
2. 选股策略要求：
   - 均线多头排列 (MA5 > MA10 > MA20 > MA60)
   - MA20 升穿 / 金叉向上发散 MA60
   - MACD 大于 0 (DIF > 0 且 HIST > 0)
   - EMA 多头排列 (EMA5 > EMA10 > EMA20 且 EMA12 > EMA26)
   - BOLL %B >= 0.75 且开口向上沿上轨上升，或者升穿中轨
3. 仓位管理：
   - 上升行情：持仓 80%-90% (基准 85%)
   - 下降行情：持仓上限必须压缩至 30%（触发强制减仓再平衡）
4. 离场保护：动态 ATR 止损 + 保本止损 + Trailing ATR 移动止盈 + MACD 死叉离场。
5. 修复 DuckDB 缺失主键时的 Binder Error 异常，增强历史表无损迁移能力。

=== 本次升级新增内容 ===
A. Matplotlib 中文字体自动探测，修复回测图表中文乱码(Tofu方框)问题。
B. DuckDB 主键迁移函数修复：原实现连续两次 str.replace() 会把临时表名
   错误叠加为 xxx_pk_migration_tmp_pk_migration_tmp，现改为单次替换 + 事务保护。
C. 市场状态评分升级：加入市场广度动量减速项(D)，并用连续的"波动率目标仓位"
   (volatility targeting) 替代原先 85%/50%/30% 的三档跳变仓位。
D. 选股逻辑升级：在原有5类技术指标硬性过滤基础上，新增横截面动量因子打分
   (波动率调整动量 + 60/120日动量 + 趋势质量 + 量能强度)，并增加质量/流动性
   过滤，剔除当日候选池中最不稳定与流动性最差的一批标的，降低动量崩溃风险。
E. 出场规则分级化：硬性风控(止损/保本/跟踪止盈/硬止盈/最长持有期)依旧全额清仓；
   趋势结构走弱(连续两日跌破MA20 且 MA5<MA10)改为只减仓50%，避免单日正常回踩
   被误判为趋势反转、导致换手率和误杀交易过高。
"""
import os
import sys
import time
import json
import gzip
import tarfile
import base64
import shutil
import logging
import tempfile
import io
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Optional, Tuple
import duckdb
import numpy as np
import pandas as pd
import requests
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager
from dotenv import load_dotenv
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    import talib
except Exception:
    talib = None

if talib is not None:
    _orig_macd = talib.MACD
    def _safe_macd(real, **kwargs):
        return _orig_macd(np.asarray(real, dtype=np.float64), **kwargs)
    talib.MACD = _safe_macd


def configure_matplotlib_chinese_font() -> None:
    """探测并启用可用的中文字体，避免图表中文显示为方框（Tofu）。"""
    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttf",
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simhei.ttf",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for font_path in candidates:
        if os.path.isfile(font_path):
            font_manager.fontManager.addfont(font_path)
            font_name = font_manager.FontProperties(fname=font_path).get_name()
            matplotlib.rcParams["font.family"] = "sans-serif"
            matplotlib.rcParams["font.sans-serif"] = [font_name, "DejaVu Sans"]
            matplotlib.rcParams["axes.unicode_minus"] = False
            return
    matplotlib.rcParams["font.family"] = "sans-serif"
    matplotlib.rcParams["font.sans-serif"] = ["DejaVu Sans"]
    matplotlib.rcParams["axes.unicode_minus"] = False


load_dotenv()
# =========================================================
# 全局配置
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "astock.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger(__name__)
configure_matplotlib_chinese_font()
CN_TZ = timezone(timedelta(hours=8))
IS_CI = os.getenv("CI", "").lower() in ("true", "1", "yes") or os.getenv("GITHUB_ACTIONS", "").lower() in ("true", "1")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
AUTH_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
DEVICE_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/devicecode"
SCOPES = "Files.ReadWrite.All Mail.Send offline_access"
CHUNK_SIZE = 10 * 1024 * 1024

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()

CONFIG = {
    "azure_client_id": _env("AZURE_CLIENT_ID"),
    "token_cache_file": os.path.join(BASE_DIR, _env("TOKEN_CACHE_FILE", "ms_token.json")),
    "email_to": _env("EMAIL_TO"),
    "onedrive_folder": "Stock",
    "cloud_db_gz_name": "Tu_A_stock.duckdb.gz",
    "local_db_gz_dir": None,
    "local_db_gz_name": None,

    # 资金与回测参数
    "init_cash": 100000.0,
    "position_cash_yuan": 40000.0,
    "max_position_stocks": 6,
    "initial_replay_trade_days": 200,      # 数据库为空时回测交易日数
    "update_window_trade_days": 380,       # 行情同步窗口（覆盖200天回测+180天指标预热）
    "adjust_cache_days": 380,
    "top_n": 15,

    # 仓位调控
    "bull_market_pos_ratio": 0.85,         # 上升行情持仓 80%-90%
    "neutral_market_pos_ratio": 0.50,      # 震荡行情持仓 50%
    "bear_market_pos_ratio": 0.30,         # 下降行情持仓严格限制在 30%

    # 交易与风控
    "atr_period": 14,
    "atr_buy_alpha": 0.5,
    "atr_stop_loss_beta": 2.0,
    "trailing_stop_atr_mult": 2.5,
    "take_profit_pct": 15.0,
    "slippage_rate": 0.001,
    "buy_fee_rate": 0.0005,
    "sell_fee_rate": 0.0010,
    "max_volume_participate_rate": 0.05,
    "buy_signal_expire_days": 2,
    "max_hold_days": 120,
    "source_cache_ttl_seconds": 6 * 3600,
}

STOCKS_TABLE = "stock_prices"
ADJUSTMENT_FACTORS_TABLE = "adjustment_factors"
QLIB_DATA_URL = "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
QLIB_DATA_DIR = os.path.expanduser("~/.qlib/qlib_data/cn_data")
QLIB_TAR_PATH = os.path.join(BASE_DIR, "qlib_bin.tar.gz")
_QLIB_INITIALIZED = False

TRADE_BUY = 0
TRADE_SELL = 1
STATUS_PENDING = 0
STATUS_FILLED = 1
STATUS_EXPIRED = 2

REASON_STOPLOSS = 1          # 动态ATR止损
REASON_BELOW_MA20 = 2        # 跌破MA20/中轨
REASON_MAX_HOLD = 4          # 持有超期
REASON_BELOW_BOLL_MID = 8    # 跌破布林中轨
REASON_MACD_DECREASE = 16    # MACD死叉弱化离场
REASON_REBALANCE = 32        # 熊市刚性降仓至30%
REASON_TAKE_PROFIT = 64      # 移动跟踪止盈
REASON_BREAKEVEN = 128       # 保本止损
REASON_BUY_T1 = 256          # T+1多头挂单成交

def decode_trade_type_label(code) -> str:
    return "🟢 买入" if code == TRADE_BUY else "🔴 卖出"

def decode_reason_text(code) -> str:
    if not code:
        return ""
    parts = []
    if code & REASON_STOPLOSS:
        parts.append("动态ATR止损")
    if code & REASON_BREAKEVEN:
        parts.append("保本止损")
    if code & REASON_TAKE_PROFIT:
        parts.append("移动跟踪止盈")
    if code & REASON_MACD_DECREASE:
        parts.append("MACD死叉弱化")
    if code & REASON_REBALANCE:
        parts.append("⚠️大盘转熊强制降仓至30%")
    if code & REASON_BELOW_MA20:
        parts.append("跌破MA20(趋势确认/部分或全部离场)")
    if code & REASON_BELOW_BOLL_MID:
        parts.append("跌破布林中轨")
    if code & REASON_MAX_HOLD:
        parts.append(f"满{CONFIG['max_hold_days']}天")
    if code & REASON_BUY_T1:
        parts.append("多头共振挂单成交")
    return " / ".join(parts) if parts else ""

# =========================================================
# 基础工具
# =========================================================
def get_target_date() -> date:
    now_beijing = datetime.now(CN_TZ)
    return (now_beijing - timedelta(days=1)).date() if now_beijing.hour < 16 else now_beijing.date()

def build_retry_session() -> requests.Session:
    retry = Retry(total=3, connect=3, read=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    s = requests.Session()
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def canonical_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if "." in raw:
        left, right = raw.split(".", 1)
        if left.lower() in {"sh", "sz", "bj"}:
            return f"{right}.{left.upper()}"
        return raw
    if raw.startswith("SH"):
        return f"{raw[2:]}.SH"
    if raw.startswith("SZ"):
        return f"{raw[2:]}.SZ"
    if raw.startswith("BJ"):
        return f"{raw[2:]}.BJ"
    return raw

def decode_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in ["open", "high", "low", "close", "buy_price", "buy_price_hfq", "last_price", "last_price_hfq", "planned_buy_price", "signal_close", "price", "adjclose", "atr_pct", "atr_pct_buy"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["volume", "amount"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out

# =========================================================
# Token 与 OneDrive 通信
# =========================================================
class TokenManager:
    def __init__(self, client_id: str, token_file: str):
        self.client_id = client_id
        self.token_file = token_file
        self._data = {}
        b64 = os.getenv("ONEDRIVE_TOKEN_CACHE_B64", "").strip()
        if IS_CI and b64:
            clean = b64.replace("\\n", "").replace(" ", "")
            self._data = json.loads(base64.b64decode(clean).decode("utf-8"))
        elif os.path.exists(token_file):
            with open(token_file, "r", encoding="utf-8") as f:
                self._data = json.load(f)

    def _save(self):
        if not IS_CI:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)

    def _refresh(self):
        rt = self._data.get("refresh_token", "")
        if not rt:
            raise RuntimeError("缺少 refresh_token，请先执行授权模式")
        resp = requests.post(AUTH_URL, data={"client_id": self.client_id, "grant_type": "refresh_token", "refresh_token": rt, "scope": SCOPES}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self._data["access_token"] = data["access_token"]
        self._data["expires_at"] = time.time() + data.get("expires_in", 3600)
        if "refresh_token" in data:
            self._data["refresh_token"] = data["refresh_token"]
        self._save()

    def get_access_token(self) -> str:
        if self._data.get("expires_at", 0) < time.time() - 60:
            self._refresh()
        return self._data.get("access_token", "")

    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

class OneDriveClient:
    def __init__(self, token_mgr: TokenManager, folder: str, remote_gz_name: str):
        self.tm = token_mgr
        self.folder = folder
        self.remote_gz_name = remote_gz_name

    def download_database_gz(self, local_path: str) -> bool:
        url = f"{GRAPH_BASE}/me/drive/root:/{self.folder}/{self.remote_gz_name}:/content"
        session = build_retry_session()
        resp = session.get(url, headers=self.tm.headers(), stream=True, timeout=60)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        total_size = int(resp.headers.get("content-length", 0))
        with open(local_path, "wb") as f:
            with tqdm(total=total_size, unit="B", unit_scale=True, desc="⬇️ 下载云端数据库") as pbar:
                for chunk in resp.iter_content(CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        return True

    def upload_database_gz(self, local_path: str):
        size = os.path.getsize(local_path)
        url = f"{GRAPH_BASE}/me/drive/root:/{self.folder}/{self.remote_gz_name}:/createUploadSession"
        session = build_retry_session()
        session_resp = session.post(url, headers={**self.tm.headers(), "Content-Type": "application/json"}, json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}, timeout=60)
        session_resp.raise_for_status()
        upload_url = session_resp.json()["uploadUrl"]
        with open(local_path, "rb") as f:
            with tqdm(total=size, unit="B", unit_scale=True, desc="⬆️ 上传更新后数据库") as pbar:
                offset = 0
                while offset < size:
                    chunk = f.read(CHUNK_SIZE)
                    end = offset + len(chunk) - 1
                    for attempt in range(1, 6):
                        try:
                            put_resp = requests.put(upload_url, headers={"Content-Range": f"bytes {offset}-{end}/{size}", "Content-Length": str(len(chunk))}, data=chunk, timeout=120)
                            put_resp.raise_for_status()
                            break
                        except Exception:
                            if attempt == 5:
                                raise
                            time.sleep(attempt * 4)
                    offset += len(chunk)
                    pbar.update(len(chunk))

# =========================================================
# 数据库表结构体检与主键自动无损迁移
# =========================================================
def _check_and_fix_pk(con, table_name: str, expected_pk: List[str], create_sql: str):
    """
    检查表是否存在且是否具备完整的 PRIMARY KEY。
    如果表存在但缺少主键（常见于历史旧库），自动执行无损去重并重建主键，杜绝 Binder Error。

    修复说明：
    - 原实现对 create_sql 做了连续两次 replace()，当第一次已把
      "CREATE TABLE IF NOT EXISTS {table}" 替换为 "CREATE TABLE {tmp}" 后，
      第二次 replace 会再次命中并把 tmp 表名错误地叠加后缀，
      产生诸如 stock_prices_pk_migration_tmp_pk_migration_tmp 的错误表名。
    - 这里改为只做一次替换，并用事务保护整个迁移过程。
    """
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if table_name not in tables:
        con.execute(create_sql)
        return

    cols = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    actual_pk = [c[1] for c in cols if len(c) > 5 and c[5] > 0]
    if set(actual_pk) == set(expected_pk) and len(actual_pk) == len(expected_pk):
        return

    log.warning(f"🔄 检测到表 [{table_name}] 主键不符合预期 (当前:{actual_pk}, 期望:{expected_pk})，执行全量无损迁移重建...")

    tmp_name = f"__{table_name}_pk_migration_tmp"
    legacy_bad_tmp_name = f"{table_name}_pk_migration_tmp_pk_migration_tmp"
    prefix_if_not_exists = f"CREATE TABLE IF NOT EXISTS {table_name}"
    prefix_plain = f"CREATE TABLE {table_name}"

    if prefix_if_not_exists in create_sql:
        tmp_create_sql = create_sql.replace(prefix_if_not_exists, f'CREATE TABLE "{tmp_name}"', 1)
    elif prefix_plain in create_sql:
        tmp_create_sql = create_sql.replace(prefix_plain, f'CREATE TABLE "{tmp_name}"', 1)
    else:
        raise ValueError(f"无法从 create_sql 中定位目标表 [{table_name}] 的 CREATE TABLE 语句。")

    pk_expr = ", ".join(f'"{c}"' for c in expected_pk)
    where_cond = " AND ".join(f'"{c}" IS NOT NULL' for c in expected_pk)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f'DROP TABLE IF EXISTS "{tmp_name}"')
        con.execute(f'DROP TABLE IF EXISTS "{legacy_bad_tmp_name}"')
        con.execute(tmp_create_sql)
        con.execute(f"""
            INSERT INTO "{tmp_name}"
            SELECT DISTINCT ON ({pk_expr}) *
            FROM "{table_name}"
            WHERE {where_cond}
        """)
        con.execute(f'DROP TABLE "{table_name}"')
        con.execute(f'ALTER TABLE "{tmp_name}" RENAME TO "{table_name}"')

        migrated_cols = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        migrated_pk = [c[1] for c in migrated_cols if len(c) > 5 and c[5] > 0]
        if set(migrated_pk) != set(expected_pk) or len(migrated_pk) != len(expected_pk):
            raise RuntimeError(f"表 [{table_name}] 主键迁移校验失败：实际字段={migrated_pk}，期望字段={expected_pk}")

        con.execute("COMMIT")
        con.execute("CHECKPOINT")
        log.info(f"✅ [{table_name}] 成功重建主键: {expected_pk}")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        log.exception(f"❌ 表 [{table_name}] 主键迁移失败，已回滚。")
        raise

def ensure_core_tables(con):
    stocks_sql = f"""
        CREATE TABLE IF NOT EXISTS {STOCKS_TABLE} (
            tradedate DATE,
            symbol VARCHAR,
            high FLOAT,
            low FLOAT,
            open FLOAT,
            close FLOAT,
            adjclose FLOAT,
            volume FLOAT,
            amount FLOAT,
            PRIMARY KEY (symbol, tradedate)
        )
    """
    _check_and_fix_pk(con, STOCKS_TABLE, ["symbol", "tradedate"], stocks_sql)

    adj_sql = f"""
        CREATE TABLE IF NOT EXISTS {ADJUSTMENT_FACTORS_TABLE} (
            tradedate DATE,
            symbol VARCHAR,
            hfq_factor DOUBLE,
            PRIMARY KEY (symbol, tradedate)
        )
    """
    _check_and_fix_pk(con, ADJUSTMENT_FACTORS_TABLE, ["symbol", "tradedate"], adj_sql)

    # 针对旧库回填 factor
    con.execute(f"""
        INSERT OR IGNORE INTO {ADJUSTMENT_FACTORS_TABLE} (tradedate, symbol, hfq_factor)
        SELECT tradedate, symbol, adjclose / NULLIF(close, 0)
        FROM {STOCKS_TABLE}
        WHERE close > 0 AND adjclose > 0
    """)

def ensure_strategy_tables(con):
    pending_sql = """
        CREATE TABLE IF NOT EXISTS pending_orders (
            symbol VARCHAR,
            signal_date DATE,
            planned_buy_price DOUBLE,
            signal_close DOUBLE,
            trade_type TINYINT,
            status TINYINT,
            signal_strength DOUBLE,
            atr_pct DOUBLE,
            PRIMARY KEY (symbol, signal_date)
        )
    """
    _check_and_fix_pk(con, "pending_orders", ["symbol", "signal_date"], pending_sql)

    port_sql = """
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            symbol VARCHAR PRIMARY KEY,
            buy_date DATE,
            buy_price DOUBLE,
            buy_price_hfq DOUBLE,
            shares BIGINT,
            atr_pct_buy DOUBLE,
            highest_price_hfq DOUBLE
        )
    """
    _check_and_fix_pk(con, "virtual_portfolio", ["symbol"], port_sql)

    account_hist_sql = """
        CREATE TABLE IF NOT EXISTS account_history (
            date DATE PRIMARY KEY,
            total_assets DOUBLE,
            available_cash DOUBLE,
            daily_pnl DOUBLE,
            daily_ret DOUBLE,
            market_value DOUBLE
        )
    """
    _check_and_fix_pk(con, "account_history", ["date"], account_hist_sql)

    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            symbol VARCHAR,
            trade_type TINYINT,
            signal_date DATE,
            trade_date DATE,
            price DOUBLE,
            shares BIGINT,
            reason INTEGER,
            pnl_pct DOUBLE,
            fee DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS account_state (
            id INTEGER PRIMARY KEY,
            init_capital DOUBLE,
            total_assets DOUBLE,
            available_cash DOUBLE,
            updated_at DATE
        )
    """)
    cnt = con.execute("SELECT count(*) FROM account_state").fetchone()[0]
    if cnt == 0:
        con.execute(f"INSERT INTO account_state(id, init_capital, total_assets, available_cash, updated_at) VALUES (1, {CONFIG['init_cash']}, {CONFIG['init_cash']}, {CONFIG['init_cash']}, CURRENT_DATE)")

def initialize_empty_database(db_path: str):
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        con.execute("CHECKPOINT")

# =========================================================
# 行情数据获取与写入（安全幂等）
# =========================================================
def prepare_latest_qlib_data() -> str:
    session = build_retry_session()
    if not (os.path.exists(QLIB_TAR_PATH) and (time.time() - os.path.getmtime(QLIB_TAR_PATH)) <= CONFIG["source_cache_ttl_seconds"]):
        log.info(f"⬇️ 下载最新行情压缩包: {QLIB_DATA_URL}")
        with session.get(QLIB_DATA_URL, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total_size = int(resp.headers.get("content-length", 0))
            with open(QLIB_TAR_PATH, "wb") as f:
                with tqdm(total=total_size, unit="B", unit_scale=True, desc="⬇️ 下载行情") as pbar:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
    stamp_file = os.path.join(QLIB_DATA_DIR, ".tar_mtime")
    if not (os.path.exists(stamp_file) and float(open(stamp_file).read().strip()) >= os.path.getmtime(QLIB_TAR_PATH)):
        os.makedirs(QLIB_DATA_DIR, exist_ok=True)
        with tarfile.open(QLIB_TAR_PATH, "r:gz") as tar:
            for member in tar.getmembers():
                parts = member.name.split("/", 1)
                if len(parts) >= 2 and parts[1]:
                    out_path = os.path.normpath(os.path.join(QLIB_DATA_DIR, parts[1]))
                    if os.path.abspath(out_path).startswith(os.path.abspath(QLIB_DATA_DIR)):
                        if member.isdir():
                            os.makedirs(out_path, exist_ok=True)
                        else:
                            os.makedirs(os.path.dirname(out_path), exist_ok=True)
                            source = tar.extractfile(member)
                            if source:
                                with open(out_path, "wb") as fo:
                                    shutil.copyfileobj(source, fo)
        with open(stamp_file, "w") as f:
            f.write(str(os.path.getmtime(QLIB_TAR_PATH)))
    return QLIB_DATA_DIR

def ensure_qlib_initialized(provider_uri: str) -> None:
    global _QLIB_INITIALIZED
    if _QLIB_INITIALIZED:
        return
    import qlib
    qlib.init(provider_uri=provider_uri, region="cn")
    _QLIB_INITIALIZED = True

def fetch_qlib_features(start_date: date, end_date: date) -> pd.DataFrame:
    from qlib.data import D
    instruments = D.instruments(market="all")
    fields = ["$high", "$low", "$open", "$close", "$adjclose", "$volume", "$amount"]
    df = D.features(instruments, fields, start_time=start_date.strftime("%Y-%m-%d"), end_time=end_date.strftime("%Y-%m-%d"))
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.reset_index().rename(columns={
        "datetime": "tradedate", "instrument": "symbol",
        "$high": "high", "$low": "low", "$open": "open",
        "$close": "close", "$adjclose": "adjclose",
        "$volume": "volume", "$amount": "amount",
    })
    out["tradedate"] = pd.to_datetime(out["tradedate"]).dt.date
    out["symbol"] = out["symbol"].map(canonical_symbol)
    for col in ["high", "low", "open", "close", "adjclose", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["open", "high", "low", "close", "adjclose", "amount"]:
        out[col] = out[col].where(out[col] > 0).round(2)
    out["volume"] = out["volume"].round(0)
    out = out.dropna(subset=["tradedate", "symbol", "open", "high", "low", "close", "adjclose"])
    return out[["tradedate", "symbol", "high", "low", "open", "close", "adjclose", "volume", "amount"]]

def get_last_trade_dates_from_qlib(target_date: date, n: int) -> List[date]:
    from qlib.data import D
    start_date = target_date - timedelta(days=max(60, n * 12))
    calendar = D.calendar(start_time=start_date.strftime("%Y-%m-%d"), end_time=target_date.strftime("%Y-%m-%d"), freq="day")
    if calendar is None or len(calendar) == 0:
        return []
    return sorted(pd.to_datetime(calendar).date)[-n:]

def investment_data_sync(db_path: str, target_date: date, trade_days: int) -> Tuple[bool, List[date]]:
    provider_uri = prepare_latest_qlib_data()
    ensure_qlib_initialized(provider_uri)
    trade_dates = get_last_trade_dates_from_qlib(target_date, trade_days)
    if not trade_dates:
        return False, []
    window_df = fetch_qlib_features(trade_dates[0], trade_dates[-1])
    if window_df.empty:
        return False, trade_dates
    window_df = window_df[window_df["tradedate"].isin(set(trade_dates))].copy()

    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        try:
            con.unregister("tmp_new_stocks")
        except Exception:
            pass
        con.register("tmp_new_stocks", window_df)

        # 【核心修复】安全幂等写入：先 DELETE 冲突行，再 INSERT，彻底规避 Binder Error
        con.execute(f"""
            DELETE FROM {STOCKS_TABLE}
            WHERE (symbol, tradedate) IN (SELECT symbol, tradedate FROM tmp_new_stocks)
        """)
        con.execute(f"""
            INSERT INTO {STOCKS_TABLE} (tradedate, symbol, high, low, open, close, adjclose, volume, amount)
            SELECT tradedate, symbol, high, low, open, close, adjclose, volume, amount
            FROM tmp_new_stocks
        """)
        con.execute(f"""
            DELETE FROM {ADJUSTMENT_FACTORS_TABLE}
            WHERE (symbol, tradedate) IN (SELECT symbol, tradedate FROM tmp_new_stocks)
        """)
        con.execute(f"""
            INSERT INTO {ADJUSTMENT_FACTORS_TABLE} (tradedate, symbol, hfq_factor)
            SELECT tradedate, symbol, adjclose / NULLIF(close, 0)
            FROM tmp_new_stocks
            WHERE close > 0 AND adjclose > 0
        """)
        con.execute("CHECKPOINT")
    log.info(f"✅ 行情同步成功: 记录数={len(window_df)}, 覆盖交易日={len(trade_dates)}天")
    return True, trade_dates

# =========================================================
# 高性能复权与缓存管理
# =========================================================
def ensure_hfq_cache_built(con, start_date: date, end_date: date):
    """全量预热构建 HFQ 后复权缓存表（回测全程仅建一次索引）"""
    con.execute("DROP TABLE IF EXISTS daily_hfq_cache")
    con.execute(f"""
        CREATE TABLE daily_hfq_cache AS
        SELECT
            symbol,
            tradedate AS date,
            ROUND(open  * COALESCE(adjclose / NULLIF(close, 0), 1.0), 2) AS open,
            ROUND(high  * COALESCE(adjclose / NULLIF(close, 0), 1.0), 2) AS high,
            ROUND(low   * COALESCE(adjclose / NULLIF(close, 0), 1.0), 2) AS low,
            ROUND(close * COALESCE(adjclose / NULLIF(close, 0), 1.0), 2) AS close,
            volume, amount
        FROM {STOCKS_TABLE}
        WHERE tradedate BETWEEN '{start_date}' AND '{end_date}'
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_hfq_sym_date ON daily_hfq_cache(symbol, date)")

def update_qfq_view(con, as_of_date: date, window_days: int):
    """毫秒级动态切换当前基准交易日前复权视图"""
    start_str = (as_of_date - timedelta(days=int(window_days * 1.6))).strftime('%Y-%m-%d')
    end_str = as_of_date.strftime('%Y-%m-%d')
    con.execute("DROP VIEW IF EXISTS daily_qfq_cache")
    con.execute(f"""
        CREATE VIEW daily_qfq_cache AS
        SELECT
            h.symbol,
            h.date,
            ROUND(h.open  / COALESCE(r.last_ratio, 1.0), 2) AS open,
            ROUND(h.high  / COALESCE(r.last_ratio, 1.0), 2) AS high,
            ROUND(h.low   / COALESCE(r.last_ratio, 1.0), 2) AS low,
            ROUND(h.close / COALESCE(r.last_ratio, 1.0), 2) AS close,
            h.volume, h.amount
        FROM daily_hfq_cache h
        JOIN (
            SELECT symbol, COALESCE(adjclose / NULLIF(close, 0), 1.0) AS last_ratio
            FROM (
                SELECT symbol, adjclose, close,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY tradedate DESC) AS rn
                FROM {STOCKS_TABLE}
                WHERE tradedate <= '{end_str}' AND close > 0
            ) t
            WHERE rn = 1
        ) r ON h.symbol = r.symbol
        WHERE h.date BETWEEN '{start_str}' AND '{end_str}'
    """)

# =========================================================
# 多因子大盘景气度评分与波动率目标仓位
# =========================================================
def _compute_breadth_series(con, trade_date: date, n: int = 10) -> List[float]:
    """近 n 个交易日的市场广度(收盘价站上MA20比例)序列，用于捕捉广度变化速率。"""
    dates_df = con.execute("""
        SELECT DISTINCT date FROM daily_qfq_cache
        WHERE date <= ? ORDER BY date DESC LIMIT ?
    """, [trade_date, n]).df()
    if dates_df.empty:
        return []
    dates_sorted = sorted(pd.to_datetime(dates_df["date"]).dt.date.tolist())

    breadth_vals = []
    for d in dates_sorted:
        row = con.execute("""
            WITH w AS (
                SELECT symbol, date, close,
                       AVG(close) OVER (PARTITION BY symbol ORDER BY date
                                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
                FROM daily_qfq_cache
                WHERE date <= ?
            )
            SELECT AVG(CASE WHEN close > ma20 THEN 1.0 ELSE 0.0 END)
            FROM w WHERE date = ?
        """, [d, d]).fetchone()
        breadth_vals.append(float(row[0]) if row and row[0] is not None else 0.0)
    return breadth_vals


def compute_volatility_target_ratio(
    con,
    trade_date: date,
    market_score: float,
    vol_low: float = 0.08,
    vol_high: float = 0.22,
    ratio_floor: float = 0.15,
    ratio_cap: float = 0.95,
) -> float:
    """
    用市场评分连续映射目标年化波动率，再除以近20日指数实现波动率，
    得到连续的目标仓位比例，替代原先 85%/50%/30% 的三档跳变。
    """
    target_vol_annual = vol_low + (vol_high - vol_low) * float(np.clip(market_score, 0.0, 1.0))

    idx_df = con.execute("""
        SELECT date, close FROM daily_qfq_cache
        WHERE symbol = '000001.SH' AND date <= ?
        ORDER BY date DESC LIMIT 21
    """, [trade_date]).df()

    if len(idx_df) < 15:
        return 0.5

    idx_df = idx_df.sort_values("date")
    daily_ret = idx_df["close"].pct_change().dropna()
    realized_vol_annual = float(daily_ret.std() * np.sqrt(250.0)) if len(daily_ret) > 3 else 0.0

    if realized_vol_annual <= 1e-6:
        return 0.5

    raw_ratio = target_vol_annual / realized_vol_annual
    return float(np.clip(raw_ratio, ratio_floor, ratio_cap))


def get_market_target_position_ratio(con, trade_date: date) -> Tuple[float, str, dict]:
    start_lookback = (trade_date - timedelta(days=120)).strftime("%Y-%m-%d")
    breadth_sql = f"""
    WITH win_data AS (
        SELECT symbol, date, close, volume,
               AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
               AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
               AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_ma20,
               LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
        FROM daily_qfq_cache
        WHERE date >= '{start_lookback}' AND date <= '{trade_date}'
    )
    SELECT 
        COUNT(*) AS total_stocks,
        AVG(CASE WHEN close > ma20 THEN 1.0 ELSE 0.0 END) AS pct_above_ma20,
        AVG(CASE WHEN close > ma60 THEN 1.0 ELSE 0.0 END) AS pct_above_ma60,
        AVG(CASE WHEN close > prev_close THEN 1.0 ELSE 0.0 END) AS pct_advancers,
        COALESCE(SUM(volume), 0) AS cur_vol,
        COALESCE(SUM(vol_ma20), 0) AS base_vol
    FROM win_data
    WHERE date = '{trade_date}'
    """
    row = con.execute(breadth_sql).fetchone()
    if not row or row[0] == 0:
        return 0.30, "数据不足(默认防御30%)", {}

    pct_above_ma20 = float(row[1] or 0.0)
    pct_above_ma60 = float(row[2] or 0.0)
    pct_advancers  = float(row[3] or 0.0)
    cur_vol        = float(row[4] or 0.0)
    base_vol       = float(row[5] or 1.0)

    B = 0.6 * pct_above_ma20 + 0.4 * pct_above_ma60
    vol_ratio = cur_vol / base_vol if base_vol > 0 else 1.0
    V = float(np.clip((vol_ratio - 0.7) / 0.6, 0.0, 1.0))
    M = pct_advancers

    idx_df = con.execute("""
        SELECT date, close FROM daily_qfq_cache
        WHERE symbol = '000001.SH' AND date <= ?
        ORDER BY date DESC LIMIT 30
    """, [trade_date]).df()

    if len(idx_df) >= 20:
        c = idx_df["close"].iloc[0]
        ma5 = idx_df["close"].iloc[:5].mean()
        ma20 = idx_df["close"].iloc[:20].mean()
        T = (0.5 if c >= ma20 else 0.0) + (0.5 if ma5 >= ma20 else 0.0)
    else:
        T = B

    # 广度动量减速项：捕捉"广度绝对水平尚可、但正在快速恶化"的早期转弱信号，
    # 弥补原评分体系只看当日水平、反应滞后的问题。
    try:
        breadth_series = _compute_breadth_series(con, trade_date, n=10)
        if len(breadth_series) >= 5:
            x = np.arange(len(breadth_series))
            slope = float(np.polyfit(x, breadth_series, 1)[0])
            D = float(np.clip(0.5 + slope * 8.0, 0.0, 1.0))
        else:
            D = 0.5
    except Exception:
        D = 0.5

    # 权重重新分配，加入广度动量减速 D，权重合计仍为 1
    S = 0.30 * T + 0.25 * B + 0.15 * V + 0.15 * M + 0.15 * D

    details = {
        "score": round(S, 3), "pct_above_ma20": round(pct_above_ma20 * 100, 1),
        "pct_above_ma60": round(pct_above_ma60 * 100, 1),
        "breadth_decel": round(D, 2),
    }

    # 用连续的波动率目标仓位替代离散三档跳变
    target_ratio = compute_volatility_target_ratio(con, trade_date, S)
    details["vol_target_ratio"] = round(target_ratio * 100.0, 1)

    if S >= 0.65:
        desc = f"🟢 上升行情 (S分:{S:.2f}, 波动目标仓位:{target_ratio*100:.0f}%)"
    elif S >= 0.45:
        desc = f"🟡 震荡行情 (S分:{S:.2f}, 波动目标仓位:{target_ratio*100:.0f}%)"
    else:
        desc = f"🔴 下降行情 (S分:{S:.2f}, 波动目标仓位:{target_ratio*100:.0f}%)"

    return target_ratio, desc, details

def get_account_state(con) -> Tuple[float, float, float]:
    row = con.execute("SELECT init_capital, total_assets, available_cash FROM account_state WHERE id = 1").fetchone()
    if not row:
        cash = float(CONFIG["init_cash"])
        return cash, cash, cash
    return row[0], row[1], row[2]

# =========================================================
# 策略核心：横截面动量因子打分 + 技术形态过滤
# =========================================================
def compute_all_signals(con, target_date: date) -> pd.DataFrame:
    """
    在原有均线/MACD/EMA/BOLL硬性入场过滤基础上，新增：
    1. 横截面动量因子打分（波动率调整动量 + 60/120日动量 + 趋势质量 + 量能强度），
       替代"5类指标全部满足=同分"的做法，让候选股之间产生可比较的相对强弱排序。
    2. 质量/流动性过滤：剔除当日候选池中最不稳定(高波动)与流动性最差的一批标的，
       降低动量崩溃风险。
    """
    start_date = (target_date - timedelta(days=260)).strftime("%Y-%m-%d")
    end_date = target_date.strftime("%Y-%m-%d")
    atr_alpha = float(CONFIG["atr_buy_alpha"])

    sql_base = """
    WITH raw_data AS (
        SELECT symbol, date, high, low, close, volume, amount,
               LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
        FROM daily_qfq_cache
        WHERE date BETWEEN ? AND ?
    ),
    tr_data AS (
        SELECT *,
               GREATEST(high - low, ABS(high - COALESCE(prev_close, close)), ABS(low - COALESCE(prev_close, close))) AS tr
        FROM raw_data
    ),
    indicators AS (
        SELECT symbol, date, high, low, close, volume, amount, prev_close,
               AVG(tr) OVER w14 AS atr14,
               AVG(close) OVER w5  AS ma5,
               AVG(close) OVER w10 AS ma10,
               AVG(close) OVER w20 AS ma20,
               AVG(close) OVER w60 AS ma60,
               STDDEV(close) OVER w20 AS std20,
               AVG(volume) OVER w5 AS vol_ma5,
               AVG(amount) OVER w20 AS amount_ma20,
               STDDEV(close / NULLIF(prev_close, 0) - 1) OVER w20 AS daily_vol20,
               LAG(close, 20) OVER w AS close_20,
               LAG(close, 60) OVER w AS close_60,
               LAG(close, 120) OVER w AS close_120
        FROM tr_data
        WINDOW
            w14 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
            w5  AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
            w10 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            w60 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
            w   AS (PARTITION BY symbol ORDER BY date)
    ),
    boll_data AS (
        SELECT *,
               ma20 AS boll_mid,
               (ma20 + 2 * std20) AS boll_upper,
               (ma20 - 2 * std20) AS boll_lower,
               (4 * std20) AS band_width,
               LAG(ma20, 1) OVER w AS ma20_prev,
               LAG(ma60, 1) OVER w AS ma60_prev,
               LAG(ma20 + 2 * std20, 1) OVER w AS boll_upper_prev,
               LAG(ma20, 1) OVER w AS boll_mid_prev,
               LAG(4 * std20, 1) OVER w AS band_width_prev
        FROM indicators
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
    )
    SELECT *
    FROM boll_data
    WHERE date = ?
      AND close > 0
      AND ma60 IS NOT NULL
      AND atr14 IS NOT NULL
      AND (amount_ma20 IS NULL OR amount_ma20 >= 20000)
      AND close <= ma20 * 1.15
      AND (close - ma20) <= 3.0 * atr14
      AND (close_20 IS NULL OR close / close_20 <= 1.40)
      AND ma5 > ma10 AND ma10 > ma20 AND ma20 > ma60
      AND ma20 >= ma60
      AND (ma20_prev <= ma60_prev OR (ma20 > ma20_prev AND (ma20 - ma60) >= (ma20_prev - ma60_prev)))
      AND (
          (prev_close < boll_mid_prev AND close >= boll_mid)
          OR
          (
              ((close - boll_lower) / NULLIF(boll_upper - boll_lower, 0)) >= 0.75
              AND boll_upper > boll_upper_prev
              AND band_width > band_width_prev
          )
      )
    """
    candidates = con.execute(sql_base, [start_date, end_date, end_date]).df()
    if candidates.empty:
        return pd.DataFrame()

    symbols = candidates['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    hist_df = con.execute(f"""
        SELECT symbol, date, close
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date ASC
    """, symbols + [start_date, end_date]).df()

    valid_records = []
    for sym, grp in hist_df.groupby("symbol"):
        if len(grp) < 60:
            continue
        c_series = grp["close"].astype(float)
        ema5 = c_series.ewm(span=5, adjust=False).mean().iloc[-1]
        ema10 = c_series.ewm(span=10, adjust=False).mean().iloc[-1]
        ema20 = c_series.ewm(span=20, adjust=False).mean().iloc[-1]
        ema12 = c_series.ewm(span=12, adjust=False).mean().iloc[-1]
        ema26 = c_series.ewm(span=26, adjust=False).mean().iloc[-1]

        if not (ema5 > ema10 and ema10 > ema20 and ema12 > ema26):
            continue

        if talib is not None:
            c_vals = c_series.values.astype(np.float64)
            macd_dif, macd_dea, macd_hist = talib.MACD(c_vals, fastperiod=12, slowperiod=26, signalperiod=9)
            dif_val, hist_val = macd_dif[-1], macd_hist[-1]
        else:
            dif_s = c_series.ewm(span=12, adjust=False).mean() - c_series.ewm(span=26, adjust=False).mean()
            dea_s = dif_s.ewm(span=9, adjust=False).mean()
            dif_val = dif_s.iloc[-1]
            hist_val = (dif_s.iloc[-1] - dea_s.iloc[-1]) * 2.0

        if not (dif_val > 0 and hist_val > 0):
            continue

        valid_records.append({"symbol": sym, "dif": dif_val, "macd_hist": hist_val})

    if not valid_records:
        return pd.DataFrame()

    picks = pd.merge(candidates, pd.DataFrame(valid_records), on="symbol", how="inner")
    picks["atr_pct"] = (picks["atr14"] / picks["close"]).round(4)
    picks["planned_buy_price"] = (picks["close"] - atr_alpha * picks["atr14"]).round(2)
    picks["planned_buy_price"] = np.where(picks["planned_buy_price"] <= 0, (picks["close"] * 0.99).round(2), picks["planned_buy_price"])
    picks["ret_20d"] = np.where(picks["close_20"] > 0, (picks["close"] / picks["close_20"]) - 1.0, 0.0)
    picks["pct_b"] = ((picks["close"] - picks["boll_lower"]) / (picks["boll_upper"] - picks["boll_lower"])).round(3)
    picks["signal_strength"] = ((picks["dif"] / picks["close"] * 100) + (picks["volume"] / picks["vol_ma5"])).round(2)
    picks["pattern_tag"] = np.where(picks["prev_close"] < picks["boll_mid_prev"], "🚀升穿布林中轨", "📈沿上轨攀升")
    picks["date"] = pd.to_datetime(picks["date"]).dt.date

    # ---------------------------------------------------------------
    # 质量/流动性过滤：剔除当日候选池中最不稳定与流动性最差的部分标的，
    # 降低动量崩溃风险，而不是靠更多技术指标硬性门槛。
    # ---------------------------------------------------------------
    if len(picks) >= 10:
        vol_cutoff = picks["daily_vol20"].quantile(0.85)
        amount_cutoff = picks["amount_ma20"].quantile(0.30)
        picks = picks[
            (picks["daily_vol20"] <= vol_cutoff)
            & (picks["amount_ma20"] >= amount_cutoff)
        ].copy()
        if picks.empty:
            return pd.DataFrame()

    # ---------------------------------------------------------------
    # 横截面动量因子打分：波动率调整动量 + 60/120日动量 + 趋势质量 + 量能强度。
    # 用排名(rank pct)组合，避免单一指标量纲差异主导排序。
    # ---------------------------------------------------------------
    picks["mom_60"] = np.where(picks["close_60"] > 0, (picks["close"] / picks["close_60"]) - 1.0, np.nan)
    picks["mom_120"] = np.where(picks["close_120"] > 0, (picks["close"] / picks["close_120"]) - 1.0, np.nan)
    picks["risk_adj_mom"] = picks["mom_60"] / picks["daily_vol20"].clip(lower=1e-4)
    picks["trend_quality"] = (picks["close"] - picks["ma60"]) / picks["ma60"]

    amount_mean = picks["amount_ma20"].mean()
    amount_std = picks["amount_ma20"].std()
    picks["amount_zscore"] = (picks["amount_ma20"] - amount_mean) / (amount_std if amount_std and amount_std > 1e-6 else 1.0)

    for col in ["mom_60", "mom_120", "risk_adj_mom", "trend_quality", "amount_zscore"]:
        picks[f"{col}_rank"] = picks[col].rank(pct=True).fillna(0.5)

    picks["factor_score"] = (
        0.30 * picks["risk_adj_mom_rank"]
        + 0.20 * picks["mom_60_rank"]
        + 0.15 * picks["mom_120_rank"]
        + 0.20 * picks["trend_quality_rank"]
        + 0.15 * picks["amount_zscore_rank"]
    )

    # 将原有的"20日涨幅+信号强度"打分与因子打分各按排名占比混合，
    # 既保留原始信号，又引入独立的横截面强弱排序，避免单一指标主导。
    legacy_score_raw = (picks["ret_20d"] * 100.0 + picks["signal_strength"])
    picks["legacy_score_rank"] = legacy_score_raw.rank(pct=True)
    picks["total_score"] = (
        0.5 * picks["legacy_score_rank"] * 100.0
        + 0.5 * picks["factor_score"] * 100.0
    ).round(2)

    return picks[[
        "symbol", "date", "close", "planned_buy_price", "atr_pct", "pct_b",
        "ret_20d", "total_score", "signal_strength", "factor_score", "pattern_tag"
    ]]

# =========================================================
# 交易撮合、强平再平衡与执行引擎
# =========================================================
def enforce_position_rebalance(con, trade_date: date, max_allowed_equity: float):
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy FROM virtual_portfolio").df()
    if holdings.empty:
        return
    raw_today = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [trade_date]).df()
    price_map = dict(zip(raw_today["symbol"].map(canonical_symbol), raw_today["close"]))
    holdings["current_price"] = holdings["symbol"].map(price_map).fillna(holdings["buy_price"])
    holdings["market_val"] = holdings["shares"] * holdings["current_price"]
    current_total_val = holdings["market_val"].sum()
    excess_val = current_total_val - max_allowed_equity
    if excess_val <= 1000.0:
        return

    holdings["pnl_pct"] = (holdings["current_price"] - holdings["buy_price"]) / holdings["buy_price"] * 100.0
    holdings = holdings.sort_values("pnl_pct", ascending=True)
    val_reduced = 0.0
    sell_fee_rate = float(CONFIG["sell_fee_rate"])

    for _, row in holdings.iterrows():
        if val_reduced >= excess_val:
            break
        sym, shares, price = row["symbol"], int(row["shares"]), float(row["current_price"])
        needed = excess_val - val_reduced
        sell_shares = shares if (shares * price <= needed or (needed / (shares * price)) > 0.6) else max(int(needed / (price * 100.0)) * 100, 100)
        sell_shares = min(sell_shares, shares)
        gross_cash = round(sell_shares * price, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)

        if sell_shares == shares:
            con.execute("DELETE FROM virtual_portfolio WHERE symbol=?", [sym])
        else:
            con.execute("UPDATE virtual_portfolio SET shares = shares - ? WHERE symbol=?", [sell_shares, sym])

        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, trade_date, price, sell_shares, REASON_REBALANCE, round(row["pnl_pct"], 2), sell_fee])
        con.execute("UPDATE account_state SET available_cash = available_cash + ? WHERE id=1", [recovered_cash])
        val_reduced += gross_cash

def process_exit_rules(con, trade_date: date):
    """
    分级离场规则：
    - 硬性风险控制（初始ATR止损、保本止损、移动跟踪止盈、阶段硬止盈、最长持有期）
      依旧触发全额清仓，保证尾部风险可控。
    - 趋势结构走弱（连续两日跌破MA20 且 MA5<MA10）改为只减仓50%，
      避免单日正常回踩被当作趋势反转清空整仓，降低误杀交易与换手率。
    """
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy, highest_price_hfq FROM virtual_portfolio").df()
    if holdings.empty:
        return
    holdings["buy_date"] = pd.to_datetime(holdings["buy_date"]).dt.date
    holdings = holdings[holdings["buy_date"] < trade_date].copy()
    if holdings.empty:
        return

    symbols = holdings['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    start_date = (trade_date - timedelta(days=70)).strftime('%Y-%m-%d')
    qfq_df = con.execute(f"""
        SELECT symbol, date, open, close,
               AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5,
               AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma10,
               AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date ASC
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()

    hfq_df = con.execute(f"""
        SELECT symbol, date, close FROM daily_hfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date ASC
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()

    raw_today = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [trade_date]).df()
    raw_map = dict(zip(raw_today["symbol"].map(canonical_symbol), raw_today["close"])) if not raw_today.empty else {}

    full_exit_rows = []
    partial_exit_rows = []
    sell_fee_rate = float(CONFIG["sell_fee_rate"])
    atr_stop_beta = float(CONFIG["atr_stop_loss_beta"])
    trailing_mult = float(CONFIG["trailing_stop_atr_mult"])
    target_tp = float(CONFIG["take_profit_pct"])
    partial_exit_ratio = 0.5

    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = row['buy_date']
        buy_price = float(row['buy_price'])
        buy_price_hfq = float(row['buy_price_hfq'] or buy_price)
        shares = int(row['shares'])
        atr_pct_buy = float(row['atr_pct_buy'] or 0.03)
        highest_hfq = float(row['highest_price_hfq'] or buy_price_hfq)

        gq = qfq_df[qfq_df['symbol'] == sym]
        if len(gq) < 2:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym]

        last_row = gq.iloc[-1]
        prev_row = gq.iloc[-2]
        last_close_qfq = float(last_row['close'])
        last_open_qfq = float(last_row['open'])
        last_ma5 = float(last_row['ma5'])
        last_ma10 = float(last_row['ma10'])
        last_ma20 = float(last_row['ma20'])
        prev_close_qfq = float(prev_row['close'])
        prev_ma20 = float(prev_row['ma20'])
        last_close_hfq = float(gh.iloc[-1]['close']) if not gh.empty else last_close_qfq

        if last_close_hfq > highest_hfq:
            highest_hfq = last_close_hfq
            con.execute("UPDATE virtual_portfolio SET highest_price_hfq = ? WHERE symbol = ?", [highest_hfq, sym])

        curr_pnl = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100.0 if buy_price_hfq > 0 else 0.0
        peak_pnl = (highest_hfq - buy_price_hfq) / buy_price_hfq * 100.0 if buy_price_hfq > 0 else 0.0

        hard_reason = 0
        if curr_pnl <= -1.0 * atr_stop_beta * atr_pct_buy * 100.0:
            hard_reason |= REASON_STOPLOSS
        if peak_pnl >= 8.0 and curr_pnl <= 0.3:
            hard_reason |= REASON_BREAKEVEN
        if peak_pnl >= 12.0 and (peak_pnl - curr_pnl) >= trailing_mult * atr_pct_buy * 100.0:
            hard_reason |= REASON_TAKE_PROFIT
        if curr_pnl >= target_tp and last_close_qfq < last_open_qfq:
            hard_reason |= REASON_TAKE_PROFIT
        if (trade_date - buy_date).days >= CONFIG['max_hold_days']:
            hard_reason |= REASON_MAX_HOLD

        if hard_reason > 0:
            sell_price_raw = raw_map.get(sym, last_close_qfq)
            full_exit_rows.append((sym, trade_date, last_close_qfq, shares, hard_reason, round(curr_pnl, 2), sell_price_raw))
            continue

        # 趋势结构确认破坏：连续两日跌破MA20 且 短均线拐头，只减仓50%，
        # 避免单日回踩被误判为趋势反转。
        trend_broken_confirmed = (
            last_close_qfq < last_ma20
            and prev_close_qfq < prev_ma20
            and last_ma5 < last_ma10
        )
        if trend_broken_confirmed:
            sell_shares = int(shares * partial_exit_ratio / 100.0) * 100
            sell_shares = max(sell_shares, 100)
            sell_price_raw = raw_map.get(sym, last_close_qfq)
            if sell_shares >= shares:
                full_exit_rows.append((sym, trade_date, last_close_qfq, shares, REASON_BELOW_MA20, round(curr_pnl, 2), sell_price_raw))
            else:
                partial_exit_rows.append((sym, trade_date, last_close_qfq, sell_shares, REASON_BELOW_MA20, round(curr_pnl, 2), sell_price_raw))

    for sym, sell_date, sell_price, shares, reason, pnl_pct, sell_price_raw in full_exit_rows:
        con.execute('DELETE FROM virtual_portfolio WHERE symbol=?', [sym])
        gross_cash = round(shares * sell_price_raw, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, sell_date, round(sell_price, 2), shares, reason, pnl_pct, round(sell_fee, 2)])
        con.execute("UPDATE account_state SET available_cash = available_cash + ? WHERE id=1", [recovered_cash])

    for sym, sell_date, sell_price, shares, reason, pnl_pct, sell_price_raw in partial_exit_rows:
        con.execute('UPDATE virtual_portfolio SET shares = shares - ? WHERE symbol=?', [shares, sym])
        gross_cash = round(shares * sell_price_raw, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, sell_date, round(sell_price, 2), shares, reason, pnl_pct, round(sell_fee, 2)])
        con.execute("UPDATE account_state SET available_cash = available_cash + ? WHERE id=1", [recovered_cash])

def process_pending_orders(con, trade_date: date, max_allowed_equity: float):
    pending_df = con.execute("""
        SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status, signal_strength, atr_pct
        FROM pending_orders
        WHERE status=0 AND signal_date < ?
        ORDER BY signal_date, symbol
    """, [trade_date]).df()
    if pending_df.empty:
        return

    qfq_today = con.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_qfq_cache WHERE date = ?", [trade_date]).df()
    hfq_today = con.execute("SELECT symbol, date, close FROM daily_hfq_cache WHERE date = ?", [trade_date]).df()
    if qfq_today.empty:
        return

    init_cap, total_assets, avail_cash = get_account_state(con)
    current_market_val = con.execute("""
        SELECT COALESCE(SUM(p.shares * s.close), 0)
        FROM virtual_portfolio p
        JOIN stock_prices s ON p.symbol = s.symbol AND s.tradedate = ?
    """, [trade_date]).fetchone()[0]

    remaining_capacity = max_allowed_equity - current_market_val
    if remaining_capacity <= 0:
        return

    max_position = int(CONFIG["max_position_stocks"])
    base_budget = min(CONFIG['position_cash_yuan'], total_assets / max_position)
    q_map = {row['symbol']: row for _, row in qfq_today.iterrows()}
    h_map = {row['symbol']: row for _, row in hfq_today.iterrows()}
    current_holdings = con.execute("SELECT COUNT(*) FROM virtual_portfolio").fetchone()[0]
    holding_symbols = {r[0] for r in con.execute("SELECT symbol FROM virtual_portfolio").fetchall()}
    filled_rows = []

    for _, row in pending_df.iterrows():
        symbol = row['symbol']
        signal_date = row['signal_date']
        planned_buy_price = float(row['planned_buy_price'])
        atr_pct = float(row['atr_pct'] or 0.03)

        n_days = con.execute(f"SELECT COUNT(DISTINCT tradedate) FROM {STOCKS_TABLE} WHERE tradedate > ? AND tradedate <= ?", [signal_date, trade_date]).fetchone()[0]
        if n_days > CONFIG["buy_signal_expire_days"] or symbol in holding_symbols or (symbol in q_map) == False:
            continue

        row_t1 = q_map[symbol]
        today_open, today_low, today_close = float(row_t1['open']), float(row_t1['low']), float(row_t1['close'])
        today_vol = float(row_t1['volume']) * 100.0

        if today_low > planned_buy_price:
            continue

        actual_buy_price = round(min(today_open, planned_buy_price) * (1.0 + CONFIG["slippage_rate"]), 2)
        risk_weight = np.clip(0.03 / atr_pct, 0.5, 2.0)
        target_cash = min(base_budget * risk_weight, remaining_capacity)

        if current_holdings >= max_position or avail_cash < target_cash * 0.5:
            continue

        lot_cost = actual_buy_price * 100.0 * (1.0 + CONFIG["buy_fee_rate"])
        shares = int(min(target_cash, avail_cash) / lot_cost) * 100
        if today_vol > 0 and shares > int(today_vol * CONFIG["max_volume_participate_rate"]):
            shares = int(today_vol * CONFIG["max_volume_participate_rate"] / 100.0) * 100
        if shares < 100:
            continue

        total_cost = round(shares * actual_buy_price * (1.0 + CONFIG["buy_fee_rate"]), 2)
        if total_cost > avail_cash:
            continue

        factor = (float(h_map[symbol]['close']) / today_close) if (symbol in h_map and today_close > 0) else 1.0
        buy_price_hfq = round(actual_buy_price * factor, 2)
        avail_cash -= total_cost
        current_holdings += 1
        remaining_capacity -= total_cost
        filled_rows.append((symbol, trade_date, actual_buy_price, buy_price_hfq, shares, round(shares * actual_buy_price * CONFIG["buy_fee_rate"], 2), atr_pct))

    if filled_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
        con.executemany("DELETE FROM virtual_portfolio WHERE symbol = ?", [(r[0],) for r in filled_rows])
        con.executemany("""
            INSERT INTO virtual_portfolio(symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy, highest_price_hfq)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(s, d, bp, bph, sh, atr, bph) for s, d, bp, bph, sh, _, atr in filled_rows])
        for s, d, bp, bph, sh, fee, _ in filled_rows:
            con.execute(f"UPDATE pending_orders SET status={STATUS_FILLED} WHERE symbol=? AND signal_date<? AND status={STATUS_PENDING}", [s, trade_date])
            con.execute("""
                INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
                VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, ?)
            """, [s, TRADE_BUY, d, bp, sh, REASON_BUY_T1, fee])

# =========================================================
# 单日评测与统计
# =========================================================
def evaluate_strategy_day(db_path: str, target_date: date, allow_exit: bool = True):
    with duckdb.connect(db_path, read_only=False) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)

        target_pos_ratio, regime_desc, regime_details = get_market_target_position_ratio(con, target_date)
        init_cap, total_assets, avail_cash = get_account_state(con)
        max_allowed_equity = total_assets * target_pos_ratio

        if allow_exit:
            process_exit_rules(con, target_date)
            enforce_position_rebalance(con, target_date, max_allowed_equity)

        process_pending_orders(con, target_date, max_allowed_equity)

        df_picks = compute_all_signals(con, target_date)
        if not df_picks.empty:
            df_picks = df_picks.sort_values(["total_score", "symbol"], ascending=[False, True]).head(CONFIG["top_n"]).reset_index(drop=True)

        con.execute(f"""
            UPDATE pending_orders SET status={STATUS_EXPIRED}
            WHERE status={STATUS_PENDING} AND signal_date <= (
                SELECT tradedate FROM {STOCKS_TABLE}
                WHERE tradedate < ? ORDER BY tradedate DESC LIMIT 1 OFFSET {CONFIG['buy_signal_expire_days']}
            )
        """, [target_date])

        holdings_df = con.execute("SELECT symbol FROM virtual_portfolio").df()
        holding_symbols = set(holdings_df["symbol"]) if not holdings_df.empty else set()
        new_orders = []
        if not df_picks.empty:
            for _, row in df_picks.iterrows():
                if row["symbol"] not in holding_symbols:
                    new_orders.append((row["symbol"], target_date, round(float(row["planned_buy_price"]), 2), round(float(row["close"]), 2), TRADE_BUY, STATUS_PENDING, float(row["signal_strength"]), float(row["atr_pct"])))
            if new_orders:
                con.executemany("DELETE FROM pending_orders WHERE symbol = ? AND signal_date = ?", [(r[0], r[1]) for r in new_orders])
                con.executemany("INSERT INTO pending_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", new_orders)

        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
            total_market_val = 0.0
        else:
            raw_today_df = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [target_date]).df()
            hfq_today_df = con.execute("SELECT symbol, close FROM daily_hfq_cache WHERE date = ?", [target_date]).df()
            df_portfolio = holdings.merge(raw_today_df.rename(columns={"close": "last_price"}), on="symbol", how="left")
            df_portfolio = df_portfolio.merge(hfq_today_df.rename(columns={"close": "last_price_hfq"}), on="symbol", how="left")
            df_portfolio["last_price"] = df_portfolio["last_price"].fillna(df_portfolio["buy_price"])
            df_portfolio["last_price_hfq"] = df_portfolio["last_price_hfq"].fillna(df_portfolio["buy_price_hfq"])
            df_portfolio["shares"] = (np.round(df_portfolio["shares"].astype(float) / 100) * 100).astype(int)
            df_portfolio["market_value"] = (df_portfolio["last_price"].astype(float) * df_portfolio["shares"]).astype(float)
            df_portfolio["pnl_pct"] = (df_portfolio["last_price_hfq"] - df_portfolio["buy_price_hfq"]) / df_portfolio["buy_price_hfq"] * 100.0
            total_market_val = df_portfolio["market_value"].sum()

        init_cap, _, avail_cash = get_account_state(con)
        new_total_assets = avail_cash + total_market_val
        con.execute("UPDATE account_state SET total_assets=?, updated_at=? WHERE id=1", [new_total_assets, target_date])

        prev_row = con.execute("SELECT total_assets FROM account_history WHERE date < ? ORDER BY date DESC LIMIT 1", [target_date]).fetchone()
        prev_assets = prev_row[0] if prev_row else init_cap
        daily_pnl = new_total_assets - prev_assets
        daily_ret = daily_pnl / prev_assets if prev_assets > 0 else 0.0

        con.execute("DELETE FROM account_history WHERE date = ?", [target_date])
        con.execute("""
            INSERT INTO account_history(date, total_assets, available_cash, daily_pnl, daily_ret, market_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [target_date, round(new_total_assets, 2), round(avail_cash, 2), round(daily_pnl, 2), round(daily_ret, 4), round(total_market_val, 2)])

        metrics = {
            "total_assets": new_total_assets, "avail_cash": avail_cash,
            "market_value": total_market_val,
            "position_pct": total_market_val / new_total_assets * 100.0 if new_total_assets > 0 else 0.0,
            "cash_pct": avail_cash / new_total_assets * 100.0 if new_total_assets > 0 else 100.0,
            "total_pnl": new_total_assets - init_cap,
            "total_pnl_pct": (new_total_assets / init_cap - 1.0) * 100.0,
            "daily_pnl": daily_pnl, "daily_ret": daily_ret,
            "market_regime": regime_desc, "target_pos_limit": target_pos_ratio * 100.0,
            "regime_details": regime_details,
        }
        df_pending = con.execute(f"SELECT * FROM pending_orders WHERE status={STATUS_PENDING} ORDER BY signal_date DESC").df()
        df_trades = con.execute("SELECT * FROM trade_history WHERE trade_date = ? ORDER BY trade_type, symbol", [target_date]).df()

    return decode_numeric_frame(df_picks), decode_numeric_frame(df_portfolio), decode_numeric_frame(df_pending), decode_numeric_frame(df_trades), metrics

# =========================================================
# 回测统计分析与专业图表绘制
# =========================================================
def compute_backtest_analytics_and_chart(db_path: str, init_cap: float) -> Tuple[dict, Optional[str]]:
    with duckdb.connect(db_path) as con:
        hist_df = con.execute("SELECT date, daily_ret, total_assets FROM account_history ORDER BY date ASC").df()
        trades_df = con.execute(f"SELECT * FROM trade_history WHERE trade_type = {TRADE_SELL}").df()

    if hist_df.empty or len(hist_df) < 5:
        return {}, None

    hist_df['date'] = pd.to_datetime(hist_df['date'])
    assets_arr = hist_df['total_assets'].values
    n_days = len(hist_df)
    total_ret = (assets_arr[-1] / init_cap) - 1.0
    ann_ret = (1.0 + total_ret) ** (250.0 / max(n_days, 1)) - 1.0

    peaks = np.maximum.accumulate(assets_arr)
    drawdowns = (peaks - assets_arr) / peaks
    max_dd = float(np.max(drawdowns))

    mean_daily = hist_df['daily_ret'].mean()
    std_daily = hist_df['daily_ret'].std()
    sharpe = float((mean_daily - 0.018 / 250.0) / std_daily * np.sqrt(250.0)) if std_daily > 0 else 0.0
    calmar = float(ann_ret / max_dd) if max_dd > 0 else 0.0

    total_trades = len(trades_df)
    win_trades = len(trades_df[trades_df['pnl_pct'] > 0]) if total_trades > 0 else 0
    win_rate = (win_trades / total_trades * 100.0) if total_trades > 0 else 0.0
    gains = trades_df[trades_df['pnl_pct'] > 0]['pnl_pct'].sum() if total_trades > 0 else 0.0
    losses = abs(trades_df[trades_df['pnl_pct'] < 0]['pnl_pct'].sum()) if total_trades > 0 else 0.0
    profit_factor = (gains / losses) if losses > 0 else (gains if gains > 0 else 1.0)

    stats = {
        "backtest_days": n_days, "total_ret": total_ret * 100.0,
        "ann_ret": ann_ret * 100.0, "max_dd": max_dd * 100.0,
        "sharpe": sharpe, "calmar": calmar,
        "total_trades": total_trades, "win_rate": win_rate,
        "profit_factor": round(profit_factor, 2)
    }

    chart_b64 = None
    try:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True, gridspec_kw={'height_ratios': [2.5, 1]})
        net_values = assets_arr / init_cap

        ax1.plot(hist_df['date'], net_values, label=f'策略净值 (Sharpe: {sharpe:.2f})', color='#2563eb', linewidth=1.8)
        ax1.axhline(1.0, linestyle='--', color='#94a3b8', linewidth=1.0)
        ax1.set_title(f"A股多头共振策略 {n_days} 交易日回测 | 累计收益: {total_ret*100:+.1f}% (年化: {ann_ret*100:+.1f}%) | 最大回撤: -{max_dd*100:.1f}%", fontsize=12)
        ax1.grid(True, linestyle=':', alpha=0.6)
        ax1.set_ylabel('净值 (Net Value)')
        ax1.legend(loc='upper left')

        ax2.fill_between(hist_df['date'], -drawdowns * 100.0, 0, color='#ef4444', alpha=0.35, label='动态回撤 (Drawdown %)')
        ax2.set_ylabel('回撤 %')
        ax2.grid(True, linestyle=':', alpha=0.6)
        ax2.legend(loc='lower left')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=120)
        plt.close()
        buf.seek(0)
        chart_b64 = base64.b64encode(buf.read()).decode('utf-8')
    except Exception as exc:
        log.error(f"图表绘制失败: {exc}")

    return stats, chart_b64

# =========================================================
# 邮件报告生成与推送
# =========================================================
def send_email_via_graph(tm: TokenManager, subject: str, html_body: str):
    to_addr = CONFIG["email_to"]
    if not to_addr:
        return
    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
        },
        "saveToSentItems": True,
    }
    session = build_retry_session()
    for attempt in range(1, 4):
        try:
            resp = session.post(f"{GRAPH_BASE}/me/sendMail", headers={**tm.headers(), "Content-Type": "application/json"}, json=message, timeout=60)
            resp.raise_for_status()
            log.info("📧 报告邮件已成功发送")
            return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(attempt * 2)

def generate_and_send_report(tm: TokenManager, df_picks: pd.DataFrame, df_portfolio: pd.DataFrame, df_trades: pd.DataFrame, target_str: str, metrics: dict, bt_stats: dict, chart_b64: Optional[str], mode_title: str):
    CSS = """<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; background:#f0f2f5; color:#333; }
.wrapper { max-width:960px; margin:0 auto; padding:20px; }
.header  { background:linear-gradient(135deg,#0f172a 0%,#1e293b 50%,#334155 100%); border-radius:16px; padding:24px 28px; margin-bottom:18px; color:#fff; display:flex; justify-content:space-between; align-items:center; }
.kpi-row { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:14px; }
.kpi-card { background:#fff; border-radius:12px; padding:16px; box-shadow:0 2px 6px rgba(0,0,0,.05); border-left:4px solid #3b82f6; }
.kpi-card.green { border-left-color:#10b981; }
.kpi-card.red   { border-left-color:#ef4444; }
.kpi-value { font-size:18px; font-weight:700; color:#1e293b; margin-top:4px; }
.kpi-label { font-size:12px; color:#64748b; }
.section   { background:#fff; border-radius:12px; padding:20px; margin-bottom:18px; box-shadow:0 2px 6px rgba(0,0,0,.05); }
.section-title { font-size:16px; font-weight:600; color:#1e293b; margin-bottom:14px; }
.data-table { width:100%; border-collapse:collapse; font-size:13px; }
.data-table th { background:#f8fafc; padding:10px; font-weight:600; color:#64748b; border-bottom:2px solid #e2e8f0; text-align:left; }
.data-table td { padding:10px; border-bottom:1px solid #f1f5f9; }
.ret-pos { color:#16a34a; font-weight:600; }
.ret-neg { color:#dc2626; font-weight:600; }
.pill { display:inline-block; background:#eff6ff; border:1px solid #bfdbfe; color:#1d4ed8; border-radius:16px; padding:4px 10px; font-size:12px; margin:3px; }
</style>"""

    picks_rows = []
    if df_picks is not None and not df_picks.empty:
        for i, r in enumerate(df_picks.itertuples(), 1):
            sign = "+" if r.ret_20d >= 0 else ""
            cls = "ret-pos" if r.ret_20d >= 0 else "ret-neg"
            picks_rows.append(f"<tr><td>{i}</td><td><b>{r.symbol}</b></td><td>¥{float(r.close):.2f}</td><td>¥{float(r.planned_buy_price):.2f}</td><td>{float(r.pct_b):.2f}</td><td>{float(r.factor_score):.2f}</td><td><b>{r.pattern_tag}</b></td><td class='{cls}'>{sign}{r.ret_20d*100:.1f}%</td></tr>")
        picks_html = f"<table class='data-table'><thead><tr><th>#</th><th>代码</th><th>收盘</th><th>动态挂单价</th><th>%B位置</th><th>因子分</th><th>形态</th><th>20日涨幅</th></tr></thead><tbody>{''.join(picks_rows)}</tbody></table>"
    else:
        picks_html = '<div style="text-align:center;padding:16px;color:#94a3b8;">今日无符合多头共振标的</div>'

    port_rows = []
    if df_portfolio is not None and not df_portfolio.empty:
        for r in df_portfolio.itertuples():
            port_rows.append(f"<tr><td><b>{r.symbol}</b></td><td>{r.buy_date}</td><td>¥{float(r.buy_price):.2f}</td><td>¥{float(r.last_price):.2f}</td><td>{int(r.shares)}</td><td>¥{r.market_value:,.2f}</td><td class='{'ret-pos' if r.pnl_pct>=0 else 'ret-neg'}'>{r.pnl_pct:+.1f}%</td></tr>")
        port_html = f"<table class='data-table'><thead><tr><th>代码</th><th>买入日</th><th>成本</th><th>现价</th><th>股数</th><th>市值</th><th>盈亏</th></tr></thead><tbody>{''.join(port_rows)}</tbody></table>"
    else:
        port_html = '<div style="text-align:center;padding:16px;color:#94a3b8;">当前空仓持币观望</div>'

    chart_html = f"<div style='text-align:center;margin-top:10px;'><img src='data:image/png;base64,{chart_b64}' style='max-width:100%;border-radius:8px;'/></div>" if chart_b64 else ""

    html = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">{CSS}</head>
<body><div class="wrapper">
<div class="header">
  <div>
    <h2>💴 A股多头共振策略 · {mode_title}</h2>
    <div style="font-size:13px;opacity:0.85;margin-top:6px;">均线多头 + MA20升穿MA60 + MACD>0 + EMA多头 + BOLL沿上轨 + 横截面动量因子 + 波动率目标仓位 + 分级离场</div>
  </div>
  <div style="text-align:right;">
    <div style="font-size:18px;font-weight:700;">{target_str}</div>
    <div style="font-size:12px;opacity:0.9;">{metrics.get('market_regime', '')}</div>
  </div>
</div>

<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-label">最新总资产</div><div class="kpi-value">¥{metrics.get('total_assets', 0):,.0f}</div></div>
  <div class="kpi-card green"><div class="kpi-label">可用余额</div><div class="kpi-value">¥{metrics.get('avail_cash', 0):,.0f}</div></div>
  <div class="kpi-card"><div class="kpi-label">当前仓位 / 受控上限</div><div class="kpi-value">{metrics.get('position_pct', 0):.1f}% / {metrics.get('target_pos_limit', 30):.0f}%</div></div>
  <div class="kpi-card {'green' if metrics.get('daily_pnl', 0)>=0 else 'red'}"><div class="kpi-label">当日盈亏</div><div class="kpi-value">¥{metrics.get('daily_pnl', 0):,.0f}</div></div>
</div>

<div class="section">
  <div class="section-title">📊 历史策略回测绩效统计 (近 {bt_stats.get('backtest_days', 0)} 交易日)</div>
  <div class="kpi-row" style="margin-bottom:0;">
    <div class="kpi-card"><div class="kpi-label">累计收益率</div><div class="kpi-value {'ret-pos' if bt_stats.get('total_ret',0)>=0 else 'ret-neg'}">{bt_stats.get('total_ret', 0):+.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">年化收益率</div><div class="kpi-value {'ret-pos' if bt_stats.get('ann_ret',0)>=0 else 'ret-neg'}">{bt_stats.get('ann_ret', 0):+.1f}%</div></div>
    <div class="kpi-card red"><div class="kpi-label">最大回撤 (MaxDD)</div><div class="kpi-value">-{bt_stats.get('max_dd', 0):.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">夏普 / 卡玛比率</div><div class="kpi-value">{bt_stats.get('sharpe', 0):.2f} / {bt_stats.get('calmar', 0):.2f}</div></div>
  </div>
  <div style="font-size:12px;color:#64748b;margin-top:10px;">
    交易统计：总交易 {bt_stats.get('total_trades', 0)} 笔 | 胜率 {bt_stats.get('win_rate', 0):.1f}% | 利润因子(PF) {bt_stats.get('profit_factor', 1.0):.2f}
  </div>
  {chart_html}
</div>

<div class="section">
  <div class="section-title">💼 当前账户持仓</div>
  {port_html}
</div>

<div class="section">
  <div class="section-title">🔍 今日多头共振选股池 (Top {CONFIG['top_n']})</div>
  {picks_html}
</div>
</div></body></html>"""

    send_email_via_graph(tm, f"💴 CN量化策略[{mode_title}] - {target_str}", html)

# =========================================================
# 自动化执行总调度
# =========================================================
def run_daily_pipeline():
    tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
    odc = OneDriveClient(tm, CONFIG["onedrive_folder"], CONFIG["cloud_db_gz_name"])
    target_date = get_target_date()
    log.info(f"🚀 量化策略执行启动 target_date={target_date}")

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "CN_stock.duckdb")
        gz_path = os.path.join(tmp, CONFIG["cloud_db_gz_name"])

        has_cloud_db = odc.download_database_gz(gz_path)
        if has_cloud_db:
            with gzip.open(gz_path, "rb") as fi, open(db_path, "wb") as fo:
                shutil.copyfileobj(fi, fo)
            log.info("✅ 成功载入云端历史数据库")
        else:
            initialize_empty_database(db_path)
            log.info("ℹ️ 云端无数据库，初始化空库并准备执行 200 交易日回测")

        # 同步足够跨度的行情数据
        synced, all_trade_dates = investment_data_sync(db_path, target_date, int(CONFIG["update_window_trade_days"]))
        if not synced or not all_trade_dates:
            log.error("❌ 行情更新失败，终止流程")
            return

        with duckdb.connect(db_path) as con:
            ensure_core_tables(con)
            ensure_strategy_tables(con)
            hist_count = con.execute("SELECT COUNT(*) FROM account_history").fetchone()[0]
            max_hist_date_row = con.execute("SELECT MAX(date) FROM account_history").fetchone()
            max_hist_date = max_hist_date_row[0] if max_hist_date_row and max_hist_date_row[0] else None

        # 构建基础 HFQ 缓存
        with duckdb.connect(db_path) as con:
            ensure_hfq_cache_built(con, all_trade_dates[0], all_trade_dates[-1])

        # 判定执行模式：200日回测模式 OR 日常增量模式
        if hist_count == 0:
            replay_n = int(CONFIG["initial_replay_trade_days"])
            replay_dates = all_trade_dates[-replay_n:]
            log.info(f"🧱 [冷启动回测] 数据库无历史记录，开始执行近 {len(replay_dates)} 个交易日回放回测 ({replay_dates[0]} ~ {replay_dates[-1]})")
            mode_title = f"冷启动回测 ({len(replay_dates)}日)"

            for idx, d in enumerate(tqdm(replay_dates, desc="⏳ 200交易日策略回测进度", unit="天")):
                with duckdb.connect(db_path) as con:
                    update_qfq_view(con, d, CONFIG["adjust_cache_days"])
                evaluate_strategy_day(db_path, d, allow_exit=(idx > 0))
            latest_day = replay_dates[-1]
        else:
            pending_dates = [d for d in all_trade_dates if d > max_hist_date and d <= target_date]
            if not pending_dates:
                log.info(f"⚡ 数据已是最新 (最新记录: {max_hist_date})，无需增量补算")
                latest_day = max_hist_date
                mode_title = "日常运行 (已同步)"
            else:
                log.info(f"⚡ [日常增量模式] 补齐 {len(pending_dates)} 个交易日 ({pending_dates[0]} ~ {pending_dates[-1]})")
                mode_title = "日常增量调度"
                for d in pending_dates:
                    with duckdb.connect(db_path) as con:
                        update_qfq_view(con, d, CONFIG["adjust_cache_days"])
                    evaluate_strategy_day(db_path, d, allow_exit=True)
                latest_day = pending_dates[-1]

        # 提取最新一天结果用于报表呈现
        with duckdb.connect(db_path) as con:
            update_qfq_view(con, latest_day, CONFIG["adjust_cache_days"])
        df_picks, df_portfolio, df_pending, df_trades, metrics = evaluate_strategy_day(db_path, latest_day, allow_exit=False)

        # 统计回测指标并生成绩效曲线图
        bt_stats, chart_b64 = compute_backtest_analytics_and_chart(db_path, CONFIG["init_cash"])

        log.info(f"🎉 任务完成: 累计收益: {bt_stats.get('total_ret', 0):+.1f}%, 最大回撤: -{bt_stats.get('max_dd', 0):.1f}%, 胜率: {bt_stats.get('win_rate', 0):.1f}%")
        generate_and_send_report(tm, df_picks, df_portfolio, df_trades, latest_day.strftime("%Y-%m-%d"), metrics, bt_stats, chart_b64, mode_title)

        # 压缩上传云端
        with duckdb.connect(db_path) as con:
            con.execute("CHECKPOINT")
            try:
                con.execute("VACUUM")
            except Exception:
                pass
        with open(db_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=6) as fo:
            shutil.copyfileobj(fi, fo)
        odc.upload_database_gz(gz_path)
        log.info("☁️ 更新后数据库已压缩并同步至 OneDrive")

def main():
    try:
        run_daily_pipeline()
    except Exception as exc:
        log.exception("❌ 执行失败: %s", exc)
        raise

if __name__ == "__main__":
    main()
