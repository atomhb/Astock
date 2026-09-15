#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1. 行情数据统一落到 stocks 表，使用 investment_data 最新发布包。
2. 选股策略：
   - 均线多头排列 (MA5 > MA10 > MA20 > MA60)
   - MA20 升穿 / 金叉向上发散 MA60
   - MACD 大于 0 (DIF > 0 且 HIST > 0)
   - EMA 多头排列 (EMA5 > EMA10 > EMA20 且 EMA12 > EMA26)
   - BOLL %B >= 0.75 带宽发散沿上轨上升 或 升穿中轨
   - 过滤高位过热与僵尸流动性
3. 仓位与风控管理：
   - 多因子宏观景气评分 S (趋势 + 广度 + 量能 + 赚钱效应)
   - 上升行情：持仓 80%-90% (基准 85%)
   - 下降行情：持仓上限刚性压缩至 30%（触发强制减仓再平衡）
   - 阶梯止盈 + 移动跟踪止损 (Trailing ATR) + MACD 死叉弱化离场
4. 交易撮合：拟真滑点、低开成交价修正、涨跌停禁交易、成交量上限约束。
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

    # 仓位与资金
    "init_cash": 100000.0,
    "position_cash_yuan": 40000.0,
    "max_position_stocks": 6,
    "bull_market_pos_ratio": 0.85,         # 上升行情持仓 80%-90%
    "neutral_market_pos_ratio": 0.50,      # 震荡行情持仓 50%
    "bear_market_pos_ratio": 0.30,         # 下降行情持仓必须 30% (强制降仓)

    # 动态 ATR 与撮合
    "atr_period": 14,
    "atr_buy_alpha": 0.5,                  # 挂单价 = 收盘价 - 0.5 * ATR(14)
    "atr_stop_loss_beta": 2.0,             # 初始止损 = -2.0 * ATR%
    "trailing_stop_atr_mult": 2.5,         # 利润回撤移动止损系数
    "take_profit_pct": 15.0,               # 阶段止盈目标
    "slippage_rate": 0.001,                # 单边滑点 0.1%
    "buy_fee_rate": 0.0005,                # 买入佣金 0.05%
    "sell_fee_rate": 0.0010,               # 卖出税费 0.1%
    "max_volume_participate_rate": 0.05,   # 单笔最大允许占当日成交量 5%
    "buy_signal_expire_days": 2,
    "max_hold_days": 120,

    # 系统运维
    "adjust_cache_days": 360,
    "source_cache_ttl_seconds": 6 * 3600,
    "update_window_trade_days": 180,
    "initial_replay_trade_days": 90,
    "top_n": 15,
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
REASON_REBALANCE = 32        # 熊市刚性降仓再平衡
REASON_TAKE_PROFIT = 64      # 目标止盈 / 移动跟踪止损
REASON_BREAKEVEN = 128       # 保本止损
REASON_BUY_T1 = 256          # T+1多头挂单成交


def decode_trade_type_label(code) -> str:
    return "🟢 买入" if code == TRADE_BUY else "🔴 卖出"


def decode_reason_text(code) -> str:
    if code is None or code == 0:
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
        parts.append("跌破MA20")
    if code & REASON_BELOW_BOLL_MID:
        parts.append("跌破布林中轨")
    if code & REASON_MAX_HOLD:
        parts.append(f"持有满{CONFIG['max_hold_days']}天")
    if code & REASON_BUY_T1:
        parts.append("多头共振挂单成交")
    return " / ".join(parts) if parts else ""

# =========================================================
# 基础工具与 Token / OneDrive
# =========================================================
def get_target_date() -> date:
    now_beijing = datetime.now(CN_TZ)
    return (now_beijing - timedelta(days=1)).date() if now_beijing.hour < 16 else now_beijing.date()

def build_retry_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def split_symbol(symbol: str) -> Tuple[str, str]:
    raw = str(symbol).strip()
    if not raw:
        return "", ""
    if "." in raw:
        left, right = raw.split(".", 1)
        left_l, right_l = left.lower(), right.lower()
        if left_l in {"sh", "sz", "bj"}:
            return left_l, right
        if right_l in {"sh", "sz", "bj"}:
            return right_l, left
    upper = raw.upper()
    if upper.startswith("SH"):
        return "sh", upper[2:]
    if upper.startswith("SZ"):
        return "sz", upper[2:]
    if upper.startswith("BJ"):
        return "bj", upper[2:]
    return "", raw

def canonical_symbol(symbol: str) -> str:
    market, code = split_symbol(symbol)
    return f"{code}.{market.upper()}" if market else str(symbol).strip().upper()

def symbol_market(symbol: str) -> str:
    return split_symbol(symbol)[0]

def symbol_code(symbol: str) -> str:
    return split_symbol(symbol)[1]

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

class TokenManager:
    def __init__(self, client_id: str, token_file: str):
        self.client_id = client_id
        self.token_file = token_file
        self._data = {}
        b64 = os.getenv("ONEDRIVE_TOKEN_CACHE_B64", "").strip()
        if IS_CI and b64:
            clean = b64.replace("\\n", "").replace(" ", "")
            self._data = json.loads(base64.b64decode(clean).decode("utf-8"))
        elif (not IS_CI) and os.path.exists(token_file):
            with open(token_file, "r", encoding="utf-8") as f:
                self._data = json.load(f)

    def _save(self):
        if not IS_CI:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)

    def _refresh(self):
        rt = self._data.get("refresh_token", "")
        if not rt:
            raise RuntimeError("缺少 refresh_token，请先授权")
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
                        except requests.exceptions.RequestException as e:
                            if attempt == 5:
                                raise
                            time.sleep(attempt * 5)
                    offset += len(chunk)
                    pbar.update(len(chunk))

def db_compress_and_upload(odc: OneDriveClient, db_path: str, gz_path: str) -> None:
    with open(db_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    odc.upload_database_gz(gz_path)
    log.info("☁️ 数据库已压缩上传至 OneDrive")

def obtain_db_gz(odc: OneDriveClient, temp_gz_path: str) -> bool:
    return odc.download_database_gz(temp_gz_path)

def load_db_gz_to_local(gz_path: str, db_path: str) -> None:
    with gzip.open(gz_path, "rb") as fi, open(db_path, "wb") as fo:
        shutil.copyfileobj(fi, fo)

# =========================================================
# 数据库表结构
# =========================================================
def ensure_core_tables(con):
    con.execute(f"""
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
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {ADJUSTMENT_FACTORS_TABLE} (
            tradedate DATE,
            symbol VARCHAR,
            hfq_factor DOUBLE,
            PRIMARY KEY (symbol, tradedate)
        )
    """)
    con.execute(f"""
        INSERT OR IGNORE INTO {ADJUSTMENT_FACTORS_TABLE} (tradedate, symbol, hfq_factor)
        SELECT tradedate, symbol, adjclose / NULLIF(close, 0)
        FROM {STOCKS_TABLE}
        WHERE close > 0 AND adjclose > 0
    """)

def ensure_strategy_tables(con):
    con.execute("""
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
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            symbol VARCHAR PRIMARY KEY,
            buy_date DATE,
            buy_price DOUBLE,
            buy_price_hfq DOUBLE,
            shares BIGINT,
            atr_pct_buy DOUBLE,
            highest_price_hfq DOUBLE
        )
    """)
    try:
        con.execute("ALTER TABLE virtual_portfolio ADD COLUMN IF NOT EXISTS highest_price_hfq DOUBLE")
    except Exception:
        pass

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
    con.execute("""
        CREATE TABLE IF NOT EXISTS account_history (
            date DATE PRIMARY KEY,
            total_assets DOUBLE,
            available_cash DOUBLE,
            daily_pnl DOUBLE,
            daily_ret DOUBLE,
            market_value DOUBLE
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

def drop_cache_tables(con):
    for t in ["daily_qfq_cache", "daily_hfq_cache"]:
        row = con.execute("SELECT table_type FROM information_schema.tables WHERE table_name = ?", [t]).fetchone()
        if row is not None:
            if row[0] == "VIEW":
                con.execute(f'DROP VIEW IF EXISTS "{t}"')
            else:
                con.execute(f'DROP TABLE IF EXISTS "{t}"')

def compact_database(con) -> None:
    con.execute("CHECKPOINT")
    try:
        con.execute("VACUUM")
    except Exception as exc:
        log.warning(f"⚠️ VACUUM 失败: {exc}")
    con.execute("CHECKPOINT")

# =========================================================
# 行情数据获取与同步
# =========================================================
def prepare_latest_qlib_data() -> str:
    session = build_retry_session()
    if not (os.path.exists(QLIB_TAR_PATH) and (time.time() - os.path.getmtime(QLIB_TAR_PATH)) <= CONFIG["source_cache_ttl_seconds"]):
        log.info(f"⬇️ 下载最新行情包: {QLIB_DATA_URL}")
        with session.get(QLIB_DATA_URL, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total_size = int(resp.headers.get("content-length", 0))
            with open(QLIB_TAR_PATH, "wb") as f:
                with tqdm(total=total_size, unit="B", unit_scale=True, desc="⬇️ 下载行情") as pbar:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
    # 解压
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

def normalize_qlib_symbol(raw_symbol: str) -> str:
    raw = str(raw_symbol).strip().upper()
    if "." in raw:
        return canonical_symbol(raw)
    if len(raw) >= 8 and raw[:2].isalpha() and raw[2:].isdigit():
        return canonical_symbol(f"{raw[:2]}.{raw[2:]}")
    return canonical_symbol(raw)

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
    out["symbol"] = out["symbol"].map(normalize_qlib_symbol)
    for col in ["high", "low", "open", "close", "adjclose", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["open", "high", "low", "close", "adjclose"]:
        out[col] = out[col].where(out[col] > 0)
    for col in ["open", "high", "low", "close", "adjclose", "amount"]:
        out[col] = out[col].round(2)
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

def _compare_and_sync_stock_rows(con, df_rows: pd.DataFrame) -> int:
    if df_rows is None or df_rows.empty:
        return 0
    tmp = df_rows.copy()
    tmp["tradedate"] = pd.to_datetime(tmp["tradedate"]).dt.date
    tmp["symbol"] = tmp["symbol"].map(canonical_symbol)
    tmp = tmp.drop_duplicates(subset=["tradedate", "symbol"], keep="last")
    for col in ["high", "low", "open", "close", "adjclose", "amount"]:
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").round(2)
    tmp["volume"] = pd.to_numeric(tmp["volume"], errors="coerce").round(0)
    tmp = tmp.dropna(subset=["tradedate", "symbol", "high", "low", "open", "close", "adjclose"])
    if tmp.empty:
        return 0

    try:
        con.unregister("tmp_new_stocks")
    except Exception:
        pass
    con.register("tmp_new_stocks", tmp)

    con.execute(f"""
        INSERT OR REPLACE INTO {STOCKS_TABLE} (tradedate, symbol, high, low, open, close, adjclose, volume, amount)
        SELECT tradedate, symbol, high, low, open, close, adjclose, volume, amount
        FROM tmp_new_stocks
    """)
    con.execute(f"""
        INSERT OR REPLACE INTO {ADJUSTMENT_FACTORS_TABLE} (tradedate, symbol, hfq_factor)
        SELECT tradedate, symbol, adjclose / NULLIF(close, 0)
        FROM tmp_new_stocks
        WHERE close > 0 AND adjclose > 0
    """)
    con.execute("CHECKPOINT")
    return len(tmp)

def investment_data_sync_recent_window(db_path: str, target_date: date, trade_days: int) -> Tuple[bool, List[date]]:
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
        n_ins = _compare_and_sync_stock_rows(con, window_df)
    log.info(f"✅ 行情同步完成: 记录数={n_ins}")
    return n_ins > 0, trade_dates

def get_recent_trade_dates(con, end_date: date, n: int) -> List[date]:
    df = con.execute(f"""
        SELECT DISTINCT tradedate AS date
        FROM {STOCKS_TABLE}
        WHERE tradedate <= ?
        ORDER BY tradedate DESC
        LIMIT ?
    """, [end_date, n]).df()
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())

# =========================================================
# 复权计算（QFQ / HFQ）
# =========================================================
def rebuild_recent_adjusted_cache(db_path: str, end_date: date, window_days: int) -> bool:
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        recent_dates = get_recent_trade_dates(con, end_date, int(window_days))
        if not recent_dates:
            return False
        start_date = recent_dates[0]

        con.execute('DROP TABLE IF EXISTS daily_hfq_cache')
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
        try:
            con.execute("CREATE INDEX idx_hfq_sym_date ON daily_hfq_cache(symbol, date)")
        except Exception:
            pass

        end_str = end_date.strftime('%Y-%m-%d')
        start_str = start_date.strftime('%Y-%m-%d')
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
    return True

# =========================================================
# 多因子宏观市场景气度评分与仓位定夺
# =========================================================
def get_account_state(con) -> Tuple[float, float, float]:
    row = con.execute("SELECT init_capital, total_assets, available_cash FROM account_state WHERE id = 1").fetchone()
    if not row:
        cash = float(CONFIG.get("init_cash", 100000.0))
        return cash, cash, cash
    return row[0], row[1], row[2]

def get_market_target_position_ratio(con, trade_date: date) -> Tuple[float, str, dict]:
    """
    重构：多因子宏观市场环境评分 (S)
    S = 0.35 * Trend + 0.30 * Breadth + 0.20 * Volume + 0.15 * Sentiment
    1. 修正后的市场广度计算：在外层筛选 target_date，完整保留 60 交易日均线窗口！
    2. 严格分层映射：
       - S >= 0.65 (强趋势): 目标持仓 85% (80%~90%)
       - 0.45 <= S < 0.65 (震荡/转强中): 目标持仓 50%
       - S < 0.45 (弱势/转熊破位): 目标持仓必须严格限制在 30%
    """
    # 1. 市场广度与量能汇总 (利用正确的窗口计算)
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
        return 0.30, "数据不足(默认防御 30%)", {}

    pct_above_ma20 = float(row[1] or 0.0)
    pct_above_ma60 = float(row[2] or 0.0)
    pct_advancers  = float(row[3] or 0.0)
    cur_vol        = float(row[4] or 0.0)
    base_vol       = float(row[5] or 1.0)

    # 广度得分 B (0~1)
    B = 0.6 * pct_above_ma20 + 0.4 * pct_above_ma60

    # 活跃度得分 V (0~1)
    vol_ratio = cur_vol / base_vol if base_vol > 0 else 1.0
    V = float(np.clip((vol_ratio - 0.7) / 0.6, 0.0, 1.0))

    # 赚钱效应得分 M (0~1)
    M = pct_advancers

    # 2. 指数趋势得分 T (以 000001.SH 或全市场平均走势为基准)
    idx_df = con.execute("""
        SELECT date, close FROM daily_qfq_cache
        WHERE symbol = '000001.SH' AND date <= ?
        ORDER BY date DESC LIMIT 30
    """, [trade_date]).df()

    if len(idx_df) >= 20:
        c = idx_df["close"].iloc[0]
        ma5 = idx_df["close"].iloc[:5].mean()
        ma20 = idx_df["close"].iloc[:20].mean()
        T = 0.0
        if c >= ma20:
            T += 0.5
        if ma5 >= ma20:
            T += 0.5
    else:
        T = B  # 若无指数则由广度替代

    # 综合宏观景气评分 S
    S = 0.35 * T + 0.30 * B + 0.20 * V + 0.15 * M

    details = {
        "score": round(S, 3), "trend_T": round(T, 2), "breadth_B": round(B, 2),
        "vol_V": round(V, 2), "sentiment_M": round(M, 2),
        "pct_above_ma20": round(pct_above_ma20 * 100, 1),
        "pct_above_ma60": round(pct_above_ma60 * 100, 1),
    }

    if S >= 0.65:
        target_ratio = float(CONFIG["bull_market_pos_ratio"])      # 85%
        desc = f"🟢 强多头行情 (S分:{S:.2f} | 站上MA20:{details['pct_above_ma20']}%)"
    elif S >= 0.45:
        target_ratio = float(CONFIG["neutral_market_pos_ratio"])   # 50%
        desc = f"🟡 震荡/修复行情 (S分:{S:.2f} | 站上MA20:{details['pct_above_ma20']}%)"
    else:
        target_ratio = float(CONFIG["bear_market_pos_ratio"])      # 30% 刚性压制
        desc = f"🔴 弱势/破位熊市 (S分:{S:.2f} | 站上MA20:{details['pct_above_ma20']}%)"

    return target_ratio, desc, details

# =========================================================
# 策略核心：强化版选股算法
# =========================================================
def compute_all_signals(con, target_date: date) -> pd.DataFrame:
    """
    强化版 5 大核心选股要求：
    1. 均线多头排列 (MA5 > MA10 > MA20 > MA60)
    2. MA20 升穿 MA60 / 金叉向上发散
    3. MACD > 0 (DIF > 0 且 HIST > 0)
    4. EMA 多头排列 (EMA5 > EMA10 > EMA20 且 EMA12 > EMA26)
    5. 布林带 %B >= 0.75 且开口向上沿上轨上升，或者当日升穿中轨
    6. 过热防追高与低流动性过滤
    """
    start_date = (target_date - timedelta(days=160)).strftime("%Y-%m-%d")
    end_date = target_date.strftime("%Y-%m-%d")
    atr_alpha = float(CONFIG.get("atr_buy_alpha", 0.5))

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
               LAG(close, 5) OVER w AS close_5,
               LAG(close, 20) OVER w AS close_20
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
      -- [流动性过滤]: 日均成交额 > 2000 万元 (千元为单位则为 20000)
      AND (amount_ma20 IS NULL OR amount_ma20 >= 20000)

      -- [过热防追高过滤]: 避免在加速赶顶末期接盘
      AND close <= ma20 * 1.15
      AND (close - ma20) <= 3.0 * atr14
      AND (close_20 IS NULL OR close / close_20 <= 1.40)

      -- [要求1: 均线多头排列]
      AND ma5 > ma10
      AND ma10 > ma20
      AND ma20 > ma60

      -- [要求2: MA20 升穿 MA60 / 金叉向上发散]
      AND ma20 >= ma60
      AND (ma20_prev <= ma60_prev OR (ma20 > ma20_prev AND (ma20 - ma60) >= (ma20_prev - ma60_prev)))

      -- [要求5: BOLL %B >= 0.75 沿上轨攀升 或 升穿中轨]
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

    # EMA 多头与 MACD > 0 验证
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

        # [要求4: EMA 多头排列]
        ema5 = c_series.ewm(span=5, adjust=False).mean().iloc[-1]
        ema10 = c_series.ewm(span=10, adjust=False).mean().iloc[-1]
        ema20 = c_series.ewm(span=20, adjust=False).mean().iloc[-1]
        ema12 = c_series.ewm(span=12, adjust=False).mean().iloc[-1]
        ema26 = c_series.ewm(span=26, adjust=False).mean().iloc[-1]

        if not (ema5 > ema10 and ema10 > ema20 and ema12 > ema26):
            continue

        # [要求3: MACD 大于 0 (DIF > 0 且 HIST > 0)]
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

    valid_df = pd.DataFrame(valid_records)
    picks = pd.merge(candidates, valid_df, on="symbol", how="inner")

    picks["atr_pct"] = (picks["atr14"] / picks["close"]).round(4)
    picks["planned_buy_price"] = (picks["close"] - atr_alpha * picks["atr14"]).round(2)
    picks["planned_buy_price"] = np.where(picks["planned_buy_price"] <= 0, (picks["close"] * 0.99).round(2), picks["planned_buy_price"])

    picks["ret_5d"] = np.where(picks["close_5"] > 0, (picks["close"] / picks["close_5"]) - 1.0, 0.0)
    picks["ret_20d"] = np.where(picks["close_20"] > 0, (picks["close"] / picks["close_20"]) - 1.0, 0.0)
    picks["pct_b"] = ((picks["close"] - picks["boll_lower"]) / (picks["boll_upper"] - picks["boll_lower"])).round(3)

    picks["signal_strength"] = ((picks["dif"] / picks["close"] * 100) + (picks["volume"] / picks["vol_ma5"])).round(2)
    picks["total_score"] = (picks["ret_20d"] * 100.0 + picks["signal_strength"] + picks["pct_b"] * 10).round(2)

    picks["pattern_tag"] = np.where(
        picks["prev_close"] < picks["boll_mid_prev"],
        "🚀升穿布林中轨",
        "📈%B≥0.75沿上轨攀升"
    )
    picks["date"] = pd.to_datetime(picks["date"]).dt.date
    return picks[["symbol", "date", "close", "planned_buy_price", "atr_pct", "pct_b", "ret_5d", "ret_20d", "total_score", "signal_strength", "pattern_tag"]]

# =========================================================
# 交易执行与强制降仓再平衡
# =========================================================
def enforce_position_rebalance(con, trade_date: date, max_allowed_equity: float) -> List[Tuple]:
    """
    【核心修复1】熊市刚性降仓再平衡：
    当账户持仓市值超过大盘受控上限时（如从牛转熊，持仓 > 30%），
    按弱势程度（浮亏大、跌破MA20深）优先强制卖出，直至持仓回归 30% 目标红线！
    """
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy FROM virtual_portfolio").df()
    if holdings.empty:
        return []

    raw_today = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [trade_date]).df()
    price_map = dict(zip(raw_today["symbol"].map(canonical_symbol), raw_today["close"]))

    holdings["current_price"] = holdings["symbol"].map(price_map).fillna(holdings["buy_price"])
    holdings["market_val"] = holdings["shares"] * holdings["current_price"]
    current_total_val = holdings["market_val"].sum()

    excess_val = current_total_val - max_allowed_equity
    if excess_val <= 1000.0:  # 容差 1000 元
        return []

    log.warning(f"🚨 [风控触发强制降仓] 当前持仓 ¥{current_total_val:,.0f} 超过大盘限制 ¥{max_allowed_equity:,.0f}，需减仓 ¥{excess_val:,.0f}")

    # 获取 MA20 计算偏离度
    syms = holdings["symbol"].tolist()
    placeholders = ','.join(['?'] * len(syms))
    start_30 = (trade_date - timedelta(days=50)).strftime('%Y-%m-%d')
    ma20_df = con.execute(f"""
        SELECT symbol, AVG(close) as ma20
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date <= ? AND date >= ?
        GROUP BY symbol
    """, syms + [trade_date.strftime('%Y-%m-%d'), start_30]).df()
    ma20_map = dict(zip(ma20_df["symbol"], ma20_df["ma20"]))

    holdings["pnl_pct"] = (holdings["current_price"] - holdings["buy_price"]) / holdings["buy_price"] * 100.0
    holdings["ma20"] = holdings["symbol"].map(ma20_map).fillna(holdings["current_price"])
    holdings["ma20_dev"] = (holdings["current_price"] - holdings["ma20"]) / holdings["ma20"]

    # 弱势股排在前面优先斩仓
    holdings = holdings.sort_values(["pnl_pct", "ma20_dev"], ascending=[True, True])

    rebalance_sold = []
    sell_fee_rate = float(CONFIG.get("sell_fee_rate", 0.0010))
    val_reduced = 0.0

    for _, row in holdings.iterrows():
        if val_reduced >= excess_val:
            break
        sym = row["symbol"]
        shares = int(row["shares"])
        price = float(row["current_price"])
        stock_val = float(row["market_val"])

        needed = excess_val - val_reduced
        if stock_val <= needed or (needed / stock_val) > 0.6:
            sell_shares = shares
        else:
            sell_shares = int(needed / (price * 100.0)) * 100
            sell_shares = max(sell_shares, 100)
            sell_shares = min(sell_shares, shares)

        gross_cash = round(sell_shares * price, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)
        pnl = round(float(row["pnl_pct"]), 2)

        if sell_shares == shares:
            con.execute("DELETE FROM virtual_portfolio WHERE symbol=?", [sym])
        else:
            con.execute("UPDATE virtual_portfolio SET shares = shares - ? WHERE symbol=?", [sell_shares, sym])

        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, trade_date, price, sell_shares, REASON_REBALANCE, pnl, sell_fee])

        con.execute("UPDATE account_state SET available_cash = available_cash + ? WHERE id=1", [recovered_cash])
        val_reduced += gross_cash
        rebalance_sold.append((sym, sell_shares, price, pnl))
        log.warning(f"⚡ [再平衡卖出] 抛售弱势标的 {sym} x{sell_shares}股 @¥{price:.2f} (盈亏 {pnl:.1f}%)，释放资金 ¥{recovered_cash:,.0f}")

    return rebalance_sold


def process_exit_rules(con, trade_date: date) -> List[Tuple]:
    """
    【核心修复3】真止盈与多层离场机制：
    1. 动态 ATR 初始止损 (Beta * ATR%)
    2. 保本止损 (Break-even): 浮盈曾超 8% 后，止损线自动抬升至成本线
    3. 利润回撤跟踪止盈 (Trailing Stop): 浮盈超 12% 后，自高点回撤 2.5*ATR 移动止盈
    4. 阶段硬止盈: 涨幅超 take_profit_pct 且当日滞涨收阴
    5. MACD 死叉或翻绿弱化离场
    6. 跌破 MA20 / 布林中轨
    """
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy, highest_price_hfq FROM virtual_portfolio").df()
    if holdings.empty:
        return []
    holdings["buy_date"] = pd.to_datetime(holdings["buy_date"]).dt.date
    holdings = holdings[holdings["buy_date"] < trade_date].copy()
    if holdings.empty:
        return []

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
        SELECT symbol, date, close
        FROM daily_hfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date ASC
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()

    raw_today = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [trade_date]).df()
    raw_map = dict(zip(raw_today["symbol"].map(canonical_symbol), raw_today["close"])) if not raw_today.empty else {}

    sold_rows = []
    sell_fee_rate = float(CONFIG.get("sell_fee_rate", 0.0010))
    atr_stop_beta = float(CONFIG.get("atr_stop_loss_beta", 2.0))
    trailing_mult = float(CONFIG.get("trailing_stop_atr_mult", 2.5))
    target_tp = float(CONFIG.get("take_profit_pct", 15.0))

    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = row['buy_date']
        buy_price = float(row['buy_price'])
        buy_price_hfq = float(row['buy_price_hfq']) if not pd.isna(row['buy_price_hfq']) else buy_price
        shares = int(row['shares'])
        atr_pct_buy = float(row['atr_pct_buy']) if 'atr_pct_buy' in row and not pd.isna(row['atr_pct_buy']) and float(row['atr_pct_buy']) > 0 else 0.03
        highest_hfq = float(row['highest_price_hfq']) if ('highest_price_hfq' in row and not pd.isna(row['highest_price_hfq']) and float(row['highest_price_hfq']) > 0) else buy_price_hfq

        gq = qfq_df[qfq_df['symbol'] == sym].copy()
        if gq.empty:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym].copy() if not hfq_df.empty else pd.DataFrame()

        last_q = gq.iloc[-1]
        last_close_qfq = float(last_q['close'])
        last_open_qfq = float(last_q['open'])
        last_ma5 = float(last_q['ma5'])
        last_ma10 = float(last_q['ma10'])
        last_ma20 = float(last_q['ma20'])
        last_close_hfq = float(gh.iloc[-1]['close']) if not gh.empty else last_close_qfq

        # 更新历史最高后复权价
        if last_close_hfq > highest_hfq:
            highest_hfq = last_close_hfq
            con.execute("UPDATE virtual_portfolio SET highest_price_hfq = ? WHERE symbol = ?", [highest_hfq, sym])

        curr_pnl_pct = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100.0 if buy_price_hfq > 0 else 0.0
        peak_pnl_pct = (highest_hfq - buy_price_hfq) / buy_price_hfq * 100.0 if buy_price_hfq > 0 else 0.0
        reason_mask = 0

        # 1. 动态 ATR 初始止损
        dynamic_stop_limit = -1.0 * atr_stop_beta * atr_pct_buy * 100.0
        if curr_pnl_pct <= dynamic_stop_limit:
            reason_mask |= REASON_STOPLOSS

        # 2. 保本止损：若峰值曾超 8%，决不允许亏损离场
        if peak_pnl_pct >= 8.0 and curr_pnl_pct <= 0.3:
            reason_mask |= REASON_BREAKEVEN

        # 3. 利润回撤移动止损 (Trailing Stop)：盈利超 12% 后，自最高点回撤 2.5*ATR 止盈
        trailing_drop_limit = trailing_mult * atr_pct_buy * 100.0
        if peak_pnl_pct >= 12.0 and (peak_pnl_pct - curr_pnl_pct) >= trailing_drop_limit:
            reason_mask |= REASON_TAKE_PROFIT

        # 4. 硬性达标止盈：涨幅超 target_tp 且冲高回落收阴
        if curr_pnl_pct >= target_tp and last_close_qfq < last_open_qfq:
            reason_mask |= REASON_TAKE_PROFIT

        # 5. 跌破 MA20 / 布林中轨
        if last_close_qfq < last_ma20:
            reason_mask |= REASON_BELOW_MA20
            reason_mask |= REASON_BELOW_BOLL_MID

        # 6. MACD 死叉或翻绿弱化离场
        if len(gq) >= 35:
            c_vals = gq['close'].values.astype(np.float64)
            if talib is not None:
                dif, dea, hist = talib.MACD(c_vals, fastperiod=12, slowperiod=26, signalperiod=9)
                if not pd.isna(hist[-1]) and not pd.isna(hist[-2]):
                    if (dif[-1] < dea[-1]) or (hist[-1] < 0 and last_close_qfq < last_ma10):
                        reason_mask |= REASON_MACD_DECREASE

        # 7. 持有到期
        if (trade_date - buy_date).days >= CONFIG['max_hold_days']:
            reason_mask |= REASON_MAX_HOLD

        if reason_mask > 0:
            sell_price_raw = raw_map.get(sym, last_close_qfq)
            sold_rows.append((sym, trade_date, last_close_qfq, shares, reason_mask, round(curr_pnl_pct, 2), sell_price_raw))

    for sym, sell_date, sell_price, shares, reason_mask, pnl_pct, sell_price_raw in sold_rows:
        con.execute('DELETE FROM virtual_portfolio WHERE symbol=?', [sym])
        gross_cash = round(shares * sell_price_raw, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, sell_date, round(sell_price, 2), shares, reason_mask, pnl_pct, round(sell_fee, 2)])
        con.execute("UPDATE account_state SET available_cash = available_cash + ? WHERE id=1", [recovered_cash])

    return sold_rows


def process_pending_orders(con, trade_date: date, max_allowed_equity: float) -> Tuple[List[Tuple], List[Tuple]]:
    """
    【核心修复4】拟真撮合买入：
    1. 触及挂单价且低开时，以开盘价加滑点 min(open, planned_price)*(1+slippage) 拟真成交；
    2. 开盘一字涨停无法买入保护；
    3. 成交股数不得超过当日成交量的 5% 流动性约束。
    """
    pending_df = con.execute("""
        SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status, signal_strength, atr_pct
        FROM pending_orders
        WHERE status=0 AND signal_date < ?
        ORDER BY signal_date, symbol
    """, [trade_date]).df()
    if pending_df.empty:
        return [], []

    qfq_today = con.execute("SELECT symbol, date, open, high, low, close, volume FROM daily_qfq_cache WHERE date = ?", [trade_date]).df()
    hfq_today = con.execute("SELECT symbol, date, close FROM daily_hfq_cache WHERE date = ?", [trade_date]).df()
    if qfq_today.empty:
        return [], []

    init_cap, total_assets, avail_cash = get_account_state(con)
    max_position = int(CONFIG.get("max_position_stocks", 6))
    buy_fee_rate = float(CONFIG.get("buy_fee_rate", 0.0005))
    slippage = float(CONFIG.get("slippage_rate", 0.001))
    max_vol_rate = float(CONFIG.get("max_volume_participate_rate", 0.05))

    current_market_val = con.execute("""
        SELECT COALESCE(SUM(p.shares * s.close), 0)
        FROM virtual_portfolio p
        JOIN stock_prices s ON p.symbol = s.symbol AND s.tradedate = ?
    """, [trade_date]).fetchone()[0]

    remaining_capacity = max_allowed_equity - current_market_val
    if remaining_capacity <= 0:
        return [], []

    base_budget = min(CONFIG['position_cash_yuan'], total_assets / max_position)
    q_map = {row['symbol']: row for _, row in qfq_today.iterrows()}
    h_map = {row['symbol']: row for _, row in hfq_today.iterrows()}

    expire_days = int(CONFIG.get("buy_signal_expire_days", 2))
    current_holdings = con.execute("SELECT COUNT(*) FROM virtual_portfolio").fetchone()[0]
    holding_symbols = {r[0] for r in con.execute("SELECT symbol FROM virtual_portfolio").fetchall()}

    filled_rows, expired_rows = [], []
    for _, row in pending_df.iterrows():
        symbol = row['symbol']
        signal_date = row['signal_date']
        planned_buy_price = float(row['planned_buy_price'])
        atr_pct = float(row['atr_pct']) if 'atr_pct' in row and not pd.isna(row['atr_pct']) and float(row['atr_pct']) > 0 else 0.03

        # 检查过期
        n_days = con.execute(f"SELECT COUNT(DISTINCT tradedate) FROM {STOCKS_TABLE} WHERE tradedate > ? AND tradedate <= ?", [signal_date, trade_date]).fetchone()[0]
        if n_days > expire_days:
            expired_rows.append((symbol, signal_date))
            continue

        if symbol in holding_symbols or symbol in {f[0] for f in filled_rows} or symbol not in q_map:
            continue

        row_t1 = q_map[symbol]
        today_open = float(row_t1['open'])
        today_high = float(row_t1['high'])
        today_low = float(row_t1['low'])
        today_close = float(row_t1['close'])
        today_vol = float(row_t1['volume']) * 100.0  # 手转股
        today_close_hfq = float(h_map[symbol]['close']) if symbol in h_map else today_close
        signal_close = float(row['signal_close']) if not pd.isna(row['signal_close']) else planned_buy_price

        # 一字涨停无法买入
        if today_open >= signal_close * 1.095 and today_low == today_high:
            continue

        # 触及挂单价
        if today_low > planned_buy_price:
            continue

        # 拟真撮合价格修正
        base_fill = min(today_open, planned_buy_price)
        actual_buy_price = round(base_fill * (1.0 + slippage), 2)

        risk_weight = np.clip(0.03 / atr_pct, 0.5, 2.0)
        target_cash = min(base_budget * risk_weight, remaining_capacity)

        if current_holdings >= max_position or avail_cash < target_cash * 0.5:
            continue

        cost_budget = min(target_cash, avail_cash)
        lot_cost = actual_buy_price * 100.0 * (1.0 + buy_fee_rate)
        shares = int(cost_budget / lot_cost) * 100

        # 流动性约束：不超过当日总成交量的 5%
        if today_vol > 0 and shares > int(today_vol * max_vol_rate):
            shares = int(today_vol * max_vol_rate / 100.0) * 100

        if shares < 100:
            continue

        gross_cost = round(shares * actual_buy_price, 2)
        buy_fee = round(gross_cost * buy_fee_rate, 2)
        total_cost = round(gross_cost + buy_fee, 2)
        if total_cost > avail_cash:
            continue

        factor = (today_close_hfq / today_close) if today_close > 0 else 1.0
        buy_price_hfq = round(actual_buy_price * factor, 2)

        avail_cash -= total_cost
        current_holdings += 1
        current_market_val += gross_cost
        remaining_capacity = max_allowed_equity - current_market_val
        filled_rows.append((symbol, trade_date, actual_buy_price, buy_price_hfq, int(shares), float(buy_fee), atr_pct))
        log.info(f"✅ 拟真撮合成交 {symbol} @¥{actual_buy_price:.2f} (挂单¥{planned_buy_price:.2f}, 低开¥{today_open:.2f}) x{int(shares)}股")

    if filled_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
        con.executemany("""
            INSERT OR REPLACE INTO virtual_portfolio(symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy, highest_price_hfq)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(s, d, bp, bph, sh, atr, bph) for s, d, bp, bph, sh, _, atr in filled_rows])
        for symbol, buy_date, buy_price, buy_price_hfq, shares, buy_fee, _ in filled_rows:
            con.execute(f"UPDATE pending_orders SET status={STATUS_FILLED} WHERE symbol=? AND signal_date<? AND status={STATUS_PENDING}", [symbol, trade_date])
            con.execute("""
                INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
                VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, ?)
            """, [symbol, TRADE_BUY, buy_date, round(buy_price, 2), shares, REASON_BUY_T1, round(buy_fee, 2)])
    if expired_rows:
        con.executemany(f"UPDATE pending_orders SET status={STATUS_EXPIRED} WHERE symbol=? AND signal_date=? AND status={STATUS_PENDING}", expired_rows)

    return filled_rows, expired_rows

# =========================================================
# 策略主调度与评估
# =========================================================
def evaluate_strategy(db_path: str, target_date: date, top_n: Optional[int] = None):
    top_n = top_n or CONFIG["top_n"]
    with duckdb.connect(db_path, read_only=False) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)

        # 1. 宏观市场环境多因子定性与目标仓位额度计算
        target_pos_ratio, regime_desc, regime_details = get_market_target_position_ratio(con, target_date)
        init_cap, total_assets, avail_cash = get_account_state(con)
        max_allowed_equity = total_assets * target_pos_ratio

        # 2. 执行个股止盈、移动止损与弱化离场规则
        process_exit_rules(con, target_date)

        # 3. 刚性再平衡：大盘转熊时强制斩仓弱势持仓至 30% 目标线
        enforce_position_rebalance(con, target_date, max_allowed_equity)

        # 4. 撮合买入挂单（严控在目标仓位额度内）
        process_pending_orders(con, target_date, max_allowed_equity)

        # 5. 生成次日选股候选与 ATR 挂单
        df_picks = compute_all_signals(con, target_date)
        if not df_picks.empty:
            df_picks = df_picks.sort_values(["total_score", "symbol"], ascending=[False, True]).head(top_n).reset_index(drop=True)

        con.execute(f"""
            UPDATE pending_orders SET status={STATUS_EXPIRED}
            WHERE status={STATUS_PENDING} AND signal_date <= (
                SELECT tradedate FROM {STOCKS_TABLE}
                WHERE tradedate < ? ORDER BY tradedate DESC LIMIT 1 OFFSET 2
            )
        """, [target_date])

        holdings_df = con.execute("SELECT symbol FROM virtual_portfolio").df()
        holding_symbols = set(holdings_df["symbol"]) if not holdings_df.empty else set()
        new_orders = []
        if not df_picks.empty:
            for _, row in df_picks.iterrows():
                symbol = row["symbol"]
                if symbol in holding_symbols:
                    continue
                atr_p = float(row["atr_pct"]) if "atr_pct" in row and not pd.isna(row["atr_pct"]) else 0.03
                new_orders.append((symbol, target_date, round(float(row["planned_buy_price"]), 2), round(float(row["close"]), 2), TRADE_BUY, STATUS_PENDING, float(row["signal_strength"]), atr_p))
            if new_orders:
                con.executemany("""
                    INSERT OR REPLACE INTO pending_orders(symbol, signal_date, planned_buy_price, signal_close, trade_type, status, signal_strength, atr_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, new_orders)

        df_pending = con.execute(f"SELECT * FROM pending_orders WHERE status={STATUS_PENDING} ORDER BY signal_date DESC, symbol").df()

        # 6. 计算持仓与资产净值
        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
            total_market_value = 0.0
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
            total_market_value = df_portfolio["market_value"].sum()

        init_cap, _, avail_cash = get_account_state(con)
        new_total_assets = avail_cash + total_market_value
        con.execute("UPDATE account_state SET total_assets=?, updated_at=? WHERE id=1", [new_total_assets, target_date])

        prev_row = con.execute("SELECT total_assets FROM account_history WHERE date < ? ORDER BY date DESC LIMIT 1", [target_date]).fetchone()
        prev_assets = prev_row[0] if prev_row else init_cap
        daily_pnl = new_total_assets - prev_assets
        daily_ret = daily_pnl / prev_assets if prev_assets > 0 else 0.0

        con.execute("""
            INSERT OR REPLACE INTO account_history(date, total_assets, available_cash, daily_pnl, daily_ret, market_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [target_date, round(new_total_assets, 2), round(avail_cash, 2), round(daily_pnl, 2), round(daily_ret, 4), round(total_market_value, 2)])

        metrics = {
            "total_assets": new_total_assets, "avail_cash": avail_cash,
            "market_value": total_market_value,
            "position_pct": total_market_value / new_total_assets * 100.0 if new_total_assets > 0 else 0.0,
            "cash_pct": avail_cash / new_total_assets * 100.0 if new_total_assets > 0 else 100.0,
            "total_pnl": new_total_assets - init_cap,
            "total_pnl_pct": (new_total_assets / init_cap - 1.0) * 100.0,
            "daily_pnl": daily_pnl, "daily_ret": daily_ret,
            "market_regime": regime_desc,
            "target_pos_limit": target_pos_ratio * 100.0,
            "regime_details": regime_details,
        }
        df_trades = con.execute("SELECT * FROM trade_history WHERE trade_date = ? ORDER BY trade_type, symbol", [target_date]).df()

    return decode_numeric_frame(df_picks), decode_numeric_frame(df_portfolio), decode_numeric_frame(df_pending), decode_numeric_frame(df_trades), metrics

# =========================================================
# 邮件日报系统
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
            log.info("📧 报告邮件已发送")
            return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(attempt * 2)

def generate_and_send_report(tm: TokenManager, df_picks: pd.DataFrame, df_portfolio: pd.DataFrame, df_pending: pd.DataFrame, df_trades: pd.DataFrame, target_str: str, metrics: dict):
    CSS = """<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; background:#f0f2f5; color:#333; }
.wrapper { max-width:960px; margin:0 auto; padding:20px; }
.header  { background:linear-gradient(135deg,#0f172a 0%,#1e293b 50%,#334155 100%); border-radius:16px; padding:24px 28px; margin-bottom:18px; color:#fff; display:flex; justify-content:space-between; align-items:center; }
.kpi-row { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:14px; }
.kpi-card { background:#fff; border-radius:12px; padding:16px; box-shadow:0 2px 6px rgba(0,0,0,.05); border-left:4px solid #3b82f6; }
.kpi-card.green { border-left-color:#10b981; }
.kpi-card.red   { border-left-color:#ef4444; }
.kpi-value { font-size:20px; font-weight:700; color:#1e293b; margin-top:4px; }
.kpi-label { font-size:12px; color:#64748b; }
.section   { background:#fff; border-radius:12px; padding:20px; margin-bottom:18px; box-shadow:0 2px 6px rgba(0,0,0,.05); }
.section-title { font-size:16px; font-weight:600; color:#1e293b; margin-bottom:14px; }
.data-table { width:100%; border-collapse:collapse; font-size:13px; }
.data-table th { background:#f8fafc; padding:10px; font-weight:600; color:#64748b; border-bottom:2px solid #e2e8f0; text-align:left; }
.data-table td { padding:10px; border-bottom:1px solid #f1f5f9; }
.ret-pos { color:#16a34a; font-weight:600; }
.ret-neg { color:#dc2626; font-weight:600; }
.pill { display:inline-block; background:#eff6ff; border:1px solid #bfdbfe; color:#1d4ed8; border-radius:16px; padding:4px 10px; font-size:12px; margin:3px; font-weight:500; }
</style>"""

    picks_rows = []
    if df_picks is not None and not df_picks.empty:
        for i, r in enumerate(df_picks.itertuples(), 1):
            sign = "+" if r.ret_20d >= 0 else ""
            cls = "ret-pos" if r.ret_20d >= 0 else "ret-neg"
            picks_rows.append(f"""
            <tr>
              <td>{i}</td><td><b>{r.symbol}</b></td><td>¥{float(r.close):.2f}</td>
              <td>¥{float(r.planned_buy_price):.2f}</td><td>{float(r.pct_b):.2f}</td>
              <td><b>{r.pattern_tag}</b></td>
              <td class="{cls}">{sign}{r.ret_20d*100:.1f}%</td>
            </tr>""")
        picks_html = f"""<table class="data-table"><thead><tr>
            <th>#</th><th>代码</th><th>收盘</th><th>拟真挂单价</th><th>%B位置</th><th>形态</th><th>20日涨幅</th>
        </tr></thead><tbody>{''.join(picks_rows)}</tbody></table>"""
    else:
        picks_html = '<div style="text-align:center;padding:20px;color:#94a3b8;">今日无符合多头共振的候选标的</div>'

    trades_rows = []
    if df_trades is not None and not df_trades.empty:
        for r in df_trades.itertuples():
            pnl_txt = f"{r.pnl_pct:+.1f}%" if not pd.isna(r.pnl_pct) else "—"
            trades_rows.append(f"""
            <tr>
              <td>{decode_trade_type_label(r.trade_type)}</td><td><b>{r.symbol}</b></td>
              <td>¥{float(r.price):.2f}</td><td>{int(r.shares)}</td>
              <td>{decode_reason_text(r.reason)}</td><td>{pnl_txt}</td>
            </tr>""")
        trades_html = f"""<table class="data-table"><thead><tr>
            <th>方向</th><th>代码</th><th>成交价</th><th>股数</th><th>执行触发原因</th><th>盈亏</th>
        </tr></thead><tbody>{''.join(trades_rows)}</tbody></table>"""
    else:
        trades_html = '<div style="text-align:center;padding:16px;color:#94a3b8;">今日无调仓记录</div>'

    details = metrics.get("regime_details", {})
    html = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">{CSS}</head>
<body><div class="wrapper">
<div class="header">
  <div>
    <h2>💴 A股强化量化策略日报</h2>
    <div style="font-size:13px;opacity:0.85;margin-top:6px;">均线多头 + MA20升穿MA60 + MACD>0 + EMA多头 + BOLL %B沿上轨 + 动态再平衡</div>
  </div>
  <div style="text-align:right;">
    <div style="font-size:18px;font-weight:700;">{target_str}</div>
    <div style="font-size:12px;opacity:0.9;">{metrics.get('market_regime', '')}</div>
  </div>
</div>

<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-label">总资产</div><div class="kpi-value">¥{metrics.get('total_assets', 0):,.0f}</div></div>
  <div class="kpi-card green"><div class="kpi-label">可用资金</div><div class="kpi-value">¥{metrics.get('avail_cash', 0):,.0f}</div></div>
  <div class="kpi-card"><div class="kpi-label">实际持仓 / 受控上限</div><div class="kpi-value">{metrics.get('position_pct', 0):.1f}% / {metrics.get('target_pos_limit', 30):.0f}%</div></div>
  <div class="kpi-card {'green' if metrics.get('daily_pnl', 0)>=0 else 'red'}"><div class="kpi-label">当日盈亏</div><div class="kpi-value">¥{metrics.get('daily_pnl', 0):,.0f}</div></div>
</div>

<div class="section">
  <div class="section-title">🧭 宏观多因子景气度评定 (S分: {details.get('score', 0)})</div>
  <div style="font-size:13px;line-height:1.6;color:#475569;">
    大盘定性：<b>{metrics.get('market_regime', '')}</b><br>
    广度诊断：站上 MA20 股票比例 <b>{details.get('pct_above_ma20', 0)}%</b> | 站上 MA60 比例 <b>{details.get('pct_above_ma60', 0)}%</b><br>
    风控准则：<b>上升行情持仓 80%~90% (85%)；下降行情严格限制持仓在 30% 且强制降仓再平衡。</b>
  </div>
</div>

<div class="section">
  <div class="section-title">⚡ 今日交易与再平衡执行</div>
  {trades_html}
</div>

<div class="section">
  <div class="section-title">🔍 今日多头共振标的池 (Top {CONFIG['top_n']})</div>
  {picks_html}
</div>

<div class="section">
  <div class="section-title">⚙️ 执行准则总结</div>
  <div>
    <span class="pill">均线多头 (MA5>10>20>60)</span>
    <span class="pill">MA20 升穿/发散 MA60</span>
    <span class="pill">MACD DIF>0 且 柱>0</span>
    <span class="pill">EMA 多头 (5>10>20)</span>
    <span class="pill">BOLL %B≥0.75 沿上轨攀升</span>
    <span class="pill">熊市 30% 刚性再平衡</span>
    <span class="pill">移动跟踪止盈 (Trailing ATR)</span>
  </div>
</div>
</div></body></html>"""

    send_email_via_graph(tm, f"💴 CN量化日报[{metrics.get('market_regime', '').split(' ')[0]}] - {target_str}", html)

# =========================================================
# 调度入口
# =========================================================
def run_daily_pipeline():
    tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
    odc = OneDriveClient(tm, CONFIG["onedrive_folder"], CONFIG["cloud_db_gz_name"])
    target_date = get_target_date()
    log.info(f"🚀 A股量化强化策略运行启动 target_date={target_date}")

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "CN_stock.duckdb")
        gz_path = os.path.join(tmp, CONFIG["cloud_db_gz_name"])

        if obtain_db_gz(odc, gz_path):
            load_db_gz_to_local(gz_path, db_path)
            log.info("✅ 已从 OneDrive 下载并解压云端数据库")
        else:
            initialize_empty_database(db_path)

        synced, trade_dates = investment_data_sync_recent_window(db_path, target_date, int(CONFIG["update_window_trade_days"]))
        if not synced and not trade_dates:
            log.error("❌ 行情更新失败")
            return

        with duckdb.connect(db_path) as con:
            latest_trade_date = con.execute(f"SELECT MAX(tradedate) FROM {STOCKS_TABLE} WHERE tradedate <= ?", [target_date]).fetchone()[0]

        if not latest_trade_date:
            log.error("❌ 数据库无有效交易日")
            return

        rebuild_recent_adjusted_cache(db_path, latest_trade_date, CONFIG["adjust_cache_days"])
        df_picks, df_portfolio, df_pending, df_trades, metrics = evaluate_strategy(db_path, latest_trade_date, CONFIG["top_n"])

        log.info(f"🎉 策略完成: 候选={len(df_picks)}, 持仓={len(df_portfolio)}, 目标上限={metrics.get('target_pos_limit')}%, 总资产={metrics.get('total_assets',0):,.0f}")
        generate_and_send_report(tm, df_picks, df_portfolio, df_pending, df_trades, latest_trade_date.strftime("%Y-%m-%d"), metrics)

        with duckdb.connect(db_path) as con:
            compact_database(con)
        db_compress_and_upload(odc, db_path, gz_path)

def main():
    try:
        run_daily_pipeline()
    except Exception as exc:
        log.exception("❌ 执行异常: %s", exc)
        raise

if __name__ == "__main__":
    main()
