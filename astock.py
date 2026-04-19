#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1. 行情数据统一落到 stocks 表，使用 investment_data 最新发布包。
2. 价格与金额直接使用人民币浮点值。
3. 每日数据源只依赖 chenditc/investment_data（Qlib）。
4. 前复权和后复权基于 stocks 表中的 close / adjclose 动态构建。
5. 策略和报表直接读取 stocks / qfq / hfq 结果表。
6. 选股信号使用前复权数据；收益率和止盈止损收益判断使用后复权数据。
7. 动量因子 + 量能 + 布林带选股。
8. 买入规则：T日收盘出信号，T+1 最低价 <= T日收盘价*0.985 才成交。
9. 卖出规则：跌破MA20 / 持有N天 / 止盈 / 止损。
10. OneDrive 没有数据库时创建空库并拉取窗口行情。
11. 日常已有数据库时执行5交易日比对增量更新。
12. 最终数据库使用 gzip 压缩上传，降低网络传输成本。
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
    # 仅以下 3 个参数来自普通环境变量
    "azure_client_id": _env("AZURE_CLIENT_ID"),
    "token_cache_file": os.path.join(BASE_DIR, _env("TOKEN_CACHE_FILE", "ms_token.json")),
    "email_to": _env("EMAIL_TO"),
    # 其余参数固定在程序内部，避免运行环境过多配置项
    "onedrive_folder": "Stock",
    "cloud_db_gz_name": "Tu_A_stock.duckdb.gz",
    "local_db_gz_dir": None,
    "local_db_gz_name": None,
    "bootstrap_days": 120,
    "position_cash_yuan": 50000.0,
    "take_profit_pct": 10.0,
    "stop_loss_pct": -5.0,
    "max_hold_days": 200,
    "top_n": 20,
    "adjust_cache_days": 500,
    "source_cache_ttl_seconds": 6 * 3600,
    "update_window_trade_days": 5,
    "initial_replay_trade_days": 30,
    "buy_fee_rate": 0.0005,
    "sell_fee_rate": 0.0010,
}

CONFIG["position_cash_cent"] = int(round(CONFIG["position_cash_yuan"] * 100))
STOCKS_TABLE = "stock_prices"
STOCK_DATE_COL = "tradedate"
STOCK_SYMBOL_COL = "symbol"
QLIB_DATA_URL = "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
QLIB_DATA_DIR = os.path.expanduser("~/.qlib/qlib_data/cn_data")
QLIB_TAR_PATH = os.path.join(BASE_DIR, "qlib_bin.tar.gz")
_QLIB_INITIALIZED = False

# ── 数据库编码常量（避免存储字符串）──
TRADE_BUY = 0
TRADE_SELL = 1
STATUS_PENDING = 0
STATUS_FILLED = 1
STATUS_EXPIRED = 2
# reason 位掩码：可组合多个卖出原因
REASON_STOPLOSS = 1        # bit0: 止损
REASON_BELOW_MA20 = 2      # bit1: 跌破MA20
REASON_MAX_HOLD = 4        # bit2: 持有超期
REASON_BELOW_BOLL_MID = 8  # bit3: 跌破布林中轨
REASON_BEAR_PATTERN = 16   # bit4: 出现下跌形态
REASON_MACD_DECREASE = 32  # bit5: MACD差值减小
REASON_BUY_T1 = 64         # bit6: T+1买入


def decode_trade_type_label(code) -> str:
    """将交易类型数字编码转为显示文本。"""
    return "🟢 买入" if code == TRADE_BUY else "🔴 卖出"


def decode_reason_text(code) -> str:
    """将卖出原因位掩码转为显示文本。"""
    if code is None or code == 0:
        return ""
    parts = []
    if code & REASON_STOPLOSS:
        parts.append(f"止损{CONFIG['stop_loss_pct']}%")
    if code & REASON_BELOW_MA20:
        parts.append("跌破MA20")
    if code & REASON_MAX_HOLD:
        parts.append(f"持有{CONFIG['max_hold_days']}天")
    if code & REASON_BELOW_BOLL_MID:
        parts.append("跌破布林中轨")
    if code & REASON_BEAR_PATTERN:
        parts.append("出现下跌形态")
    if code & REASON_MACD_DECREASE:
        parts.append("MACD差值减小")
    if code & REASON_BUY_T1:
        parts.append("T+1回落1.5%成交")
    return " / ".join(parts) if parts else ""

def _resolve_local_db_gz_path() -> Optional[str]:
    local_dir = CONFIG.get("local_db_gz_dir")
    local_name = CONFIG.get("local_db_gz_name")
    if not local_dir or not local_name:
        return None
    return os.path.join(local_dir, local_name)

LOCAL_DB_GZ_PATH = _resolve_local_db_gz_path()

# =========================================================
# 基础工具
# =========================================================
def get_target_date() -> date:
    now_beijing = datetime.now(CN_TZ)
    # now_beijing = datetime(2026, 4, 10, 17, 0, 0, tzinfo=CN_TZ)
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
        left_l = left.lower()
        right_l = right.lower()
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
    for col in ["open", "high", "low", "close", "buy_price", "buy_price_hfq", "last_price", "last_price_hfq", "planned_buy_price", "signal_close", "price", "adjclose"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["volume", "amount"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    # market_value 和 cost 在 evaluate_strategy 中已经以元为单位计算，不需要再次缩放
    return out
# =========================================================
# Token / OneDrive
# =========================================================
class TokenManager:
    def __init__(self, client_id: str, token_file: str):
        self.client_id = client_id
        self.token_file = token_file
        self._data = {}
        # CI / CL 平台仅从 ONEDRIVE_TOKEN_CACHE_B64 读取一次性注入的 token 缓存
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
            raise RuntimeError("缺少 refresh_token，请先执行 auth 授权。")
        resp = requests.post(
            AUTH_URL,
            data={
                "client_id": self.client_id,
                "grant_type": "refresh_token",
                "refresh_token": rt,
                "scope": SCOPES,
            },
            timeout=30,
        )
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
        token = self._data.get("access_token", "")
        if not token:
            raise RuntimeError("尚未完成授权，请先执行：python astock_strict_integer.py auth")
        return token
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}
    def device_code_auth(self):
        resp = requests.post(DEVICE_URL, data={"client_id": self.client_id, "scope": SCOPES}, timeout=30).json()
        print(f"\n🔗 请在浏览器打开：{resp['verification_uri']}")
        print(f"🔑 输入代码：{resp['user_code']}\n")
        deadline = time.time() + resp.get("expires_in", 900)
        while time.time() < deadline:
            time.sleep(resp.get("interval", 5))
            pr = requests.post(
                AUTH_URL,
                data={
                    "client_id": self.client_id,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": resp["device_code"],
                },
                timeout=30,
            ).json()
            if "access_token" in pr:
                self._data = {
                    "access_token": pr["access_token"],
                    "refresh_token": pr.get("refresh_token", ""),
                    "expires_at": time.time() + pr.get("expires_in", 3600),
                }
                self._save()
                log.info("✅ 授权成功")
                return
            if pr.get("error") != "authorization_pending":
                raise RuntimeError(f"设备码授权失败: {pr}")
        raise TimeoutError("设备码授权超时")
    def export_base64_cache(self) -> str:
        return base64.b64encode(json.dumps(self._data, ensure_ascii=False).encode("utf-8")).decode()
class OneDriveClient:
    def __init__(self, token_mgr: TokenManager, folder: str, remote_gz_name: str):
        self.tm = token_mgr
        self.folder = folder
        self.remote_gz_name = remote_gz_name
    def download_database_gz(self, local_path: str) -> bool:
        url = f"{GRAPH_BASE}/me/drive/root:/{self.folder}/{self.remote_gz_name}:/content"
        resp = requests.get(url, headers=self.tm.headers(), stream=True, timeout=60)
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
        session_resp = requests.post(
            url,
            headers={**self.tm.headers(), "Content-Type": "application/json"},
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            timeout=60,
        )
        session_resp.raise_for_status()
        upload_url = session_resp.json()["uploadUrl"]
        with open(local_path, "rb") as f:
            with tqdm(total=size, unit="B", unit_scale=True, desc="⬆️ 上传更新后数据库") as pbar:
                offset = 0
                while offset < size:
                    chunk = f.read(CHUNK_SIZE)
                    end = offset + len(chunk) - 1
                    put_resp = requests.put(
                        upload_url,
                        headers={
                            "Content-Range": f"bytes {offset}-{end}/{size}",
                            "Content-Length": str(len(chunk)),
                        },
                        data=chunk,
                        timeout=120,
                    )
                    put_resp.raise_for_status()
                    offset += len(chunk)
                    pbar.update(len(chunk))
                    
# =========================================================
# 本地 DB ↔ OneDrive gz 工具
# =========================================================
_DB_GZ_NAME = CONFIG["cloud_db_gz_name"]
def db_compress_and_upload(odc: OneDriveClient, db_path: str, gz_path: str) -> None:
    """将 db_path 压缩为 gz_path，然后上传到 OneDrive。"""
    with open(db_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    size_mb = os.path.getsize(gz_path) / 1024 / 1024
    log.info(f"📦 压缩完成 {size_mb:.1f} MB → 开始上传 ...")
    odc.upload_database_gz(gz_path)
    log.info("☁️  数据库已上传")


def db_compress_to_local(db_path: str, local_gz_path: str) -> None:
    """将 db_path 压缩并保存到本地 gz 文件。"""
    local_dir = os.path.dirname(local_gz_path)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)
    with open(db_path, "rb") as fi, gzip.open(local_gz_path, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    size_mb = os.path.getsize(local_gz_path) / 1024 / 1024
    log.info(f"📦 压缩完成 {size_mb:.1f} MB → 已保存本地 {local_gz_path}")


def db_decompress_from_download(gz_path: str, db_path: str) -> None:
    """将下载好的 gz_path 解压到 db_path（原始 duckdb 文件）。"""
    with gzip.open(gz_path, "rb") as fi, open(db_path, "wb") as fo:
        shutil.copyfileobj(fi, fo)


def obtain_db_gz(local_gz_path: Optional[str], odc: OneDriveClient, temp_gz_path: str) -> Tuple[str, bool]:
    """
    获取数据库压缩包：先尝试读取固定本地路径，失败后再从 OneDrive 下载到临时路径。
    返回 (source, ready)，source 为 local/cloud/none。
    """
    if local_gz_path and os.path.isfile(local_gz_path) and os.path.getsize(local_gz_path) > 1024:
        log.info(f"📁 使用本地数据库压缩文件: {local_gz_path}")
        shutil.copyfile(local_gz_path, temp_gz_path)
        return "local", True
    log.info("☁️ 本地数据库压缩文件不存在或无效，尝试从 OneDrive 下载")
    if odc.download_database_gz(temp_gz_path):
        return "cloud", True
    return "none", False


def load_db_gz_to_local(gz_path: str, db_path: str) -> None:
    """将 gz 文件解压为本地 duckdb 文件。"""
    db_decompress_from_download(gz_path, db_path)
# =========================================================
# 数据库表结构
# =========================================================
def ensure_core_tables(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {STOCKS_TABLE} (
            tradedate DATE,
            symbol VARCHAR,
            high DOUBLE,
            low DOUBLE,
            open DOUBLE,
            close DOUBLE,
            adjclose DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            PRIMARY KEY (symbol, tradedate)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_qfq_cache (
            symbol VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            PRIMARY KEY (symbol, date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_hfq_cache (
            symbol VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            PRIMARY KEY (symbol, date)
        )
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
            PRIMARY KEY (symbol, signal_date)
        )
    """)
    try:
        con.execute("ALTER TABLE pending_orders ADD COLUMN IF NOT EXISTS trade_type TINYINT")
        con.execute(f"UPDATE pending_orders SET trade_type = {TRADE_BUY} WHERE trade_type IS NULL")
    except Exception:
        pass
    con.execute("""
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            symbol VARCHAR PRIMARY KEY,
            buy_date DATE,
            buy_price DOUBLE,
            buy_price_hfq DOUBLE,
            shares BIGINT
        )
    """)
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
        con.execute("INSERT INTO account_state(id, init_capital, total_assets, available_cash, updated_at) VALUES (1, 1000000.0, 1000000.0, 1000000.0, CURRENT_DATE)")
def initialize_empty_database(db_path: str):
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        con.execute("CHECKPOINT")


def drop_cache_tables(con):
    """清理临时缓存表（每次运行重建），保留策略持久表（持仓/账户/交易记录）。"""
    for t in ["daily_qfq_cache", "daily_hfq_cache"]:
        con.execute(f'DROP TABLE IF EXISTS "{t}"')

def _migrate_db_schema(con):
    """一次性 schema 迁移: stock_prices TIMESTAMP→DATE + PRIMARY KEY + ROUND 精度。"""
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if STOCKS_TABLE not in tables:
        return
    cols = con.execute(f"PRAGMA table_info('{STOCKS_TABLE}')").fetchall()
    col_types = {c[1]: c[2] for c in cols}
    if col_types.get("tradedate") != "TIMESTAMP":
        return
    log.info("🔄 迁移 stock_prices: TIMESTAMP→DATE + PRIMARY KEY + ROUND精度 ...")
    con.execute("""
        CREATE TABLE stock_prices_new (
            tradedate DATE,
            symbol VARCHAR,
            high DOUBLE,
            low DOUBLE,
            open DOUBLE,
            close DOUBLE,
            adjclose DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            PRIMARY KEY (symbol, tradedate)
        )
    """)
    con.execute(f"""
        INSERT INTO stock_prices_new
        SELECT CAST(tradedate AS DATE), symbol,
               ROUND(high, 2), ROUND(low, 2), ROUND(open, 2),
               ROUND(close, 2), ROUND(adjclose, 2),
               ROUND(volume, 0), ROUND(amount, 2)
        FROM {STOCKS_TABLE}
    """)
    con.execute(f"DROP TABLE {STOCKS_TABLE}")
    con.execute("ALTER TABLE stock_prices_new RENAME TO stock_prices")
    con.execute("CHECKPOINT")
    log.info("✅ stock_prices 迁移完成")


# =========================================================
# investment_data 数据更新
# =========================================================
def _is_fresh_file(path: str, ttl_seconds: int) -> bool:
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= ttl_seconds


def _safe_extract_tar_strip_first(tar_path: str, target_dir: str) -> None:
    os.makedirs(target_dir, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            parts = member.name.split("/", 1)
            if len(parts) < 2 or not parts[1]:
                continue
            rel_name = parts[1]
            out_path = os.path.normpath(os.path.join(target_dir, rel_name))
            abs_target = os.path.abspath(target_dir)
            abs_out = os.path.abspath(out_path)
            if not abs_out.startswith(abs_target):
                continue
            if member.isdir():
                os.makedirs(abs_out, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(abs_out), exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                continue
            with open(abs_out, "wb") as f:
                shutil.copyfileobj(source, f)


def prepare_latest_qlib_data() -> str:
    session = build_retry_session()
    if _is_fresh_file(QLIB_TAR_PATH, int(CONFIG["source_cache_ttl_seconds"])):
        log.info("📦 命中 investment_data 缓存包，跳过下载")
    else:
        log.info(f"⬇️ 下载最新数据包: {QLIB_DATA_URL}")
        with session.get(QLIB_DATA_URL, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total_size = int(resp.headers.get("content-length", 0))
            with open(QLIB_TAR_PATH, "wb") as f:
                with tqdm(total=total_size, unit="B", unit_scale=True, desc="⬇️ 下载GitHub行情数据") as pbar:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
    _safe_extract_tar_strip_first(QLIB_TAR_PATH, QLIB_DATA_DIR)
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
    fields = ["$high", "$low", "$open", "$close", "$adjclose", "$volume", "$amount", "$factor"]
    df = D.features(
        instruments,
        fields,
        start_time=start_date.strftime("%Y-%m-%d"),
        end_time=end_date.strftime("%Y-%m-%d"),
    )
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.reset_index().rename(columns={
        "datetime": "tradedate",
        "instrument": "symbol",
        "$high": "high",
        "$low": "low",
        "$open": "open",
        "$close": "close",
        "$adjclose": "adjclose",
        "$volume": "volume",
        "$amount": "amount",
        "$factor": "factor",
    })
    out["tradedate"] = pd.to_datetime(out["tradedate"]).dt.date
    out["symbol"] = out["symbol"].map(normalize_qlib_symbol)
    for col in ["high", "low", "open", "close", "adjclose", "volume", "amount", "factor"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Qlib 的 OHLC 常为标准化值，需除以 factor 还原为可交易价格。
    factor = out["factor"].replace(0, np.nan).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    for col in ["open", "high", "low", "close"]:
        out[col] = out[col] / factor

    adj_raw = out["adjclose"].copy()
    adj_div = adj_raw / factor
    close_safe = out["close"].replace(0, np.nan)
    ratio_raw = (adj_raw / close_safe).replace([np.inf, -np.inf], np.nan)
    ratio_div = (adj_div / close_safe).replace([np.inf, -np.inf], np.nan)
    median_raw = ratio_raw.dropna().median()
    median_div = ratio_div.dropna().median()
    raw_ok = pd.notna(median_raw) and 0.2 <= float(median_raw) <= 5.0
    div_ok = pd.notna(median_div) and 0.2 <= float(median_div) <= 5.0
    if raw_ok:
        out["adjclose"] = adj_raw
    elif div_ok:
        out["adjclose"] = adj_div
    else:
        out["adjclose"] = out["close"]

    for col in ["open", "high", "low", "close", "adjclose"]:
        out[col] = out[col].where(out[col] > 0)
    for col in ["open", "high", "low", "close", "adjclose", "amount"]:
        out[col] = out[col].round(2)
    out["volume"] = out["volume"].round(0)

    out = out.dropna(subset=["tradedate", "symbol", "open", "high", "low", "close", "adjclose"])
    out = out[["tradedate", "symbol", "high", "low", "open", "close", "adjclose", "volume", "amount"]]
    return out


def get_last_trade_dates_from_qlib(target_date: date, n: int) -> List[date]:
    from qlib.data import D

    start_date = target_date - timedelta(days=max(60, n * 12))
    calendar = D.calendar(
        start_time=start_date.strftime("%Y-%m-%d"),
        end_time=target_date.strftime("%Y-%m-%d"),
        freq="day",
    )
    if calendar is None or len(calendar) == 0:
        return []
    dates = sorted(pd.to_datetime(calendar).date)
    return dates[-n:]


def _compare_and_sync_stock_rows(con, df_rows: pd.DataFrame) -> Tuple[int, int, int]:
    if df_rows is None or df_rows.empty:
        return 0, 0, 0
    tmp = df_rows.copy()
    tmp["tradedate"] = pd.to_datetime(tmp["tradedate"]).dt.date
    tmp["symbol"] = tmp["symbol"].map(canonical_symbol)
    tmp = tmp.drop_duplicates(subset=["tradedate", "symbol"], keep="last")
    for col in ["high", "low", "open", "close", "adjclose", "volume", "amount"]:
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
    for col in ["high", "low", "open", "close", "adjclose", "amount"]:
        tmp[col] = tmp[col].round(2)
    tmp["volume"] = tmp["volume"].round(0)
    tmp = tmp.dropna(subset=["tradedate", "symbol", "high", "low", "open", "close", "adjclose"])
    if tmp.empty:
        return 0, 0, 0

    try:
        con.unregister("tmp_new_stocks")
    except Exception:
        pass
    con.register("tmp_new_stocks", tmp)

    update_count = con.execute(f"""
        SELECT COUNT(*)
        FROM {STOCKS_TABLE} s
        JOIN tmp_new_stocks n
          ON s.tradedate = n.tradedate AND s.symbol = n.symbol
        WHERE COALESCE(s.open, -1) != COALESCE(n.open, -1)
           OR COALESCE(s.high, -1) != COALESCE(n.high, -1)
           OR COALESCE(s.low, -1) != COALESCE(n.low, -1)
           OR COALESCE(s.close, -1) != COALESCE(n.close, -1)
           OR COALESCE(s.adjclose, -1) != COALESCE(n.adjclose, -1)
           OR COALESCE(s.volume, -1) != COALESCE(n.volume, -1)
           OR COALESCE(s.amount, -1) != COALESCE(n.amount, -1)
    """).fetchone()[0]

    insert_count = con.execute(f"""
        SELECT COUNT(*)
        FROM tmp_new_stocks n
        LEFT JOIN {STOCKS_TABLE} s
          ON s.tradedate = n.tradedate AND s.symbol = n.symbol
        WHERE s.symbol IS NULL
    """).fetchone()[0]

    con.execute(f"""
        UPDATE {STOCKS_TABLE} AS s
        SET open = n.open,
            high = n.high,
            low = n.low,
            close = n.close,
            adjclose = n.adjclose,
            volume = n.volume,
            amount = n.amount
        FROM tmp_new_stocks n
        WHERE s.tradedate = n.tradedate
          AND s.symbol = n.symbol
          AND (
            COALESCE(s.open, -1) != COALESCE(n.open, -1)
            OR COALESCE(s.high, -1) != COALESCE(n.high, -1)
            OR COALESCE(s.low, -1) != COALESCE(n.low, -1)
            OR COALESCE(s.close, -1) != COALESCE(n.close, -1)
            OR COALESCE(s.adjclose, -1) != COALESCE(n.adjclose, -1)
            OR COALESCE(s.volume, -1) != COALESCE(n.volume, -1)
            OR COALESCE(s.amount, -1) != COALESCE(n.amount, -1)
          )
    """)

    con.execute(f"""
        INSERT INTO {STOCKS_TABLE} (tradedate, open, high, low, close, volume, amount, adjclose, symbol)
        SELECT n.tradedate, n.open, n.high, n.low, n.close, n.volume, n.amount, n.adjclose, n.symbol
        FROM tmp_new_stocks n
        LEFT JOIN {STOCKS_TABLE} s
          ON s.tradedate = n.tradedate AND s.symbol = n.symbol
        WHERE s.symbol IS NULL
    """)
    con.execute("CHECKPOINT")
    skip_count = int(len(tmp) - insert_count - update_count)
    return int(insert_count), int(update_count), max(skip_count, 0)


def investment_data_sync_recent_window(db_path: str, target_date: date, trade_days: int) -> Tuple[bool, List[date]]:
    provider_uri = prepare_latest_qlib_data()
    ensure_qlib_initialized(provider_uri)
    trade_dates = get_last_trade_dates_from_qlib(target_date, trade_days)
    if not trade_dates:
        log.warning("⚠️ Qlib 未返回可用交易日，跳过更新")
        return False, []

    window_start = trade_dates[0]
    window_end = trade_dates[-1]
    window_df = fetch_qlib_features(window_start, window_end)
    if window_df.empty:
        log.warning(f"⚠️ Qlib 未返回窗口数据: {window_start} ~ {window_end}")
        return False, trade_dates
    window_df = window_df[window_df["tradedate"].isin(set(trade_dates))].copy()
    if window_df.empty:
        return False, trade_dates

    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        inserted, updated, skipped = _compare_and_sync_stock_rows(con, window_df)
    log.info(f"✅ {STOCKS_TABLE} 5交易日比对完成: 插入={inserted}, 更新={updated}, 跳过={skipped}")
    return (inserted + updated) > 0 or skipped > 0, trade_dates


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
# 复权计算（前复权 QFQ / 后复权 HFQ）
# =========================================================
def calc_qfq_from_raw(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("tradedate").reset_index(drop=True)
    if g.empty:
        return g
    out = g.copy()
    out["adj_ratio"] = pd.to_numeric(out.get("adj_ratio", 1.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(1.0).astype(float)
    last_ratio = float(out["window_last_ratio"].iloc[0]) if "window_last_ratio" in out.columns else float(out["adj_ratio"].iloc[-1])
    if not np.isfinite(last_ratio) or last_ratio == 0:
        last_ratio = 1.0
    factor = out["adj_ratio"] / last_ratio
    for col in ["open", "high", "low", "close"]:
        scaled = pd.to_numeric(out[col], errors="coerce").astype(float) * factor
        out[col] = np.round(np.maximum(scaled, 0.01), 2)
    out["date"] = pd.to_datetime(out["tradedate"]).dt.date
    out["symbol"] = out["symbol"].map(canonical_symbol)
    return out[["symbol", "date", "open", "high", "low", "close", "volume", "amount"]]


def calc_hfq_from_raw(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("tradedate").reset_index(drop=True)
    if g.empty:
        return g
    out = g.copy()
    out["adj_ratio"] = pd.to_numeric(out.get("adj_ratio", 1.0), errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(1.0).astype(float)
    factor = out["adj_ratio"].replace([np.inf, -np.inf], np.nan).fillna(1.0)
    for col in ["open", "high", "low", "close"]:
        scaled = pd.to_numeric(out[col], errors="coerce").astype(float) * factor
        out[col] = np.round(np.maximum(scaled, 0.01), 2)
    out["date"] = pd.to_datetime(out["tradedate"]).dt.date
    out["symbol"] = out["symbol"].map(canonical_symbol)
    return out[["symbol", "date", "open", "high", "low", "close", "volume", "amount"]]


def rebuild_recent_adjusted_cache(db_path: str, end_date: date, window_days: int) -> bool:
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        recent_dates = get_recent_trade_dates(con, end_date, window_days)
        if not recent_dates:
            log.warning(f"没有足够的 {STOCKS_TABLE} 数据，无法构建复权缓存。")
            return False
        start_date = recent_dates[0]
        raw_df = con.execute(f"""
            SELECT symbol, tradedate, open, high, low, close, volume, amount, adjclose
            FROM {STOCKS_TABLE}
            WHERE tradedate BETWEEN ? AND ?
            ORDER BY symbol, tradedate
        """, [start_date, end_date]).df()
        if raw_df.empty:
            return False
        raw_df["tradedate"] = pd.to_datetime(raw_df["tradedate"]).dt.date
        raw_df["close"] = pd.to_numeric(raw_df["close"], errors="coerce")
        raw_df["adjclose"] = pd.to_numeric(raw_df["adjclose"], errors="coerce")
        raw_df["adj_ratio"] = (raw_df["adjclose"] / raw_df["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
        q_parts, h_parts = [], []
        for sym, g in raw_df.groupby("symbol"):
            g = g.sort_values("tradedate").reset_index(drop=True)
            g["window_first_ratio"] = float(g["adj_ratio"].iloc[0]) if not pd.isna(g["adj_ratio"].iloc[0]) else 1.0
            g["window_last_ratio"] = float(g["adj_ratio"].iloc[-1]) if not pd.isna(g["adj_ratio"].iloc[-1]) else 1.0
            q_parts.append(calc_qfq_from_raw(g.copy()))
            h_parts.append(calc_hfq_from_raw(g.copy()))
        con.execute("DELETE FROM daily_qfq_cache WHERE date BETWEEN ? AND ?", [start_date, end_date])
        con.execute("DELETE FROM daily_hfq_cache WHERE date BETWEEN ? AND ?", [start_date, end_date])
        if q_parts:
            df_q = pd.concat(q_parts, ignore_index=True)
            con.register("tmp_q_cache", df_q)
            con.execute("""
                INSERT INTO daily_qfq_cache
                SELECT symbol, date, open, high, low, close, volume, amount
                FROM tmp_q_cache
            """)
            latest_q = df_q[df_q["date"] == end_date]
            if len(latest_q) > 100 and float((latest_q["close"] < 2.0).mean()) > 0.8:
                log.warning("⚠️ qfq 价格大比例低于2元，请检查源数据尺度与factor处理")
        if h_parts:
            df_h = pd.concat(h_parts, ignore_index=True)
            con.register("tmp_h_cache", df_h)
            con.execute("""
                INSERT INTO daily_hfq_cache
                SELECT symbol, date, open, high, low, close, volume, amount
                FROM tmp_h_cache
            """)
            latest_h = df_h[df_h["date"] == end_date]
            if len(latest_h) > 100 and float((latest_h["close"] < 2.0).mean()) > 0.8:
                log.warning("⚠️ hfq 价格大比例低于2元，请检查源数据尺度与adjclose处理")
        con.execute("CHECKPOINT")
    log.info(f"✅ 已重建最近 {window_days} 个交易日复权缓存")
    return True
# =========================================================
# 策略逻辑
# =========================================================
def get_account_state(con) -> Tuple[float, float, float]:
    row = con.execute("SELECT init_capital, total_assets, available_cash FROM account_state WHERE id = 1").fetchone()
    if not row:
        return 1000000.0, 1000000.0, 1000000.0
    return row[0], row[1], row[2]

def detect_kline_patterns(open_s, high_s, low_s, close_s) -> Tuple[str, float, str, float]:
    # Returns (BullishPatternName, BullishScore, BearishPatternName, BearishScore)
    o = open_s.values.astype(float)
    h = high_s.values.astype(float)
    l = low_s.values.astype(float)
    c = close_s.values.astype(float)
    
    bull_patterns = []
    bear_patterns = []
    
    cdl_names = {
        'CDL2CROWS': '两只乌鸦', 'CDL3BLACKCROWS': '三只乌鸦', 'CDL3INSIDE': '三内部', 
        'CDL3LINESTRIKE': '三线打击', 'CDL3OUTSIDE': '三外部', 'CDL3STARSINSOUTH': '南方三星', 
        'CDL3WHITESOLDIERS': '三个白兵', 'CDLABANDONEDBABY': '弃婴', 'CDLADVANCEBLOCK': '大敌当前', 
        'CDLBELTHOLD': '捉腰带线', 'CDLBREAKAWAY': '脱离', 'CDLCLOSINGMARUBOZU': '收盘缺影线', 
        'CDLCONCEALBABYSWALL': '藏婴吞没', 'CDLCOUNTERATTACK': '反击线', 'CDLDARKCLOUDCOVER': '乌云压顶', 
        'CDLDOJI': '十字', 'CDLDOJISTAR': '十字星', 'CDLDRAGONFLYDOJI': '蜻蜓十字', 
        'CDLENGULFING': '吞噬模式', 'CDLEVENINGDOJISTAR': '十字暮星', 'CDLEVENINGSTAR': '暮星', 
        'CDLGAPSIDESIDEWHITE': '跳空并列阳线', 'CDLGRAVESTONEDOJI': '墓碑十字', 'CDLHAMMER': '锤头', 
        'CDLHANGINGMAN': '上吊线', 'CDLHARAMI': '母子线', 'CDLHARAMICROSS': '十字孕线', 
        'CDLHIGHWAVE': '风高浪大线', 'CDLHIKKAKE': '陷阱', 'CDLHIKKAKEMOD': '修正陷阱', 
        'CDLHOMINGPIGEON': '家鸽', 'CDLIDENTICAL3CROWS': '三胞胎乌鸦', 'CDLINNECK': '颈内线', 
        'CDLINVERTEDHAMMER': '倒锤头', 'CDLKICKING': '反冲形态', 'CDLKICKINGBYLENGTH': '较长缺影线反冲', 
        'CDLLADDERBOTTOM': '梯底', 'CDLLONGLEGGEDDOJI': '长脚十字', 'CDLLONGLINE': '长蜡烛', 
        'CDLMARUBOZU': '光头光脚', 'CDLMATCHINGLOW': '相同低价', 'CDLMATHOLD': '铺垫', 
        'CDLMORNINGDOJISTAR': '十字晨星', 'CDLMORNINGSTAR': '晨星', 'CDLONNECK': '颈上线', 
        'CDLPIERCING': '刺透形态', 'CDLRICKSHAWMAN': '黄包车夫', 'CDLRISEFALL3METHODS': '上升/下降三法', 
        'CDLSEPARATINGLINES': '分离线', 'CDLSHOOTINGSTAR': '射击之星', 'CDLSHORTLINE': '短蜡烛', 
        'CDLSPINNINGTOP': '纺锤', 'CDLSTALLEDPATTERN': '停顿形态', 'CDLSTICKSANDWICH': '条形三明治', 
        'CDLTAKURI': '探水竿', 'CDLTASUKIGAP': '跳空并列阴阳线', 'CDLTHRUSTING': '插入', 
        'CDLTRISTAR': '三星', 'CDLUNIQUE3RIVER': '奇特三河床', 'CDLUPSIDEGAP2CROWS': '向上跳空两只乌鸦', 
        'CDLXSIDEGAP3METHODS': '跳空三法'
    }
    
    if talib is not None:
        for func_name, cn_name in cdl_names.items():
            if hasattr(talib, func_name):
                func = getattr(talib, func_name)
                try:
                    res = func(o, h, l, c)
                    if len(res) > 0:
                        if res[-1] > 0:
                            bull_patterns.append(cn_name)
                        elif res[-1] < 0:
                            bear_patterns.append(cn_name)
                except:
                    pass

    # Custom patterns
    if len(c) >= 3:
        prev_body = abs(o[-2] - c[-2])
        if c[-2] < o[-2] and prev_body > (c[-2] * 0.03):
            if o[-1] > c[-2] and c[-1] > o[-2]:
                bull_patterns.append("旭日东升")
            if o[-1] < c[-2] and abs(c[-1] - c[-2]) / c[-2] < 0.005:
                bull_patterns.append("好友反攻")
        if all(c[i] > o[i] for i in range(-3, 0)) and c[-1] > c[-2] > c[-3]:
            bull_patterns.append("由于连阳")
            
    body = abs(c[-1] - o[-1])
    upper = h[-1] - max(o[-1], c[-1])
    lower = min(o[-1], c[-1]) - l[-1]
    if body > 0 and upper > 1.5 * body and lower > 1.5 * body and c[-1] < np.mean(c[-10:]):
        bull_patterns.append("低位螺旋桨")
        
    if len(c) >= 15:
        last_15 = c[-15:]
        min_idx = np.argmin(last_15)
        if 2 < min_idx < 12 and last_15[-1] > last_15[min_idx] * 1.05 and last_15[0] > last_15[min_idx] * 1.05:
            bull_patterns.append("疑似圆弧底")
            
    bull_text = " | ".join(bull_patterns[:3]) if bull_patterns else "无明显上涨形态"
    bear_text = " | ".join(bear_patterns[:3]) if bear_patterns else "无明显下跌形态"
    
    return bull_text, min(len(bull_patterns), 5.0), bear_text, min(len(bear_patterns), 5.0)
def process_pending_orders(con, trade_date: date) -> Tuple[List[Tuple], List[Tuple]]:
    pending_df = con.execute("""
        SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status
        FROM pending_orders
        WHERE status=0 AND signal_date < ?
    """, [trade_date]).df()
    if pending_df.empty:
        return [], []
    qfq_today = con.execute("SELECT symbol, date, low, close FROM daily_qfq_cache WHERE date = ?", [trade_date]).df()
    hfq_today = con.execute("SELECT symbol, date, close FROM daily_hfq_cache WHERE date = ?", [trade_date]).df()
    if qfq_today.empty:
        return [], []
        
    init_cap, total_assets, avail_cash = get_account_state(con)
    # Check reset condition
    if total_assets <= 0:
        log.warning("资产归零或异常，执行重置恢复至100万元初始资金")
        init_cap = total_assets = avail_cash = 1000000.0
        con.execute("DELETE FROM virtual_portfolio")
        con.execute("UPDATE account_state SET init_capital=?, total_assets=?, available_cash=? WHERE id=1", [init_cap, total_assets, avail_cash])

    # 每只股票的目标资金：取 CONFIG 配置值与动态均分资金中的较小值
    position_cash_yuan = min(CONFIG['position_cash_yuan'], total_assets / max(1, CONFIG['top_n']))
    buy_fee_rate = float(CONFIG.get("buy_fee_rate", 0.0005))
    
    q_map = {row['symbol']: row for _, row in qfq_today.iterrows()}
    h_map = {row['symbol']: row for _, row in hfq_today.iterrows()}
    filled_rows, expired_rows = [], []
    for _, row in pending_df.iterrows():
        symbol = row['symbol']
        signal_date = row['signal_date']
        planned_buy_price = float(row['planned_buy_price'])
        if symbol not in q_map:
            expired_rows.append((symbol, signal_date))
            continue
        today_low_qfq = float(q_map[symbol]['low'])
        today_close_qfq = float(q_map[symbol]['close'])
        today_close_hfq = float(h_map[symbol]['close']) if symbol in h_map else today_close_qfq
        
        # Determine if budget allows buying
        if avail_cash >= position_cash_yuan * 0.5:  # ensure at least 50% of intended budget is available
            cost_yuan_budget = min(position_cash_yuan, avail_cash)
        else:
            expired_rows.append((symbol, signal_date))
            continue

        if today_low_qfq <= planned_buy_price:
            factor = (today_close_hfq / today_close_qfq) if today_close_qfq > 0 else 1.0
            buy_price_hfq = round(planned_buy_price * factor, 2)
            
            # Request: A股股数全是整百，不存在碎股
            lot_cost = planned_buy_price * 100.0 * (1.0 + buy_fee_rate)
            shares = int(cost_yuan_budget / lot_cost) * 100
            if shares < 100:
                expired_rows.append((symbol, signal_date))
                continue
            
            gross_cost = round(shares * planned_buy_price, 2)
            buy_fee = round(gross_cost * buy_fee_rate, 2)
            actual_cost = round(gross_cost + buy_fee, 2)
            if actual_cost > avail_cash:
                shares = int(avail_cash / lot_cost) * 100
                if shares < 100:
                    expired_rows.append((symbol, signal_date))
                    continue
                gross_cost = round(shares * planned_buy_price, 2)
                buy_fee = round(gross_cost * buy_fee_rate, 2)
                actual_cost = round(gross_cost + buy_fee, 2)
            
            avail_cash -= actual_cost
            filled_rows.append((symbol, trade_date, planned_buy_price, buy_price_hfq, int(shares), float(buy_fee)))
        else:
            expired_rows.append((symbol, signal_date))
            
    if filled_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
        con.executemany("""
            INSERT OR REPLACE INTO virtual_portfolio(symbol, buy_date, buy_price, buy_price_hfq, shares)
            VALUES (?, ?, ?, ?, ?)
        """, [(s, d, bp, bph, sh) for s, d, bp, bph, sh, _ in filled_rows])
        for symbol, buy_date, buy_price, buy_price_hfq, shares, buy_fee in filled_rows:
            con.execute(f"UPDATE pending_orders SET status={STATUS_FILLED} WHERE symbol=? AND status={STATUS_PENDING}", [symbol])
            con.execute("""
                INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
                VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, ?)
            """, [symbol, TRADE_BUY, buy_date, round(buy_price, 2), shares, REASON_BUY_T1, round(buy_fee, 2)])
    if expired_rows:
        con.executemany(f"UPDATE pending_orders SET status={STATUS_EXPIRED} WHERE symbol=? AND signal_date=? AND status={STATUS_PENDING}", expired_rows)
    return filled_rows, expired_rows
def process_exit_rules(con, trade_date: date) -> List[Tuple]:
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares FROM virtual_portfolio").df()
    if holdings.empty:
        return []
    symbols = holdings['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    start_date = (trade_date - timedelta(days=80)).strftime('%Y-%m-%d')
    qfq_df = con.execute(f"""
        SELECT symbol, date, open, high, low, close
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()
    hfq_df = con.execute(f"""
        SELECT symbol, date, close
        FROM daily_hfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()
    # 获取当日原始价格（用于计算卖出回收现金）
    raw_today = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [trade_date]).df()
    if not raw_today.empty:
        raw_today["symbol"] = raw_today["symbol"].map(canonical_symbol)
    raw_map = {row['symbol']: float(row['close']) for _, row in raw_today.iterrows()} if not raw_today.empty else {}
    if qfq_df.empty:
        return []
    qfq_df['date'] = pd.to_datetime(qfq_df['date'])
    if not hfq_df.empty:
        hfq_df['date'] = pd.to_datetime(hfq_df['date'])
    sold_rows = []
    init_cap, total_assets, avail_cash = get_account_state(con)
    sell_fee_rate = float(CONFIG.get("sell_fee_rate", 0.0010))
    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = pd.to_datetime(row['buy_date']).date()
        buy_price = float(row['buy_price'])
        buy_price_hfq = float(row['buy_price_hfq']) if not pd.isna(row['buy_price_hfq']) else buy_price
        shares = int(row['shares'])  # 整百股
        gq = qfq_df[qfq_df['symbol'] == sym].copy()
        if gq.empty:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym].copy() if not hfq_df.empty else pd.DataFrame()
        gq['close_f'] = gq['close'].astype(float)
        gq['ma20_f'] = gq['close_f'].rolling(20).mean()
        last_q = gq.iloc[-1]
        last_close_qfq = float(last_q['close'])
        
        last_close_hfq = float(gh.iloc[-1]['close']) if not gh.empty else last_close_qfq
        hold_days = (trade_date - buy_date).days
        pnl_pct = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100 if buy_price_hfq > 0 else 0.0
        reason_mask = 0
        
        # 移除止盈比例检查，保留止损
        if pnl_pct <= CONFIG['stop_loss_pct']:
            reason_mask |= REASON_STOPLOSS
            
        gq['ma20_f'] = gq['close_f'].rolling(20).mean()
        gq['bb_mid'] = gq['close_f'].rolling(20).mean()
        gq['bb_std'] = gq['close_f'].rolling(20).std()
        gq['bb_lower'] = gq['bb_mid'] - 2 * gq['bb_std']
        
        last_close_f = float(last_q['close_f'])
        prev_close_f = float(gq.iloc[-2]['close_f']) if len(gq) >= 2 else last_close_f
        last_mid = float(gq['bb_mid'].iloc[-1]) if not pd.isna(gq['bb_mid'].iloc[-1]) else 0
        ma20_float = None if pd.isna(last_q['ma20_f']) else float(last_q['ma20_f'])
        
        if ma20_float is not None and last_close_qfq < ma20_float:
            reason_mask |= REASON_BELOW_MA20
        if hold_days >= CONFIG['max_hold_days']:
            reason_mask |= REASON_MAX_HOLD

        # 布林带形态卖出：跌破布林中轨或下轨
        if last_mid > 0 and prev_close_f > last_mid and last_close_f < last_mid:
            reason_mask |= REASON_BELOW_BOLL_MID
            
        # K线形态卖出：含有明确的下跌形态
        o_s = gq['open'].astype(float)
        h_s = gq['high'].astype(float)
        l_s = gq['low'].astype(float)
        c_s = gq['close_f']
        bull_text, bull_score, bear_text, bear_score = detect_kline_patterns(o_s, h_s, l_s, c_s)
        
        if bear_score >= 1.0:
            reason_mask |= REASON_BEAR_PATTERN

        if talib is not None and len(gq) > 30:
            macd, macdsignal, macdhist = talib.MACD(gq['close_f'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            if not pd.isna(macdhist[-1]) and not pd.isna(macdhist[-2]):
                if macdhist[-1] < macdhist[-2]:
                    reason_mask |= REASON_MACD_DECREASE
                    
        if reason_mask > 0:
            # 使用原始价格计算卖出回收现金（不折价，以信号当天收盘价卖出）
            sell_price_raw = raw_map.get(sym, last_close_qfq)  # 优先用raw，否则用qfq
            sold_rows.append((sym, trade_date, last_close_qfq, shares, reason_mask, round(pnl_pct, 2), sell_price_raw))
            
    for sym, sell_date, sell_price, shares, reason_mask, pnl_pct, sell_price_raw in sold_rows:
        con.execute('DELETE FROM virtual_portfolio WHERE symbol=?', [sym])
        gross_cash = round(shares * sell_price_raw, 2)
        sell_fee = round(gross_cash * sell_fee_rate, 2)
        recovered_cash = round(gross_cash - sell_fee, 2)
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
        """, [sym, TRADE_SELL, sell_date, round(sell_price, 2), shares, reason_mask, pnl_pct, round(sell_fee, 2)])
        avail_cash += recovered_cash
        
    if sold_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
    return sold_rows
def evaluate_strategy(db_path: str, target_date: date, top_n: Optional[int] = None, allow_exit_on_date: bool = True):
    top_n = top_n or CONFIG["top_n"]
    with duckdb.connect(db_path, read_only=False) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        process_pending_orders(con, target_date)
        history_before = con.execute("SELECT COUNT(*) FROM account_history WHERE date < ?", [target_date]).fetchone()[0]
        if allow_exit_on_date and int(history_before) > 0:
            process_exit_rules(con, target_date)
        else:
            log.info(f"🛡️ 初始交易日 {target_date} 仅允许买入，跳过卖出规则")
        start_date = (target_date - timedelta(days=420)).strftime("%Y-%m-%d")
        end_date = target_date.strftime("%Y-%m-%d")
        df = con.execute("""
            SELECT symbol, date, open, high, low, close, volume
            FROM daily_qfq_cache
            WHERE date BETWEEN ? AND ?
            ORDER BY symbol, date
        """, [start_date, end_date]).df()
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}
        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        picks_rows = []
        for sym, g in df.groupby("symbol"):
            g = g.sort_values("date").reset_index(drop=True)
            if len(g) < 80:
                continue
            close = g["close"].astype(float)
            volume = g["volume"].astype(float)

            # ── 布林带计算（第一优先级过滤器）──
            bb_period = 20
            g["bb_mid"] = close.rolling(bb_period).mean()
            g["bb_std"] = close.rolling(bb_period).std()
            g["bb_upper"] = g["bb_mid"] + 2 * g["bb_std"]
            g["bb_lower"] = g["bb_mid"] - 2 * g["bb_std"]
            g["bb_width"] = g["bb_upper"] - g["bb_lower"]

            if len(g) < 22 or pd.isna(g["bb_mid"].iloc[-1]) or pd.isna(g["bb_mid"].iloc[-2]):
                continue

            last = g.iloc[-1]
            prev = g.iloc[-2]

            bb_expanding = last["bb_width"] > prev["bb_width"]   # 布林带开口
            bb_mid_up = last["bb_mid"] > prev["bb_mid"]          # 中轨向上
            bb_mid_not_down = last["bb_mid"] >= prev["bb_mid"]   # 中轨不向下

            # 条件1: 布林带开口 + 股价沿上布林带向上（当日最低价不能大于上布林带）
            cond_bb1 = bb_expanding and bb_mid_up and last["low"] <= last["bb_upper"]
            # 条件2: 股价接近中轨 + 布林带趋势向上
            cond_bb2 = (abs(last["close"] - last["bb_mid"]) / last["bb_mid"] < 0.03) and bb_mid_up
            # 条件3: 股价跌穿下布林带 + 布林带趋势不向下
            cond_bb3 = last["close"] <= last["bb_lower"] and bb_mid_not_down

            if not (cond_bb1 or cond_bb2 or cond_bb3):
                continue

            # ── 其他因子 ──
            g["ret_20"] = close.pct_change(20)
            g["ret_60"] = close.pct_change(60)
            g["ma20"] = close.rolling(20).mean()
            g["ma60"] = close.rolling(60).mean()
            g["vol_ma5"] = volume.rolling(5).mean()
            g["vol_ma20"] = volume.rolling(20).mean()
            g["vol_ratio"] = volume / g["vol_ma20"]
            g["up_day"] = close > close.shift(1)
            g["vol_up"] = g["up_day"] & (volume > g["vol_ma20"])
            g["vol_up_count_20"] = g["vol_up"].rolling(20).sum()

            # MACD 趋势判断
            macd_trend_increasing = True
            if talib is not None:
                _, _, macdhist = talib.MACD(close.values, fastperiod=12, slowperiod=26, signalperiod=9)
                g["macdhist"] = macdhist
                g["macdhist_diff"] = g["macdhist"].diff()
                if len(g) > 30:
                    recent_diffs = g["macdhist_diff"].iloc[-5:].values
                    macd_trend_increasing = (sum(recent_diffs > 0) >= 3) and (g["macdhist"].iloc[-1] > g["macdhist"].iloc[-2])

            if not macd_trend_increasing:
                continue

            # 刷新 last（因为新增了列）
            last = g.iloc[-1]
            req = ["ret_20", "ret_60", "ma20", "ma60", "vol_ma5", "vol_ma20", "vol_ratio", "vol_up_count_20"]
            if any(pd.isna(last[c]) for c in req):
                continue
            cond_momentum = last["ret_20"] > 0 and last["ret_60"] > 0 and last["close"] > last["ma20"] > last["ma60"]
            cond_volume = last["vol_ratio"] > 1.2 and last["vol_ma5"] > last["vol_ma20"] and last["vol_up_count_20"] >= 3
            if not (cond_momentum and cond_volume):
                continue

            # ── 综合评分 ──
            momentum_score = last["ret_20"] * 40 + last["ret_60"] * 30 + ((last["close"] / last["ma20"]) - 1) * 100 * 15 + ((last["ma20"] / last["ma60"]) - 1) * 100 * 15
            volume_score = min(last["vol_ratio"], 3.0) * 15 + min(last["vol_ma5"] / last["vol_ma20"], 2.0) * 10 + min(last["vol_up_count_20"], 10) * 2

            # 布林带评分
            boll_score = 0.0
            if cond_bb1:
                boll_score += 3.0  # 开口向上沿上轨
            if cond_bb2:
                boll_score += 2.0  # 接近中轨且趋势向上
            if cond_bb3:
                boll_score += 1.5  # 跌穿下轨但趋势不向下
            if bb_expanding:
                boll_score += 1.0  # 额外开口加分
            boll_score = boll_score * 10.0

            bull_text, bull_score, bear_text, bear_score = detect_kline_patterns(g["open"], g["high"], g["low"], g["close"])
            pattern_score = bull_score * 10.0
            pattern_text = bull_text

            total_score = momentum_score + volume_score + boll_score + pattern_score
            planned_buy_price = round(float(last["close"]) * 0.985, 2)

            # 记录命中的布林带条件
            bb_tags = []
            if cond_bb1:
                bb_tags.append("开口上行")
            if cond_bb2:
                bb_tags.append("中轨回踩")
            if cond_bb3:
                bb_tags.append("下轨支撑")
            bb_text = " | ".join(bb_tags) if bb_tags else "—"

            picks_rows.append({
                "symbol": sym,
                "date": last["date"].date(),
                "close": round(float(last["close"]), 2),
                "planned_buy_price": planned_buy_price,
                "ret_20": round(float(last["ret_20"]), 2),
                "ret_60": round(float(last["ret_60"]), 2),
                "ma20": round(float(last["ma20"]), 2),
                "ma60": round(float(last["ma60"]), 2),
                "vol_ratio": round(float(last["vol_ratio"]), 2),
                "vol_up_count_20": float(last["vol_up_count_20"]),
                "pattern": pattern_text,
                "bb_condition": bb_text,
                "momentum_score": round(momentum_score, 2),
                "volume_score": round(volume_score, 2),
                "boll_score": round(boll_score, 2),
                "pattern_score": round(pattern_score, 2),
                "total_score": round(total_score, 2),
            })
        df_picks = pd.DataFrame(picks_rows)
        if not df_picks.empty:
            df_picks = df_picks.sort_values("total_score", ascending=False).head(top_n).reset_index(drop=True)

        # ── 挂单逻辑：清除旧挂单，仅从当日候选票中挂单 ──
        con.execute(f"UPDATE pending_orders SET status={STATUS_EXPIRED} WHERE status={STATUS_PENDING}")
        holdings_df = con.execute("SELECT symbol FROM virtual_portfolio").df()
        holding_symbols = set(holdings_df["symbol"]) if not holdings_df.empty else set()
        new_orders = []
        if not df_picks.empty:
            for _, row in df_picks.iterrows():
                symbol = row["symbol"]
                if symbol in holding_symbols:
                    continue
                new_orders.append((symbol, target_date, round(float(row["planned_buy_price"]), 2), round(float(row["close"]), 2), TRADE_BUY, STATUS_PENDING))
            if new_orders:
                con.executemany("""
                    INSERT OR REPLACE INTO pending_orders(symbol, signal_date, planned_buy_price, signal_close, trade_type, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, new_orders)

        df_pending = con.execute(f"""
            SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status
            FROM pending_orders
            WHERE status={STATUS_PENDING}
            ORDER BY signal_date DESC, symbol
        """).df()

        # ── 持仓与市值计算（使用原始价格）──
        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
            total_market_value = 0.0
        else:
            # 使用原始价格计算市值（非复权价格）
            raw_today_df = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [target_date]).df()
            if not raw_today_df.empty:
                raw_today_df["symbol"] = raw_today_df["symbol"].map(canonical_symbol)
            hfq_today_df = con.execute("SELECT symbol, close FROM daily_hfq_cache WHERE date = ?", [target_date]).df()
            df_portfolio = holdings.merge(raw_today_df.rename(columns={"close": "last_price"}), on="symbol", how="left")
            df_portfolio = df_portfolio.merge(hfq_today_df.rename(columns={"close": "last_price_hfq"}), on="symbol", how="left")
            df_portfolio["last_price"] = df_portfolio["last_price"].fillna(df_portfolio["buy_price"])
            df_portfolio["last_price_hfq"] = df_portfolio["last_price_hfq"].fillna(df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]))
            # 股数强制为整百（四舍五入到最近的100）
            df_portfolio["shares"] = (np.round(df_portfolio["shares"].astype(float) / 100) * 100).astype(int)
            # 过滤不足100股的无效持仓（清理历史脏数据）
            invalid_symbols = df_portfolio[df_portfolio["shares"] < 100]["symbol"].tolist()
            if invalid_symbols:
                log.warning(f"⚠️ 过滤不足100股的无效持仓: {invalid_symbols}")
                for sym in invalid_symbols:
                    con.execute("DELETE FROM virtual_portfolio WHERE symbol=?", [sym])
                df_portfolio = df_portfolio[df_portfolio["shares"] >= 100].copy()
            # 使用原始价格计算市值
            df_portfolio["market_value"] = (df_portfolio["last_price"].astype(float) * df_portfolio["shares"]).astype(float)
            df_portfolio["cost"] = (df_portfolio["buy_price"].astype(float) * df_portfolio["shares"]).astype(float)
            df_portfolio["pnl_pct"] = (df_portfolio["last_price_hfq"] - df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"])) / df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]) * 100
            df_portfolio["holding_days"] = df_portfolio["buy_date"].apply(lambda x: (target_date - pd.to_datetime(x).date()).days)
            total_market_value = df_portfolio["market_value"].sum()

        # Account tracking update
        init_cap, _, avail_cash = get_account_state(con)
        new_total_assets = avail_cash + total_market_value
        con.execute("UPDATE account_state SET total_assets=?, updated_at=? WHERE id=1", [new_total_assets, target_date])

        prev_assets_row = con.execute("SELECT total_assets FROM account_history WHERE date < ? ORDER BY date DESC LIMIT 1", [target_date]).fetchone()
        prev_assets = prev_assets_row[0] if prev_assets_row else init_cap
        daily_pnl = new_total_assets - prev_assets
        daily_ret = daily_pnl / prev_assets if prev_assets > 0 else 0.0

        con.execute("""
            INSERT OR REPLACE INTO account_history(date, total_assets, available_cash, daily_pnl, daily_ret, market_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [target_date, round(new_total_assets, 2), round(avail_cash, 2), round(daily_pnl, 2), round(daily_ret, 4), round(total_market_value, 2)])

        # ── 绩效指标计算 ──
        chart_b64 = None
        sharpe = 0.0
        max_drawdown = 0.0
        calmar = 0.0
        annual_ret = 0.0

        hist_df = con.execute("SELECT date, daily_ret, total_assets FROM account_history ORDER BY date ASC").df()
        if len(hist_df) >= 2:
            try:
                hist_df['date'] = pd.to_datetime(hist_df['date'])
                # 夏普比率（无风险利率 1.8%，日化）
                rf_annual = 0.018
                rf_daily = rf_annual / 252
                mean_ret = hist_df['daily_ret'].mean()
                std_ret = hist_df['daily_ret'].std()
                sharpe = (mean_ret - rf_daily) / std_ret * np.sqrt(252) if std_ret > 0 else 0

                # 最大回撤
                assets_arr = hist_df['total_assets'].values
                peak = assets_arr[0]
                for v in assets_arr:
                    if v > peak:
                        peak = v
                    dd = (peak - v) / peak if peak > 0 else 0
                    if dd > max_drawdown:
                        max_drawdown = dd

                # 年化收益率
                n_days = len(hist_df)
                total_ret = (new_total_assets / init_cap) - 1.0
                annual_ret = (1 + total_ret) ** (252 / max(n_days, 1)) - 1.0

                # 收益回撤比（Calmar Ratio）
                calmar = annual_ret / max_drawdown if max_drawdown > 0 else 0.0

                # 绘图
                plt.figure(figsize=(10, 4))
                ax1 = plt.subplot(1, 1, 1)
                ax1.plot(hist_df['date'], hist_df['total_assets'] / init_cap, color='#3b82f6', linewidth=2, label='Portfolio Net Value')
                ax1.set_title(f"Portfolio Curve (Sharpe: {sharpe:.2f} | MaxDD: {max_drawdown*100:.1f}% | Calmar: {calmar:.2f})")
                ax1.grid(True, linestyle='--', alpha=0.6)
                ax1.set_ylabel('Net Value')
                ax1.legend(loc='upper left')
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                plt.tight_layout()
                buf = io.BytesIO()
                plt.savefig(buf, format='png', dpi=120)
                plt.close()
                buf.seek(0)
                chart_b64 = base64.b64encode(buf.read()).decode('utf-8')
            except Exception as e:
                log.error(f"Plotting failed: {e}")

        metrics = {
            "total_assets": new_total_assets,
            "avail_cash": avail_cash,
            "market_value": total_market_value,
            "position_pct": total_market_value / new_total_assets * 100 if new_total_assets > 0 else 0,
            "cash_pct": avail_cash / new_total_assets * 100 if new_total_assets > 0 else 100,
            "total_pnl": new_total_assets - init_cap,
            "total_pnl_pct": (new_total_assets / init_cap - 1) * 100,
            "daily_pnl": daily_pnl,
            "daily_ret": daily_ret,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
            "calmar": calmar,
            "chart_b64": chart_b64,
        }

        df_trades = con.execute("SELECT * FROM trade_history WHERE trade_date = ? ORDER BY trade_type, symbol", [target_date]).df()
    return decode_numeric_frame(df_picks), decode_numeric_frame(df_portfolio), decode_numeric_frame(df_pending), decode_numeric_frame(df_trades), metrics
# =========================================================
# 报告邮件
# =========================================================
def send_email_via_graph(tm: TokenManager, subject: str, html_body: str, attachments: Optional[List] = None):
    to_addr = CONFIG["email_to"]
    if not to_addr:
        log.warning("未配置 EMAIL_TO，跳过邮件发送")
        return
    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
        },
        "saveToSentItems": True,
    }
    if attachments:
        message["message"]["attachments"] = attachments
        
    session = build_retry_session()
    for attempt in range(1, 4):
        try:
            resp = session.post(
                f"{GRAPH_BASE}/me/sendMail",
                headers={**tm.headers(), "Content-Type": "application/json"},
                json=message,
                timeout=60,
            )
            resp.raise_for_status()
            log.info("📧 报告邮件发送成功")
            return
        except Exception as e:
            if attempt == 3:
                log.error(f"❌ 无法发送邮件，已达到最大重试次数: {e}")
                raise
            log.warning(f"⚠️ 发送邮件尝试 {attempt} 失败，将重试: {e}")
            time.sleep(attempt * 2)

# =========================================================
# 报告邮件（美化版）
# =========================================================
def _market_badge(symbol: str) -> str:
    market = symbol_market(symbol)
    code = symbol_code(symbol)
    if market == "sh":
        if code.startswith("688"):
            return f'<span class="badge badge-kcb">科创</span> {code}'
        return f'<span class="badge badge-sh">沪主</span> {code}'
    if market == "sz":
        if code.startswith("300"):
            return f'<span class="badge badge-cy">创业</span> {code}'
        if code.startswith("002"):
            return f'<span class="badge badge-zx">中小</span> {code}'
        return f'<span class="badge badge-sz">深主</span> {code}'
    if market == "bj" or code.startswith("920"):
        return f'<span class="badge badge-bj">北交</span> {code}'
    return f'<span class="badge badge-sz">{symbol}</span>'
def _score_bar(value: float, max_val: float = 1180.0) -> str:
    pct = min(int(value / max_val * 100), 100)
    return (
        f'<div class="score-bar-wrap">'
        f'<div class="score-bar"><div class="score-fill" style="width:{pct}%"></div></div>'
        f'<span class="score-text">{value:.1f}</span>'
        f'</div>'
    )
def _ret_cell(v: float) -> str:
    sign = "+" if v >= 0 else ""
    cls = "ret-pos" if v >= 0 else "ret-neg"
    return f'<span class="{cls}">{sign}{v*100:.1f}%</span>'
def _picks_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>今日无候选股票</div></div>'
    max_score = float(df["total_score"].max()) if not df.empty else 1.0
    rows = []
    for i, r in enumerate(df.itertuples(), 1):
        bb_cond = getattr(r, 'bb_condition', '—')
        rows.append(f"""
        <tr>
          <td>{i}</td>
          <td>{_market_badge(r.symbol)}</td>
          <td>¥{float(r.close):.2f}</td>
          <td>¥{float(r.planned_buy_price):.2f}</td>
          <td>{_ret_cell(r.ret_20)}</td>
          <td>{_ret_cell(r.ret_60)}</td>
          <td>{r.vol_ratio:.2f}</td>
          <td>{int(r.vol_up_count_20)}</td>
          <td><span class="badge badge-bb">{bb_cond}</span></td>
          <td><span class="badge badge-pending">{r.pattern}</span></td>
          <td>{_score_bar(r.total_score, max_score)}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>#</th><th>代码</th><th>收盘价</th><th>挂单价</th>
        <th>近20日涨幅</th><th>近60日涨幅</th><th>量比</th>
        <th>量增天数</th><th>布林带</th><th>K线形态</th><th>综合得分</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""
def _pending_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>暂无待成交挂单</div></div>'
    rows = []
    for r in df.itertuples():
                pbp = float(r.planned_buy_price)  # 挂单价
                sc = float(r.signal_close) if not pd.isna(r.signal_close) else pbp
                disc = (pbp - sc) / sc * 100 if sc else 0.0
                type_str = decode_trade_type_label(getattr(r, 'trade_type', TRADE_BUY))
                rows.append(f"""
                <tr>
                    <td>{_market_badge(r.symbol)}</td>
                    <td>{type_str}</td>
                    <td>{r.signal_date}</td>
                    <td>¥{pbp:.2f}</td>
                    <td>¥{sc:.2f}</td>
                    <td><span class="ret-neg">{disc:.2f}%</span></td>
                    <td><span class="badge badge-pending">⏳ 待成交</span></td>
                </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>代码</th><th>方向</th><th>信号日期</th><th>挂单价</th>
        <th>信号收盘</th><th>折价幅度</th><th>状态</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""
def _trades_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>今日暂无成交记录</div></div>'
    rows = []
    for r in df.itertuples():
        pnl = "" if pd.isna(r.pnl_pct) else _ret_cell(r.pnl_pct / 100)
        type_label = decode_trade_type_label(r.trade_type)
        shares_display = int(round(r.shares / 100) * 100) if not pd.isna(r.shares) else 0
        reason_text = decode_reason_text(int(r.reason)) if not pd.isna(r.reason) else ""
        fee_val = float(r.fee) if hasattr(r, 'fee') and not pd.isna(getattr(r, 'fee', None)) else 0.0
        reason_display = reason_text
        if fee_val > 0:
            reason_display += f" (手续费¥{fee_val:.2f})"
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{type_label}</td>
          <td>{r.trade_date}</td>
                    <td>¥{float(r.price):.2f}</td>
          <td>{shares_display}</td>
          <td>{reason_display or '—'}</td>
          <td>{pnl or '—'}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>代码</th><th>方向</th><th>日期</th>
        <th>价格</th><th>股数</th><th>原因</th><th>收益</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""
def _portfolio_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">🏦</div><div>当前无持仓，满仓观望</div></div>'
    rows = []
    for r in df.itertuples():
        pnl_pct = float(r.pnl_pct) if not pd.isna(r.pnl_pct) else 0.0
        shares_display = int(r.shares) if not pd.isna(r.shares) else 0
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{r.buy_date}</td>
                    <td>¥{float(r.buy_price):.2f}</td>
                    <td>¥{float(r.last_price):.2f}</td>
          <td>{shares_display}</td>
          <td>¥{r.market_value:,.2f}</td>
          <td>{r.holding_days}天</td>
          <td>{_ret_cell(pnl_pct / 100)}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>代码</th><th>买入日</th><th>成本价</th><th>现价</th>
        <th>股数</th><th>市值</th><th>持有</th><th>浮盈</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""
def generate_and_send_report(
    tm: TokenManager,
    df_picks: pd.DataFrame,
    df_portfolio: pd.DataFrame,
    df_pending: pd.DataFrame,
    df_trades: pd.DataFrame,
    target_str: str,
    metrics: dict = None
):
    if metrics is None:
        metrics = {}
    
    n_picks     = 0 if df_picks is None or df_picks.empty else len(df_picks)
    n_pending   = 0 if df_pending is None or df_pending.empty else len(df_pending)
    n_trades    = 0 if df_trades is None or df_trades.empty else len(df_trades)
    n_portfolio = 0 if df_portfolio is None or df_portfolio.empty else len(df_portfolio)
    CSS = """
<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;
       background:#f0f2f5; color:#333; }
.wrapper { max-width:900px; margin:0 auto; padding:20px; }
.header  { background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);
           border-radius:16px; padding:32px 36px; margin-bottom:20px;
           display:flex; align-items:center; justify-content:space-between; }
.header-left h1 { color:#fff; font-size:24px; font-weight:700; letter-spacing:1px; }
.header-left .subtitle { color:#a0b4d6; font-size:13px; margin-top:6px; }
.header-badge { background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.2);
                border-radius:10px; padding:10px 18px; text-align:center; }
.header-badge .date  { color:#e2e8f0; font-size:18px; font-weight:600; }
.header-badge .label { color:#94a3b8; font-size:11px; margin-top:2px; }
.kpi-row { display:grid; grid-template-columns:repeat(2,1fr); gap:14px; margin-bottom:14px; }
.kpi-card { background:#fff; border-radius:12px; padding:18px 16px;
            box-shadow:0 2px 8px rgba(0,0,0,.06); border-left:4px solid #3b82f6; }
.kpi-card.green { border-left-color:#10b981; }
.kpi-card.amber { border-left-color:#f59e0b; }
.kpi-card.red   { border-left-color:#ef4444; }
.kpi-card.purple { border-left-color:#8b5cf6; }
.kpi-card.cyan   { border-left-color:#06b6d4; }
.kpi-icon  { font-size:22px; margin-bottom:8px; }
.kpi-value { font-size:22px; font-weight:700; color:#1e293b; line-height:1; }
.kpi-label { font-size:12px; color:#64748b; margin-top:4px; }
.section   { background:#fff; border-radius:14px; padding:24px 24px 20px;
             margin-bottom:20px; box-shadow:0 2px 8px rgba(0,0,0,.06); }
.section-header { display:flex; align-items:center; gap:10px; margin-bottom:16px;
                  padding-bottom:12px; border-bottom:1px solid #e8edf2; }
.section-icon { width:32px; height:32px; border-radius:8px; display:flex;
                align-items:center; justify-content:center; font-size:16px; }
.icon-blue  { background:#eff6ff; }
.icon-green { background:#f0fdf4; }
.icon-amber { background:#fffbeb; }
.icon-purple{ background:#faf5ff; }
.section-title { font-size:16px; font-weight:600; color:#1e293b; }
.section-count { background:#f1f5f9; color:#64748b; font-size:12px;
                 padding:2px 8px; border-radius:20px; margin-left:auto; }
.strategy-pills { display:flex; flex-wrap:wrap; gap:8px; }
.pill      { background:#f1f5f9; border:1px solid #e2e8f0; border-radius:20px;
             padding:4px 12px; font-size:12px; color:#475569; }
.pill.buy  { background:#fef9c3; border-color:#fde68a; color:#92400e; }
.pill.sell { background:#fee2e2; border-color:#fca5a5; color:#991b1b; }
.data-table { width:100%; border-collapse:collapse; font-size:13px; }
.data-table thead tr { background:#f8fafc; }
.data-table th { padding:10px 12px; text-align:left; font-weight:600;
                 color:#64748b; font-size:11px; text-transform:uppercase;
                 letter-spacing:.5px; border-bottom:2px solid #e2e8f0; white-space:nowrap; }
.data-table td { padding:10px 12px; border-bottom:1px solid #f1f5f9; vertical-align:middle; }
.data-table tbody tr:hover { background:#fafbfc; }
.data-table tbody tr:last-child td { border-bottom:none; }
.table-wrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
.score-bar-wrap { display:flex; align-items:center; gap:8px; }
.score-bar  { height:6px; border-radius:3px; background:#e2e8f0; flex:1; min-width:60px; }
.score-fill { height:6px; border-radius:3px; background:linear-gradient(90deg,#3b82f6,#8b5cf6); }
.score-text { font-weight:600; color:#1e293b; min-width:48px; text-align:right; font-size:12px; }
.badge        { display:inline-block; padding:2px 8px; border-radius:4px;
                font-size:11px; font-weight:600; white-space:nowrap; }
.badge-sh     { background:#fee2e2; color:#b91c1c; }
.badge-sz     { background:#dbeafe; color:#1d4ed8; }
.badge-kcb    { background:#fef9c3; color:#92400e; }
.badge-cy     { background:#dcfce7; color:#166534; }
.badge-zx     { background:#cffafe; color:#155e75; }
.badge-bj     { background:#ffedd5; color:#9a3412; }
.badge-pending{ background:#fffbeb; color:#b45309; border:1px solid #fde68a; }
.badge-bb     { background:#ede9fe; color:#6d28d9; border:1px solid #c4b5fd; }
.ret-pos { color:#16a34a; font-weight:600; }
.ret-neg { color:#dc2626; font-weight:600; }
.empty-state { text-align:center; padding:32px 16px; color:#94a3b8; font-size:14px; }
.empty-icon  { font-size:32px; margin-bottom:8px; }
.footer  { text-align:center; padding:20px; color:#94a3b8; font-size:12px; }
/* ── 响应式适配 ── */
@media screen and (max-width: 768px) {
  .wrapper { padding:10px; }
  .header { flex-direction:column; text-align:center; gap:12px; padding:20px 16px; }
  .kpi-row { grid-template-columns:1fr 1fr; }
  .kpi-value { font-size:18px; }
  .section { padding:16px 12px 14px; }
  .data-table { font-size:12px; }
  .data-table th, .data-table td { padding:8px 6px; }
  .strategy-pills { gap:6px; }
  .pill { font-size:11px; padding:3px 8px; }
}
@media screen and (max-width: 480px) {
  .kpi-row { grid-template-columns:1fr 1fr; }
  .header-left h1 { font-size:18px; }
  .kpi-value { font-size:16px; }
  .kpi-card { padding:10px 8px; }
  .kpi-icon { font-size:18px; margin-bottom:4px; }
  .section { padding:12px 8px 10px; border-radius:10px; }
  .data-table th, .data-table td { padding:6px 4px; font-size:11px; }
  .score-bar { min-width:40px; }
}
</style>"""
    html = f"""<!DOCTYPE html><html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">{CSS}</head>
<body><div class="wrapper">
<div class="header">
  <div class="header-left">
    <h1>📈 A股量化日报</h1>
    <div class="subtitle">布林带 · 动量 · 量能 三因子策略 &nbsp;|&nbsp; 前复权信号 / 后复权止盈止损</div>
  </div>
  <div class="header-badge">
    <div class="date">{target_str}</div>
    <div class="label">交易日报告</div>
  </div>
</div>
<!-- 第一行：总资产、可用余额 -->
<div class="kpi-row">
  <div class="kpi-card blue"><div class="kpi-icon">💰</div>
    <div class="kpi-value">¥{metrics.get('total_assets', 0):,.0f}</div><div class="kpi-label">总资产</div></div>
  <div class="kpi-card green"><div class="kpi-icon">💸</div>
    <div class="kpi-value">¥{metrics.get('avail_cash', 0):,.0f}</div><div class="kpi-label">可用余额 ({metrics.get('cash_pct', 100):.1f}%)</div></div>
</div>
<!-- 第二行：持仓市值、当日盈亏 -->
<div class="kpi-row">
  <div class="kpi-card purple"><div class="kpi-icon">📊</div>
    <div class="kpi-value">¥{metrics.get('market_value', 0):,.0f}</div><div class="kpi-label">持仓市值 ({metrics.get('position_pct', 0):.1f}% 仓位)</div></div>
  <div class="kpi-card {'green' if metrics.get('daily_pnl', 0) >= 0 else 'red'}"><div class="kpi-icon">📅</div>
    <div class="kpi-value">¥{metrics.get('daily_pnl', 0):,.0f}</div><div class="kpi-label">当日盈亏 ({_ret_cell(metrics.get('daily_ret', 0))})</div></div>
</div>
<!-- 第三行：总盈亏、最大回撤/夏普/收益回撤比 -->
<div class="kpi-row">
  <div class="kpi-card amber"><div class="kpi-icon">📈</div>
    <div class="kpi-value">{_ret_cell(metrics.get('total_pnl_pct', 0)/100)}</div><div class="kpi-label">累计盈亏 ¥{metrics.get('total_pnl', 0):,.0f}</div></div>
  <div class="kpi-card cyan"><div class="kpi-icon">📉</div>
    <div class="kpi-value">-{metrics.get('max_drawdown', 0)*100:.1f}%</div><div class="kpi-label">最大回撤<br>夏普比率 {metrics.get('sharpe', 0):.2f}<br>收益回撤比 {metrics.get('calmar', 0):.2f}</div></div>
</div>
{f'<div class="section"><div class="section-header"><div class="section-icon icon-blue">📊</div><div class="section-title">绩效曲线 (Sharpe)</div></div><div style="text-align:center;"><img src="data:image/png;base64,{metrics.get("chart_b64")}" style="max-width:100%;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,0.1);" /></div></div>' if metrics.get("chart_b64") else ''}
<!-- 1. 当前持仓 -->
<div class="section">
  <div class="section-header">
    <div class="section-icon icon-purple">💼</div>
    <div class="section-title">当前持仓</div>
    <span class="section-count">{n_portfolio} 只</span>
  </div>
  <div class="table-wrap">{_portfolio_table(df_portfolio)}</div>
</div>
<!-- 2. 候选股 -->
<div class="section">
  <div class="section-header">
    <div class="section-icon icon-blue">🔍</div>
    <div class="section-title">当日候选股票</div>
    <span class="section-count">{n_picks} 只</span>
  </div>
  <div class="table-wrap">{_picks_table(df_picks)}</div>
</div>
<!-- 3. 挂单信息 -->
<div class="section">
  <div class="section-header">
    <div class="section-icon icon-amber">⏳</div>
    <div class="section-title">待成交挂单</div>
    <span class="section-count">{n_pending} 只</span>
  </div>
  <p style="font-size:12px;color:#64748b;margin-bottom:14px;">
    规则：T+1 日最低价 ≤ 挂单价时成交（收盘价 × 98.5%）</p>
  <div class="table-wrap">{_pending_table(df_pending)}</div>
</div>
<!-- 4. 交易记录 -->
<div class="section">
  <div class="section-header">
    <div class="section-icon icon-green">✅</div>
    <div class="section-title">当日成交记录</div>
    <span class="section-count">{n_trades} 笔</span>
  </div>
  <div class="table-wrap">{_trades_table(df_trades)}</div>
</div>
<!-- 策略参数 -->
<div class="section">
  <div class="section-header">
    <div class="section-icon icon-purple">⚙️</div>
    <div class="section-title">策略参数</div>
  </div>
  <div class="strategy-pills">
    <span class="pill">每仓资金 ¥{CONFIG['position_cash_yuan']:,.0f}</span>
    <span class="pill buy">买入：T+1 最低价 ≤ 收盘价 × 98.5%</span>
    <span class="pill sell">止盈 +{CONFIG['take_profit_pct']}%</span>
    <span class="pill sell">止损 {CONFIG['stop_loss_pct']}%</span>
    <span class="pill sell">跌破 MA20 离场</span>
    <span class="pill sell">最长持有 {CONFIG['max_hold_days']} 天</span>
    <span class="pill">选股 Top {CONFIG['top_n']}</span>
    <span class="pill">复权窗口 {CONFIG['adjust_cache_days']} 交易日</span>
  </div>
</div>
<div class="footer">
  <p>本报告由量化程序自动生成 · {target_str} 收盘后运行</p>
        <p style="margin-top:4px;">数据来源：chenditc/investment_data (Qlib) · stocks 表 · 策略：布林带 + 动量 + 量能三因子 · 仅供参考，不构成投资建议</p>
</div>
</div></body></html>"""
    attachments = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "rb") as f:
            content_bytes = base64.b64encode(f.read()).decode("utf-8")
        attachments.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": os.path.basename(LOG_FILE),
            "contentBytes": content_bytes,
        })
    send_email_via_graph(tm, f"📈 A股-Github-量化日报 - {target_str}", html, attachments)
# =========================================================
# 主流程
# =========================================================
def _latest_trade_date_in_db(con, target_date: date) -> Optional[date]:
    row = con.execute(f"SELECT MAX(tradedate) FROM {STOCKS_TABLE} WHERE tradedate <= ?", [target_date]).fetchone()
    if not row or row[0] is None:
        return None
    return pd.to_datetime(row[0]).date()


def run_strategy_with_replay_if_needed(db_path: str, target_date: date):
    with duckdb.connect(db_path) as con:
        # 仅清理临时缓存表（每次重建），保留持仓/账户等持久表
        drop_cache_tables(con)
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        latest_trade_date = _latest_trade_date_in_db(con, target_date)
        if latest_trade_date is None:
            return None, (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {})
        has_history = con.execute("SELECT COUNT(*) FROM account_history").fetchone()[0] > 0

    if has_history:
        # 日常模式：已有策略数据，仅计算最新交易日
        log.info("⚡ 日常模式：仅计算最新交易日策略")
        rebuild_recent_adjusted_cache(db_path, latest_trade_date, CONFIG["adjust_cache_days"])
        return latest_trade_date, evaluate_strategy(db_path, latest_trade_date, CONFIG["top_n"], allow_exit_on_date=True)

    # 首次模式：无策略数据，回放最近N个交易日
    with duckdb.connect(db_path) as con:
        replay_dates = get_recent_trade_dates(con, latest_trade_date, int(CONFIG["initial_replay_trade_days"]))
    if not replay_dates:
        return latest_trade_date, (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {})
    log.info(f"🧱 首次模式：回放最近 {len(replay_dates)} 个交易日策略")
    latest_result = None
    latest_day = replay_dates[-1]
    for idx, d in enumerate(replay_dates):
        rebuild_recent_adjusted_cache(db_path, d, CONFIG["adjust_cache_days"])
        latest_result = evaluate_strategy(
            db_path,
            d,
            CONFIG["top_n"],
            allow_exit_on_date=(idx > 0),
        )
    return latest_day, latest_result


def run_daily_pipeline():
    tm  = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
    odc = OneDriveClient(tm, CONFIG["onedrive_folder"], _DB_GZ_NAME)
    target_date = get_target_date()
    print(f"[RUN] A股策略任务启动 target_date={target_date}", flush=True)
    with tempfile.TemporaryDirectory() as tmp:
        db_path  = os.path.join(tmp, "CN_stock.duckdb")   # 原始 duckdb（仅本地临时）
        gz_path  = os.path.join(tmp, _DB_GZ_NAME)             # 压缩版（下载/上传用）
        # ── 1. 从本地固定路径或 OneDrive 拉取压缩数据库并解压 ──
        db_source, db_gz_ready = obtain_db_gz(LOCAL_DB_GZ_PATH, odc, gz_path)
        if db_gz_ready:
            load_db_gz_to_local(gz_path, db_path)
            log.info("✅ 已载入历史数据库（gz 解压完成）")
        else:
            log.info("ℹ️ 本地/OneDrive 均无历史数据库，执行全量初始化")
            initialize_empty_database(db_path)
        with duckdb.connect(db_path) as con:
            _migrate_db_schema(con)
            ensure_core_tables(con)
            ensure_strategy_tables(con)
            latest_before = _latest_trade_date_in_db(con, target_date)
            log.info(f"ℹ️ 更新前数据库最新交易日: {latest_before}")

        synced, _ = investment_data_sync_recent_window(
            db_path,
            target_date,
            int(CONFIG["update_window_trade_days"]),
        )
        if not synced:
            log.warning("⚠️ 本次未获取到窗口行情，继续使用已有数据库数据")

        latest_trade_date, result = run_strategy_with_replay_if_needed(db_path, target_date)
        if latest_trade_date is None or result is None:
            log.warning("⚠️ 无可用交易日数据，结束当日流程")
            print("[RUN] 无可用交易日数据，本次结束", flush=True)
            return
        df_picks, df_portfolio, df_pending, df_trades, metrics = result
        target_str = latest_trade_date.strftime("%Y-%m-%d")
        print(
            f"RESULT {target_str} | 候选:{len(df_picks)} 持仓:{len(df_portfolio)} "
            f"挂单:{len(df_pending)} 成交:{len(df_trades)} 总资产:{metrics.get('total_assets', 0):.2f}"
        )
        # ── 4. 发送报告 ──
        try:
            generate_and_send_report(tm, df_picks, df_portfolio, df_pending, df_trades, target_str, metrics)
        except Exception as e:
            log.error(f"发送日报失败: {e}")
        # ── 5. 清理派生表后压缩上传（仅保留 stock_prices 以减少存储）──
        with duckdb.connect(db_path) as con:
            drop_cache_tables(con)
            con.execute("CHECKPOINT")
        if db_source == "local" and LOCAL_DB_GZ_PATH:
            db_compress_to_local(db_path, LOCAL_DB_GZ_PATH)
        else:
            db_compress_and_upload(odc, db_path, gz_path)
        log.info("🎉 今日流程完成")
        print(f"[RUN] 执行完成 latest_trade_date={target_str}", flush=True)
# =========================================================
# CLI
# =========================================================
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "auth":
        tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
        tm.device_code_auth()
    elif mode == "export":
        tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
        print("\n👇 请将以下内容保存为 CI Secret: ONEDRIVE_TOKEN_CACHE_B64\n")
        print(tm.export_base64_cache())
    elif mode == "run":
        try:
            run_daily_pipeline()
        except Exception:
            log.exception("❌ 运行失败")
            print(f"[ERROR] 运行失败，请检查日志: {LOG_FILE}", flush=True)
            raise
    elif mode == "daemon":
        import schedule
        log.info("🕒 调度服务已启动，每天 17:00 自动执行")
        schedule.every().day.at("17:00").do(run_daily_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        raise ValueError(f"不支持的模式: {mode}")
