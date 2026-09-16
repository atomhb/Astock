#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1. 行情数据统一落到 stocks 表，使用 investment_data 最新发布包。
2. 价格与金额直接使用人民币浮点值，不再做旧版整数缩放。
3. 每日数据源只依赖 chenditc/investment_data（Qlib）与 DoltHub。
4. 前复权和后复权基于 stocks 表中的 close / adjclose 动态构建。
5. 策略和报表直接读取 stocks / qfq / hfq 结果表。
6. 选股信号使用前复权数据；收益率和止盈止损收益判断使用后复权数据。
7. 布林带量价MACD共振选股。
8. 动态 ATR (14) 挂单与止损：挂单价 = T日收盘价 - alpha * ATR(14)；止损阈值 = -beta * ATR_pct。
9. 风险平价 (Risk Parity) + 大盘环境多级仓位管理。
10. 卖出规则：跌破MA20 / 动态ATR止损 / 跌破布林中轨 / 持有N天。
11. OneDrive 没有数据库时创建空库并拉取窗口行情。
12. 日常已有数据库时执行5交易日比对增量更新。
13. 最终数据库使用 gzip 压缩上传，降低网络传输成本。
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

# ── 启用中文字体链（兼容 Linux/GitHub Actions runner 与本地环境，负号防乱码）──
plt.rcParams['font.sans-serif'] = ['Noto Sans CJK SC', 'WenQuanYi Micro Hei', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

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
    # 环境变量配置
    "azure_client_id": _env("AZURE_CLIENT_ID"),
    "token_cache_file": os.path.join(BASE_DIR, _env("TOKEN_CACHE_FILE", "ms_token.json")),
    "email_to": _env("EMAIL_TO"),
    # 程序核心参数
    "onedrive_folder": "Stock",
    "cloud_db_gz_name": "Tu_A_stock.duckdb.gz",
    "local_db_gz_dir": None,
    "local_db_gz_name": None,
    "bootstrap_days": 120,
    "position_cash_yuan": 50000.0,
    "take_profit_pct": 10.0,
    "stop_loss_pct": -5.0,                  # 备用固定止损（若无ATR数据时降级使用）
    "max_hold_days": 200,
    "top_n": 20,
    "adjust_cache_days": 320,
    "source_cache_ttl_seconds": 6 * 3600,
    "update_window_trade_days": 150,
    "initial_replay_trade_days": 200,       # 初始回测天数
    "buy_fee_rate": 0.0005,
    "sell_fee_rate": 0.0010,

    # ── 动态 ATR (14) 参数配置 ──
    "atr_period": 14,                      # ATR 计算周期
    "atr_buy_alpha": 0.5,                  # 挂单价系数：挂单价 = T日收盘价 - 0.5 * ATR(14)
    "atr_stop_loss_beta": 2.0,             # 动态止损系数：止损触发点 = -2.0 * ATR_pct

    "buy_confirm_day_drop_limit": -0.03,   # (保留参数) T+1日内跌幅上限
    "buy_confirm_vol_ratio_min": 0.5,      # (保留参数) T+1相对量比下限
    "buy_confirm_ma20_margin": 0.99,       # (保留参数) 允许跌破MA20的容差
    "buy_signal_expire_days": 2,           # 挂单最长有效天数（按交易日计）
    "buy_confirm_checks": False,           # 是否在挂单成交时附加质量确认
    "market_health_check": True,           # 是否启用大盘过滤
    "filter_gem_star": False,              # 是否过滤创业板/科创板
    "init_cash": 100000.0,                 # 初始资金参数
    "max_position_stocks": 5,              # 持仓中最多有的股票数
}

CONFIG["position_cash_cent"] = int(round(CONFIG["position_cash_yuan"] * 100))


def _format_size_mb(size_mb: float) -> str:
    if size_mb >= 1024:
        return f"{size_mb / 1024:.2f} GB"
    return f"{size_mb:.1f} MB"


def _file_size_mb(path: Optional[str]) -> float:
    if not path or not os.path.isfile(path):
        return 0.0
    return os.path.getsize(path) / 1024 / 1024


STOCKS_TABLE = "stock_prices"
ADJUSTMENT_FACTORS_TABLE = "adjustment_factors"
STOCK_DATE_COL = "tradedate"
STOCK_SYMBOL_COL = "symbol"
QLIB_DATA_URL = "https://github.com/chenditc/investment_data/releases/latest/download/qlib_bin.tar.gz"
DOLTHUB_CSV_URL = "https://www.dolthub.com/csv/chenditc/investment_data/master/ts_a_stock_eod_price"
DOLTHUB_API_URL  = "https://www.dolthub.com/api/v1alpha1/chenditc/investment_data/master"
QLIB_DATA_DIR = os.path.expanduser("~/.qlib/qlib_data/cn_data")
QLIB_TAR_PATH = os.path.join(BASE_DIR, "qlib_bin.tar.gz")
_QLIB_INITIALIZED = False

# ── 数据库编码常量 ──
TRADE_BUY = 0
TRADE_SELL = 1
STATUS_PENDING = 0
STATUS_FILLED = 1
STATUS_EXPIRED = 2

REASON_STOPLOSS = 1        # bit0: 动态ATR/固定止损
REASON_BELOW_MA20 = 2      # bit1: 跌破MA20
REASON_MAX_HOLD = 4        # bit2: 持有超期
REASON_BELOW_BOLL_MID = 8  # bit3: 跌破布林中轨
REASON_BEAR_PATTERN = 16   # bit4: 出现下跌形态
REASON_MACD_DECREASE = 32  # bit5: MACD差值减小
REASON_BUY_T1 = 64         # bit6: T+1买入


def decode_trade_type_label(code) -> str:
    return "🟢 买入" if code == TRADE_BUY else "🔴 卖出"


def decode_reason_text(code) -> str:
    if code is None or code == 0:
        return ""
    parts = []
    if code & REASON_STOPLOSS:
        parts.append("动态ATR止损")
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
        parts.append("T+1动态ATR挂单成交")
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
    for col in ["open", "high", "low", "close", "buy_price", "buy_price_hfq", "last_price", "last_price_hfq", "planned_buy_price", "signal_close", "price", "adjclose", "atr_pct", "atr_pct_buy"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["volume", "amount"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out

# =========================================================
# Token / OneDrive
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
            raise RuntimeError("尚未完成授权，请先执行授权模式。")
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
        session_resp = session.post(
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

                    max_retries = 5
                    for attempt in range(1, max_retries + 1):
                        try:
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
                            break
                        except requests.exceptions.RequestException as e:
                            if attempt == max_retries:
                                log.error(f"❌ 分块上传失败 ({offset}-{end}/{size})，已达最大重试次数")
                                raise
                            wait_time = attempt * 5
                            log.warning(f"⚠️ 分块上传异常 ({e})，{wait_time} 秒后进行第 {attempt + 1}/{max_retries} 次重试...")
                            time.sleep(wait_time)

                    offset += len(chunk)
                    pbar.update(len(chunk))

# =========================================================
# 本地 DB ↔ OneDrive gz 工具
# =========================================================
_DB_GZ_NAME = CONFIG["cloud_db_gz_name"]
_DB_GZIP_COMPRESSLEVEL = 6

def db_compress_and_upload(odc: OneDriveClient, db_path: str, gz_path: str) -> None:
    raw_size_mb = _file_size_mb(db_path)
    with open(db_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=_DB_GZIP_COMPRESSLEVEL) as fo:
        shutil.copyfileobj(fi, fo)
    gz_size_mb = _file_size_mb(gz_path)
    ratio = gz_size_mb / raw_size_mb * 100 if raw_size_mb > 0 else 0.0
    log.info(f"📦 数据库 {_format_size_mb(raw_size_mb)} → {_format_size_mb(gz_size_mb)} (压缩率 {ratio:.1f}%)，开始上传 ...")
    odc.upload_database_gz(gz_path)
    log.info("☁️  数据库已上传")


def db_compress_to_local(db_path: str, local_gz_path: str) -> None:
    local_dir = os.path.dirname(local_gz_path)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)
    raw_size_mb = _file_size_mb(db_path)
    with open(db_path, "rb") as fi, gzip.open(local_gz_path, "wb", compresslevel=_DB_GZIP_COMPRESSLEVEL) as fo:
        shutil.copyfileobj(fi, fo)
    gz_size_mb = _file_size_mb(local_gz_path)
    ratio = gz_size_mb / raw_size_mb * 100 if raw_size_mb > 0 else 0.0
    log.info(f"📦 数据库 {_format_size_mb(raw_size_mb)} → {_format_size_mb(gz_size_mb)} (压缩率 {ratio:.1f}%)，已保存本地 {local_gz_path}")


def db_decompress_from_download(gz_path: str, db_path: str) -> None:
    with gzip.open(gz_path, "rb") as fi, open(db_path, "wb") as fo:
        shutil.copyfileobj(fi, fo)


def obtain_db_gz(local_gz_path: Optional[str], odc: OneDriveClient, temp_gz_path: str) -> Tuple[str, bool]:
    if local_gz_path and os.path.isfile(local_gz_path) and os.path.getsize(local_gz_path) > 1024:
        log.info(f"📁 使用本地数据库压缩文件: {local_gz_path}")
        shutil.copyfile(local_gz_path, temp_gz_path)
        return "local", True
    log.info("☁️ 本地数据库压缩文件不存在或无效，尝试从 OneDrive 下载")
    if odc.download_database_gz(temp_gz_path):
        return "cloud", True
    return "none", False


def load_db_gz_to_local(gz_path: str, db_path: str) -> None:
    db_decompress_from_download(gz_path, db_path)

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
    try:
        con.execute("ALTER TABLE pending_orders ADD COLUMN IF NOT EXISTS trade_type TINYINT")
        con.execute(f"UPDATE pending_orders SET trade_type = {TRADE_BUY} WHERE trade_type IS NULL")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE pending_orders ADD COLUMN IF NOT EXISTS signal_strength DOUBLE")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE pending_orders ADD COLUMN IF NOT EXISTS atr_pct DOUBLE")
    except Exception:
        pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            symbol VARCHAR PRIMARY KEY,
            buy_date DATE,
            buy_price DOUBLE,
            buy_price_hfq DOUBLE,
            shares BIGINT,
            atr_pct_buy DOUBLE
        )
    """)
    try:
        con.execute("ALTER TABLE virtual_portfolio ADD COLUMN IF NOT EXISTS atr_pct_buy DOUBLE")
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
        row = con.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = ?", [t]
        ).fetchone()
        if row is None:
            continue
        if row[0] == "VIEW":
            con.execute(f'DROP VIEW IF EXISTS "{t}"')
        else:
            con.execute(f'DROP TABLE IF EXISTS "{t}"')


def compact_database(con) -> None:
    con.execute("CHECKPOINT")
    try:
        con.execute("VACUUM")
    except Exception as exc:
        log.warning(f"⚠️ 数据库 VACUUM 失败，继续使用 CHECKPOINT 结果: {exc}")
    con.execute("CHECKPOINT")


def _prune_pending_orders(con, keep_rows: int = 10000) -> None:
    rows = con.execute(
        f"""
        SELECT symbol, signal_date
        FROM pending_orders
        ORDER BY CASE WHEN status = {STATUS_PENDING} THEN 1 ELSE 0 END DESC,
                 signal_date DESC,
                 symbol ASC
        """
    ).fetchall()
    if len(rows) <= keep_rows:
        return
    con.executemany(
        "DELETE FROM pending_orders WHERE symbol=? AND signal_date=?",
        rows[keep_rows:],
    )


def _prune_account_history(con, keep_rows: int = 2000) -> None:
    rows = con.execute("SELECT date FROM account_history ORDER BY date DESC").fetchall()
    if len(rows) <= keep_rows:
        return
    con.executemany(
        "DELETE FROM account_history WHERE date=?",
        [(row[0],) for row in rows[keep_rows:]],
    )


def _prune_trade_history(con, keep_trade_days: int = 500) -> None:
    rows = con.execute(
        "SELECT DISTINCT trade_date FROM trade_history WHERE trade_date IS NOT NULL ORDER BY trade_date DESC"
    ).fetchall()
    if len(rows) <= keep_trade_days:
        return
    con.executemany(
        "DELETE FROM trade_history WHERE trade_date=?",
        [(row[0],) for row in rows[keep_trade_days:]],
    )


def apply_history_retention(con) -> None:
    _prune_pending_orders(con)
    _prune_account_history(con)
    _prune_trade_history(con, keep_trade_days=500)

def _migrate_db_schema(con):
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if STOCKS_TABLE not in tables:
        return
    cols = con.execute(f"PRAGMA table_info('{STOCKS_TABLE}')").fetchall()
    col_types = {c[1]: c[2] for c in cols}
    if col_types.get("tradedate") != "TIMESTAMP" or col_types.get("open") != "DOUBLE":
        return
    log.info("🔄 迁移 stock_prices: TIMESTAMP→DATE + PRIMARY KEY + FLOAT精度 ...")
    con.execute("""
        CREATE TABLE stock_prices_new (
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
        INSERT INTO stock_prices_new
        SELECT CAST(tradedate AS DATE), symbol,
               CAST(ROUND(high, 2) AS FLOAT), CAST(ROUND(low, 2) AS FLOAT), CAST(ROUND(open, 2) AS FLOAT),
               CAST(ROUND(close, 2) AS FLOAT), CAST(ROUND(adjclose, 2) AS FLOAT),
               CAST(ROUND(volume, 0) AS FLOAT), CAST(ROUND(amount, 2) AS FLOAT)
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


def _get_tar_mtime(tar_path: str) -> float:
    return os.path.getmtime(tar_path) if os.path.exists(tar_path) else 0.0

def _is_qlib_dir_fresh(tar_path: str, data_dir: str) -> bool:
    stamp_file = os.path.join(data_dir, ".tar_mtime")
    if not os.path.exists(stamp_file):
        return False
    with open(stamp_file, "r") as f:
        return float(f.read().strip()) >= _get_tar_mtime(tar_path)

def _safe_extract_tar_strip_first(tar_path: str, target_dir: str) -> None:
    if _is_qlib_dir_fresh(tar_path, target_dir):
        log.info("📦 Qlib 数据目录已是最新，跳过解压")
        return
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
    with open(os.path.join(target_dir, ".tar_mtime"), "w") as f:
        f.write(str(_get_tar_mtime(tar_path)))


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
    fields = ["$high", "$low", "$open", "$close", "$adjclose", "$volume", "$amount"]
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
    })
    out["tradedate"] = pd.to_datetime(out["tradedate"]).dt.date
    out["symbol"] = out["symbol"].map(normalize_qlib_symbol)
    for col in ["high", "low", "open", "close", "adjclose", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    ratio_sanity = (out["adjclose"] / out["close"].replace(0, np.nan)).dropna()
    if ratio_sanity.median() < 1.0:
        log.warning("⚠️ adjclose/close 中位数 < 1，数据源复权约定可能与预期不同，请人工核对")

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


def get_first_trade_date_from_qlib(target_date: date) -> Optional[date]:
    from qlib.data import D

    calendar = D.calendar(
        start_time="1990-01-01",
        end_time=target_date.strftime("%Y-%m-%d"),
        freq="day",
    )
    if calendar is None or len(calendar) == 0:
        return None
    return pd.to_datetime(calendar).date.min()


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
    return len(tmp), 0, 0


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


def investment_data_sync_full_history(db_path: str, target_date: date) -> Tuple[bool, List[date]]:
    provider_uri = prepare_latest_qlib_data()
    ensure_qlib_initialized(provider_uri)
    first_date = get_first_trade_date_from_qlib(target_date)
    if first_date is None:
        log.warning("⚠️ Qlib 未返回可用历史交易日，跳过全历史初始化")
        return False, []

    all_dates: List[date] = []
    inserted = updated = skipped = 0
    chunk_start = first_date
    while chunk_start <= target_date:
        chunk_end = min(target_date, date(chunk_start.year + 1, 1, 1) - timedelta(days=1))
        log.info(f"📦 [全历史] 拉取 {chunk_start} ~ {chunk_end}")
        chunk_df = fetch_qlib_features(chunk_start, chunk_end)
        if not chunk_df.empty:
            chunk_df = chunk_df[
                (chunk_df["tradedate"] >= chunk_start)
                & (chunk_df["tradedate"] <= chunk_end)
            ].copy()
            if not chunk_df.empty:
                dates = sorted(pd.to_datetime(chunk_df["tradedate"]).dt.date.unique().tolist())
                all_dates.extend(dates)
                with duckdb.connect(db_path) as con:
                    ensure_core_tables(con)
                    ins, upd, skip = _compare_and_sync_stock_rows(con, chunk_df)
                inserted += ins
                updated += upd
                skipped += skip
        chunk_start = date(chunk_end.year + 1, 1, 1)

    all_dates = sorted(set(all_dates))
    log.info(
        f"✅ [全历史] stock_prices 完成: 插入={inserted}, 更新={updated}, "
        f"跳过={skipped}, 交易日={len(all_dates)}, 起始={first_date}, 结束={target_date}"
    )
    return inserted + updated > 0 or skipped > 0, all_dates


def investment_data_sync_gap(db_path: str, start_date: date, target_date: date) -> Tuple[bool, List[date]]:
    """使用 Qlib 将 start_date 至 target_date 之间的历史断层分年补齐"""
    provider_uri = prepare_latest_qlib_data()
    ensure_qlib_initialized(provider_uri)

    all_dates: List[date] = []
    inserted = updated = skipped = 0
    chunk_start = start_date

    while chunk_start <= target_date:
        chunk_end = min(target_date, date(chunk_start.year + 1, 1, 1) - timedelta(days=1))
        log.info(f"📦 [补齐数据断层] 正在拉取并写库: {chunk_start} ~ {chunk_end} …")
        chunk_df = fetch_qlib_features(chunk_start, chunk_end)
        if not chunk_df.empty:
            chunk_df = chunk_df[
                (chunk_df["tradedate"] >= chunk_start)
                & (chunk_df["tradedate"] <= chunk_end)
            ].copy()
            if not chunk_df.empty:
                dates = sorted(pd.to_datetime(chunk_df["tradedate"]).dt.date.unique().tolist())
                all_dates.extend(dates)
                with duckdb.connect(db_path) as con:
                    ensure_core_tables(con)
                    ins, upd, skip = _compare_and_sync_stock_rows(con, chunk_df)
                inserted += ins
                updated += upd
                skipped += skip
        chunk_start = date(chunk_end.year + 1, 1, 1)

    all_dates = sorted(set(all_dates))
    log.info(
        f"✅ [补齐数据断层] 完成: 累计补入 {inserted + updated:,} 条，"
        f"覆盖 {len(all_dates)} 个交易日，数据库最新交易日已成功拉平至 {all_dates[-1] if all_dates else target_date}"
    )
    return (inserted + updated) > 0 or skipped > 0, all_dates


# =========================================================
# DoltHub CSV 数据源（带 1.24GB 完整性硬校验与 100MB 报告）
# =========================================================
def _clean_dolthub_chunk(df: pd.DataFrame) -> pd.DataFrame:
    required = ["tradedate", "symbol", "high", "low", "open", "close"]
    if any(c not in df.columns for c in required):
        return pd.DataFrame()
    if "adjclose" not in df.columns:
        df["adjclose"] = df["close"]
    if "amount" not in df.columns:
        df["amount"] = 0.0
    if "volume" not in df.columns:
        df["volume"] = 0.0
    df["tradedate"] = pd.to_datetime(df["tradedate"].astype(str), errors="coerce").dt.date
    df["symbol"]    = df["symbol"].map(canonical_symbol)
    for col in ["high", "low", "open", "close", "adjclose", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").round(0)
    df = df.dropna(subset=["tradedate", "symbol", "open", "high", "low", "close", "adjclose"])
    df = df.drop_duplicates(subset=["tradedate", "symbol"], keep="last")
    return df[["tradedate", "symbol", "high", "low", "open", "close", "adjclose", "volume", "amount"]]


def dolthub_stream_to_db(db_path: str) -> Tuple[bool, List[date]]:
    STREAM_CHUNK_ROWS = 1000_000
    DOWNLOAD_CHUNK_BYTES = 2 * 1024 * 1024        # 2MB 网络缓冲
    REPORT_INTERVAL_BYTES = 100 * 1024 * 1024     # 严格每 100 MB 报告一次
    MIN_EXPECTED_BYTES = int(1.15 * 1024 * 1024 * 1024)  # 完整文件为 1.24 GB，设置 1.15 GB 完整性底线
    MAX_RETRIES = 3

    url = DOLTHUB_CSV_URL
    session = build_retry_session()

    for attempt in range(1, MAX_RETRIES + 1):
        log.info(f"⬇️ [DoltHub 流式CSV] 开始下载并写库 (第 {attempt}/{MAX_RETRIES} 次尝试): {url}")
        try:
            resp = session.get(url, stream=True, timeout=(30, 900))
            resp.raise_for_status()
        except Exception as exc:
            log.error(f"❌ [DoltHub 流式CSV] 连接建立失败: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(5)
                continue
            return False, []

        downloaded = 0
        last_download_report = 0
        t_start = time.time()
        t_last = t_start
        total_inserted = 0
        all_dates: set = set()

        with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024, mode="w+b") as spooled:
            # ── 1. 下载阶段（彻底弃用 tqdm 刷屏，按 100MB 步进汇报）──
            try:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if chunk:
                        spooled.write(chunk)
                        downloaded += len(chunk)

                        if downloaded - last_download_report >= REPORT_INTERVAL_BYTES:
                            now = time.time()
                            speed = (downloaded - last_download_report) / (now - t_last) / 1024 / 1024 if now > t_last else 0
                            curr_mb = downloaded / 1024 / 1024
                            log.info(
                                f"⬇️ [DoltHub 流式CSV] 下载进度: {curr_mb:.0f} MB / 约 1.24 GB "
                                f"({curr_mb / 1269 * 100:.1f}%) | 速度: {speed:.1f} MB/s"
                            )
                            last_download_report = downloaded
                            t_last = now
            except Exception as stream_exc:
                log.warning(f"⚠️ [DoltHub 流式CSV] 网络读取异常: {stream_exc}")

            curr_mb = downloaded / 1024 / 1024

            # ── 2. 完整性校验：拦截中途断线 ──
            if downloaded < MIN_EXPECTED_BYTES:
                log.warning(
                    f"⚠️ [DoltHub 流式CSV] 数据不完整！仅接收 {curr_mb:.1f} MB < 预期 1.24 GB "
                    f"(远端连接中途意外关闭)，正在自动重新下载..."
                )
                if attempt < MAX_RETRIES:
                    time.sleep(5)
                    continue
                else:
                    log.error("❌ [DoltHub 流式CSV] 达到最大重试次数，未能下载完整文件")
                    return False, []

            log.info(f"✅ [DoltHub 流式CSV] 完整下载校验通过！共 {curr_mb:.1f} MB，开始分块解析写库 …")

            # ── 3. 分块解析写库（每处理 100MB 报告一次）──
            spooled.seek(0)
            try:
                reader = pd.read_csv(
                    spooled,
                    chunksize=STREAM_CHUNK_ROWS,
                    low_memory=True,
                    dtype=str,
                )
            except Exception as exc:
                log.error(f"❌ [DoltHub 流式CSV] CSV 解析器初始化失败: {exc}")
                return False, []

            last_parse_report = 0
            with duckdb.connect(db_path) as con:
                ensure_core_tables(con)
                for i, chunk_df in enumerate(reader):
                    cleaned = _clean_dolthub_chunk(chunk_df)
                    if cleaned.empty:
                        continue
                    all_dates.update(cleaned["tradedate"].unique())
                    ins, _, _ = _compare_and_sync_stock_rows(con, cleaned)
                    total_inserted += ins

                    curr_pos = spooled.tell()
                    if curr_pos - last_parse_report >= REPORT_INTERVAL_BYTES:
                        c_mb = curr_pos / 1024 / 1024
                        pct = (curr_pos / downloaded * 100) if downloaded > 0 else 0
                        log.info(
                            f"⚙️ [DoltHub 流式CSV] 解析入库: 已处理 {c_mb:.0f} MB / {curr_mb:.0f} MB ({pct:.1f}%)，"
                            f"累计写入 {total_inserted:,} 条数据"
                        )
                        last_parse_report = curr_pos

            all_dates_sorted = sorted(all_dates)
            log.info(
                f"✅ [DoltHub 流式CSV] 写库完成: 共写入 {total_inserted:,} 条，"
                f"覆盖 {len(all_dates_sorted)} 个交易日，最新交易日: {all_dates_sorted[-1] if all_dates_sorted else '无'}"
            )
            return total_inserted > 0, all_dates_sorted

    return False, []


def dolthub_sync_recent_window(db_path: str, target_date: date, trade_days: int) -> Tuple[bool, List[date]]:
    success, all_dates = dolthub_stream_to_db(db_path)
    if not success:
        return False, []
    trade_dates = all_dates[-trade_days:] if len(all_dates) >= trade_days else all_dates
    return True, list(trade_dates)


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
# 策略逻辑
# =========================================================
def get_account_state(con) -> Tuple[float, float, float]:
    row = con.execute("SELECT init_capital, total_assets, available_cash FROM account_state WHERE id = 1").fetchone()
    if not row:
        cash = float(CONFIG.get("init_cash", 100000.0))
        return cash, cash, cash
    return row[0], row[1], row[2]

def detect_kline_patterns(open_s, high_s, low_s, close_s) -> Tuple[str, float, str, float]:
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
                except Exception:
                    pass

    if len(c) >= 3:
        prev_body = abs(o[-2] - c[-2])
        if c[-2] < o[-2] and prev_body > (c[-2] * 0.03):
            if o[-1] > c[-2] and c[-1] > o[-2]:
                bull_patterns.append("旭日东升")
            if o[-1] < c[-2] and abs(c[-1] - c[-2]) / c[-2] < 0.005:
                bull_patterns.append("好友反攻")
        if all(c[i] > o[i] for i in range(-3, 0)) and c[-1] > c[-2] > c[-3]:
            bull_patterns.append("三连阳")

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


# ── 大盘环境多级仓位管理 (Regime Switching) ──
def get_market_target_position_ratio(con, trade_date: date, index_symbol="000001.SH") -> float:
    if not CONFIG.get("market_health_check", True):
        return 1.0
    df = con.execute("""
        SELECT date, close FROM daily_qfq_cache
        WHERE symbol = ? AND date <= ?
        ORDER BY date DESC LIMIT 20
    """, [index_symbol, trade_date]).df()
    if len(df) < 20:
        return 1.0
    ma5  = df["close"].iloc[:5].mean()
    ma20 = df["close"].mean()
    if ma5 >= ma20:
        return 1.0
    elif ma5 >= ma20 * 0.98:
        return 0.5
    else:
        return 0.3


def _trading_day_gap(con, d1: date, d2: date) -> int:
    n = con.execute(
        f"SELECT COUNT(DISTINCT tradedate) FROM {STOCKS_TABLE} WHERE tradedate > ? AND tradedate <= ?",
        [d1, d2],
    ).fetchone()[0]
    return int(n)


def process_pending_orders(con, trade_date: date) -> Tuple[List[Tuple], List[Tuple]]:
    market_pos_ratio = get_market_target_position_ratio(con, trade_date)

    pending_df = con.execute("""
        SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status, signal_strength, atr_pct
        FROM pending_orders
        WHERE status=0 AND signal_date < ?
        ORDER BY signal_date, symbol
    """, [trade_date]).df()
    if pending_df.empty:
        return [], []

    qfq_today = con.execute("SELECT symbol, date, open, low, close, volume FROM daily_qfq_cache WHERE date = ?", [trade_date]).df()
    hfq_today = con.execute("SELECT symbol, date, close FROM daily_hfq_cache WHERE date = ?", [trade_date]).df()
    if qfq_today.empty:
        return [], []

    init_cap, total_assets, avail_cash = get_account_state(con)
    if total_assets <= 0:
        log.warning(f"资产归零或异常，执行重置恢复至{CONFIG.get('init_cash', 100000.0)}初始资金")
        init_cap = total_assets = avail_cash = float(CONFIG.get("init_cash", 100000.0))
        con.execute("DELETE FROM virtual_portfolio")
        con.execute("UPDATE account_state SET init_capital=?, total_assets=?, available_cash=? WHERE id=1", [init_cap, total_assets, avail_cash])

    max_position = int(CONFIG.get("max_position_stocks", 5))
    buy_fee_rate = float(CONFIG.get("buy_fee_rate", 0.0005))

    max_allowed_stock_equity = total_assets * market_pos_ratio
    current_market_val = con.execute("""
        SELECT COALESCE(SUM(p.shares * s.close), 0)
        FROM virtual_portfolio p
        JOIN stock_prices s ON p.symbol = s.symbol AND s.tradedate = ?
    """, [trade_date]).fetchone()[0]

    remaining_market_capacity = max_allowed_stock_equity - current_market_val
    if remaining_market_capacity <= 0:
        log.info(f"🛡️ 当前持仓市值 (¥{current_market_val:,.0f}) 已达大盘环境受控上限 ({market_pos_ratio*100:.0f}%)，暂停新建仓")
        return [], []

    base_stock_budget = min(CONFIG['position_cash_yuan'], total_assets / max_position)

    symbols = pending_df['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    start_date_60 = (trade_date - timedelta(days=90)).strftime('%Y-%m-%d')
    hist_df = con.execute(f"""
        SELECT symbol, date, close, volume
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date <= ? AND date >= ?
        ORDER BY symbol, date
    """, symbols + [trade_date.strftime('%Y-%m-%d'), start_date_60]).df()

    q_map = {row['symbol']: row for _, row in qfq_today.iterrows()}
    h_map = {row['symbol']: row for _, row in hfq_today.iterrows()}

    vol_ratio_min = float(CONFIG.get("buy_confirm_vol_ratio_min", 0.5))
    ma20_margin = float(CONFIG.get("buy_confirm_ma20_margin", 0.99))
    expire_days = int(CONFIG.get("buy_signal_expire_days", 2))
    confirm_checks = bool(CONFIG.get("buy_confirm_checks", False))

    current_holdings = con.execute("SELECT COUNT(*) FROM virtual_portfolio").fetchone()[0]
    holding_symbols = {r[0] for r in con.execute("SELECT symbol FROM virtual_portfolio").fetchall()}

    filled_rows, expired_rows = [], []
    stats = {"filled": 0, "expired_timeout": 0, "skip_no_data": 0, "skip_no_touch": 0,
             "skip_capacity": 0, "skip_cash": 0, "skip_confirm": 0, "skip_dup": 0}
    for _, row in pending_df.iterrows():
        symbol = row['symbol']
        signal_date = row['signal_date']
        planned_buy_price = float(row['planned_buy_price'])
        atr_pct = float(row['atr_pct']) if 'atr_pct' in row and not pd.isna(row['atr_pct']) and float(row['atr_pct']) > 0 else 0.03

        trade_gap = _trading_day_gap(con, pd.to_datetime(signal_date).date(), trade_date)
        if trade_gap > expire_days:
            expired_rows.append((symbol, signal_date))
            stats["expired_timeout"] += 1
            log.info(f"⏳ 挂单过期 {symbol} (信号{signal_date}, 已{trade_gap}个交易日>{expire_days})")
            continue

        if symbol in holding_symbols or symbol in {f[0] for f in filled_rows}:
            stats["skip_dup"] += 1
            log.info(f"⏭️ 跳过 {symbol}: 已持仓/当日已成交，避免重复买入")
            continue

        if symbol not in q_map:
            stats["skip_no_data"] += 1
            continue

        row_t1 = q_map[symbol]
        today_open_qfq = float(row_t1['open'])
        today_low_qfq = float(row_t1['low'])
        today_close_qfq = float(row_t1['close'])
        today_vol_qfq = float(row_t1['volume'])
        today_close_hfq = float(h_map[symbol]['close']) if symbol in h_map else today_close_qfq
        signal_close = float(row['signal_close']) if not pd.isna(row['signal_close']) else planned_buy_price

        touched = (today_low_qfq <= planned_buy_price)

        if confirm_checks and touched:
            sym_hist = hist_df[hist_df['symbol'] == symbol].copy()
            if len(sym_hist) < 20:
                stats["skip_confirm"] += 1
                continue
            ma20_t1 = float(sym_hist['close'].tail(20).mean())
            vol_ma5_prev = float(sym_hist['volume'].iloc[:-1].tail(5).mean()) if len(sym_hist) > 5 else 0.0
            if vol_ma5_prev > 0 and (today_vol_qfq / vol_ma5_prev) < vol_ratio_min:
                stats["skip_confirm"] += 1
                log.info(f"⏭️ 确认未过 {symbol}: 量比{(today_vol_qfq / vol_ma5_prev):.2f}<{vol_ratio_min}")
                continue
            if today_close_qfq < ma20_t1 * ma20_margin:
                stats["skip_confirm"] += 1
                log.info(f"⏭️ 确认未过 {symbol}: 收盘{today_close_qfq:.2f}<MA20*{ma20_margin:.2f}")
                continue
            if talib is not None and len(sym_hist) >= 40:
                c_vals = sym_hist['close'].values.astype(np.float64)
                macd, macdsignal, macdhist = talib.MACD(c_vals, fastperiod=12, slowperiod=26, signalperiod=9)
                if not pd.isna(macdhist[-1]) and not pd.isna(macdhist[-2]) and not pd.isna(macdhist[-3]):
                    if macdhist[-1] <= 0 or (macdhist[-1] < macdhist[-2] and macdhist[-2] < macdhist[-3]):
                        stats["skip_confirm"] += 1
                        log.info(f"⏭️ 确认未过 {symbol}: MACD柱≤0或走弱")
                        continue

        risk_weight = np.clip(0.03 / atr_pct, 0.5, 2.0)
        target_stock_cash = min(base_stock_budget * risk_weight, remaining_market_capacity)

        if current_holdings >= max_position:
            stats["skip_capacity"] += 1
            log.info(f"⏭️ 跳过 {symbol}: 持仓数已达上限 {max_position}")
            continue
        if avail_cash < target_stock_cash * 0.5:
            stats["skip_cash"] += 1
            log.info(f"⏭️ 跳过 {symbol}: 可用资金不足 (¥{avail_cash:,.0f}<¥{target_stock_cash*0.5:,.0f})")
            continue
        cost_yuan_budget = min(target_stock_cash, avail_cash)

        signal_strength = float(row['signal_strength']) if 'signal_strength' in row and not pd.isna(row['signal_strength']) else 0.0
        HIGH_CONFIDENCE = 2.0

        actual_buy_price_qfq = None
        if touched:
            actual_buy_price_qfq = planned_buy_price
        elif signal_strength >= HIGH_CONFIDENCE and today_open_qfq <= signal_close * 1.02:
            actual_buy_price_qfq = today_open_qfq

        if actual_buy_price_qfq is None:
            stats["skip_no_touch"] += 1
            continue

        if actual_buy_price_qfq * 100.0 * (1.0 + buy_fee_rate) > target_stock_cash:
            stats["skip_capacity"] += 1
            log.info(f"⏭️ 跳过 {symbol}: 剩余大盘仓位容量不足一手")
            continue

        factor = (today_close_hfq / today_close_qfq) if today_close_qfq > 0 else 1.0
        buy_price_hfq = round(actual_buy_price_qfq * factor, 2)

        lot_cost = actual_buy_price_qfq * 100.0 * (1.0 + buy_fee_rate)
        shares = int(cost_yuan_budget / lot_cost) * 100
        if shares < 100:
            expired_rows.append((symbol, signal_date))
            stats["skip_cash"] += 1
            continue

        gross_cost = round(shares * actual_buy_price_qfq, 2)
        buy_fee = round(gross_cost * buy_fee_rate, 2)
        actual_cost = round(gross_cost + buy_fee, 2)
        if actual_cost > avail_cash:
            shares = int(avail_cash / lot_cost) * 100
            if shares < 100:
                expired_rows.append((symbol, signal_date))
                stats["skip_cash"] += 1
                continue
            gross_cost = round(shares * actual_buy_price_qfq, 2)
            buy_fee = round(gross_cost * buy_fee_rate, 2)
            actual_cost = round(gross_cost + buy_fee, 2)

        avail_cash -= actual_cost
        current_holdings += 1
        current_market_val += gross_cost
        remaining_market_capacity = max_allowed_stock_equity - current_market_val
        filled_rows.append((symbol, trade_date, actual_buy_price_qfq, buy_price_hfq, int(shares), float(buy_fee), atr_pct))
        stats["filled"] += 1
        log.info(f"✅ 买入成交 {symbol} @¥{actual_buy_price_qfq:.2f} x{int(shares)}股 (挂单¥{planned_buy_price:.2f}, 信号{signal_date}, 今日最低¥{today_low_qfq:.2f})")

    if filled_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
        con.executemany("""
            INSERT OR REPLACE INTO virtual_portfolio(symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [(s, d, bp, bph, sh, atr) for s, d, bp, bph, sh, _, atr in filled_rows])
        for symbol, buy_date, buy_price, buy_price_hfq, shares, buy_fee, _ in filled_rows:
            con.execute(f"UPDATE pending_orders SET status={STATUS_FILLED} WHERE symbol=? AND signal_date<? AND status={STATUS_PENDING}", [symbol, trade_date])
            con.execute("""
                INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct, fee)
                VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, ?)
            """, [symbol, TRADE_BUY, buy_date, round(buy_price, 2), shares, REASON_BUY_T1, round(buy_fee, 2)])
    if expired_rows:
        con.executemany(f"UPDATE pending_orders SET status={STATUS_EXPIRED} WHERE symbol=? AND signal_date=? AND status={STATUS_PENDING}", expired_rows)
    if stats["filled"] or any(v > 0 for v in stats.values()):
        log.info(
            f"📋 挂单处理 [{trade_date}] 成交={stats['filled']} 超时作废={stats['expired_timeout']} "
            f"未触价保留={stats['skip_no_touch']} 停牌跳过={stats['skip_no_data']} "
            f"仓位满={stats['skip_capacity']} 资金不足={stats['skip_cash']} "
            f"确认拦截={stats['skip_confirm']} 重复={stats['skip_dup']}"
        )
    return filled_rows, expired_rows


def process_exit_rules(con, trade_date: date) -> List[Tuple]:
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares, atr_pct_buy FROM virtual_portfolio").df()
    if holdings.empty:
        return []
    holdings["buy_date"] = pd.to_datetime(holdings["buy_date"], errors="coerce")
    holdings = holdings[holdings["buy_date"].notna()].copy()
    holdings["buy_date"] = holdings["buy_date"].dt.date
    holdings = holdings[holdings["buy_date"] < trade_date].copy()
    if holdings.empty:
        return []
    symbols = holdings['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    start_date = (trade_date - timedelta(days=60)).strftime('%Y-%m-%d')
    qfq_df = con.execute(f"""
        WITH raw_data AS (
            SELECT symbol, date, open, high, low, close
            FROM daily_qfq_cache
            WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ),
        indicators AS (
            SELECT *,
                   AVG(close) OVER w20 AS ma20_f,
                   STDDEV(close) OVER w20 AS std20
            FROM raw_data
            WINDOW w20 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        SELECT *, (ma20_f - 2 * std20) AS bb_lower
        FROM indicators
        ORDER BY symbol, date
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()
    hfq_df = con.execute(f"""
        SELECT symbol, date, close
        FROM daily_hfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date
    """, symbols + [start_date, trade_date.strftime('%Y-%m-%d')]).df()

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
    atr_stop_beta = float(CONFIG.get("atr_stop_loss_beta", 2.0))

    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = pd.to_datetime(row['buy_date']).date()
        buy_price = float(row['buy_price'])
        buy_price_hfq = float(row['buy_price_hfq']) if not pd.isna(row['buy_price_hfq']) else buy_price
        shares = int(row['shares'])
        atr_pct_buy = float(row['atr_pct_buy']) if 'atr_pct_buy' in row and not pd.isna(row['atr_pct_buy']) and float(row['atr_pct_buy']) > 0 else 0.03

        gq = qfq_df[qfq_df['symbol'] == sym].copy()
        if gq.empty:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym].copy() if not hfq_df.empty else pd.DataFrame()
        gq['close'] = gq['close'].astype(float)
        last_q = gq.iloc[-1]
        last_close_qfq = float(last_q['close'])

        last_close_hfq = float(gh.iloc[-1]['close']) if not gh.empty else last_close_qfq
        hold_days = (trade_date - buy_date).days
        pnl_pct = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100 if buy_price_hfq > 0 else 0.0
        reason_mask = 0

        dynamic_stop_loss_limit_pct = -1.0 * atr_stop_beta * atr_pct_buy * 100.0
        if pnl_pct <= dynamic_stop_loss_limit_pct:
            reason_mask |= REASON_STOPLOSS

        last_close_f = float(last_q['close'])
        prev_close_f = float(gq.iloc[-2]['close']) if len(gq) >= 2 else last_close_f
        last_mid = float(last_q['ma20_f']) if not pd.isna(last_q['ma20_f']) else 0
        ma20_float = float(last_q['ma20_f']) if not pd.isna(last_q['ma20_f']) else None

        if ma20_float is not None and last_close_qfq < ma20_float:
            reason_mask |= REASON_BELOW_MA20
        if hold_days >= CONFIG['max_hold_days']:
            reason_mask |= REASON_MAX_HOLD

        if last_mid > 0 and prev_close_f > last_mid and last_close_f < last_mid:
            reason_mask |= REASON_BELOW_BOLL_MID

        o_s = gq['open'].astype(float).tail(15)
        h_s = gq['high'].astype(float).tail(15)
        l_s = gq['low'].astype(float).tail(15)
        c_s = gq['close'].tail(15)
        bull_text, bull_score, bear_text, bear_score = detect_kline_patterns(o_s, h_s, l_s, c_s)

        if bear_score >= 1.0:
            reason_mask |= REASON_BEAR_PATTERN

        if talib is not None and len(gq) > 30:
            c_vals = gq['close'].values.astype(np.float64)
            macd, macdsignal, macdhist = talib.MACD(c_vals, fastperiod=12, slowperiod=26, signalperiod=9)
            if not pd.isna(macdhist[-1]) and not pd.isna(macdhist[-2]):
                if macdhist[-1] < macdhist[-2]:
                    reason_mask |= REASON_MACD_DECREASE

        if reason_mask > 0:
            sell_price_raw = raw_map.get(sym, last_close_qfq)
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


def compute_all_signals(con, target_date: date) -> pd.DataFrame:
    start_date = (target_date - timedelta(days=120)).strftime("%Y-%m-%d")
    end_date = target_date.strftime("%Y-%m-%d")
    atr_alpha = float(CONFIG.get("atr_buy_alpha", 0.5))

    _SIGNAL_CTE = """
    WITH raw_data AS (
        SELECT symbol, date, high, low, close, volume,
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
        SELECT symbol, date, high, low, close, volume,
               AVG(tr) OVER w14 AS atr14,
               AVG(close) OVER w20 AS ma20,
               STDDEV(close) OVER w20 AS std20,
               AVG(volume) OVER w5 AS vol_ma5,
               LAG(close, 5) OVER w AS close_5,
               LAG(close, 20) OVER w AS close_20
        FROM tr_data
        WINDOW
            w14 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            w5 AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
            w AS (PARTITION BY symbol ORDER BY date)
    ),
    derived AS (
        SELECT *,
               4 * std20 AS band_width,
               LAG(ma20, 1) OVER w AS ma20_1,
               LAG(ma20, 2) OVER w AS ma20_2,
               LAG(vol_ma5, 1) OVER w AS vol_ma5_1
        FROM indicators
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
    ),
    derived2 AS (
        SELECT *,
               LAG(band_width, 1) OVER w AS band_width_1,
               LAG(band_width, 2) OVER w AS band_width_2
        FROM derived
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
    )
    """
    _SIGNAL_SELECT = """
    SELECT symbol, date, close, volume, ma20, std20, atr14, vol_ma5, close_5, close_20,
           band_width, band_width_1, band_width_2, ma20_1, ma20_2, vol_ma5_1
    FROM derived2
    WHERE date = ?
      AND band_width > band_width_1
      AND band_width_1 > band_width_2
      AND ma20 > ma20_1
      AND ma20_1 > ma20_2
      AND volume IS NOT NULL AND volume > 0
      AND vol_ma5_1 IS NOT NULL
      AND volume > vol_ma5_1
      AND close > 0
      AND ma20 IS NOT NULL
      AND atr14 IS NOT NULL
    """
    query = _SIGNAL_CTE + _SIGNAL_SELECT
    candidates = con.execute(query, [start_date, end_date, end_date]).df()
    candidates['close'] = candidates['close'].astype(np.float64)
    if candidates.empty:
        return pd.DataFrame()

    symbols = candidates['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    macd_start = (target_date - timedelta(days=60)).strftime("%Y-%m-%d")
    hist_df = con.execute(f"""
        SELECT symbol, date, open, high, low, close
        FROM daily_qfq_cache
        WHERE symbol IN ({placeholders}) AND date BETWEEN ? AND ?
        ORDER BY symbol, date
    """, symbols + [macd_start, end_date]).df()

    macd_results = []
    for sym, grp in hist_df.groupby("symbol"):
        if len(grp) < 34:
            continue
        c_vals = grp["close"].values.astype(np.float64)
        macd_val, macd_1_val = 0.0, 0.0
        if talib is not None:
            macd, _, _ = talib.MACD(c_vals, fastperiod=12, slowperiod=26, signalperiod=9)
            if not pd.isna(macd[-1]) and not pd.isna(macd[-2]) and macd[-1] > macd[-2]:
                macd_val, macd_1_val = macd[-1], macd[-2]
        else:
            ema12 = grp['close'].ewm(span=12, adjust=False).mean().values
            ema26 = grp['close'].ewm(span=26, adjust=False).mean().values
            macd = ema12 - ema26
            if macd[-1] > macd[-2]:
                macd_val, macd_1_val = macd[-1], macd[-2]

        if macd_val > macd_1_val:
            o_s = grp['open'].tail(15)
            h_s = grp['high'].tail(15)
            l_s = grp['low'].tail(15)
            c_s = grp['close'].tail(15)
            bull_text, bull_score, bear_text, bear_score = detect_kline_patterns(o_s, h_s, l_s, c_s)
            macd_results.append({'symbol': sym, 'macd': macd_val, 'macd_1': macd_1_val, 'kline_pattern': bull_text})

    macd_df = pd.DataFrame(macd_results)
    if macd_df.empty:
        return pd.DataFrame()

    picks = pd.merge(candidates, macd_df, on='symbol', how='inner')

    picks["vol_bb_break"] = "✅放量开口"
    picks["lower"] = picks["ma20"] - 2 * picks["std20"]
    picks["ret_5d"] = np.where(picks["close_5"] > 0, (picks["close"] / picks["close_5"]) - 1.0, 0.0)
    picks["ret_20d"] = np.where(picks["close_20"] > 0, (picks["close"] / picks["close_20"]) - 1.0, 0.0)
    picks["macd_strength"] = np.where(picks["close"] > 0, picks["macd"] / picks["close"] * 100.0, 0.0)
    picks["vol_ratio"] = np.where(picks["vol_ma5_1"] > 0, picks["volume"] / picks["vol_ma5_1"], 1.0)
    picks["bb_breakout"] = np.where(picks["band_width"] > 0, (picks["close"] - picks["lower"]) / picks["band_width"], 0.0)

    picks["atr_pct"] = (picks["atr14"] / picks["close"]).round(4)
    picks["planned_buy_price"] = (picks["close"] - atr_alpha * picks["atr14"]).round(2)
    picks["planned_buy_price"] = np.where(picks["planned_buy_price"] <= 0, (picks["close"] * 0.99).round(2), picks["planned_buy_price"])

    picks["total_score"] = (picks["ret_20d"] * 100.0).round(2)
    picks["signal_strength"] = (picks["macd_strength"] + picks["vol_ratio"] + picks["bb_breakout"]).round(2)
    picks["close"] = picks["close"].round(2)

    picks["date"] = pd.to_datetime(picks["date"]).dt.date

    if CONFIG.get("filter_gem_star", False):
        picks = picks[~picks["symbol"].str.contains("^(?:300|301|688)")].copy()

    return picks[["symbol", "date", "close", "planned_buy_price", "atr_pct", "ret_5d", "ret_20d", "total_score", "signal_strength", "kline_pattern", "vol_bb_break"]]


def evaluate_strategy(db_path: str, target_date: date, top_n: Optional[int] = None, allow_exit_on_date: bool = True):
    top_n = top_n or CONFIG["top_n"]
    with duckdb.connect(db_path, read_only=False) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        history_before = con.execute("SELECT COUNT(*) FROM account_history WHERE date < ?", [target_date]).fetchone()[0]
        if allow_exit_on_date and int(history_before) > 0:
            process_exit_rules(con, target_date)
        else:
            log.info(f"🛡️ 初始交易日 {target_date} 仅允许买入，跳过卖出规则")
        process_pending_orders(con, target_date)

        df_picks = compute_all_signals(con, target_date)
        if not df_picks.empty:
            df_picks = df_picks.sort_values(["total_score", "symbol"], ascending=[False, True]).head(top_n).reset_index(drop=True)

        _exp = int(CONFIG.get("buy_signal_expire_days", 2))
        con.execute(f"""
            UPDATE pending_orders SET status={STATUS_EXPIRED}
            WHERE status={STATUS_PENDING} AND signal_date <= (
                SELECT tradedate FROM {STOCKS_TABLE}
                WHERE tradedate < ? ORDER BY tradedate DESC LIMIT 1 OFFSET ?
            )
        """, [target_date, _exp])
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

        df_pending = con.execute(f"""
            SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status, signal_strength, atr_pct
            FROM pending_orders
            WHERE status={STATUS_PENDING}
            ORDER BY signal_date DESC, symbol
        """).df()

        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
            total_market_value = 0.0
        else:
            raw_today_df = con.execute(f"SELECT symbol, close FROM {STOCKS_TABLE} WHERE tradedate = ?", [target_date]).df()
            if not raw_today_df.empty:
                raw_today_df["symbol"] = raw_today_df["symbol"].map(canonical_symbol)
            hfq_today_df = con.execute("SELECT symbol, close FROM daily_hfq_cache WHERE date = ?", [target_date]).df()
            df_portfolio = holdings.merge(raw_today_df.rename(columns={"close": "last_price"}), on="symbol", how="left")
            df_portfolio = df_portfolio.merge(hfq_today_df.rename(columns={"close": "last_price_hfq"}), on="symbol", how="left")
            df_portfolio["last_price"] = df_portfolio["last_price"].fillna(df_portfolio["buy_price"])
            df_portfolio["last_price_hfq"] = df_portfolio["last_price_hfq"].fillna(df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]))
            df_portfolio["shares"] = (np.round(df_portfolio["shares"].astype(float) / 100) * 100).astype(int)
            invalid_symbols = df_portfolio[df_portfolio["shares"] < 100]["symbol"].tolist()
            if invalid_symbols:
                log.warning(f"⚠️ 过滤不足100股的无效持仓: {invalid_symbols}")
                for sym in invalid_symbols:
                    con.execute("DELETE FROM virtual_portfolio WHERE symbol=?", [sym])
                df_portfolio = df_portfolio[df_portfolio["shares"] >= 100].copy()
            df_portfolio["market_value"] = (df_portfolio["last_price"].astype(float) * df_portfolio["shares"]).astype(float)
            df_portfolio["cost"] = (df_portfolio["buy_price"].astype(float) * df_portfolio["shares"]).astype(float)
            df_portfolio["pnl_pct"] = (df_portfolio["last_price_hfq"] - df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"])) / df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]) * 100
            df_portfolio["holding_days"] = df_portfolio["buy_date"].apply(lambda x: (target_date - pd.to_datetime(x).date()).days)

            advice_list = []
            for r in df_portfolio.itertuples():
                if r.pnl_pct > CONFIG["take_profit_pct"] * 0.8:
                    advice_list.append("💡 接近止盈")
                elif r.holding_days >= CONFIG["max_hold_days"] - 1:
                    advice_list.append("⏳ 接近期限")
                elif r.pnl_pct < 0:
                    advice_list.append("⚠️ 浮亏持有")
                else:
                    advice_list.append("✅ 继续持有")
            df_portfolio["holding_advice"] = advice_list

            total_market_value = df_portfolio["market_value"].sum()

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

        chart_b64 = None
        sharpe = 0.0
        max_drawdown = 0.0
        calmar = 0.0
        annual_ret = 0.0
        n_days = 0

        hist_df = con.execute("SELECT date, daily_ret, total_assets FROM account_history ORDER BY date ASC").df()
        if len(hist_df) >= 2:
            try:
                hist_df['date'] = pd.to_datetime(hist_df['date'])
                rf_annual = 0.018
                rf_daily = rf_annual / 252
                mean_ret = hist_df['daily_ret'].mean()
                std_ret = hist_df['daily_ret'].std()
                sharpe = (mean_ret - rf_daily) / std_ret * np.sqrt(252) if std_ret > 0 else 0

                net_values = hist_df['total_assets'].values / init_cap
                peaks = np.maximum.accumulate(net_values)
                drawdowns = (peaks - net_values) / peaks
                max_drawdown = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

                n_days = len(hist_df)
                total_ret = (new_total_assets / init_cap) - 1.0
                annual_ret = (1 + total_ret) ** (252 / max(n_days, 1)) - 1.0
                calmar = annual_ret / max_drawdown if max_drawdown > 0 else 0.0

                # ── 绘制专业净值与回撤双子图 ──
                plt.close('all')
                fig, (ax1, ax2) = plt.subplots(
                    2, 1, figsize=(11, 6.2), sharex=True,
                    gridspec_kw={'height_ratios': [3.2, 1.1]}
                )
                fig.patch.set_facecolor('#ffffff')
                ax1.set_facecolor('#ffffff')
                ax2.set_facecolor('#ffffff')

                dates = hist_df['date']
                ax1.plot(dates, net_values, color='#2563eb', linewidth=2.0, label=f'策略净值 (Sharpe: {sharpe:.2f})')
                ax1.axhline(1.0, color='#94a3b8', linestyle='--', linewidth=1.1, alpha=0.85)
                ax1.set_title(
                    f"A股多头共振策略 {n_days} 交易日回测 | 累计收益: {total_ret*100:+.1f}% (年化: {annual_ret*100:+.1f}%) | 最大回撤: -{max_drawdown*100:.1f}%",
                    fontsize=12, fontweight='bold', pad=10, color='#1e293b'
                )
                ax1.set_ylabel('净值 (Net Value)', fontsize=10, color='#334155')
                ax1.grid(True, linestyle=':', color='#cbd5e1', alpha=0.8)
                ax1.legend(loc='upper left', frameon=True, facecolor='#ffffff', edgecolor='#cbd5e1')
                ax1.tick_params(colors='#334155')

                dd_pct = drawdowns * 100.0
                ax2.fill_between(dates, -dd_pct, 0, color='#fca5a5', alpha=0.75, label='动态回撤 (Drawdown %)')
                ax2.plot(dates, -dd_pct, color='#ef4444', linewidth=0.8, alpha=0.5)
                ax2.set_ylabel('回撤 %', fontsize=10, color='#334155')
                ax2.grid(True, linestyle=':', color='#cbd5e1', alpha=0.8)
                ax2.legend(loc='lower left', frameon=True, facecolor='#ffffff', edgecolor='#cbd5e1')
                ax2.tick_params(colors='#334155')
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))

                plt.tight_layout()
                buf = io.BytesIO()
                plt.savefig(buf, format='png', dpi=140, bbox_inches='tight')
                plt.close()
                buf.seek(0)
                chart_b64 = base64.b64encode(buf.read()).decode('utf-8')
            except Exception as e:
                log.error(f"Plotting failed: {e}")

        # ── 历史交易表现统计（总交易笔数、胜率、盈亏比）──
        trade_stats_row = con.execute("""
            SELECT 
                COUNT(*) AS total_sells,
                COUNT(CASE WHEN pnl_pct > 0 THEN 1 END) AS win_sells,
                AVG(CASE WHEN pnl_pct > 0 THEN pnl_pct END) AS avg_win_pct,
                AVG(CASE WHEN pnl_pct < 0 THEN ABS(pnl_pct) END) AS avg_loss_pct
            FROM trade_history
            WHERE trade_type = ? AND pnl_pct IS NOT NULL
        """, [TRADE_SELL]).fetchone()

        total_trades = int(trade_stats_row[0] or 0)
        win_trades = int(trade_stats_row[1] or 0)
        win_rate = (win_trades / total_trades * 100.0) if total_trades > 0 else 0.0
        avg_win = float(trade_stats_row[2] or 0.0)
        avg_loss = float(trade_stats_row[3] or 0.0)
        profit_loss_ratio = (avg_win / avg_loss) if avg_loss > 0 else (1.0 if avg_win > 0 else 0.0)

        market_pos_ratio = get_market_target_position_ratio(con, target_date)

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
            "annual_ret": annual_ret,
            "n_days": n_days,
            "market_pos_ratio": market_pos_ratio,
            "total_trades": total_trades,
            "win_rate": win_rate,
            "profit_loss_ratio": profit_loss_ratio,
            "chart_b64": chart_b64,
            "hist_df": hist_df,  # 传递历史数据用于构建交互式图表
        }
        df_trades = con.execute("SELECT * FROM trade_history WHERE trade_date = ? ORDER BY trade_type, symbol", [target_date]).df()
        apply_history_retention(con)
    return decode_numeric_frame(df_picks), decode_numeric_frame(df_portfolio), decode_numeric_frame(df_pending), decode_numeric_frame(df_trades), metrics

# =========================================================
# 交互式图表生成器 (ECharts Standalone HTML)
# =========================================================
def build_interactive_chart_html(hist_df: pd.DataFrame, metrics: dict, target_str: str) -> str:
    """生成无需额外 Python 依赖的 ECharts 独立交互式 HTML 回测图表"""
    if hist_df is None or len(hist_df) < 2:
        return ""

    init_cap = float(CONFIG.get("init_cash", 100000.0))
    dates = [pd.to_datetime(d).strftime('%Y-%m-%d') for d in hist_df['date']]
    net_values = [round(float(v), 4) for v in (hist_df['total_assets'] / init_cap)]
    peaks = np.maximum.accumulate(net_values)
    drawdowns = [round(float(-(p - v) / p * 100), 2) if p > 0 else 0.0 for p, v in zip(peaks, net_values)]

    dates_json = json.dumps(dates)
    net_values_json = json.dumps(net_values)
    drawdowns_json = json.dumps(drawdowns)

    sharpe = metrics.get('sharpe', 0.0)
    total_ret = metrics.get('total_pnl_pct', 0.0)
    annual_ret = metrics.get('annual_ret', 0.0) * 100.0
    max_dd = metrics.get('max_drawdown', 0.0) * 100.0

    html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <title>A股多头共振策略 - 交互式回测图表 ({target_str})</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
  <style>
    body {{ margin:0; padding:16px; background:#121418; color:#f1f5f9; font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif; }}
    .header {{ text-align:center; margin-bottom:16px; }}
    .header h2 {{ font-size:20px; margin:0 0 6px 0; color:#fff; }}
    .header p {{ font-size:13px; color:#94a3b8; margin:0; }}
    #main {{ width:100%; height:82vh; background:#1a1e24; border-radius:12px; border:1px solid #282f3c; box-shadow:0 6px 20px rgba(0,0,0,0.4); }}
  </style>
</head>
<body>
  <div class="header">
    <h2>📈 A股多头共振策略 · 交互式回测图表</h2>
    <p>统计区间: {dates[0]} ~ {dates[-1]} | 累计收益: {total_ret:+.1f}% | 年化: {annual_ret:+.1f}% | 最大回撤: -{max_dd:.1f}% | 夏普比率: {sharpe:.2f}</p>
  </div>
  <div id="main"></div>

  <script>
    var chartDom = document.getElementById('main');
    var myChart = echarts.init(chartDom, 'dark');
    var dates = {dates_json};
    var netValues = {net_values_json};
    var drawdowns = {drawdowns_json};

    var option = {{
      backgroundColor: '#1a1e24',
      animation: true,
      tooltip: {{
        trigger: 'axis',
        axisPointer: {{ type: 'cross', lineStyle: {{ color: '#94a3b8', type: 'dashed' }} }},
        backgroundColor: 'rgba(24, 27, 34, 0.95)',
        borderColor: '#3b82f6',
        borderWidth: 1,
        textStyle: {{ color: '#f8fafc', fontSize: 13 }},
        formatter: function(params) {{
          var res = '<div style="font-weight:bold;margin-bottom:4px;">' + params[0].axisValue + '</div>';
          params.forEach(function(item) {{
            if (item.seriesName === '策略净值') {{
              res += '<span style="color:#3b82f6">●</span> 净值: <b>' + item.data + '</b><br/>';
            }} else if (item.seriesName === '动态回撤') {{
              res += '<span style="color:#ef4444">●</span> 回撤: <b>' + item.data + '%</b>';
            }}
          }});
          return res;
        }}
      }},
      legend: {{
        data: ['策略净值', '动态回撤'],
        top: 10,
        textStyle: {{ color: '#cbd5e1' }}
      }},
      toolbox: {{
        right: 20,
        top: 10,
        feature: {{
          dataZoom: {{ yAxisIndex: 'none' }},
          restore: {{}},
          saveAsImage: {{ title: '保存图片', pixelRatio: 2 }}
        }},
        iconStyle: {{ borderColor: '#94a3b8' }}
      }},
      axisPointer: {{ link: [{{ xAxisIndex: 'all' }}] }},
      grid: [
        {{ left: '55px', right: '30px', top: '12%', height: '54%' }},
        {{ left: '55px', right: '30px', top: '72%', height: '18%' }}
      ],
      xAxis: [
        {{
          type: 'category',
          data: dates,
          scale: true,
          boundaryGap: false,
          axisLine: {{ lineStyle: {{ color: '#475569' }} }},
          splitLine: {{ show: true, lineStyle: {{ color: '#252b36', type: 'dashed' }} }},
          axisLabel: {{ show: false }}
        }},
        {{
          type: 'category',
          gridIndex: 1,
          data: dates,
          boundaryGap: false,
          axisLine: {{ lineStyle: {{ color: '#475569' }} }},
          splitLine: {{ show: true, lineStyle: {{ color: '#252b36', type: 'dashed' }} }},
          axisLabel: {{ color: '#94a3b8' }}
        }}
      ],
      yAxis: [
        {{
          scale: true,
          splitArea: {{ show: false }},
          axisLine: {{ lineStyle: {{ color: '#475569' }} }},
          splitLine: {{ lineStyle: {{ color: '#252b36', type: 'dashed' }} }},
          axisLabel: {{ color: '#94a3b8' }}
        }},
        {{
          gridIndex: 1,
          scale: true,
          splitArea: {{ show: false }},
          axisLine: {{ lineStyle: {{ color: '#475569' }} }},
          splitLine: {{ lineStyle: {{ color: '#252b36', type: 'dashed' }} }},
          axisLabel: {{
            color: '#94a3b8',
            formatter: '{{value}}%'
          }}
        }}
      ],
      dataZoom: [
        {{ type: 'inside', xAxisIndex: [0, 1] }},
        {{
          type: 'slider',
          xAxisIndex: [0, 1],
          bottom: 10,
          height: 20,
          borderColor: '#2d3748',
          textStyle: {{ color: '#94a3b8' }}
        }}
      ],
      series: [
        {{
          name: '策略净值',
          type: 'line',
          data: netValues,
          smooth: true,
          showSymbol: false,
          lineStyle: {{ width: 2.5, color: '#3b82f6' }},
          markLine: {{
            silent: true,
            symbol: 'none',
            lineStyle: {{ color: '#94a3b8', type: 'dashed' }},
            data: [{{ yAxis: 1.0 }}]
          }}
        }},
        {{
          name: '动态回撤',
          type: 'line',
          xAxisIndex: 1,
          yAxisIndex: 1,
          data: drawdowns,
          smooth: true,
          showSymbol: false,
          lineStyle: {{ width: 1, color: '#ef4444' }},
          areaStyle: {{
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              {{ offset: 0, color: 'rgba(239, 68, 68, 0.4)' }},
              {{ offset: 1, color: 'rgba(239, 68, 68, 0.05)' }}
            ])
          }}
        }}
      ]
    }};
    myChart.setOption(option);
    window.addEventListener('resize', function() {{ myChart.resize(); }});
  </script>
</body>
</html>"""
    return html_content

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

def _ret_cell(v: float) -> str:
    sign = "+" if v >= 0 else ""
    cls = "ret-pos" if v >= 0 else "ret-neg"
    return f'<span class="{cls}">{sign}{v*100:.1f}%</span>'

def _picks_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>今日无候选股票</div></div>'
    rows = []
    for i, r in enumerate(df.itertuples(), 1):
        kline_p = getattr(r, "kline_pattern", "")
        bb_break = getattr(r, "vol_bb_break", "")
        atr_pct_val = float(getattr(r, "atr_pct", 0.03)) * 100
        rows.append(f"""
        <tr>
          <td>{i}</td>
          <td>{_market_badge(r.symbol)}</td>
          <td>¥{float(r.close):.2f}</td>
          <td>¥{float(r.planned_buy_price):.2f}</td>
          <td>{atr_pct_val:.2f}%</td>
          <td>{kline_p or '—'}</td>
          <td>{bb_break or '—'}</td>
          <td>{_ret_cell(getattr(r, 'ret_5d', 0.0))}</td>
          <td>{_ret_cell(getattr(r, 'ret_20d', 0.0))}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>#</th><th>代码</th><th>收盘价</th><th>动态ATR挂单价</th>
        <th>ATR(14)波幅</th><th>K线形态</th><th>量价共振</th>
        <th>5日涨幅</th><th>20日涨幅</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""

def _pending_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>暂无待成交挂单</div></div>'
    rows = []
    for r in df.itertuples():
        pbp = float(r.planned_buy_price)
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
        <th>信号收盘</th><th>回调幅度</th><th>状态</th>
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
        advice = getattr(r, "holding_advice", "✅ 继续持有")
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
          <td>{advice}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>代码</th><th>买入日</th><th>成本价</th><th>现价</th>
        <th>股数</th><th>市值</th><th>持有</th><th>浮盈</th><th>持仓建议</th>
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

    n_days = int(metrics.get("n_days", 90))
    pos_limit_ratio = float(metrics.get("market_pos_ratio", 1.0))
    pos_limit_pct = pos_limit_ratio * 100.0

    if pos_limit_ratio >= 1.0:
        regime_badge = '🟢 上升行情 (满仓上限)'
    elif pos_limit_ratio >= 0.5:
        regime_badge = f'🟡 震荡行情 (受控上限: {pos_limit_pct:.0f}%)'
    else:
        regime_badge = f'🔴 下降行情 (受控上限: {pos_limit_pct:.0f}%)'

    calmar_ratio = metrics.get('calmar', 0.0)
    sharpe_ratio = metrics.get('sharpe', 0.0)
    total_ret = metrics.get('total_pnl_pct', 0.0)
    ann_ret = metrics.get('annual_ret', 0.0) * 100.0
    max_dd = metrics.get('max_drawdown', 0.0) * 100.0

    CSS = """
<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;
       background:#121418; color:#f1f5f9; -webkit-font-smoothing:antialiased; }
.wrapper { max-width:980px; margin:0 auto; padding:24px 16px; }

/* ── 顶部 Header ── */
.top-header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:20px; flex-wrap:wrap; gap:12px; }
.strategy-title-wrap h1 { font-size:20px; font-weight:700; color:#ffffff; display:flex; align-items:center; gap:8px; }
.strategy-subtitle { font-size:12px; color:#94a3b8; margin-top:6px; letter-spacing:0.3px; }
.header-meta { text-align:right; }
.meta-date { font-size:16px; font-weight:700; color:#f8fafc; }
.meta-regime { font-size:12px; color:#ef4444; font-weight:600; margin-top:4px; display:inline-block; }

/* ── 仪表盘卡片网格 ── */
.kpi-deck { display:grid; grid-template-columns:repeat(4, 1fr); gap:12px; margin-bottom:22px; }
.stat-card { background:#1a1e24; border-radius:10px; padding:16px 14px; position:relative; overflow:hidden; border:1px solid #282f3c; }
.stat-card::before { content:''; position:absolute; left:0; top:0; bottom:0; width:4px; }
.stat-card.blue::before { background:#3b82f6; }
.stat-card.green::before { background:#10b981; }
.stat-card.cyan::before { background:#06b6d4; }
.stat-card.red::before { background:#ef4444; }
.stat-card.purple::before { background:#8b5cf6; }

.stat-label { font-size:11px; color:#94a3b8; text-transform:uppercase; margin-bottom:6px; }
.stat-val { font-size:20px; font-weight:700; color:#f8fafc; line-height:1.2; }
.stat-val.pos { color:#10b981; }
.stat-val.neg { color:#ef4444; }

/* ── 回测绩效统计栏目 ── */
.section-headline { font-size:14px; font-weight:700; color:#f1f5f9; margin-bottom:12px; display:flex; align-items:center; gap:8px; }
.trade-summary-bar { font-size:12px; color:#94a3b8; margin-top:-10px; margin-bottom:18px; padding-left:2px; }

/* ── 绩效曲线展示 ── */
.chart-container { background:#ffffff; border-radius:12px; padding:12px; margin-bottom:22px; box-shadow:0 4px 16px rgba(0,0,0,0.4); text-align:center; }
.chart-img { max-width:100%; height:auto; display:block; margin:0 auto; border-radius:8px; }

/* ── 业务数据模块 ── */
.section-card { background:#1a1e24; border-radius:12px; border:1px solid #282f3c; padding:20px; margin-bottom:20px; }
.section-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:14px; }
.section-title { font-size:15px; font-weight:600; color:#f8fafc; display:flex; align-items:center; gap:8px; }
.section-count { background:#252b36; color:#94a3b8; font-size:11px; padding:3px 8px; border-radius:12px; }

.table-wrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
.data-table { width:100%; border-collapse:collapse; font-size:12px; color:#cbd5e1; }
.data-table th { padding:10px 10px; text-align:left; font-weight:600; color:#94a3b8; font-size:11px; border-bottom:1px solid #2d3545; white-space:nowrap; background:#16191f; }
.data-table td { padding:10px 10px; border-bottom:1px solid #252b36; vertical-align:middle; white-space:nowrap; }
.data-table tbody tr:hover { background:#20252e; }

.badge { display:inline-block; padding:2px 6px; border-radius:4px; font-size:11px; font-weight:600; white-space:nowrap; }
.badge-sh { background:#450a0a; color:#f87171; border:1px solid #7f1d1d; }
.badge-sz { background:#172554; color:#60a5fa; border:1px solid #1e3a8a; }
.badge-kcb { background:#422006; color:#facc15; border:1px solid #713f12; }
.badge-cy { background:#052e16; color:#4ade80; border:1px solid #14532d; }
.badge-zx { background:#083344; color:#38bdf8; border:1px solid #164e63; }
.badge-bj { background:#431407; color:#fb923c; border:1px solid #7c2d12; }
.badge-pending { background:#451a03; color:#f59e0b; border:1px solid #78350f; }

.ret-pos { color:#10b981; font-weight:600; }
.ret-neg { color:#ef4444; font-weight:600; }
.empty-state { text-align:center; padding:28px 16px; color:#64748b; font-size:13px; }
.empty-icon { font-size:28px; margin-bottom:6px; }

.strategy-pills { display:flex; flex-wrap:wrap; gap:8px; }
.pill { background:#252b36; border:1px solid #333c4d; border-radius:16px; padding:4px 10px; font-size:11px; color:#cbd5e1; }
.pill.buy { background:#2d2605; border-color:#59490b; color:#fbbf24; }
.pill.sell { background:#3b1111; border-color:#652323; color:#f87171; }

.footer { text-align:center; padding:18px; color:#64748b; font-size:11px; }

@media screen and (max-width: 768px) {
  .wrapper { padding:12px 8px; }
  .kpi-deck { grid-template-columns:repeat(2, 1fr); }
  .top-header { flex-direction:column; gap:8px; }
  .header-meta { text-align:left; }
}
</style>"""

    html = f"""<!DOCTYPE html><html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">{CSS}</head>
<body><div class="wrapper">

<!-- 顶部状态栏 -->
<div class="top-header">
  <div class="strategy-title-wrap">
    <h1>🗂️ A股多头共振策略 · 冷启动回测 ({n_days}日)</h1>
    <div class="strategy-subtitle">均线多头 + MA20升穿MA60 + MACD>0 + EMA多头 + BOLL沿上轨 + 动态再平衡</div>
  </div>
  <div class="header-meta">
    <div class="meta-date">{target_str}</div>
    <div class="meta-regime">{regime_badge}</div>
  </div>
</div>

<!-- 核心资产状态（第一行 4 列） -->
<div class="kpi-deck">
  <div class="stat-card blue">
    <div class="stat-label">最新总资产</div>
    <div class="stat-val">¥{metrics.get('total_assets', 0):,.0f}</div>
  </div>
  <div class="stat-card green">
    <div class="stat-label">可用余额</div>
    <div class="stat-val">¥{metrics.get('avail_cash', 0):,.0f}</div>
  </div>
  <div class="stat-card cyan">
    <div class="stat-label">当前仓位 / 受控上限</div>
    <div class="stat-val">{metrics.get('position_pct', 0):.1f}% / {pos_limit_pct:.0f}%</div>
  </div>
  <div class="stat-card {'green' if metrics.get('daily_pnl', 0) >= 0 else 'red'}">
    <div class="stat-label">当日盈亏</div>
    <div class="stat-val {'pos' if metrics.get('daily_pnl', 0) >= 0 else 'neg'}">¥{metrics.get('daily_pnl', 0):,.0f}</div>
  </div>
</div>

<!-- 历史策略回测绩效统计（第二行 4 列） -->
<div class="section-headline">📊 历史策略回测绩效统计 (近 {n_days} 交易日)</div>
<div class="kpi-deck">
  <div class="stat-card blue">
    <div class="stat-label">累计收益率</div>
    <div class="stat-val {'pos' if total_ret >= 0 else 'neg'}">{total_ret:+.1f}%</div>
  </div>
  <div class="stat-card cyan">
    <div class="stat-label">年化收益率</div>
    <div class="stat-val {'pos' if ann_ret >= 0 else 'neg'}">{ann_ret:+.1f}%</div>
  </div>
  <div class="stat-card red">
    <div class="stat-label">最大回撤 (MaxDD)</div>
    <div class="stat-val neg">-{max_dd:.1f}%</div>
  </div>
  <div class="stat-card purple">
    <div class="stat-label">夏普 / 卡玛比率</div>
    <div class="stat-val">{sharpe_ratio:.2f} / {calmar_ratio:.2f}</div>
  </div>
</div>

<!-- 交易统计汇总条 -->
<div class="trade-summary-bar">
  交易统计：总交易 {metrics.get('total_trades', 0)} 笔 | 胜率 {metrics.get('win_rate', 0):.1f}% | 盈亏比 {metrics.get('profit_loss_ratio', 0):.2f}
</div>

<!-- 绩效曲线（Matplotlib 静态预览 + 交互图引导） -->
{f'''
<div class="chart-container">
  <img class="chart-img" src="data:image/png;base64,{metrics.get("chart_b64")}" alt="回测净值曲线" />
  <div style="margin-top:12px;text-align:center;">
    <span style="display:inline-block;background:#1e293b;border:1px solid #3b82f6;border-radius:20px;padding:6px 14px;font-size:12px;color:#60a5fa;">
      ✨ 已随邮件附带 <b>交互式动态图表.html</b>，点击附件即可开启十字光标追踪与滚轮局部缩放！
    </span>
  </div>
</div>
''' if metrics.get("chart_b64") else ''}

<!-- 1. 当前持仓 -->
<div class="section-card">
  <div class="section-header">
    <div class="section-title">💼 当前持仓</div>
    <span class="section-count">{n_portfolio} 只</span>
  </div>
  <div class="table-wrap">{_portfolio_table(df_portfolio)}</div>
</div>

<!-- 2. 候选股 -->
<div class="section-card">
  <div class="section-header">
    <div class="section-title">🔍 当日候选股票</div>
    <span class="section-count">{n_picks} 只</span>
  </div>
  <div class="table-wrap">{_picks_table(df_picks)}</div>
</div>

<!-- 3. 挂单信息 -->
<div class="section-card">
  <div class="section-header">
    <div class="section-title">⏳ 待成交挂单</div>
    <span class="section-count">{n_pending} 只</span>
  </div>
  <p style="font-size:12px;color:#94a3b8;margin-bottom:12px;">
    规则：T+1 日最低价 ≤ 挂单价（收盘价 - {CONFIG['atr_buy_alpha']} × ATR14）才成交
  </p>
  <div class="table-wrap">{_pending_table(df_pending)}</div>
</div>

<!-- 4. 成交记录 -->
<div class="section-card">
  <div class="section-header">
    <div class="section-title">✅ 当日成交记录</div>
    <span class="section-count">{n_trades} 笔</span>
  </div>
  <div class="table-wrap">{_trades_table(df_trades)}</div>
</div>

<!-- 5. 策略参数说明 -->
<div class="section-card">
  <div class="section-header">
    <div class="section-title">⚙️ 策略执行规则</div>
  </div>
  <div class="strategy-pills">
    <span class="pill buy">挂单：T日收盘 - {CONFIG['atr_buy_alpha']}×ATR(14)</span>
    <span class="pill sell">动态止损：-{CONFIG['atr_stop_loss_beta']}×ATR_pct</span>
    <span class="pill">仓位：风险平价 Risk Parity</span>
    <span class="pill">风控：大盘多级受控（100%/50%/30%）</span>
    <span class="pill sell">跌破 MA20 离场</span>
    <span class="pill sell">最长持有 {CONFIG['max_hold_days']} 天</span>
    <span class="pill">选股 Top {CONFIG['top_n']}</span>
  </div>
</div>

<div class="footer">
  <p>本报告由量化程序自动生成 · {target_str} 收盘后运行</p>
  <p style="margin-top:4px;">数据源：chenditc/investment_data (Qlib) · 策略：布林带量价MACD共振 + 动态ATR · 仅供参考，不构成投资建议</p>
</div>

</div></body></html>"""

    attachments = []
    
    # ── 1. 自动挂载独立交互式 HTML 回测图表 ──
    hist_df = metrics.get("hist_df")
    if hist_df is not None and not hist_df.empty:
        interactive_html = build_interactive_chart_html(hist_df, metrics, target_str)
        if interactive_html:
            interactive_b64 = base64.b64encode(interactive_html.encode('utf-8')).decode('utf-8')
            attachments.append({
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": f"A股多头共振策略_交互式回测图表_{target_str}.html",
                "contentBytes": interactive_b64,
                "contentType": "text/html"
            })

    # ── 2. 挂载日志文件 ──
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "rb") as f:
            content_bytes = base64.b64encode(f.read()).decode("utf-8")
        attachments.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": os.path.basename(LOG_FILE),
            "contentBytes": content_bytes,
        })

    send_email_via_graph(tm, f"💴 CN量化日报 - {target_str}", html, attachments)

# =========================================================
# 主流程
# =========================================================
def _latest_trade_date_in_db(con, target_date: date) -> Optional[date]:
    row = con.execute(f"SELECT MAX(tradedate) FROM {STOCKS_TABLE} WHERE tradedate <= ?", [target_date]).fetchone()
    if not row or row[0] is None:
        return None
    return pd.to_datetime(row[0]).date()


ASTOCK_VERSION = "fixed-v3-pro-dashboard"


def self_check(db_path: str, target_date: date) -> None:
    try:
        with duckdb.connect(db_path) as con:
            ensure_core_tables(con)
            latest = _latest_trade_date_in_db(con, target_date)
            shape = con.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT symbol),
                       SUM(CASE WHEN volume IS NOT NULL AND volume > 0 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN adjclose IS NOT NULL AND adjclose > 0 THEN 1 ELSE 0 END),
                       AVG(close), AVG(volume)
                FROM {STOCKS_TABLE}
            """).fetchone()
            log.info(
                f"🩺 自检: 最新交易日={latest} 行={shape[0]} 股票={shape[1]} "
                f"volume非空={shape[2]} adjclose非空={shape[3]} "
                f"均价={(float(shape[4]) if shape[4] else 0):.2f} 均量={(float(shape[5]) if shape[5] else 0):.0f}"
            )
            if latest is None:
                log.error("❌ 自检: stock_prices 无数据，候选必为0")
                return
            rebuild_recent_adjusted_cache(db_path, latest, CONFIG["adjust_cache_days"])
            picks = compute_all_signals(con, latest)
            log.info(f"🩺 自检: 最新交易日 {latest} 候选数={len(picks)}")
    except Exception as exc:
        log.error(f"❌ 自检异常: {exc}")


def run_strategy_with_replay_if_needed(db_path: str, target_date: date):
    with duckdb.connect(db_path) as con:
        drop_cache_tables(con)
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        latest_trade_date = _latest_trade_date_in_db(con, target_date)
        if latest_trade_date is None:
            return None, (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {})
        has_history = con.execute("SELECT COUNT(*) FROM account_history").fetchone()[0] > 0

    if has_history:
        log.info("⚡ 日常模式：仅计算最新交易日策略")
        rebuild_recent_adjusted_cache(db_path, latest_trade_date, CONFIG["adjust_cache_days"])
        return latest_trade_date, evaluate_strategy(db_path, latest_trade_date, CONFIG["top_n"], allow_exit_on_date=True)

    with duckdb.connect(db_path) as con:
        replay_dates = get_recent_trade_dates(con, latest_trade_date, int(CONFIG["initial_replay_trade_days"]))
    if not replay_dates:
        return latest_trade_date, (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {})
    log.info(f"🧱 首次模式：回放最近 {len(replay_dates)} 个交易日策略")
    latest_result = None
    latest_day = replay_dates[-1]
    for idx, d in enumerate(tqdm(replay_dates, desc="⏳ 策略回测进度", unit="天")):
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
    print(f"[RUN] A股策略任务启动 target_date={target_date} version={ASTOCK_VERSION}", flush=True)
    log.info(f"🚀 astock {ASTOCK_VERSION} 启动")
    with tempfile.TemporaryDirectory() as tmp:
        db_path  = os.path.join(tmp, "CN_stock.duckdb")
        gz_path  = os.path.join(tmp, _DB_GZ_NAME)

        db_source, db_gz_ready = obtain_db_gz(LOCAL_DB_GZ_PATH, odc, gz_path)
        if db_gz_ready:
            load_db_gz_to_local(gz_path, db_path)
            log.info("✅ 已载入历史数据库（gz 解压完成）")
        else:
            log.info("ℹ️ 本地/OneDrive 均无历史数据库，准备全量初始化")
            initialize_empty_database(db_path)

        with duckdb.connect(db_path) as con:
            _migrate_db_schema(con)
            ensure_core_tables(con)
            ensure_strategy_tables(con)
            latest_before = _latest_trade_date_in_db(con, target_date)
            log.info(f"ℹ️ 当前库内已有最新交易日: {latest_before}")

        _trade_days = int(CONFIG["update_window_trade_days"])
        log.info("📦 开始同步与校验行情数据完整性 …")

        # ── 步骤 1：若无数据库，拉取 DoltHub 1.24G 全量数据（带 1.15G 底线完整校验与 100MB 汇报）──
        if not db_gz_ready:
            log.info("📚 未发现数据库：使用 DoltHub ts_a_stock_eod_price 拉取全量历史数据 …")
            try:
                dolthub_stream_to_db(db_path)
            except Exception as e:
                log.warning(f"⚠️ DoltHub 拉取异常: {e}")

        # ── 步骤 2：二次核实库中实际最新交易日 ──
        with duckdb.connect(db_path) as con:
            latest_in_db = _latest_trade_date_in_db(con, target_date)
            log.info(f"ℹ️ 核验库内最新交易日: {latest_in_db}")

        # ── 步骤 3：根据断层状态，智能通过 Qlib 补平至 2026 当下 ──
        synced = False
        try:
            if latest_in_db is None:
                log.info("📦 库内无数据，使用 Qlib 进行全历史初始化 …")
                synced, _ = investment_data_sync_full_history(db_path, target_date)
            elif (target_date - latest_in_db).days > 30:
                # 若存在截断或跨年断层，自动使用 Qlib 逐年增量拼接补齐
                gap_start = latest_in_db + timedelta(days=1)
                log.info(f"🚀 检测到数据断层 ({latest_in_db} ➔ {target_date})，启动 Qlib 增量追齐 …")
                synced, _ = investment_data_sync_gap(db_path, gap_start, target_date)
            else:
                # 仅差几天，日常窗口增量同步即可
                log.info(f"🔄 库内数据健康，增量同步最近 {_trade_days} 个交易日 …")
                synced, _ = investment_data_sync_recent_window(db_path, target_date, _trade_days)
        except Exception as _qlib_exc:
            log.warning(f"⚠️ Qlib 增量同步异常: {_qlib_exc}")
            synced = False

        if not synced and latest_in_db is None:
            log.error("❌ 行情数据同步失败且库内无可用数据，终止流程")
            return None, None

        self_check(db_path, target_date)

        latest_day, result = run_strategy_with_replay_if_needed(db_path, target_date)
        if latest_day is None:
            log.error("❌ 数据库无行情数据")
            return None, None
        df_picks, df_portfolio, df_pending, df_trades, metrics = result

        target_str = latest_day.strftime("%Y-%m-%d")
        print(f"RESULT {target_str} | 候选:{len(df_picks)} 持仓:{len(df_portfolio)} 挂单:{len(df_pending)} 成交:{len(df_trades)} 总资产:{metrics.get('total_assets', 0):.2f}", flush=True)

        try:
            generate_and_send_report(tm, df_picks, df_portfolio, df_pending, df_trades, target_str, metrics)
        except Exception as _mail_exc:
            log.error(f"❌ 报告邮件发送失败: {_mail_exc}")

        with duckdb.connect(db_path) as con:
            compact_database(con)

        try:
            db_compress_and_upload(odc, db_path, gz_path)
        except Exception as _up_exc:
            log.error(f"❌ 数据库上传失败: {_up_exc}")

        final_size = _file_size_mb(db_path)
        print(f"[RUN] 最终数据库大小={final_size:.1f} MB", flush=True)
        log.info(f"🎉 今日流程完成 latest_trade_date={latest_day}")
        return latest_day, result


def main():
    try:
        run_daily_pipeline()
    except Exception as exc:
        log.exception("❌ 主流程异常: %s", exc)
        raise


if __name__ == "__main__":
    main()
