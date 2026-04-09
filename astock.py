#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1. 原始日线 daily_raw 与除权除息事件 xdxr_events 分表保存，保证事实数据可追溯。
2. 价格与金额使用整数缩放后存储，减少浮点误差：
   - 价格: 元 -> 毫元（*1000）
   - 金额: 元 -> 分（*100）
3. 每天只尝试写入目标交易日的数据；若通达信最后一条记录不是当天，则跳过。
4. 每天只增量更新：
   - daily_raw 当天原始行情
   - xdxr_events 当天除权除息事件
5. 依据 daily_raw + xdxr_events 重建最近窗口的 qfq / hfq 缓存表，兼顾严格性和速度。
6. 选股信号使用前复权数据；收益率和止盈止损收益判断使用后复权数据。
7. 动量因子 + 量能 + 布林带选股。
8. 买入规则：T日收盘出信号，T+1 最低价 <= T日收盘价*0.985 才成交。
9. 卖出规则：跌破MA20 / 持有N天 / 止盈 / 止损。
10. OneDrive 没有数据库时执行全量初始化；初始化过程带详细进度日志，并支持断点续传。
11. 日常已有数据库时再走增量更新。
12. 最终数据库使用 gzip 压缩上传，降低网络传输成本。
"""

import os
import sys
import time
import json
import gzip
import base64
import shutil
import random
import struct
import zipfile
import logging
import tempfile
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import baostock as bs
from pytdx.hq import TdxHq_API
from pytdx.util.best_ip import select_best_ip

try:
    import talib
except Exception:
    talib = None

load_dotenv()

# =========================================================
# 全局配置
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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
TDX_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"

# 数值缩放因子
PRICE_SCALE = 1000   # 价格：元 -> 毫元
AMOUNT_SCALE = 100   # 金额：元 -> 分

MARKET_MAP = {
    "sh": {"prefix": "sh", "market_id": 1, "subdir": "sh"},
    "sz": {"prefix": "sz", "market_id": 0, "subdir": "sz"},
}

STOCK_PREFIX = {
    "sh": ("600", "601", "603", "605", "688"),
    "sz": ("000", "001", "002", "003", "300", "301"),
}

CUSTOM_TDX_IPS = [
    ("124.71.187.122", 7709), ("122.51.120.217", 7709), ("111.229.247.189", 7709),
    ("124.70.176.52", 7709), ("123.60.186.45", 7709), ("122.51.232.182", 7709),
    ("118.25.98.114", 7709), ("124.70.199.56", 7709), ("121.36.225.169", 7709),
    ("123.60.70.228", 7709), ("123.60.73.44", 7709), ("124.70.133.119", 7709),
    ("124.71.187.72", 7709), ("123.60.84.66", 7709), ("124.71.85.110", 7709),
    ("139.9.51.18", 7709), ("139.159.239.163", 7709), ("124.71.9.153", 7709),
    ("116.205.163.254", 7709), ("116.205.171.132", 7709), ("116.205.183.150", 7709),
    ("111.230.186.52", 7709), ("110.41.4.4", 7709), ("110.41.2.72", 7709),
    ("110.41.154.219", 7709), ("110.41.147.114", 7709), ("119.97.185.59", 7709),
    ("8.129.13.54", 7709), ("120.24.149.49", 7709), ("47.113.94.204", 7709),
    ("139.9.81.150", 7709), ("139.159.226.137", 7709), ("116.205.178.103", 7709),
]


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


CONFIG = {
    # 仅以下 3 个参数来自普通环境变量
    "azure_client_id": _env("AZURE_CLIENT_ID"),
    "token_cache_file": os.path.join(BASE_DIR, _env("TOKEN_CACHE_FILE", "ms_token.json")),
    "email_to": _env("EMAIL_TO"),

    # 其余参数固定在程序内部，避免运行环境过多配置项
    "onedrive_folder": "Stock",
    "db_filename": "CN_stock.duckdb",
    "bootstrap_days": 120,
    "tdx_verify_ssl": False,
    "position_cash_yuan": 10000.0,
    "take_profit_pct": 10.0,
    "stop_loss_pct": -5.0,
    "max_hold_days": 5,
    "top_n": 30,
    "adjust_cache_days": 260,
}
CONFIG["position_cash_cent"] = int(round(CONFIG["position_cash_yuan"] * 100))


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


def yuan_to_price_int(v: float) -> int:
    return int(round(max(float(v), 0.001) * PRICE_SCALE))


def price_int_to_yuan(v: int) -> float:
    return round(float(v) / PRICE_SCALE, 3)


def yuan_to_amount_int(v: float) -> int:
    return int(round(float(v) * AMOUNT_SCALE))


def amount_int_to_yuan(v: int) -> float:
    return round(float(v) / AMOUNT_SCALE, 2)


def safe_price_int(v: float) -> int:
    return yuan_to_price_int(max(float(v), 0.001))


def sanitize_adjusted_price(v: float) -> float:
    """
    复权计算在极端分红/配股序列下，历史价格可能被推到 0 以下。
    这里统一夹到最小正值，避免数据库里出现负数价格。
    """
    return max(float(v), 0.001)


def is_trade_candidate_date(last_row_date: date, target_date: date) -> bool:
    return last_row_date == target_date


def decode_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in ["open", "high", "low", "close", "buy_price", "last_price", "planned_buy_price"]:
        if col in out.columns:
            out[col] = out[col].astype(float) / PRICE_SCALE
    if "amount" in out.columns:
        out["amount"] = out["amount"].astype(float) / AMOUNT_SCALE
    if "cost" in out.columns:
        out["cost"] = out["cost"].astype(float) / AMOUNT_SCALE
    if "market_value" in out.columns:
        out["market_value"] = out["market_value"].astype(float) / AMOUNT_SCALE
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

# 从 CONFIG 派生，不修改 CONFIG
_DB_GZ_NAME = CONFIG["db_filename"] + ".gz"   # 云端文件名：CN_stock.duckdb.gz


def db_compress_and_upload(odc: OneDriveClient, db_path: str, gz_path: str) -> None:
    """将 db_path 压缩为 gz_path，然后上传到 OneDrive。"""
    with open(db_path, "rb") as fi, gzip.open(gz_path, "wb", compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    size_mb = os.path.getsize(gz_path) / 1024 / 1024
    log.info(f"📦 压缩完成 {size_mb:.1f} MB → 开始上传 ...")
    odc.upload_database_gz(gz_path)
    log.info("☁️  数据库已上传")


def db_decompress_from_download(gz_path: str, db_path: str) -> None:
    """将下载好的 gz_path 解压到 db_path（原始 duckdb 文件）。"""
    with gzip.open(gz_path, "rb") as fi, open(db_path, "wb") as fo:
        shutil.copyfileobj(fi, fo)


# =========================================================
# 数据库表结构
# =========================================================
def ensure_core_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_raw (
            symbol VARCHAR,
            date DATE,
            open BIGINT,
            high BIGINT,
            low BIGINT,
            close BIGINT,
            volume BIGINT,
            amount BIGINT,
            PRIMARY KEY (symbol, date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS xdxr_events (
            symbol VARCHAR,
            date DATE,
            category INTEGER,
            fenhong BIGINT,
            songzhuangu BIGINT,
            peigu BIGINT,
            peigujia BIGINT,
            PRIMARY KEY (symbol, date, category)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_qfq_cache (
            symbol VARCHAR,
            date DATE,
            open BIGINT,
            high BIGINT,
            low BIGINT,
            close BIGINT,
            volume BIGINT,
            amount BIGINT,
            PRIMARY KEY (symbol, date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_hfq_cache (
            symbol VARCHAR,
            date DATE,
            open BIGINT,
            high BIGINT,
            low BIGINT,
            close BIGINT,
            volume BIGINT,
            amount BIGINT,
            PRIMARY KEY (symbol, date)
        )
    """)


def ensure_strategy_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS pending_orders (
            symbol VARCHAR,
            signal_date DATE,
            planned_buy_price BIGINT,
            signal_close BIGINT,
            status VARCHAR,
            PRIMARY KEY (symbol, signal_date)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bootstrap_state (
            symbol VARCHAR PRIMARY KEY,
            raw_done BOOLEAN,
            xdxr_done BOOLEAN,
            raw_rows BIGINT,
            xdxr_rows BIGINT,
            last_error VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS virtual_portfolio (
            symbol VARCHAR PRIMARY KEY,
            buy_date DATE,
            buy_price BIGINT,
            buy_price_hfq BIGINT,
            shares DOUBLE
        )
    """)
    try:
        con.execute("ALTER TABLE virtual_portfolio ADD COLUMN IF NOT EXISTS buy_price_hfq BIGINT")
    except Exception:
        pass
    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            symbol VARCHAR,
            trade_type VARCHAR,
            signal_date DATE,
            trade_date DATE,
            price BIGINT,
            shares DOUBLE,
            reason VARCHAR,
            pnl_pct DOUBLE
        )
    """)




def get_bootstrap_stats(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            COUNT(*) AS total_symbols,
            SUM(CASE WHEN raw_done THEN 1 ELSE 0 END) AS raw_done_symbols,
            SUM(CASE WHEN xdxr_done THEN 1 ELSE 0 END) AS xdxr_done_symbols,
            SUM(COALESCE(raw_rows, 0)) AS total_raw_rows,
            SUM(COALESCE(xdxr_rows, 0)) AS total_xdxr_rows
        FROM bootstrap_state
    """).df()


def upsert_bootstrap_state(con, symbol: str, raw_done: bool, xdxr_done: bool, raw_rows: int, xdxr_rows: int, last_error: str = ""):
    now_ts = datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')
    con.execute("""
        INSERT OR REPLACE INTO bootstrap_state(symbol, raw_done, xdxr_done, raw_rows, xdxr_rows, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [symbol, raw_done, xdxr_done, raw_rows, xdxr_rows, last_error[:500], now_ts])


def pending_bootstrap_symbols(con, all_symbols: List[str]) -> set:
    done = con.execute("""
        SELECT symbol FROM bootstrap_state
        WHERE raw_done = TRUE AND xdxr_done = TRUE
    """).df()
    done_set = set(done['symbol']) if not done.empty else set()
    return set(all_symbols) - done_set


def log_bootstrap_progress(con, processed: int, total: int, recent_symbol: str):
    stats = get_bootstrap_stats(con)
    if stats.empty:
        return
    s = stats.iloc[0]
    log.info(
        "📊 全量初始化进度 | 已扫描 %s/%s | raw完成 %s | xdxr完成 %s | raw行数 %s | xdxr行数 %s | 最近 %s",
        processed,
        total,
        int(s['raw_done_symbols'] or 0),
        int(s['xdxr_done_symbols'] or 0),
        int(s['total_raw_rows'] or 0),
        int(s['total_xdxr_rows'] or 0),
        recent_symbol,
    )

def initialize_empty_database(db_path: str):
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        con.execute("CHECKPOINT")


# =========================================================
# 通达信下载与解析
# =========================================================
def download_tdx_zip(dest_zip: str) -> bool:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.tdx.com.cn/",
        "Connection": "keep-alive",
    }
    session = build_retry_session()
    for attempt in range(1, 4):
        try:
            log.info(f"⬇️ 第 {attempt} 次尝试下载通达信数据")
            resp = session.get(TDX_URL, headers=headers, stream=True, timeout=(15, 120), verify=CONFIG["tdx_verify_ssl"])
            resp.raise_for_status()
            total_size = int(resp.headers.get("content-length", 0))
            with open(dest_zip, "wb") as f:
                with tqdm(total=total_size, unit="B", unit_scale=True, desc="下载通达信数据") as pbar:
                    for chunk in resp.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            if os.path.exists(dest_zip) and os.path.getsize(dest_zip) > 1024:
                return True
        except Exception as e:
            log.warning(f"⚠️ 下载失败：{e}")
            time.sleep(2 * attempt)
    return False


def connect_best_tdx(api: TdxHq_API) -> bool:
    try:
        best = select_best_ip()
        if best and api.connect(best["ip"], best["port"]):
            log.info(f"✅ 默认最优 IP 连接成功: {best['ip']}:{best['port']}")
            return True
    except Exception as e:
        log.warning(f"⚠️ 默认最优 IP 失败: {e}")

    random.shuffle(CUSTOM_TDX_IPS)
    for ip, port in CUSTOM_TDX_IPS:
        try:
            if api.connect(ip, port):
                cnt = api.get_security_count(0)
                if cnt and cnt > 0:
                    log.info(f"✅ 备用节点连接成功: {ip}:{port}")
                    return True
                api.disconnect()
        except Exception:
            try:
                api.disconnect()
            except Exception:
                pass
    return False


def list_all_stocks(tdx_dir: str) -> List[Dict]:
    stocks = []
    for mkt, cfg in MARKET_MAP.items():
        lday_dir = os.path.join(tdx_dir, cfg["subdir"], "lday")
        if not os.path.isdir(lday_dir):
            continue
        for fname in os.listdir(lday_dir):
            if not fname.endswith(".day"):
                continue
            code = fname[2:8]
            if not code.startswith(STOCK_PREFIX[mkt]):
                continue
            stocks.append({
                "code": code,
                "market_id": cfg["market_id"],
                "filepath": os.path.join(lday_dir, fname),
                "symbol": f"{code}.{mkt.upper()}",
            })
    return stocks


def read_tdx_day_last_row(filepath: str) -> pd.DataFrame:
    file_size = os.path.getsize(filepath)
    if file_size < 32:
        return pd.DataFrame()
    with open(filepath, "rb") as f:
        f.seek(-32, os.SEEK_END)
        chunk = f.read(32)
    if len(chunk) < 32:
        return pd.DataFrame()
    date_i, o, h, l, c, amount, vol, _ = struct.unpack("<IIIIIfII", chunk)
    return pd.DataFrame([{
        "date": pd.to_datetime(str(date_i), format="%Y%m%d").date(),
        "open": o / 100.0,
        "high": h / 100.0,
        "low": l / 100.0,
        "close": c / 100.0,
        "volume": int(vol),
        "amount": float(amount),
    }])




def read_tdx_day_all(filepath: str) -> pd.DataFrame:
    """
    读取单个 .day 文件的全部日线记录。
    通达信 .day 单条记录固定 32 字节。
    """
    if not os.path.exists(filepath) or os.path.getsize(filepath) < 32:
        return pd.DataFrame()
    arr = np.fromfile(
        filepath,
        dtype=np.dtype([
            ('date', '<u4'),
            ('open', '<u4'),
            ('high', '<u4'),
            ('low', '<u4'),
            ('close', '<u4'),
            ('amount', '<f4'),
            ('volume', '<u4'),
            ('reserved', '<u4'),
        ])
    )
    if arr.size == 0:
        return pd.DataFrame()
    df = pd.DataFrame({
        'date': pd.to_datetime(arr['date'].astype(str), format='%Y%m%d').date,
        'open': arr['open'] / 100.0,
        'high': arr['high'] / 100.0,
        'low': arr['low'] / 100.0,
        'close': arr['close'] / 100.0,
        'volume': arr['volume'].astype('int64'),
        'amount': arr['amount'].astype(float),
    })
    return df


def flush_bootstrap_batch(con, raw_batches, xdxr_batches):
    if raw_batches:
        df_raw = pd.concat(raw_batches, ignore_index=True)
        con.register('tmp_boot_raw', df_raw)
        con.execute("""
            INSERT OR REPLACE INTO daily_raw
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM tmp_boot_raw
        """)
        raw_batches.clear()
    if xdxr_batches:
        df_x = pd.concat(xdxr_batches, ignore_index=True)
        con.register('tmp_boot_xdxr', df_x)
        con.execute("""
            INSERT OR REPLACE INTO xdxr_events
            SELECT symbol, date, category, fenhong, songzhuangu, peigu, peigujia
            FROM tmp_boot_xdxr
        """)
        xdxr_batches.clear()


def bootstrap_full_from_tdx(stocks: List[Dict], db_path: str) -> bool:
    api = TdxHq_API(raise_exception=False, auto_retry=True)
    if not connect_best_tdx(api):
        log.error("PyTDX 节点不可用，无法执行全量初始化。")
        return False

    all_symbols = [s["symbol"] for s in stocks]
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)
        remain = pending_bootstrap_symbols(con, all_symbols)
        if not remain:
            stats = get_bootstrap_stats(con)
            _rows = None if stats.empty else stats.iloc[0]["total_raw_rows"]
            if not stats.empty and not pd.isna(_rows) and int(_rows) > 0:
                log.info("✅ 全量初始化已完成，跳过")
                return True
            remain = set(all_symbols)

        todo = [s for s in stocks if s["symbol"] in remain]
        total = len(todo)
        log.info(f"🚀 全量初始化待处理: {total} / 总数 {len(stocks)}")

        raw_batches, xdxr_batches = [], []
        loaded_raw = loaded_x = 0
        try:
            for idx, stock in enumerate(tqdm(todo, desc="全量初始化 raw / xdxr", unit="只"), 1):
                raw_done = xdxr_done = False
                raw_count = xdxr_count = 0
                last_error = ""
                try:
                    hist = read_tdx_day_all(stock["filepath"])
                    if not hist.empty:
                        hist["symbol"] = stock["symbol"]
                        hist["date"]   = pd.to_datetime(hist["date"]).dt.date
                        hist["open"]   = hist["open"].apply(yuan_to_price_int)
                        hist["high"]   = hist["high"].apply(yuan_to_price_int)
                        hist["low"]    = hist["low"].apply(yuan_to_price_int)
                        hist["close"]  = hist["close"].apply(yuan_to_price_int)
                        hist["amount"] = hist["amount"].apply(yuan_to_amount_int)
                        hist = hist[["symbol","date","open","high","low","close","volume","amount"]]
                        raw_batches.append(hist)
                        raw_count = len(hist)
                        loaded_raw += raw_count
                    raw_done = True

                    xdf = get_xdxr(api, stock["code"], stock["market_id"])
                    if not xdf.empty:
                        xenc = encode_xdxr_df(stock["symbol"], xdf)
                        if not xenc.empty:
                            xdxr_batches.append(xenc)
                            xdxr_count = len(xenc)
                            loaded_x += xdxr_count
                    xdxr_done = True
                except Exception as e:
                    last_error = str(e)
                    log.debug(f"全量初始化失败 {stock['symbol']}: {e}")
                finally:
                    upsert_bootstrap_state(con, stock["symbol"], raw_done, xdxr_done,
                                           raw_count, xdxr_count, last_error)

                if idx % 100 == 0:
                    flush_bootstrap_batch(con, raw_batches, xdxr_batches)
                    con.execute("CHECKPOINT")
                    log_bootstrap_progress(con, idx, total, stock["symbol"])

            flush_bootstrap_batch(con, raw_batches, xdxr_batches)
            con.execute("CHECKPOINT")
            log_bootstrap_progress(con, total, total, "FINAL")
        finally:
            api.disconnect()

    log.info(f"✅ 全量初始化完成: raw={loaded_raw} 行, xdxr={loaded_x} 行")
    return True


def get_recent_trade_dates(con, end_date: date, n: int) -> List[date]:
    df = con.execute("""
        SELECT DISTINCT date
        FROM daily_raw
        WHERE date <= ?
        ORDER BY date DESC
        LIMIT ?
    """, [end_date, n]).df()
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())

# =========================================================
# 复权计算（前复权 QFQ / 后复权 HFQ）
# =========================================================

def _calc_xdxr_factor(prev_close_int: int, ev: pd.Series) -> float:
    """
    根据单条 xdxr_events 记录计算除权除息系数。
    返回值 factor < 1 表示价格缩减（例如分红后除权）。
    """
    prev_close_yuan = prev_close_int / PRICE_SCALE
    if prev_close_yuan <= 0:
        return 1.0

    # xdxr_events 的存储格式：
    #   fenhong   -> AMOUNT_SCALE (元/分, 每10股)
    #   songzhuangu, peigu, peigujia -> PRICE_SCALE (毫元)
    fenhong_yuan      = float(ev.get("fenhong")    or 0) / AMOUNT_SCALE       # 元/10股
    songzhuangu_ratio = float(ev.get("songzhuangu") or 0) / PRICE_SCALE / 10.0 # 每股送转比
    peigu_ratio       = float(ev.get("peigu")       or 0) / PRICE_SCALE / 10.0 # 每股配股比
    peigujia_yuan     = float(ev.get("peigujia")    or 0) / PRICE_SCALE        # 配股价(元)

    # 标准除权价公式：
    #   price_after = (P - D + R * Rp) / (1 + S + R)
    #   factor = price_after / P
    numerator   = prev_close_yuan - fenhong_yuan + peigu_ratio * peigujia_yuan
    denominator = prev_close_yuan * (1.0 + songzhuangu_ratio + peigu_ratio)

    if denominator <= 0 or numerator <= 0:
        return 1.0
    return numerator / denominator


def calc_qfq_from_events(g: pd.DataFrame, xdf: pd.DataFrame) -> pd.DataFrame:
    """
    前复权 (QFQ)：以窗口最末日价格为锚，向前调整历史价格，令除权日前后价格连续。
    g   : 单只股票原始日线（整数存储），已含 symbol 列，按 date 排序。
    xdf : 该股的 xdxr_events（可为空 DataFrame）。
    """
    result = g.copy().sort_values("date").reset_index(drop=True)
    price_cols = ["open", "high", "low", "close"]

    if xdf is None or xdf.empty:
        return result

    dates = result["date"].to_numpy()
    factors = np.ones(len(result), dtype=float)

    # 从最新事件往最旧处理：每个事件的系数累乘到其之前的所有行
    for _, ev in xdf.sort_values("date", ascending=False).iterrows():
        ev_date = ev["date"]
        mask_before = dates < ev_date
        if not mask_before.any():
            continue

        prev_close_int = int(result.loc[mask_before, "close"].iloc[-1])
        factor = _calc_xdxr_factor(prev_close_int, ev)
        if factor == 1.0:
            continue
        factors[mask_before] *= factor

    for col in price_cols:
        adjusted = result[col].astype(float) * factors
        result[col] = adjusted.apply(lambda v: safe_price_int(v / PRICE_SCALE))

    return result


def calc_hfq_from_events(g: pd.DataFrame, xdf: pd.DataFrame) -> pd.DataFrame:
    """
    后复权 (HFQ)：以最早日期价格为锚，历史行不动，每个除权日起往后价格乘以逆系数放大。
    g   : 单只股票原始日线（整数存储），已含 symbol 列，按 date 排序。
    xdf : 该股的 xdxr_events（可为空 DataFrame）。
    """
    result = g.copy().sort_values("date").reset_index(drop=True)
    price_cols = ["open", "high", "low", "close"]

    if xdf is None or xdf.empty:
        return result

    dates = result["date"].to_numpy()
    factors = np.ones(len(result), dtype=float)

    # 从最旧事件往最新处理：每个事件的逆系数累乘到其之后（含当日）的所有行
    for _, ev in xdf.sort_values("date", ascending=True).iterrows():
        ev_date = ev["date"]
        mask_before = dates < ev_date
        if not mask_before.any():
            continue

        prev_close_int = int(result.loc[mask_before, "close"].iloc[-1])
        factor = _calc_xdxr_factor(prev_close_int, ev)
        if factor == 1.0:
            continue
        # 后复权：从除权日起价格 ÷ factor（即 × 1/factor，因 factor<1 故后期价格放大）
        mask_from = ~mask_before  # date >= ev_date
        factors[mask_from] /= factor

    for col in price_cols:
        adjusted = result[col].astype(float) * factors
        result[col] = adjusted.apply(lambda v: safe_price_int(v / PRICE_SCALE))

    return result

def rebuild_recent_adjusted_cache(db_path: str, end_date: date, window_days: int) -> bool:
    with duckdb.connect(db_path) as con:
        ensure_core_tables(con)

        recent_dates = get_recent_trade_dates(con, end_date, window_days)
        if not recent_dates:
            log.warning("没有足够的 daily_raw 数据，无法构建复权缓存。")
            return False

        start_date = recent_dates[0]

        raw_df = con.execute("""
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM daily_raw
            WHERE date BETWEEN ? AND ?
            ORDER BY symbol, date
        """, [start_date, end_date]).df()
        if raw_df.empty:
            return False
        raw_df["date"] = pd.to_datetime(raw_df["date"]).dt.date

        x_qfq = con.execute("""
            SELECT symbol, date, category, fenhong, songzhuangu, peigu, peigujia
            FROM xdxr_events
            WHERE date BETWEEN ? AND ?
            ORDER BY symbol, date
        """, [start_date, end_date]).df()
        if not x_qfq.empty:
            x_qfq["date"] = pd.to_datetime(x_qfq["date"]).dt.date

        x_hfq = con.execute("""
            SELECT symbol, date, category, fenhong, songzhuangu, peigu, peigujia
            FROM xdxr_events
            WHERE date <= ?
            ORDER BY symbol, date
        """, [end_date]).df()
        if not x_hfq.empty:
            x_hfq["date"] = pd.to_datetime(x_hfq["date"]).dt.date

        q_parts, h_parts = [], []
        for sym, g in raw_df.groupby("symbol"):
            qx = x_qfq[x_qfq["symbol"] == sym] if not x_qfq.empty else pd.DataFrame()
            hx = x_hfq[x_hfq["symbol"] == sym] if not x_hfq.empty else pd.DataFrame()
            q_parts.append(calc_qfq_from_events(g.copy(), qx))
            h_parts.append(calc_hfq_from_events(g.copy(), hx))

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

        if h_parts:
            df_h = pd.concat(h_parts, ignore_index=True)
            con.register("tmp_h_cache", df_h)
            con.execute("""
                INSERT INTO daily_hfq_cache
                SELECT symbol, date, open, high, low, close, volume, amount
                FROM tmp_h_cache
            """)

        con.execute("CHECKPOINT")

    log.info(f"✅ 已重建最近 {window_days} 个交易日复权缓存")
    return True


# =========================================================
# 策略逻辑
# =========================================================
def detect_kline_patterns(open_s, high_s, low_s, close_s) -> str:
    if talib is None:
        return "未知"
    try:
        result = talib.CDLENGULFING(
            open_s.values.astype(float),
            high_s.values.astype(float),
            low_s.values.astype(float),
            close_s.values.astype(float),
        )
        if len(result) == 0:
            return "无明显形态"
        if result[-1] > 0:
            return "看涨吞没"
        if result[-1] < 0:
            return "看跌吞没"
        return "无明显形态"
    except Exception:
        return "未知"


def process_pending_orders(con, trade_date: date) -> Tuple[List[Tuple], List[Tuple]]:
    pending_df = con.execute("""
        SELECT symbol, signal_date, planned_buy_price, signal_close, status
        FROM pending_orders
        WHERE status='pending' AND signal_date < ?
    """, [trade_date]).df()
    if pending_df.empty:
        return [], []

    qfq_today = con.execute("""
        SELECT symbol, date, low, close
        FROM daily_qfq_cache
        WHERE date = ?
    """, [trade_date]).df()
    hfq_today = con.execute("""
        SELECT symbol, date, close
        FROM daily_hfq_cache
        WHERE date = ?
    """, [trade_date]).df()
    if qfq_today.empty:
        return [], []

    q_map = {row['symbol']: row for _, row in qfq_today.iterrows()}
    h_map = {row['symbol']: row for _, row in hfq_today.iterrows()}

    filled_rows, expired_rows = [], []
    for _, row in pending_df.iterrows():
        symbol = row['symbol']
        signal_date = row['signal_date']
        planned_buy_price = int(row['planned_buy_price'])
        if symbol not in q_map:
            expired_rows.append((symbol, signal_date))
            continue

        today_low_qfq = int(q_map[symbol]['low'])
        today_close_qfq = int(q_map[symbol]['close'])
        today_close_hfq = int(h_map[symbol]['close']) if symbol in h_map else today_close_qfq

        if today_low_qfq <= planned_buy_price:
            factor = (today_close_hfq / today_close_qfq) if today_close_qfq > 0 else 1.0
            buy_price_hfq = safe_price_int((planned_buy_price / PRICE_SCALE) * factor)
            shares = round((CONFIG['position_cash_yuan'] * PRICE_SCALE) / planned_buy_price, 2)
            filled_rows.append((symbol, trade_date, planned_buy_price, buy_price_hfq, shares))
        else:
            expired_rows.append((symbol, signal_date))

    if filled_rows:
        con.executemany("""
            INSERT OR REPLACE INTO virtual_portfolio(symbol, buy_date, buy_price, buy_price_hfq, shares)
            VALUES (?, ?, ?, ?, ?)
        """, filled_rows)
        for symbol, buy_date, buy_price, buy_price_hfq, shares in filled_rows:
            con.execute("UPDATE pending_orders SET status='filled' WHERE symbol=? AND status='pending'", [symbol])
            con.execute("""
                INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct)
                VALUES (?, 'buy', NULL, ?, ?, ?, 'T+1 回落1.5%成交', NULL)
            """, [symbol, buy_date, buy_price, shares])

    if expired_rows:
        con.executemany("UPDATE pending_orders SET status='expired' WHERE symbol=? AND signal_date=? AND status='pending'", expired_rows)

    return filled_rows, expired_rows


def process_exit_rules(con, trade_date: date) -> List[Tuple]:
    holdings = con.execute("SELECT symbol, buy_date, buy_price, buy_price_hfq, shares FROM virtual_portfolio").df()
    if holdings.empty:
        return []

    symbols = holdings['symbol'].tolist()
    placeholders = ','.join(['?'] * len(symbols))
    start_date = (trade_date - timedelta(days=80)).strftime('%Y-%m-%d')

    qfq_df = con.execute(f"""
        SELECT symbol, date, close
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
    if qfq_df.empty:
        return []

    qfq_df['date'] = pd.to_datetime(qfq_df['date'])
    if not hfq_df.empty:
        hfq_df['date'] = pd.to_datetime(hfq_df['date'])

    sold_rows = []
    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = pd.to_datetime(row['buy_date']).date()
        buy_price = int(row['buy_price'])
        buy_price_hfq = int(row['buy_price_hfq']) if not pd.isna(row['buy_price_hfq']) else buy_price
        shares = float(row['shares'])

        gq = qfq_df[qfq_df['symbol'] == sym].copy()
        if gq.empty:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym].copy() if not hfq_df.empty else pd.DataFrame()

        gq['close_f'] = gq['close'].astype(float) / PRICE_SCALE
        gq['ma20_f'] = gq['close_f'].rolling(20).mean()
        last_q = gq.iloc[-1]
        last_close_qfq = int(last_q['close'])
        ma20_int = None if pd.isna(last_q['ma20_f']) else yuan_to_price_int(float(last_q['ma20_f']))

        if gh.empty:
            last_close_hfq = last_close_qfq
        else:
            last_close_hfq = int(gh.iloc[-1]['close'])

        hold_days = (trade_date - buy_date).days
        pnl_pct = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100 if buy_price_hfq > 0 else 0.0

        reasons = []
        if ma20_int is not None and last_close_qfq < ma20_int:
            reasons.append('跌破MA20')
        if hold_days >= CONFIG['max_hold_days']:
            reasons.append(f"持有{CONFIG['max_hold_days']}天")
        if pnl_pct >= CONFIG['take_profit_pct']:
            reasons.append(f"止盈{CONFIG['take_profit_pct']}%")
        if pnl_pct <= CONFIG['stop_loss_pct']:
            reasons.append(f"止损{CONFIG['stop_loss_pct']}%")

        if reasons:
            sold_rows.append((sym, trade_date, last_close_qfq, shares, ' / '.join(reasons), pnl_pct))

    for sym, sell_date, sell_price, shares, reason, pnl_pct in sold_rows:
        con.execute('DELETE FROM virtual_portfolio WHERE symbol=?', [sym])
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct)
            VALUES (?, 'sell', NULL, ?, ?, ?, ?, ?)
        """, [sym, sell_date, sell_price, shares, reason, pnl_pct])

    return sold_rows


def evaluate_strategy(db_path: str, target_date: date, top_n: Optional[int] = None):
    top_n = top_n or CONFIG["top_n"]
    with duckdb.connect(db_path, read_only=False) as con:
        ensure_core_tables(con)
        ensure_strategy_tables(con)

        process_pending_orders(con, target_date)
        process_exit_rules(con, target_date)

        start_date = (target_date - timedelta(days=420)).strftime("%Y-%m-%d")
        end_date = target_date.strftime("%Y-%m-%d")
        df = con.execute("""
            SELECT symbol, date, open, high, low, close, volume
            FROM daily_qfq_cache
            WHERE date BETWEEN ? AND ?
            ORDER BY symbol, date
        """, [start_date, end_date]).df()
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float) / PRICE_SCALE
        df["volume"] = df["volume"].astype(float)

        picks_rows = []
        for sym, g in df.groupby("symbol"):
            g = g.sort_values("date").reset_index(drop=True)
            if len(g) < 80:
                continue
            close = g["close"].astype(float)
            volume = g["volume"].astype(float)
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
            g["boll_mid"] = close.rolling(20).mean()
            g["boll_std"] = close.rolling(20).std()
            g["boll_up"] = g["boll_mid"] + 2 * g["boll_std"]
            g["boll_dn"] = g["boll_mid"] - 2 * g["boll_std"]
            g["band_width"] = (g["boll_up"] - g["boll_dn"]) / g["boll_mid"]

            last = g.iloc[-1]
            req = ["ret_20", "ret_60", "ma20", "ma60", "vol_ma5", "vol_ma20", "vol_ratio", "vol_up_count_20", "boll_mid", "boll_up", "boll_dn", "band_width"]
            if any(pd.isna(last[c]) for c in req):
                continue

            cond_momentum = last["ret_20"] > 0 and last["ret_60"] > 0 and last["close"] > last["ma20"] > last["ma60"]
            cond_volume = last["vol_ratio"] > 1.2 and last["vol_ma5"] > last["vol_ma20"] and last["vol_up_count_20"] >= 3
            cond_boll = last["close"] > last["boll_mid"] and last["close"] < last["boll_up"] * 1.03 and last["band_width"] > 0.08
            if not (cond_momentum and cond_volume and cond_boll):
                continue

            momentum_score = last["ret_20"] * 40 + last["ret_60"] * 30 + ((last["close"] / last["ma20"]) - 1) * 100 * 15 + ((last["ma20"] / last["ma60"]) - 1) * 100 * 15
            volume_score = min(last["vol_ratio"], 3.0) * 15 + min(last["vol_ma5"] / last["vol_ma20"], 2.0) * 10 + min(last["vol_up_count_20"], 10) * 2
            boll_position = (last["close"] - last["boll_mid"]) / (last["boll_up"] - last["boll_mid"] + 1e-9)
            boll_score = max(0, min(boll_position, 1.2)) * 20 + min(last["band_width"], 0.3) * 50
            total_score = momentum_score + volume_score + boll_score
            planned_buy_price = yuan_to_price_int(float(last["close"]) * 0.985)
            pattern = detect_kline_patterns(g["open"], g["high"], g["low"], g["close"])

            picks_rows.append({
                "symbol": sym,
                "date": last["date"].date(),
                "close": yuan_to_price_int(float(last["close"])),
                "planned_buy_price": planned_buy_price,
                "ret_20": float(last["ret_20"]),
                "ret_60": float(last["ret_60"]),
                "ma20": yuan_to_price_int(float(last["ma20"])),
                "ma60": yuan_to_price_int(float(last["ma60"])),
                "vol_ratio": float(last["vol_ratio"]),
                "vol_up_count_20": float(last["vol_up_count_20"]),
                "band_width": float(last["band_width"]),
                "pattern": pattern,
                "momentum_score": round(momentum_score, 3),
                "volume_score": round(volume_score, 3),
                "boll_score": round(boll_score, 3),
                "total_score": round(total_score, 3),
            })

        df_picks = pd.DataFrame(picks_rows)
        if not df_picks.empty:
            df_picks = df_picks.sort_values("total_score", ascending=False).head(top_n).reset_index(drop=True)
            existing_pending = con.execute("SELECT symbol FROM pending_orders WHERE status='pending'").df()
            pending_symbols = set(existing_pending["symbol"]) if not existing_pending.empty else set()
            holdings = con.execute("SELECT symbol FROM virtual_portfolio").df()
            holding_symbols = set(holdings["symbol"]) if not holdings.empty else set()
            new_orders = []
            for _, row in df_picks.iterrows():
                symbol = row["symbol"]
                if symbol in pending_symbols or symbol in holding_symbols:
                    continue
                new_orders.append((symbol, target_date, int(row["planned_buy_price"]), int(row["close"]), "pending"))
            if new_orders:
                con.executemany("""
                    INSERT OR REPLACE INTO pending_orders(symbol, signal_date, planned_buy_price, signal_close, status)
                    VALUES (?, ?, ?, ?, ?)
                """, new_orders)

        df_pending = con.execute("""
            SELECT symbol, signal_date, planned_buy_price, signal_close, status
            FROM pending_orders
            WHERE status='pending'
            ORDER BY signal_date DESC, symbol
        """).df()

        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
        else:
            qfq_today_df = con.execute("SELECT symbol, close FROM daily_qfq_cache WHERE date = ?", [target_date]).df()
            hfq_today_df = con.execute("SELECT symbol, close FROM daily_hfq_cache WHERE date = ?", [target_date]).df()
            df_portfolio = holdings.merge(qfq_today_df, on="symbol", how="left")
            df_portfolio.rename(columns={"close": "last_price"}, inplace=True)
            df_portfolio = df_portfolio.merge(hfq_today_df.rename(columns={"close": "last_price_hfq"}), on="symbol", how="left")
            df_portfolio["last_price"] = df_portfolio["last_price"].fillna(df_portfolio["buy_price"])
            df_portfolio["last_price_hfq"] = df_portfolio["last_price_hfq"].fillna(df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]))
            df_portfolio["market_value"] = (df_portfolio["last_price"] * df_portfolio["shares"]).round().astype("int64") * 1000 // PRICE_SCALE
            df_portfolio["cost"] = (df_portfolio["buy_price"] * df_portfolio["shares"]).round().astype("int64") * 1000 // PRICE_SCALE
            df_portfolio["pnl_pct"] = (df_portfolio["last_price_hfq"] - df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"])) / df_portfolio["buy_price_hfq"].fillna(df_portfolio["buy_price"]) * 100
            df_portfolio["holding_days"] = df_portfolio["buy_date"].apply(lambda x: (target_date - pd.to_datetime(x).date()).days)

        df_trades = con.execute("SELECT * FROM trade_history WHERE trade_date = ? ORDER BY trade_type, symbol", [target_date]).df()

    return decode_numeric_frame(df_picks), decode_numeric_frame(df_portfolio), decode_numeric_frame(df_pending), decode_numeric_frame(df_trades)


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
    resp = requests.post(
        f"{GRAPH_BASE}/me/sendMail",
        headers={**tm.headers(), "Content-Type": "application/json"},
        json=message,
        timeout=60,
    )
    resp.raise_for_status()
    log.info("📧 报告邮件发送成功")


# =========================================================
# 报告邮件（美化版）
# =========================================================

def _market_badge(symbol: str) -> str:
    code = symbol.split(".")[0]
    if symbol.endswith(".SH"):
        if code.startswith("688"):
            return f'<span class="badge badge-kcb">科创</span> {code}'
        return f'<span class="badge badge-sh">SH</span> {code}'
    return f'<span class="badge badge-sz">SZ</span> {code}'


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
        rows.append(f"""
        <tr>
          <td>{i}</td>
          <td>{_market_badge(r.symbol)}</td>
          <td>¥{r.close:.2f}</td>
          <td>¥{r.planned_buy_price:.3f}</td>
          <td>{_ret_cell(r.ret_20)}</td>
          <td>{_ret_cell(r.ret_60)}</td>
          <td>{r.vol_ratio:.2f}</td>
          <td>{int(r.vol_up_count_20)}</td>
          <td>{r.band_width:.3f}</td>
          <td>{_score_bar(r.total_score, max_score)}</td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>#</th><th>代码</th><th>收盘价</th><th>挂单价</th>
        <th>近20日涨幅</th><th>近60日涨幅</th><th>量比</th>
        <th>量增天数</th><th>布林宽度</th><th>综合得分</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>"""


def _pending_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return '<div class="empty-state"><div class="empty-icon">📭</div><div>暂无待成交挂单</div></div>'
    rows = []
    for r in df.itertuples():
        pbp = r.planned_buy_price # 挂单价
        sc  = r.signal_close / PRICE_SCALE
        disc = (pbp - sc) / sc * 100
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{r.signal_date}</td>
          <td>¥{pbp:.3f}</td>
          <td>¥{sc:.3f}</td>
          <td><span class="ret-neg">{disc:.2f}%</span></td>
          <td><span class="badge badge-pending">⏳ 待成交</span></td>
        </tr>""")
    return f"""
    <table class="data-table">
      <thead><tr>
        <th>代码</th><th>信号日期</th><th>挂单价</th>
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
        type_label = "🟢 买入" if r.trade_type == "buy" else "🔴 卖出"
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{type_label}</td>
          <td>{r.trade_date}</td>
          <td>¥{r.price / PRICE_SCALE:.3f}</td>
          <td>{r.shares:.2f}</td>
          <td>{r.reason or '—'}</td>
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
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{r.buy_date}</td>
          <td>¥{r.buy_price:.3f}</td>
          <td>¥{r.last_price:.3f}</td>
          <td>{r.shares:.2f}</td>
          <td>¥{r.market_value:.2f}</td>
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
):
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
.kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:20px; }
.kpi-card { background:#fff; border-radius:12px; padding:18px 16px;
            box-shadow:0 2px 8px rgba(0,0,0,.06); border-left:4px solid #3b82f6; }
.kpi-card.green { border-left-color:#10b981; }
.kpi-card.amber { border-left-color:#f59e0b; }
.kpi-card.red   { border-left-color:#ef4444; }
.kpi-icon  { font-size:22px; margin-bottom:8px; }
.kpi-value { font-size:26px; font-weight:700; color:#1e293b; line-height:1; }
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
.score-bar-wrap { display:flex; align-items:center; gap:8px; }
.score-bar  { height:6px; border-radius:3px; background:#e2e8f0; flex:1; min-width:60px; }
.score-fill { height:6px; border-radius:3px; background:linear-gradient(90deg,#3b82f6,#8b5cf6); }
.score-text { font-weight:600; color:#1e293b; min-width:48px; text-align:right; font-size:12px; }
.badge        { display:inline-block; padding:2px 8px; border-radius:4px;
                font-size:11px; font-weight:600; white-space:nowrap; }
.badge-sh     { background:#fee2e2; color:#b91c1c; }
.badge-sz     { background:#dbeafe; color:#1d4ed8; }
.badge-kcb    { background:#fef9c3; color:#92400e; }
.badge-pending{ background:#fffbeb; color:#b45309; border:1px solid #fde68a; }
.ret-pos { color:#16a34a; font-weight:600; }
.ret-neg { color:#dc2626; font-weight:600; }
.empty-state { text-align:center; padding:32px 16px; color:#94a3b8; font-size:14px; }
.empty-icon  { font-size:32px; margin-bottom:8px; }
.two-col { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-bottom:20px; }
.footer  { text-align:center; padding:20px; color:#94a3b8; font-size:12px; }
</style>"""

    html = f"""<!DOCTYPE html><html lang="zh-CN">
<head><meta charset="UTF-8">{CSS}</head>
<body><div class="wrapper">

<div class="header">
  <div class="header-left">
    <h1>📈 A股量化日报</h1>
    <div class="subtitle">动量 · 量能 · 布林带 三因子策略 &nbsp;|&nbsp; 前复权信号 / 后复权止盈止损</div>
  </div>
  <div class="header-badge">
    <div class="date">{target_str}</div>
    <div class="label">交易日报告</div>
  </div>
</div>

<div class="kpi-grid">
  <div class="kpi-card blue"><div class="kpi-icon">🎯</div>
    <div class="kpi-value">{n_picks}</div><div class="kpi-label">今日候选股票</div></div>
  <div class="kpi-card amber"><div class="kpi-icon">⏳</div>
    <div class="kpi-value">{n_pending}</div><div class="kpi-label">待成交挂单</div></div>
  <div class="kpi-card green"><div class="kpi-icon">✅</div>
    <div class="kpi-value">{n_trades}</div><div class="kpi-label">今日成交</div></div>
  <div class="kpi-card {'green' if n_portfolio > 0 else 'red'}"><div class="kpi-icon">💼</div>
    <div class="kpi-value">{n_portfolio}</div><div class="kpi-label">当前持仓</div></div>
</div>

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

<div class="section">
  <div class="section-header">
    <div class="section-icon icon-blue">🔍</div>
    <div class="section-title">当日候选股票</div>
    <span class="section-count">{n_picks} 只</span>
  </div>
  {_picks_table(df_picks)}
</div>

<div class="section">
  <div class="section-header">
    <div class="section-icon icon-amber">⏳</div>
    <div class="section-title">待成交挂单</div>
    <span class="section-count">{n_pending} 只</span>
  </div>
  <p style="font-size:12px;color:#64748b;margin-bottom:14px;">
    规则：T+1 日最低价 ≤ 挂单价时成交（收盘价 × 98.5%）</p>
  {_pending_table(df_pending)}
</div>

<div class="two-col">
  <div class="section">
    <div class="section-header">
      <div class="section-icon icon-green">✅</div>
      <div class="section-title">当日成交记录</div>
      <span class="section-count">{n_trades} 笔</span>
    </div>
    {_trades_table(df_trades)}
  </div>
  <div class="section">
    <div class="section-header">
      <div class="section-icon icon-purple">💼</div>
      <div class="section-title">当前持仓</div>
      <span class="section-count">{n_portfolio} 只</span>
    </div>
    {_portfolio_table(df_portfolio)}
  </div>
</div>

<div class="footer">
  <p>本报告由量化程序自动生成 · {target_str} 收盘后运行</p>
  <p style="margin-top:4px;">数据来源：通达信 · 策略：动量 + 量能 + 布林带三因子 · 仅供参考，不构成投资建议</p>
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

    send_email_via_graph(tm, f"📈 A股量化日报 - {target_str}", html, attachments)


# =========================================================
# 主流程
# =========================================================
def run_daily_pipeline():
    tm  = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
    odc = OneDriveClient(tm, CONFIG["onedrive_folder"], _DB_GZ_NAME)

    target_date = get_target_date()
    target_str  = target_date.strftime("%Y-%m-%d")
    if target_date.weekday() >= 5:
        log.info(f"🎯 {target_str} 为非交易日，跳过")
        return

    with tempfile.TemporaryDirectory() as tmp:
        db_path  = os.path.join(tmp, CONFIG["db_filename"])   # 原始 duckdb（仅本地临时）
        gz_path  = os.path.join(tmp, _DB_GZ_NAME)             # 压缩版（下载/上传用）

        # ── 1. 从 OneDrive 拉取压缩数据库并解压 ──
        need_full_bootstrap = False
        if odc.download_database_gz(gz_path):
            db_decompress_from_download(gz_path, db_path)
            log.info("✅ 已载入云端历史数据库（gz 解压完成）")

                    # 检查是否为未完成的 bootstrap（部分上传情况）
            with duckdb.connect(db_path, read_only=True) as _con:
                _stats = get_bootstrap_stats(_con)
            _raw_rows = None if _stats.empty else _stats.iloc[0]["total_raw_rows"]
            if _stats.empty or pd.isna(_raw_rows) or int(_raw_rows) == 0:
                log.warning("⚠️ 云端 db 数据量为 0，视为未完成初始化，重新全量 bootstrap")
                need_full_bootstrap = True
        else:
            log.info("ℹ️ OneDrive 无历史数据库，执行全量初始化")
            initialize_empty_database(db_path)
            need_full_bootstrap = True

        # ── 2. 通达信数据下载 & 解压 ──
        tdx_zip = os.path.join(tmp, "hsjday.zip")
        updated = False
        if download_tdx_zip(tdx_zip):
            extract_dir = os.path.join(tmp, "hsjday")
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(tdx_zip, "r") as zf:
                zf.extractall(extract_dir)
            stocks = list_all_stocks(extract_dir)

            if need_full_bootstrap:
                updated = bootstrap_full_from_tdx(stocks, db_path)
            else:
                updated = upsert_today_raw_and_xdxr(stocks, db_path, target_date)
        else:
            if need_full_bootstrap:
                raise RuntimeError("OneDrive 无历史数据库且通达信下载失败，无法完成首轮初始化。")
            log.warning("⚠️ 通达信下载失败，使用 Baostock 兜底")
            updated = baostock_upsert_today_raw(db_path, target_date)

        if not updated:
            log.warning("⚠️ 未写入当日行情，使用已有缓存继续")

        # ── 3. 复权缓存 & 策略 ──
        rebuild_recent_adjusted_cache(db_path, target_date, CONFIG["adjust_cache_days"])
        df_picks, df_portfolio, df_pending, df_trades = evaluate_strategy(
            db_path, target_date, CONFIG["top_n"]
        )

        # ── 4. 发送报告 ──
        try:
            generate_and_send_report(tm, df_picks, df_portfolio, df_pending, df_trades, target_str)
        except Exception as e:
            log.error(f"发送日报失败: {e}")

        # ── 5. 压缩并上传（不修改 CONFIG，gz 文件名由 _DB_GZ_NAME 派生）──
        db_compress_and_upload(odc, db_path, gz_path)
        log.info("🎉 今日流程完成")


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
        run_daily_pipeline()
    elif mode == "daemon":
        import schedule
        log.info("🕒 调度服务已启动，每天 17:00 自动执行")
        schedule.every().day.at("17:00").do(run_daily_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        raise ValueError(f"不支持的模式: {mode}")
