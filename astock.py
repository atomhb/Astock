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
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
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
import baostock as bs
# from pytdx.hq import TdxHq_API
# from pytdx.util.best_ip import select_best_ip
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
# 仅用于通达信下载相关的备用 IP，不用于 OneDrive 文件访问
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
    "cloud_db_gz_name": "CN_Stock.duckdb.gz",
    "local_db_gz_dir": 'D:\\OneDrive - 准雀\\Stock',
    "local_db_gz_name": 'CN_stock2026.duckdb.gz',
    "tdx_local_zip_path": None,
    "bootstrap_days": 120,
    "tdx_verify_ssl": False,
    "position_cash_yuan": 50000.0,
    "take_profit_pct": 10.0,
    "stop_loss_pct": -5.0,
    "max_hold_days": 200,
    "top_n": 20,
    "adjust_cache_days": 260,
}

CONFIG["position_cash_cent"] = int(round(CONFIG["position_cash_yuan"] * 100))
TDX_LOCAL_ZIP_PATH = CONFIG["tdx_local_zip_path"]  # 可设置为本地目录或具体 zip 文件路径，None 则强制云端下载

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


def probe_tdx_download_candidate(candidate_url: str, request_headers: Dict[str, str]) -> bool:
    probe_headers = {**request_headers, "Range": "bytes=0-0"}
    try:
        resp = requests.get(
            candidate_url,
            headers=probe_headers,
            stream=True,
            timeout=(5, 12),
            verify=CONFIG["tdx_verify_ssl"],
        )
        try:
            return resp.status_code != 404 and resp.status_code < 500
        finally:
            resp.close()
    except Exception:
        return False


def build_available_tdx_candidates(
    download_path: str,
    base_headers: Dict[str, str],
    show_progress: bool = True,
) -> List[Tuple[str, Dict[str, str]]]:
    parsed_url = requests.utils.urlparse(TDX_URL)
    ip_candidates = [
        (f"http://{ip}{download_path}", {"Host": parsed_url.netloc})
        for ip, _port in CUSTOM_TDX_IPS
    ]
    if not ip_candidates:
        return []

    available_candidates: List[Optional[Tuple[str, Dict[str, str]]]] = [None] * len(ip_candidates)
    max_workers = min(8, len(ip_candidates))
    progress = tqdm(
        total=len(ip_candidates),
        desc="测试通达信IP",
        unit="个",
        dynamic_ncols=True,
        leave=True,
        disable=not show_progress,
    )
    success_count = 0
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    probe_tdx_download_candidate,
                    candidate_url,
                    {**base_headers, **extra_headers},
                ): (index, (candidate_url, extra_headers))
                for index, (candidate_url, extra_headers) in enumerate(ip_candidates)
            }
            for future in as_completed(future_map):
                index, candidate = future_map[future]
                try:
                    if future.result():
                        available_candidates[index] = candidate
                        success_count += 1
                finally:
                    progress.update(1)
                    progress.set_postfix_str(f"可用 {success_count}/{len(ip_candidates)}")
    finally:
        progress.close()

    return [candidate for candidate in available_candidates if candidate is not None]
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
            trade_type VARCHAR,
            status VARCHAR,
            PRIMARY KEY (symbol, signal_date)
        )
    """)
    try:
        con.execute("ALTER TABLE pending_orders ADD COLUMN IF NOT EXISTS trade_type VARCHAR")
        con.execute("UPDATE pending_orders SET trade_type = 'buy' WHERE trade_type IS NULL")
    except Exception:
        pass
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
            shares BIGINT
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
            shares BIGINT,
            reason VARCHAR,
            pnl_pct DOUBLE
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
        WHERE raw_done = TRUE
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
    parsed_url = requests.utils.urlparse(TDX_URL)
    download_path = parsed_url.path or "/vipdoc/hsjday.zip"
    candidates = build_available_tdx_candidates(download_path, headers, show_progress=not IS_CI)
    if candidates:
        log.info(f"✅ 通达信可用 IP 数量: {len(candidates)} / {len(CUSTOM_TDX_IPS)}")
    else:
        log.info("ℹ️ 未探测到可用的通达信 IP，将直接使用原始下载地址")
    candidates.append((TDX_URL, {}))
    for attempt in range(1, 4):
        try:
            log.info(f"⬇️ 第 {attempt} 次尝试下载通达信数据")
            for candidate_url, extra_headers in candidates:
                try:
                    log.info(f"⬇️ 尝试通达信地址: {candidate_url}")
                    request_headers = {**headers, **extra_headers}
                    resp = session.get(
                        candidate_url,
                        headers=request_headers,
                        stream=True,
                        timeout=(15, 120),
                        verify=CONFIG["tdx_verify_ssl"],
                    )
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
                    log.warning(f"⚠️ 通达信地址下载失败 {candidate_url}: {e}")
        except Exception as e:
            log.warning(f"⚠️ 下载失败：{e}")
            time.sleep(2 * attempt)
    return False


def _resolve_local_tdx_zip(local_zip_path: Optional[str]) -> Optional[str]:
    local_zip_path = (local_zip_path or "").strip()
    if not local_zip_path:
        return None
    if os.path.isfile(local_zip_path):
        return local_zip_path if local_zip_path.lower().endswith(".zip") else None
    if not os.path.isdir(local_zip_path):
        return None
    preferred = os.path.join(local_zip_path, "hsjday.zip")
    if os.path.isfile(preferred):
        return preferred
    zip_files = sorted(
        os.path.join(local_zip_path, name)
        for name in os.listdir(local_zip_path)
        if name.lower().endswith(".zip")
    )
    return zip_files[0] if zip_files else None


def obtain_tdx_zip(dest_zip: str, local_zip_path: Optional[str] = None) -> bool:
    """
    获取通达信压缩包：优先使用本地 zip 文件；如果 local_zip_path 为 None 或未找到文件，则走云端下载。
    """
    source_zip = _resolve_local_tdx_zip(local_zip_path)
    if source_zip:
        try:
            log.info(f"📁 使用本地通达信压缩文件: {source_zip}")
            shutil.copyfile(source_zip, dest_zip)
            return os.path.exists(dest_zip) and os.path.getsize(dest_zip) > 1024
        except Exception as e:
            log.warning(f"⚠️ 读取本地通达信压缩文件失败，改用云端下载：{e}")
    return download_tdx_zip(dest_zip)


def extract_tdx_zip(zip_path: str, extract_dir: str) -> None:
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

# def connect_best_tdx(api: TdxHq_API) -> bool:
#     try:
#         best = select_best_ip()
#         if best and api.connect(best["ip"], best["port"]):
#             log.info(f"✅ 默认最优 IP 连接成功: {best['ip']}:{best['port']}")
#             return True
#     except Exception as e:
#         log.warning(f"⚠️ 默认最优 IP 失败: {e}")
#     random.shuffle(CUSTOM_TDX_IPS)
#     for ip, port in CUSTOM_TDX_IPS:
#         try:
#             if api.connect(ip, port):
#                 cnt = api.get_security_count(0)
#                 if cnt and cnt > 0:
#                     log.info(f"✅ 备用节点连接成功: {ip}:{port}")
#                     return True
#                 api.disconnect()
#         except Exception:
#             try:
#                 api.disconnect()
#             except Exception:
#                 pass
#     return False

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

def flush_bootstrap_batch(con, raw_batches):
    if raw_batches:
        df_raw = pd.concat(raw_batches, ignore_index=True)
        con.register('tmp_boot_raw', df_raw)
        con.execute("""
            INSERT OR REPLACE INTO daily_raw
            SELECT symbol, date, open, high, low, close, volume, amount
            FROM tmp_boot_raw
        """)
        raw_batches.clear()


def bootstrap_full_from_tdx(stocks: List[Dict], db_path: str) -> bool:
    # api = TdxHq_API(raise_exception=False, auto_retry=True)
    # if not connect_best_tdx(api):
    #     log.error("PyTDX 节点不可用，无法执行全量初始化。")
    #     return False
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
        raw_batches = []
        loaded_raw = 0
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
                except Exception as e:
                    last_error = str(e)
                    log.debug(f"全量初始化失败 {stock['symbol']}: {e}")
                finally:
                    upsert_bootstrap_state(con, stock["symbol"], raw_done, False,
                                           raw_count, 0, last_error)
                if idx % 100 == 0:
                    flush_bootstrap_batch(con, raw_batches)
                    con.execute("CHECKPOINT")
                    log_bootstrap_progress(con, idx, total, stock["symbol"])
            flush_bootstrap_batch(con, raw_batches)
            con.execute("CHECKPOINT")
            log_bootstrap_progress(con, total, total, "FINAL")
        finally:
            log.info(f"✅")
    log.info(f"✅ 全量初始化完成: raw={loaded_raw} 行")
    return True


def upsert_today_raw_and_xdxr(stocks: List[Dict], db_path: str, target_date: date) -> bool:
    """
    增量更新：仅写入目标交易日的 daily_raw 与 xdxr_events。
    遍历通达信 .day 文件，读取末行判断是否为 target_date，是则写入。
    同时通过 pytdx 获取当日 xdxr 事件。
    """
    # 先检查数据库是否已有当日数据，有则跳过
    with duckdb.connect(db_path, read_only=True) as _con:
        cnt = _con.execute(
            "SELECT COUNT(*) FROM daily_raw WHERE date = ?", [target_date]
        ).fetchone()[0]
        if cnt > 0:
            log.info(f"✅ daily_raw 已有 {target_date} 的 {cnt} 条记录，跳过增量更新")
            return True
    inserted_raw = 0
    inserted_xdxr = 0
    raw_batches: List[pd.DataFrame] = []
    try:
        with duckdb.connect(db_path) as con:
            ensure_core_tables(con)
            for stock in tqdm(stocks, desc="增量更新 daily_raw", unit="只"):
                try:
                    last_row = read_tdx_day_last_row(stock["filepath"])
                    if last_row.empty:
                        continue
                    row_date = last_row.iloc[0]["date"]
                    if not is_trade_candidate_date(row_date, target_date):
                        continue
                    last_row["symbol"] = stock["symbol"]
                    last_row["open"]   = last_row["open"].apply(yuan_to_price_int)
                    last_row["high"]   = last_row["high"].apply(yuan_to_price_int)
                    last_row["low"]    = last_row["low"].apply(yuan_to_price_int)
                    last_row["close"]  = last_row["close"].apply(yuan_to_price_int)
                    last_row["amount"] = last_row["amount"].apply(yuan_to_amount_int)
                    last_row = last_row[["symbol", "date", "open", "high", "low", "close", "volume", "amount"]]
                    raw_batches.append(last_row)
                    inserted_raw += 1
                except Exception as e:
                    log.debug(f"增量更新失败 {stock['symbol']}: {e}")
                # 每 100 只批量写入一次
                if len(raw_batches) >= 100:
                    flush_bootstrap_batch(con, raw_batches)
            # 写入剩余
            flush_bootstrap_batch(con, raw_batches)
            con.execute("CHECKPOINT")
    finally:
        log.info(f"✅")
    log.info(f"✅ 增量更新: raw={inserted_raw} 行")
    return inserted_raw > 0
def baostock_upsert_today_raw(db_path: str, target_date: date) -> bool:
    """
    使用 Baostock 作为兜底数据源，获取当日全 A 股日线数据并写入 daily_raw。
    仅在通达信下载失败时调用。
    """
    # 先检查数据库是否已有当日数据，有则跳过
    with duckdb.connect(db_path, read_only=True) as _con:
        cnt = _con.execute(
            "SELECT COUNT(*) FROM daily_raw WHERE date = ?", [target_date]
        ).fetchone()[0]
        if cnt > 0:
            log.info(f"✅ daily_raw 已有 {target_date} 的 {cnt} 条记录，跳过 Baostock 兜底")
            return True
    target_str = target_date.strftime("%Y-%m-%d")
    lg = bs.login()
    if lg.error_code != '0':
        log.error(f"Baostock 登录失败: {lg.error_msg}")
        return False
    inserted = 0
    try:
        with duckdb.connect(db_path) as con:
            ensure_core_tables(con)
            # 获取当日所有 A 股代码
            stock_rs = bs.query_all_stock(day=target_str)
            all_codes: List[str] = []
            while stock_rs.error_code == '0' and stock_rs.next():
                row = stock_rs.get_row_data()
                code = row[0]  # e.g. sh.600000
                if code.startswith("sh.") or code.startswith("sz."):
                    market = code[:2]
                    code_num = code[3:]
                    if code_num.startswith(STOCK_PREFIX.get(market, ())):
                        all_codes.append(code)
            raw_batches: List[pd.DataFrame] = []
            for bs_code in tqdm(all_codes, desc="Baostock 兜底更新", unit="只"):
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=target_str,
                        end_date=target_str,
                        frequency="d",
                        adjustflag="3",  # 不复权
                    )
                    rows = []
                    while rs.error_code == '0' and rs.next():
                        rows.append(rs.get_row_data())
                    if not rows:
                        continue
                    r = rows[0]
                    if not r[1] or float(r[1]) <= 0:
                        continue
                    market = bs_code[:2]
                    code_num = bs_code[3:]
                    symbol = f"{code_num}.{market.upper()}"
                    df_row = pd.DataFrame([{
                        "symbol": symbol,
                        "date":   pd.to_datetime(r[0]).date(),
                        "open":   yuan_to_price_int(float(r[1])),
                        "high":   yuan_to_price_int(float(r[2])),
                        "low":    yuan_to_price_int(float(r[3])),
                        "close":  yuan_to_price_int(float(r[4])),
                        "volume": int(float(r[5])),
                        "amount": yuan_to_amount_int(float(r[6])),
                    }])
                    raw_batches.append(df_row)
                    inserted += 1
                    if len(raw_batches) >= 100:
                        df_batch = pd.concat(raw_batches, ignore_index=True)
                        con.register('tmp_bs_raw', df_batch)
                        con.execute("""
                            INSERT OR REPLACE INTO daily_raw
                            SELECT symbol, date, open, high, low, close, volume, amount
                            FROM tmp_bs_raw
                        """)
                        raw_batches.clear()
                except Exception as e:
                    log.debug(f"Baostock 获取失败 {bs_code}: {e}")
            if raw_batches:
                df_batch = pd.concat(raw_batches, ignore_index=True)
                con.register('tmp_bs_raw', df_batch)
                con.execute("""
                    INSERT OR REPLACE INTO daily_raw
                    SELECT symbol, date, open, high, low, close, volume, amount
                    FROM tmp_bs_raw
                """)
            con.execute("CHECKPOINT")
    finally:
        bs.logout()
    log.info(f"✅ Baostock 兜底更新: {inserted} 行")
    return inserted > 0


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
def __get_daily_factors(g: pd.DataFrame) -> np.ndarray:
    close_prev = g["close"].shift(1).copy()
    close_prev.iloc[0] = g["close"].iloc[0]  # prevent NaN logic
    close_prev_f = close_prev.astype(float)
    open_curr_f = g["open"].astype(float)
    
    sym = g["symbol"].iloc[0]
    limit_drop = 0.81 if sym.startswith(("688.", "300.", "301.", "8", "4", "83", "87")) else 0.91
        
    mask = (close_prev_f > 0) & ((open_curr_f / close_prev_f) < limit_drop)
    daily_factor = np.ones(len(g), dtype=float)
    daily_factor[mask] = (open_curr_f / close_prev_f)[mask]
    daily_factor[0] = 1.0
    return daily_factor

def calc_qfq_from_raw(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("date").reset_index(drop=True)
    if len(g) < 2: return g
    daily_factor = __get_daily_factors(g)
    cum_factor = np.cumprod(daily_factor[::-1])[::-1]
    adj_factor = np.append(cum_factor[1:], 1.0)
    
    for col in ["open", "high", "low", "close"]:
        adjusted = g[col].astype(float) * adj_factor
        g[col] = adjusted.apply(lambda v: safe_price_int(v / PRICE_SCALE))
    return g

def calc_hfq_from_raw(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("date").reset_index(drop=True)
    if len(g) < 2: return g
    daily_factor = __get_daily_factors(g)
    cum_factor = np.cumprod(daily_factor)
    adj_factor = 1.0 / cum_factor
    
    for col in ["open", "high", "low", "close"]:
        adjusted = g[col].astype(float) * adj_factor
        g[col] = adjusted.apply(lambda v: safe_price_int(v / PRICE_SCALE))
    return g

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
        q_parts, h_parts = [], []
        for sym, g in raw_df.groupby("symbol"):
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
        WHERE status='pending' AND signal_date < ?
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
        
        # Determine if budget allows buying
        if avail_cash >= position_cash_yuan * 0.5:  # ensure at least 50% of intended budget is available
            cost_yuan_budget = min(position_cash_yuan, avail_cash)
        else:
            expired_rows.append((symbol, signal_date))
            continue

        if today_low_qfq <= planned_buy_price:
            factor = (today_close_hfq / today_close_qfq) if today_close_qfq > 0 else 1.0
            buy_price_hfq = safe_price_int((planned_buy_price / PRICE_SCALE) * factor)
            
            # Request: A股股数全是整百，不存在碎股
            shares = int((cost_yuan_budget * PRICE_SCALE) / planned_buy_price) // 100 * 100
            if shares < 100:
                expired_rows.append((symbol, signal_date))
                continue
            
            actual_cost = round(shares * planned_buy_price / PRICE_SCALE, 2)
            if actual_cost > avail_cash:
                shares = int((avail_cash * PRICE_SCALE) / planned_buy_price) // 100 * 100
                if shares < 100:
                    expired_rows.append((symbol, signal_date))
                    continue
                actual_cost = round(shares * planned_buy_price / PRICE_SCALE, 2)
            
            avail_cash -= actual_cost
            filled_rows.append((symbol, trade_date, planned_buy_price, buy_price_hfq, int(shares)))
        else:
            expired_rows.append((symbol, signal_date))
            
    if filled_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
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
    raw_today = con.execute("SELECT symbol, close FROM daily_raw WHERE date = ?", [trade_date]).df()
    raw_map = {row['symbol']: int(row['close']) for _, row in raw_today.iterrows()} if not raw_today.empty else {}
    if qfq_df.empty:
        return []
    qfq_df['date'] = pd.to_datetime(qfq_df['date'])
    if not hfq_df.empty:
        hfq_df['date'] = pd.to_datetime(hfq_df['date'])
    sold_rows = []
    init_cap, total_assets, avail_cash = get_account_state(con)
    for _, row in holdings.iterrows():
        sym = row['symbol']
        buy_date = pd.to_datetime(row['buy_date']).date()
        buy_price = int(row['buy_price'])
        buy_price_hfq = int(row['buy_price_hfq']) if not pd.isna(row['buy_price_hfq']) else buy_price
        shares = int(row['shares'])  # 整百股
        gq = qfq_df[qfq_df['symbol'] == sym].copy()
        if gq.empty:
            continue
        gh = hfq_df[hfq_df['symbol'] == sym].copy() if not hfq_df.empty else pd.DataFrame()
        gq['close_f'] = gq['close'].astype(float) / PRICE_SCALE
        gq['ma20_f'] = gq['close_f'].rolling(20).mean()
        last_q = gq.iloc[-1]
        last_close_qfq = int(last_q['close'])
        
        g_raw = raw_today[raw_today['symbol'] == sym]
        if g_raw.empty:
            g_raw = con.execute("SELECT close FROM daily_raw WHERE symbol = ? AND date <= ? ORDER BY date DESC LIMIT 2", [sym, trade_date]).df()
        
        last_close_hfq = int(gh.iloc[-1]['close']) if not gh.empty else last_close_qfq
        hold_days = (trade_date - buy_date).days
        pnl_pct = (last_close_hfq - buy_price_hfq) / buy_price_hfq * 100 if buy_price_hfq > 0 else 0.0
        reasons = []
        
        # 移除止盈比例检查，保留止损
        if pnl_pct <= CONFIG['stop_loss_pct']:
            reasons.append(f"止损{CONFIG['stop_loss_pct']}%")
            
        gq['close_f'] = gq['close'].astype(float) / PRICE_SCALE
        gq['ma20_f'] = gq['close_f'].rolling(20).mean()
        gq['bb_mid'] = gq['close_f'].rolling(20).mean()
        gq['bb_std'] = gq['close_f'].rolling(20).std()
        gq['bb_lower'] = gq['bb_mid'] - 2 * gq['bb_std']
        
        last_close_f = float(last_q['close_f'])
        prev_close_f = float(gq.iloc[-2]['close_f']) if len(gq) >= 2 else last_close_f
        last_mid = float(gq['bb_mid'].iloc[-1]) if not pd.isna(gq['bb_mid'].iloc[-1]) else 0
        ma20_int = None if pd.isna(last_q['ma20_f']) else yuan_to_price_int(float(last_q['ma20_f']))
        
        if ma20_int is not None and last_close_qfq < ma20_int:
            reasons.append('跌破MA20')
        if hold_days >= CONFIG['max_hold_days']:
            reasons.append(f"持有{CONFIG['max_hold_days']}天")

        # 布林带形态卖出：跌破布林中轨或下轨
        if last_mid > 0 and prev_close_f > last_mid and last_close_f < last_mid:
            reasons.append('跌破布林中轨')
            
        # K线形态卖出：含有明确的下跌形态
        o_s = gq['open'].astype(float) / PRICE_SCALE
        h_s = gq['high'].astype(float) / PRICE_SCALE
        l_s = gq['low'].astype(float) / PRICE_SCALE
        c_s = gq['close_f']
        bull_text, bull_score, bear_text, bear_score = detect_kline_patterns(o_s, h_s, l_s, c_s)
        
        if bear_score >= 1.0:
            reasons.append(f'出现下跌形态({bear_text})')

        if talib is not None and len(gq) > 30:
            macd, macdsignal, macdhist = talib.MACD(gq['close_f'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            if not pd.isna(macdhist[-1]) and not pd.isna(macdhist[-2]):
                if macdhist[-1] < macdhist[-2]:
                    reasons.append('MACD差值减小')
                    
        if reasons:
            # 使用原始价格计算卖出回收现金（不折价，以信号当天收盘价卖出）
            sell_price_raw = raw_map.get(sym, last_close_qfq)  # 优先用raw，否则用qfq
            sold_rows.append((sym, trade_date, last_close_qfq, shares, ' / '.join(reasons), pnl_pct, sell_price_raw))
            
    for sym, sell_date, sell_price, shares, reason, pnl_pct, sell_price_raw in sold_rows:
        con.execute('DELETE FROM virtual_portfolio WHERE symbol=?', [sym])
        con.execute("""
            INSERT INTO trade_history(symbol, trade_type, signal_date, trade_date, price, shares, reason, pnl_pct)
            VALUES (?, 'sell', NULL, ?, ?, ?, ?, ?)
        """, [sym, sell_date, sell_price, shares, reason, pnl_pct])
        recovered_cash = round(shares * sell_price_raw / PRICE_SCALE, 2)
        avail_cash += recovered_cash
        
    if sold_rows:
        con.execute("UPDATE account_state SET available_cash=? WHERE id=1", [avail_cash])
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
            low_s = g["low"].astype(float)
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
            planned_buy_price = yuan_to_price_int(float(last["close"]) * 0.985)

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
                "close": yuan_to_price_int(float(last["close"])),
                "planned_buy_price": planned_buy_price,
                "ret_20": float(last["ret_20"]),
                "ret_60": float(last["ret_60"]),
                "ma20": yuan_to_price_int(float(last["ma20"])),
                "ma60": yuan_to_price_int(float(last["ma60"])),
                "vol_ratio": float(last["vol_ratio"]),
                "vol_up_count_20": float(last["vol_up_count_20"]),
                "pattern": pattern_text,
                "bb_condition": bb_text,
                "momentum_score": round(momentum_score, 3),
                "volume_score": round(volume_score, 3),
                "boll_score": round(boll_score, 3),
                "pattern_score": round(pattern_score, 3),
                "total_score": round(total_score, 3),
            })
        df_picks = pd.DataFrame(picks_rows)
        if not df_picks.empty:
            df_picks = df_picks.sort_values("total_score", ascending=False).head(top_n).reset_index(drop=True)

        # ── 挂单逻辑：清除旧挂单，仅从当日候选票中挂单 ──
        con.execute("UPDATE pending_orders SET status='expired' WHERE status='pending'")
        holdings_df = con.execute("SELECT symbol FROM virtual_portfolio").df()
        holding_symbols = set(holdings_df["symbol"]) if not holdings_df.empty else set()
        new_orders = []
        if not df_picks.empty:
            for _, row in df_picks.iterrows():
                symbol = row["symbol"]
                if symbol in holding_symbols:
                    continue
                new_orders.append((symbol, target_date, int(row["planned_buy_price"]), int(row["close"]), "buy", "pending"))
            if new_orders:
                con.executemany("""
                    INSERT OR REPLACE INTO pending_orders(symbol, signal_date, planned_buy_price, signal_close, trade_type, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, new_orders)

        df_pending = con.execute("""
            SELECT symbol, signal_date, planned_buy_price, signal_close, trade_type, status
            FROM pending_orders
            WHERE status='pending'
            ORDER BY signal_date DESC, symbol
        """).df()

        # ── 持仓与市值计算（使用原始价格 daily_raw）──
        holdings = con.execute("SELECT * FROM virtual_portfolio ORDER BY symbol").df()
        if holdings.empty:
            df_portfolio = pd.DataFrame()
            total_market_value = 0.0
        else:
            # 使用原始价格计算市值（非复权价格）
            raw_today_df = con.execute("SELECT symbol, close FROM daily_raw WHERE date = ?", [target_date]).df()
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
            df_portfolio["market_value"] = (df_portfolio["last_price"].astype(float) / PRICE_SCALE * df_portfolio["shares"]).astype(float)
            df_portfolio["cost"] = (df_portfolio["buy_price"].astype(float) / PRICE_SCALE * df_portfolio["shares"]).astype(float)
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
        """, [target_date, new_total_assets, avail_cash, daily_pnl, daily_ret, total_market_value])

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
        bb_cond = getattr(r, 'bb_condition', '—')
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
        pbp = r.planned_buy_price # 挂单价
        sc  = r.signal_close / PRICE_SCALE
        disc = (pbp - sc) / sc * 100
        type_str = "🟢 买入" if getattr(r, 'trade_type', 'buy') == 'buy' else "🔴 卖出"
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{type_str}</td>
          <td>{r.signal_date}</td>
          <td>¥{pbp:.3f}</td>
          <td>¥{sc:.3f}</td>
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
        type_label = "🟢 买入" if r.trade_type == "buy" else "🔴 卖出"
        shares_display = int(round(r.shares / 100) * 100) if not pd.isna(r.shares) else 0
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{type_label}</td>
          <td>{r.trade_date}</td>
          <td>¥{r.price / PRICE_SCALE:.3f}</td>
          <td>{shares_display}</td>
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
        shares_display = int(r.shares) if not pd.isna(r.shares) else 0
        rows.append(f"""
        <tr>
          <td>{_market_badge(r.symbol)}</td>
          <td>{r.buy_date}</td>
          <td>¥{r.buy_price:.3f}</td>
          <td>¥{r.last_price:.3f}</td>
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
  <p style="margin-top:4px;">数据来源：通达信 · 策略：布林带 + 动量 + 量能三因子 · 仅供参考，不构成投资建议</p>
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
        db_path  = os.path.join(tmp, "CN_stock.duckdb")   # 原始 duckdb（仅本地临时）
        gz_path  = os.path.join(tmp, _DB_GZ_NAME)             # 压缩版（下载/上传用）
        # ── 1. 从本地固定路径或 OneDrive 拉取压缩数据库并解压 ──
        need_full_bootstrap = False
        has_today_data = False
        db_source, db_gz_ready = obtain_db_gz(LOCAL_DB_GZ_PATH, odc, gz_path)
        if db_gz_ready:
            load_db_gz_to_local(gz_path, db_path)
            log.info("✅ 已载入历史数据库（gz 解压完成）")
            # 检查是否为未完成的 bootstrap（部分上传情况）
            with duckdb.connect(db_path, read_only=True) as _con:
                _stats = get_bootstrap_stats(_con)
                _raw_rows = None if _stats.empty else _stats.iloc[0]["total_raw_rows"]
                if _stats.empty or pd.isna(_raw_rows) or int(_raw_rows) == 0:
                    log.warning("⚠️  db 数据量为 0，视为未完成初始化，重新全量 bootstrap")
                    need_full_bootstrap = True
                else:
                    # 检查是否已有当日数据
                    cnt = _con.execute(
                        "SELECT COUNT(*) FROM daily_raw WHERE date = ?", [target_date]
                    ).fetchone()[0]
                    if cnt > 0:
                        has_today_data = True
                        log.info(f"✅ 数据库已包含 {target_str} 的 {cnt} 条记录，跳过通达信下载")
        else:
            log.info("ℹ️ 本地/OneDrive 均无历史数据库，执行全量初始化")
            initialize_empty_database(db_path)
            need_full_bootstrap = True

        # ── 2. 通达信数据下载/本地读取 & 解压（仅在缺少当日数据时执行）──
        updated = has_today_data  # 已有数据则视为已更新
        if not has_today_data:
            tdx_zip = os.path.join(tmp, "hsjday.zip")
            if obtain_tdx_zip(tdx_zip, TDX_LOCAL_ZIP_PATH):
                extract_dir = os.path.join(tmp, "hsjday")
                extract_tdx_zip(tdx_zip, extract_dir)
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
        df_picks, df_portfolio, df_pending, df_trades, metrics = evaluate_strategy(
            db_path, target_date, CONFIG["top_n"]
        )
        # ── 4. 发送报告 ──
        try:
            generate_and_send_report(tm, df_picks, df_portfolio, df_pending, df_trades, target_str, metrics)
        except Exception as e:
            log.error(f"发送日报失败: {e}")
        # ── 5. 压缩并上传（不修改 CONFIG，gz 文件名由 _DB_GZ_NAME 派生）──
        if db_source == "local" and LOCAL_DB_GZ_PATH:
            db_compress_to_local(db_path, LOCAL_DB_GZ_PATH)
        else:
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