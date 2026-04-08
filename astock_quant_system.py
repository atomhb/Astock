#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股每日量化分析系统
依赖：baostock duckdb pandas requests tqdm python-dotenv
"""

import os, sys, base64, json, logging, tempfile, gzip, shutil, time
import smtplib, duckdb, baostock as bs
import requests
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

# ---------------------------------------------------------
# 日志（控制台 + 文件双输出）
# ---------------------------------------------------------
try:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _BASE_DIR = os.path.abspath(".")

_LOG_FILE = os.path.join(
    _BASE_DIR,
    f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)
log.info(f"📝 日志文件: {_LOG_FILE}")

CN_TZ = timezone(timedelta(hours=8))

# ---------------------------------------------------------
# CI 环境检测
# ---------------------------------------------------------
IS_CI = (
    os.getenv("CI", "").lower() in ("true", "1", "yes")
    or os.getenv("GITHUB_ACTIONS", "").lower() in ("true", "1")
)

# ---------------------------------------------------------
# Microsoft Graph API 常量
# ---------------------------------------------------------
_GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
_AUTH_URL    = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
_DEVICE_URL  = "https://login.microsoftonline.com/common/oauth2/v2.0/devicecode"
_SCOPES      = "Files.ReadWrite.All offline_access"
_CHUNK_SIZE  = 10 * 1024 * 1024  # 10 MB 分块上传


# ---------------------------------------------------------
# 配置
# ---------------------------------------------------------
def _get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


CONFIG = {
    "azure_client_id":  _get_env("AZURE_CLIENT_ID"),
    "onedrive_folder":  _get_env("ONEDRIVE_FOLDER", "Stock"),
    "db_filename":      _get_env("DB_FILENAME", "A_stock.duckdb"),
    "token_cache_file": os.path.join(
        _BASE_DIR, _get_env("TOKEN_CACHE_FILE", "ms_token.json")
    ),
    "smtp_host":        _get_env("SMTP_HOST", "smtp.office365.com"),
    "smtp_port":        int(_get_env("SMTP_PORT", "587")),
    "email_user":       _get_env("EMAIL_USER"),
    "email_password":   _get_env("EMAIL_PASSWORD"),
    "email_to":         _get_env("EMAIL_TO"),
    "bootstrap_days":   90,
}


# ---------------------------------------------------------
# Token 管理器
# 本地：读写 ms_token.json（包含 access_token / refresh_token / expires_at）
# CI  ：从 ONEDRIVE_TOKEN_CACHE_B64 解码后读入内存，不落盘
# ---------------------------------------------------------
class TokenManager:
    """
    Token 格式（JSON）：
    {
        "access_token":  "eyJ...",
        "refresh_token": "0.A...",
        "expires_at":    1234567890.0   ← Unix 时间戳，access_token 过期时间
    }
    """

    def __init__(self, client_id: str, token_file: str):
        self.client_id  = client_id
        self.token_file = token_file
        self._data: dict = {}

        # CI：从 B64 环境变量加载
        b64 = os.getenv("ONEDRIVE_TOKEN_CACHE_B64", "").strip()
        if IS_CI and b64:
            try:
                clean = b64.replace("\\n", "").replace("\n", "").replace("\r", "").replace(" ", "")
                self._data = json.loads(base64.b64decode(clean).decode("utf-8"))
                log.info(f"✅ CI Token 已从 B64 加载（长度: {len(b64)} 字符）")
            except Exception as e:
                log.error(f"❌ CI Token 解码失败: {e}")
        elif not IS_CI and os.path.exists(token_file):
            with open(token_file, "r", encoding="utf-8") as f:
                self._data = json.load(f)
            log.info(f"✅ 本地 Token 已加载: {token_file}")

    # --------------------------------------------------
    def _save(self):
        """本地模式才落盘，CI 不写文件。"""
        if not IS_CI:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)

    # --------------------------------------------------
    def _is_expired(self) -> bool:
        exp = self._data.get("expires_at", 0)
        return time.time() > exp - 60   # 提前 60 秒刷新

    # --------------------------------------------------
    def _refresh(self):
        """用 refresh_token 静默换取新的 access_token。"""
        rt = self._data.get("refresh_token", "")
        if not rt:
            raise RuntimeError(
                "❌ 未找到 refresh_token！\n"
                "请在本地运行 `python astock_quant_system.py auth` 重新授权，\n"
                "然后 `python astock_quant_system.py export` 更新 GitHub Secret。"
            )

        log.info("🔄 access_token 已过期，正在用 refresh_token 静默刷新...")
        resp = requests.post(
            _AUTH_URL,
            data={
                "client_id":     self.client_id,
                "grant_type":    "refresh_token",
                "refresh_token": rt,
                "scope":         _SCOPES,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token_resp = resp.json()

        if "error" in token_resp:
            raise RuntimeError(
                f"❌ Token 刷新失败: {token_resp.get('error_description', token_resp)}\n"
                "请重新运行 auth + export 更新 Token。"
            )

        self._data["access_token"]  = token_resp["access_token"]
        self._data["expires_at"]    = time.time() + token_resp.get("expires_in", 3600)
        if "refresh_token" in token_resp:
            self._data["refresh_token"] = token_resp["refresh_token"]

        self._save()
        log.info("✅ Token 刷新成功。")

    # --------------------------------------------------
    def get_access_token(self) -> str:
        """返回有效的 access_token，必要时自动刷新。"""
        if not self._data:
            raise RuntimeError(
                "❌ Token 数据为空！\n"
                "请先运行 `python astock_quant_system.py auth`。"
            )
        if self._is_expired():
            self._refresh()
        return self._data["access_token"]

    # --------------------------------------------------
    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    # --------------------------------------------------
    def device_code_auth(self):
        """
        设备代码流授权（本地 CLI 使用，无需配置 redirect URI）。
        用户在浏览器中输入 user_code 完成授权，程序自动轮询获取 token。
        """
        # Step 1：获取设备码
        resp = requests.post(
            _DEVICE_URL,
            data={
                "client_id": self.client_id,
                "scope":     _SCOPES,
            },
            timeout=30,
        )
        resp.raise_for_status()
        dc = resp.json()

        print("\n" + "=" * 60)
        print(f"🔗 请在浏览器打开：{dc['verification_uri']}")
        print(f"🔑 输入代码：       {dc['user_code']}")
        print("=" * 60)
        print("等待授权完成（程序将自动轮询）...\n")

        # Step 2：轮询直到用户完成授权
        interval  = dc.get("interval", 5)
        device_cd = dc["device_code"]
        deadline  = time.time() + dc.get("expires_in", 900)

        while time.time() < deadline:
            time.sleep(interval)
            poll = requests.post(
                _AUTH_URL,
                data={
                    "client_id":   self.client_id,
                    "grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_cd,
                },
                timeout=30,
            )
            pr = poll.json()

            if "access_token" in pr:
                self._data = {
                    "access_token":  pr["access_token"],
                    "refresh_token": pr.get("refresh_token", ""),
                    "expires_at":    time.time() + pr.get("expires_in", 3600),
                }
                self._save()
                log.info(f"✅ 授权成功！Token 已保存至: {self.token_file}")
                return

            err = pr.get("error", "")
            if err == "authorization_pending":
                print("⏳ 等待授权...", end="\r")
                continue
            elif err == "slow_down":
                interval += 5
                continue
            else:
                raise RuntimeError(f"❌ 授权失败: {pr.get('error_description', pr)}")

        raise RuntimeError("❌ 授权超时，请重新运行 auth 模式。")

    # --------------------------------------------------
    def export_b64(self) -> str:
        """将 token JSON 序列化后 Base64 编码，用于写入 GitHub Secret。"""
        if not self._data:
            raise FileNotFoundError(
                f"未找到 Token 数据，请先运行 `python astock_quant_system.py auth`。"
            )
        token_json = json.dumps(self._data, ensure_ascii=False)
        return base64.b64encode(token_json.encode("utf-8")).decode()


# ---------------------------------------------------------
# OneDrive 操作（纯 requests + Graph API）
# ---------------------------------------------------------
class OneDriveClient:
    def __init__(self, token_mgr: TokenManager, folder: str, remote_gz_name: str):
        self.tm             = token_mgr
        self.folder         = folder
        self.remote_gz_name = remote_gz_name

    # --------------------------------------------------
    def _item_path(self, filename: str) -> str:
        return f"{self.folder}/{filename}"

    # --------------------------------------------------
    def download_gz(self, local_gz_path: str) -> bool:
        """
        下载远端 .gz 文件到本地。
        使用 /me/drive/root:/{path}:/content，Graph API 会 302 重定向到真实下载 URL。
        """
        url  = f"{_GRAPH_BASE}/me/drive/root:/{self._item_path(self.remote_gz_name)}:/content"
        resp = requests.get(url, headers=self.tm.headers(), allow_redirects=True,
                            stream=True, timeout=180)

        if resp.status_code == 404:
            log.info("☁️  云端暂无备份文件，视为首次运行。")
            return False
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        size_mb = total / 1024 / 1024 if total else 0
        log.info(f"⬇️  正在下载: {self.remote_gz_name} ({size_mb:.2f} MB)")

        with open(local_gz_path, "wb") as f, \
             tqdm(total=total or None, unit="B", unit_scale=True, desc="下载进度") as bar:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                bar.update(len(chunk))

        log.info("✅ 下载完成。")
        return True

    # --------------------------------------------------
    def upload_gz(self, local_gz_path: str):
        """
        大文件分块上传（Upload Session），支持任意大小。
        """
        file_size = os.path.getsize(local_gz_path)
        size_mb   = file_size / 1024 / 1024
        log.info(f"⬆️  准备上传: {self.remote_gz_name} ({size_mb:.2f} MB)")

        # Step 1：创建上传会话
        session_url = (
            f"{_GRAPH_BASE}/me/drive/root:/{self._item_path(self.remote_gz_name)}"
            f":/createUploadSession"
        )
        sess_resp = requests.post(
            session_url,
            headers={**self.tm.headers(), "Content-Type": "application/json"},
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            timeout=30,
        )
        sess_resp.raise_for_status()
        upload_url = sess_resp.json()["uploadUrl"]

        # Step 2：分块上传
        with open(local_gz_path, "rb") as f, \
             tqdm(total=file_size, unit="B", unit_scale=True, desc="上传进度") as bar:
            offset = 0
            while offset < file_size:
                chunk = f.read(_CHUNK_SIZE)
                if not chunk:
                    break
                end = offset + len(chunk) - 1
                headers = {
                    "Content-Range":  f"bytes {offset}-{end}/{file_size}",
                    "Content-Length": str(len(chunk)),
                }
                put = requests.put(upload_url, headers=headers, data=chunk, timeout=120)
                if put.status_code not in (200, 201, 202):
                    raise RuntimeError(f"上传失败 HTTP {put.status_code}: {put.text}")
                bar.update(len(chunk))
                offset += len(chunk)

        log.info("✅ 云端备份完成。")


# ---------------------------------------------------------
# 工具函数
# ---------------------------------------------------------
def is_trading_day(d: date) -> bool:
    return d.weekday() < 5


def get_db_last_date(db_path: str):
    if not os.path.exists(db_path):
        return None
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            exists = con.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = 'daily_qfq'"
            ).fetchone()[0]
            if not exists:
                return None
            row = con.execute("SELECT MAX(date) FROM daily_qfq").fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        log.warning(f"读取最新日期失败: {e}")
        return None


# ---------------------------------------------------------
# 建表
# ---------------------------------------------------------
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    symbol  VARCHAR,
    date    DATE,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume  DOUBLE,
    amount  DOUBLE,
    PRIMARY KEY (symbol, date)
)
"""


def _ensure_tables(db_path: str):
    with duckdb.connect(db_path) as con:
        for tbl in ("daily_qfq", "daily_hfq"):
            con.execute(_CREATE_TABLE_SQL.format(table=tbl))


# ---------------------------------------------------------
# Baostock 拉取单只股票
# ---------------------------------------------------------
def _fetch_symbol(sym: str, start_str: str, end_str: str) -> tuple[list, list]:
    fields = "date,open,high,low,close,volume,amount"

    def _pull(flag: str) -> list:
        rows = []
        rs = bs.query_history_k_data_plus(
            sym, fields,
            start_date=start_str, end_date=end_str,
            frequency="d", adjustflag=flag
        )
        while rs.next():
            rows.append([sym] + rs.get_row_data())
        return rows

    return _pull("2"), _pull("1")


# ---------------------------------------------------------
# 数据清洗
# ---------------------------------------------------------
def _clean_df(rows: list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows,
        columns=["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    )
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.dropna(subset=["close"], inplace=True)
    return df


# ---------------------------------------------------------
# 增量写入
# ---------------------------------------------------------
def _upsert_batch(db_path: str,
                  batch_qfq: list[pd.DataFrame],
                  batch_hfq: list[pd.DataFrame]):
    with duckdb.connect(db_path) as con:
        if batch_qfq:
            combined_qfq = pd.concat(batch_qfq, ignore_index=True)
            con.register("_tmp_qfq", combined_qfq)
            con.execute("""
                INSERT INTO daily_qfq
                    SELECT symbol, date, open, high, low, close, volume, amount
                    FROM _tmp_qfq
                ON CONFLICT (symbol, date) DO NOTHING
            """)
            con.unregister("_tmp_qfq")
        if batch_hfq:
            combined_hfq = pd.concat(batch_hfq, ignore_index=True)
            con.register("_tmp_hfq", combined_hfq)
            con.execute("""
                INSERT INTO daily_hfq
                    SELECT symbol, date, open, high, low, close, volume, amount
                    FROM _tmp_hfq
                ON CONFLICT (symbol, date) DO NOTHING
            """)
            con.unregister("_tmp_hfq")
        con.execute("CHECKPOINT")


# ---------------------------------------------------------
# 增量更新主流程
# ---------------------------------------------------------
def incremental_update(db_path: str) -> bool:
    now_beijing = datetime.now(CN_TZ).date()

    if not is_trading_day(now_beijing):
        log.info(f"今日 ({now_beijing}) 为非交易日，跳过拉取。")
        return False

    _ensure_tables(db_path)

    last_date = get_db_last_date(db_path)
    if last_date and last_date >= now_beijing:
        log.info(f"✅ 数据已是最新 (最新日期: {last_date})，跳过拉取。")
        return False

    start_str = (
        (last_date + timedelta(days=1)).strftime("%Y-%m-%d") if last_date
        else (now_beijing - timedelta(days=CONFIG["bootstrap_days"])).strftime("%Y-%m-%d")
    )
    end_str = now_beijing.strftime("%Y-%m-%d")
    log.info(f"🚀 增量更新范围: {start_str} → {end_str}")

    bs.login()
    try:
        stock_rs = bs.query_stock_basic()
        stock_df = stock_rs.get_data()
        symbols = [
            row[0] for row in stock_df.values
            if len(row) > 5 and row[4] == "1" and row[5] == "1"
        ]
        log.info(f"共获取 {len(symbols)} 只在市 A 股标的。")

        BATCH_SIZE = 150
        for i in tqdm(range(0, len(symbols), BATCH_SIZE), desc="📊 采集 K 线"):
            batch = symbols[i: i + BATCH_SIZE]
            batch_qfq, batch_hfq = [], []
            for sym in batch:
                try:
                    rows_qfq, rows_hfq = _fetch_symbol(sym, start_str, end_str)
                    df_qfq = _clean_df(rows_qfq)
                    df_hfq = _clean_df(rows_hfq)
                    if not df_qfq.empty:
                        batch_qfq.append(df_qfq)
                    if not df_hfq.empty:
                        batch_hfq.append(df_hfq)
                except Exception as e:
                    log.warning(f"拉取 {sym} 失败，已跳过: {e}")
            _upsert_batch(db_path, batch_qfq, batch_hfq)
    finally:
        bs.logout()

    log.info("✅ 增量数据入库完成。")
    return True


# ---------------------------------------------------------
# 选股策略 + HTML 邮件
# ---------------------------------------------------------
def run_strategy_and_email(db_path: str):
    sql = """
    WITH base AS (
        SELECT
            symbol, date, close, volume, amount,
            AVG(close)  OVER (PARTITION BY symbol ORDER BY date
                              ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            AVG(volume) OVER (PARTITION BY symbol ORDER BY date
                              ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vma20,
            LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date)       AS close_20d_ago
        FROM daily_qfq
    ),
    filtered AS (
        SELECT
            symbol,
            close,
            ROUND((close / NULLIF(close_20d_ago, 0) - 1) * 100, 2)  AS momentum_pct,
            ROUND(volume / NULLIF(vma20, 0), 2)                      AS volume_ratio,
            ROUND(amount / 1e6, 2)                                   AS amount_m
        FROM base
        WHERE date          = (SELECT MAX(date) FROM daily_qfq)
          AND close         > ma20
          AND volume        > vma20 * 1.5
          AND amount        > 30000000
          AND close_20d_ago IS NOT NULL
          AND close_20d_ago > 0
    )
    SELECT
        symbol       AS "代码",
        close        AS "现价",
        momentum_pct AS "20日涨幅%",
        volume_ratio AS "量比",
        amount_m     AS "成交额(百万)"
    FROM filtered
    ORDER BY momentum_pct DESC
    LIMIT 15
    """

    with duckdb.connect(db_path, read_only=True) as con:
        res_df = con.execute(sql).df()

    today_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    subject   = f"📈 A股选股决策报告 - {today_str}"

    if res_df.empty:
        body = f"<h3>{today_str}：今日市场未发现符合突破条件的标的，建议保持观望。</h3>"
    else:
        body = f"""
        <h2>A股量化趋势日报 &mdash; {today_str}</h2>
        <p><b>筛选逻辑：</b>
           收盘 &gt; 20日均线 ｜ 量比 &gt; 1.5 ｜ 成交额 &gt; 3000万 ｜ 20日动量排序
        </p>
        {res_df.to_html(index=False, border=0)}
        <hr/>
        <p><small>
          ⚠️ 本报告由量化系统自动生成，仅供参考，不构成投资建议。股市有风险，入市须谨慎。
        </small></p>
        """

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/>
<style>
  body  {{ font-family:'Segoe UI',sans-serif; color:#333; padding:20px; }}
  h2    {{ color:#1a1a1a; }}
  table {{ border-collapse:collapse; width:100%; margin-top:12px; }}
  th    {{ background:#1a73e8; color:#fff; padding:10px 14px; text-align:center; }}
  td    {{ border:1px solid #ddd; padding:8px 12px; text-align:center; }}
  tr:nth-child(even) {{ background:#f5f5f5; }}
  tr:hover           {{ background:#e8f0fe; }}
  hr    {{ border:none; border-top:1px solid #eee; margin:16px 0; }}
</style>
</head>
<body>{body}</body>
</html>"""

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"]    = CONFIG["email_user"]
    msg["To"]      = CONFIG["email_to"]
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        recipients = [r.strip() for r in CONFIG["email_to"].split(",") if r.strip()]
        with smtplib.SMTP(CONFIG["smtp_host"], CONFIG["smtp_port"], timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(CONFIG["email_user"], CONFIG["email_password"])
            server.sendmail(CONFIG["email_user"], recipients, msg.as_string())
        log.info("📧 策略报告已成功发送。")
    except smtplib.SMTPException as e:
        log.error(f"邮件发送失败 (SMTP): {e}")
    except Exception as e:
        log.error(f"邮件发送失败 (未知): {e}")


# ---------------------------------------------------------
# 主调度
# ---------------------------------------------------------
def main_task():
    log.info("=" * 55)
    log.info("  启动每日 A 股量化分析任务")
    log.info("=" * 55)

    tm  = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
    odc = OneDriveClient(
        tm,
        folder=CONFIG["onedrive_folder"],
        remote_gz_name=CONFIG["db_filename"] + ".gz",
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path    = os.path.join(tmp, CONFIG["db_filename"])
        local_gz   = db_path + ".gz"

        # 下载 → 解压
        if odc.download_gz(local_gz):
            log.info("🔓 正在解压缩...")
            with gzip.open(local_gz, "rb") as fi, open(db_path, "wb") as fo:
                shutil.copyfileobj(fi, fo)
            os.remove(local_gz)
            log.info(f"✅ 数据库已恢复: {db_path}")

        # 增量更新
        is_updated = incremental_update(db_path)

        # 压缩 → 上传
        if is_updated:
            log.info("📦 生成 Gzip 压缩备份...")
            with open(db_path, "rb") as fi, \
                 gzip.open(local_gz, "wb", compresslevel=6) as fo:
                shutil.copyfileobj(fi, fo)
            odc.upload_gz(local_gz)
        else:
            log.info("数据无更新，跳过云端回传。")

        # 选股 + 发邮件
        run_strategy_and_email(db_path)

    log.info("=" * 55)
    log.info("  任务完成")
    log.info("=" * 55)


# ---------------------------------------------------------
# 入口
# ---------------------------------------------------------
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"

    if mode == "auth":
        # 本地设备代码授权，无需 redirect URI
        tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
        tm.device_code_auth()

    elif mode == "export":
        # 导出 Token → 写入 GitHub Secret ONEDRIVE_TOKEN_CACHE_B64
        tm = TokenManager(CONFIG["azure_client_id"], CONFIG["token_cache_file"])
        token_str = tm.export_b64()
        print("\n" + "=" * 60)
        print("👇 请将以下内容完整设为 GitHub Secret [ONEDRIVE_TOKEN_CACHE_B64]")
        print("=" * 60 + "\n")
        print(token_str)
        print("\n" + "=" * 60)

    elif mode == "run":
        main_task()

    else:
        print(f"无效模式: '{mode}'，可用: auth | export | run")
        sys.exit(1)
