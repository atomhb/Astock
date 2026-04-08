#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, base64, logging, tempfile, gzip, shutil, requests
import smtplib, duckdb, baostock as bs
import pandas as pd
from datetime import datetime, timedelta, date, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from O365 import Account, FileSystemTokenBackend
from O365.utils import EnvTokenBackend
from tqdm import tqdm
load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

CN_TZ = timezone(timedelta(hours=8))

# ---------------------------------------------------------
# CI 环境检测（模块级，全局可用）
# ---------------------------------------------------------
IS_CI = (
    os.getenv("CI", "").lower() in ("true", "1", "yes")
    or os.getenv("GITHUB_ACTIONS", "").lower() in ("true", "1")
)

# EnvTokenBackend 使用的环境变量键名
_ENV_TOKEN_KEY = "O365_TOKEN_CACHE"


# ---------------------------------------------------------
# 配置
# ---------------------------------------------------------
def _get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# __file__ 在 Jupyter 环境中不存在，安全降级到当前目录
try:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _BASE_DIR = os.path.abspath(".")

CONFIG = {
    "azure_client_id":  _get_env("AZURE_CLIENT_ID"),
    "onedrive_folder":  _get_env("ONEDRIVE_FOLDER", "Stock"),
    "db_filename":      _get_env("DB_FILENAME", "A_stock.duckdb"),
    # 本地 token 文件使用脚本同目录的绝对路径
    "token_cache_file": os.path.join(
        _BASE_DIR, _get_env("TOKEN_CACHE_FILE", "o365_token.txt")
    ),
    "smtp_host":        _get_env("SMTP_HOST", "smtp.office365.com"),
    "smtp_port":        int(_get_env("SMTP_PORT", "587")),
    "email_user":       _get_env("EMAIL_USER"),
    "email_password":   _get_env("EMAIL_PASSWORD"),
    "email_to":         _get_env("EMAIL_TO"),
    "bootstrap_days":   90,
    "min_history_days": 60,
    "pool_size":        15,
}


# ---------------------------------------------------------
# Token 注入（GitHub Actions 专用）
# ✅ 核心修改：
#   1. 失败时绝不删除本地文件
#   2. CI 环境使用 EnvTokenBackend，完全绕开文件读写
#   3. 解码后内容直接写入内存环境变量，不再落盘
# ---------------------------------------------------------
_b64_raw = os.getenv("ONEDRIVE_TOKEN_CACHE_B64", "").strip()
if _b64_raw and IS_CI:
    try:
        _b64_clean = _b64_raw.replace("\\n", "").replace("\n", "").replace("\r", "").replace(" ", "")
        _token_json = base64.b64decode(_b64_clean).decode("utf-8")
        if "{" not in _token_json:
            raise ValueError("Token 内容格式非法，不包含 JSON 特征。")
        # 将 JSON 直接存入环境变量，供 EnvTokenBackend 读取
        os.environ[_ENV_TOKEN_KEY] = _token_json
        log.info("✅ Token 已注入至内存环境变量（EnvTokenBackend 模式）。")
    except Exception as e:
        # ✅ 只报错，绝不删文件，让后续流程自然失败并给出明确提示
        log.error(f"❌ Token 注入失败: {e}")


# ---------------------------------------------------------
# OneDrive 管理
# ---------------------------------------------------------
class OneDriveManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.remote_gz_name = cfg["db_filename"] + ".gz"

        if IS_CI:
            # CI: 直接从内存环境变量加载，零文件 I/O
            self.token_backend = EnvTokenBackend(token_env_name=_ENV_TOKEN_KEY)
            log.info("🔧 Token 后端: EnvTokenBackend（CI 模式）")
        else:
            # 本地: 使用文件系统，绝对路径避免工作目录漂移
            token_abs  = os.path.abspath(cfg["token_cache_file"])
            token_dir  = os.path.dirname(token_abs)
            token_file = os.path.basename(token_abs)
            self.token_backend = FileSystemTokenBackend(
                token_path=token_dir,
                token_filename=token_file,
            )
            log.info(f"🔧 Token 后端: FileSystemTokenBackend → {token_abs}")

        self.account = Account(
            (cfg["azure_client_id"],),
            auth_flow_type="public",
            token_backend=self.token_backend,
        )

    def ensure_auth(self):
        """
        验证 Token 有效性。
        - 本地：Token 无效则打开浏览器完成交互式授权。
        - CI  ：Token 无效直接报错，不尝试交互。
        """
        try:
            authenticated = self.account.is_authenticated
        except ValueError as e:
            # ✅ 新版 O365 对旧格式 token 文件抛 ValueError
            # "The token you are trying to load is not valid anymore."
            log.error(
                f"❌ Token 文件格式已过时（O365 库升级后不再兼容旧格式）。\n"
                f"   错误: {e}\n"
                f"   请删除旧 token 文件后重新运行 `python script.py auth`：\n"
                f"   rm {self.cfg['token_cache_file']}"
            )
            authenticated = False

        if authenticated:
            log.info("✅ Token 有效，无需重新授权。")
            return

        if IS_CI:
            raise RuntimeError(
                "❌ CI 环境 Token 无效或已过期！\n"
                "请在本地执行以下步骤后更新 GitHub Secret：\n"
                "  1. python script.py auth    ← 浏览器授权\n"
                "  2. python script.py export  ← 复制输出字符串\n"
                "  3. 粘贴到 GitHub Secret: ONEDRIVE_TOKEN_CACHE_B64"
            )

        # 本地交互式授权
        log.info("🔑 未发现有效 Token，启动本地交互式授权（将打开浏览器）...")
        if not self.account.authenticate(
            scopes=["Files.ReadWrite.All", "offline_access"]
        ):
            raise RuntimeError("OneDrive 授权失败，请重试。")
        log.info("✅ 授权成功，Token 已保存。")

    def export_token_b64(self) -> str:
        """
        将本地 Token 文件直接读取并进行 Base64 编码，用于 GitHub Secret。
        这是最稳健的方法，避开了 SDK 内部序列化器的版本兼容性问题。
        """
        token_path = os.path.abspath(self.cfg["token_cache_file"])
        
        if not os.path.exists(token_path):
            raise FileNotFoundError(
                f"❌ 未找到 Token 文件: {token_path}\n"
                f"   请先运行 `python astock_quant_system.py auth` 完成授权。"
            )

        # 直接读取文件字节流
        with open(token_path, "rb") as f:
            token_bytes = f.read()

        # 验证一下读取的内容是否为有效的 JSON，防止把空文件发出去
        import json
        try:
            json.loads(token_bytes)
        except json.JSONDecodeError:
            raise ValueError(f"❌ Token 文件内容损坏或不是有效的 JSON，请删除 {token_path} 后重新授权。")

        # 返回 Base64 字符串
        return base64.b64encode(token_bytes).decode("utf-8")

    def _get_folder(self, create: bool = True):
        self.ensure_auth()
        drive = self.account.storage().get_default_drive()
        folder_name = self.cfg["onedrive_folder"]
        try:
            return drive.get_item_by_path(f"/{folder_name}")
        except Exception:
            if create:
                log.info(f"云端文件夹 '{folder_name}' 不存在，正在创建...")
                return drive.get_root_folder().create_child_folder(folder_name)
            return None

    def download_db(self, local_db_path: str) -> bool:
        """从 OneDrive 下载并解压数据库"""
        folder = self._get_folder(create=False)
        if not folder:
            log.info("云端文件夹不存在，视为首次运行。")
            return False

        target_item = None
        for item in folder.get_items():
            if item.name == self.remote_gz_name:
                target_item = item
                break

        if not target_item:
            log.info("云端暂无压缩数据库备份，视为首次运行。")
            return False

        size_mb = target_item.size / 1024 / 1024
        log.info(f"⬇️  正在下载: {target_item.name} ({size_mb:.2f} MB)")

        temp_gz = local_db_path + ".gz"

        # ✅ 使用 O365 官方 download(output=) 接口，无需访问私有属性
        with open(temp_gz, "wb") as f:
            ok = target_item.download(output=f, chunk_size=1024 * 1024)

        if not ok:
            if os.path.exists(temp_gz):
                os.remove(temp_gz)
            raise RuntimeError("❌ OneDrive 文件下载失败，请检查权限或网络连接。")

        log.info("🔓 正在解压缩...")
        with gzip.open(temp_gz, "rb") as f_in, open(local_db_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(temp_gz)
        log.info(f"✅ 数据库已恢复: {local_db_path}")
        return True

    def upload_db(self, local_db_path: str):
        """压缩并上传至 OneDrive"""
        folder = self._get_folder(create=True)
        local_gz = local_db_path + ".gz"

        log.info("📦 生成 Gzip 压缩备份...")
        with open(local_db_path, "rb") as f_in, \
             gzip.open(local_gz, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

        gz_size = os.path.getsize(local_gz) / 1024 / 1024
        log.info(f"⬆️  上传: {self.remote_gz_name} ({gz_size:.2f} MB)")
        folder.upload_file(local_gz, item_name=self.remote_gz_name)
        os.remove(local_gz)
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
# Baostock 拉取单只股票（前复权 + 后复权）
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
# ✅ 使用 con.register() 显式注册 DataFrame
# ---------------------------------------------------------
def _upsert_batch(db_path: str, batch_qfq: list[pd.DataFrame], batch_hfq: list[pd.DataFrame]):
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
    subject = f"📈 A股选股决策报告 - {today_str}"

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

    odm = OneDriveManager(CONFIG)

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, CONFIG["db_filename"])

        odm.download_db(db_path)
        is_updated = incremental_update(db_path)

        if is_updated:
            odm.upload_db(db_path)
        else:
            log.info("数据无更新，跳过云端回传。")

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
        # 本地首次/重新授权
        odm = OneDriveManager(CONFIG)
        odm.ensure_auth()
        log.info("✅ 授权成功，Token 已保存。")

    elif mode == "export":
        # 导出 Token → 用于 GitHub Secret
        odm = OneDriveManager(CONFIG)
        token_str = odm.export_token_b64()
        print("\n" + "=" * 60)
        print("👇 请将以下内容完整设为 GitHub Secret [ONEDRIVE_TOKEN_CACHE_B64]")
        print("\n"*3)
        print(token_str)
        print("\n"*3)
        print('上面全部')

    elif mode == "run":
        main_task()

    else:
        print(f"无效模式: '{mode}'，可用: auth | export | run")
        sys.exit(1)
