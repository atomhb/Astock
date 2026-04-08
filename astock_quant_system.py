#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, base64, logging, tempfile, gzip, shutil, requests, smtplib
import duckdb
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta, date, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from O365 import Account, FileSystemTokenBackend
from tqdm import tqdm

load_dotenv()

# =========================================================
# 1. 基础配置与日志
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# 锁定北京时区
CN_TZ = timezone(timedelta(hours=8))
IS_CI = os.getenv("GITHUB_ACTIONS") == "true"

def _get_env(key, default=""):
    return os.getenv(key, default).strip()

CONFIG = {
    "azure_client_id":  _get_env("AZURE_CLIENT_ID"),
    "onedrive_folder":  _get_env("ONEDRIVE_FOLDER", "Stock"),
    "db_filename":      _get_env("DB_FILENAME", "A_stock.duckdb"),
    "token_cache_file": "o365_token.txt", # 统一由外部文件提供
    "smtp_host":        _get_env("SMTP_HOST", "smtp.office365.com"),
    "smtp_port":        int(_get_env("SMTP_PORT", "587")),
    "email_user":       _get_env("EMAIL_USER"),
    "email_password":   _get_env("EMAIL_PASSWORD"),
    "email_to":         _get_env("EMAIL_TO"),
    "bootstrap_days":   90,
}

# =========================================================
# 2. OneDrive 管理模块 (支持自动刷新与压缩)
# =========================================================
class OneDriveManager:
    def __init__(self, cfg):
        self.cfg = cfg
        self.remote_gz_name = cfg["db_filename"] + ".gz"
        self.scopes = ["Files.ReadWrite.All", "offline_access"]
        
        # 路径处理
        token_abs = os.path.abspath(cfg["token_cache_file"])
        self.token_backend = FileSystemTokenBackend(
            token_path=os.path.dirname(token_abs),
            token_filename=os.path.basename(token_abs)
        )
        
        # 初始化账户
        self.account = Account(
            cfg["azure_client_id"],
            auth_flow_type="public",
            token_backend=self.token_backend,
            scopes=self.scopes
        )

    def ensure_auth(self):
        """核心：验证并自动刷新 Token"""
        exists = self.token_backend.load_token()
        
        # 检查当前 Access Token 是否有效
        if self.account.is_authenticated:
            log.info("✅ Token 当前有效。")
            return

        # 如果失效但文件存在，尝试自动刷新
        if exists:
            log.info("🔄 Access Token 已过期，正在尝试使用 Refresh Token 续期...")
            try:
                if self.account.connection.refresh_token():
                    log.info("✅ Token 自动续期成功。")
                    return
                else:
                    log.error("❌ 自动续期返回失败。")
            except Exception as e:
                log.error(f"❌ 自动续期异常: {e}")

        # 如果是 CI 环境且走到这，说明必须重新授权
        if IS_CI:
            raise RuntimeError("❌ 令牌已失效且无法自动刷新。请在本地运行 auth 并更新 GitHub Secret。")

        # 本地环境交互式授权
        log.info("🔑 正在启动浏览器进行授权...")
        if not self.account.authenticate(scopes=self.scopes):
            raise RuntimeError("授权失败。")

    def download_db(self, local_path) -> bool:
        """从云端获取压缩库并解压"""
        self.ensure_auth()
        drive = self.account.storage().get_default_drive()
        try:
            folder = drive.get_item_by_path(f"/{self.cfg['onedrive_folder']}")
            item = next((i for i in folder.get_items() if i.name == self.remote_gz_name), None)
            if not item:
                log.info("OneDrive 上暂无数据库压缩文件，视为首次运行。")
                return False
            
            temp_gz = local_path + ".gz"
            log.info(f"⬇️ 正在下载: {self.remote_gz_name} ({item.size/1024/1024:.2f} MB)")
            
            # 执行下载
            with open(temp_gz, "wb") as f:
                item.download(output=f)
            
            # 解压缩
            with gzip.open(temp_gz, "rb") as f_in, open(local_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(temp_gz)
            return True
        except Exception as e:
            log.warning(f"下载失败: {e}")
            return False

    def upload_db(self, local_path):
        """压缩本地库并回传"""
        self.ensure_auth()
        drive = self.account.storage().get_default_drive()
        try:
            folder = drive.get_item_by_path(f"/{self.cfg['onedrive_folder']}")
        except:
            folder = drive.get_root_folder().create_child_folder(self.cfg['onedrive_folder'])
            
        local_gz = local_path + ".gz"
        log.info("📦 正在生成 Gzip 压缩包...")
        with open(local_path, "rb") as f_in, gzip.open(local_gz, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        
        log.info(f"⬆️ 上传至云端: {self.remote_gz_name} ({os.path.getsize(local_gz)/1024/1024:.2f} MB)")
        folder.upload_file(local_gz, item_name=self.remote_gz_name)
        os.remove(local_gz)

    def export_token_b64(self) -> str:
        """导出本地 Token 文件为 Base64 字符串"""
        path = os.path.abspath(self.cfg["token_cache_file"])
        if not os.path.exists(path):
            raise FileNotFoundError("未找到 Token 文件，请先运行 auth。")
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

# =========================================================
# 3. 数据采集与增量逻辑
# =========================================================
def get_db_last_date(db_path):
    with duckdb.connect(db_path, read_only=True) as con:
        exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'daily_qfq'").fetchone()[0]
        if not exists: return None
        r = con.execute("SELECT MAX(date) FROM daily_qfq").fetchone()
        return r[0] if r and r[0] else None

def incremental_update(db_path):
    now_beijing = datetime.now(CN_TZ).date()
    
    # 确保表结构
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_qfq (
                symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, 
                low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE, 
                PRIMARY KEY(symbol, date)
            )
        """)
    
    last = get_db_last_date(db_path)
    
    # 核心判断逻辑
    if last and last >= now_beijing:
        log.info(f"✅ 数据库已经是最新 ({last})，无需拉取。")
        return False

    start_str = (last + timedelta(days=1)).strftime("%Y-%m-%d") if last else \
                (now_beijing - timedelta(days=CONFIG["bootstrap_days"])).strftime("%Y-%m-%d")
    end_str = now_beijing.strftime("%Y-%m-%d")

    log.info(f"🚀 开始增量拉取: {start_str} ~ {end_str}")
    
    bs.login()
    stock_df = bs.query_stock_basic().get_data()
    symbols = [row[0] for row in stock_df.values if row[4] == '1' and row[5] == '1']
    
    batch_size = 150
    for i in tqdm(range(0, len(symbols), batch_size), desc="K线采集"):
        batch = symbols[i : i + batch_size]
        rows = []
        for sym in batch:
            k_rs = bs.query_history_k_data_plus(sym, "date,open,high,low,close,volume,amount", 
                                                start_date=start_str, end_date=end_str, 
                                                frequency="d", adjustflag="2")
            while k_rs.next():
                rows.append([sym] + k_rs.get_row_data())
        
        if rows:
            df = pd.DataFrame(rows, columns=['symbol','date','open','high','low','close','vol','amount'])
            df[['open','high','low','close','vol','amount']] = df[['open','high','low','close','vol','amount']].apply(pd.to_numeric)
            df['date'] = pd.to_datetime(df['date']).dt.date
            with duckdb.connect(db_path) as con:
                con.execute("INSERT INTO daily_qfq SELECT * FROM df ON CONFLICT DO NOTHING")
    bs.logout()
    return True

# =========================================================
# 4. 选股策略与 HTML 汇报
# =========================================================
def run_strategy_and_email(db_path):
    sql = """
    WITH base AS (
        SELECT symbol, date, close, vol, amount,
            AVG(close) OVER(PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as ma20,
            AVG(vol) OVER(PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as vma20,
            LAG(close, 20) OVER(PARTITION BY symbol ORDER BY date) as c20
        FROM daily_qfq
    )
    SELECT symbol as '代码', close as '现价', ROUND((close/c20-1)*100, 2) as '20日涨跌%', ROUND(vol/vma20, 2) as '量比'
    FROM base WHERE date = (SELECT MAX(date) FROM daily_qfq)
    AND close > ma20 AND vol > vma20 * 1.5 AND (close/c20) > 1.05
    ORDER BY '20日涨跌%' DESC LIMIT 15
    """
    with duckdb.connect(db_path, read_only=True) as con:
        res_df = con.execute(sql).df()
    
    today_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    subject = f"📈 选股决策报告 - {today_str}"
    
    if res_df.empty:
        html = f"<h3>{today_str}：今日市场未发现符合右侧突破条件的信号。</h3>"
    else:
        html = f"""
        <html><body>
        <h2>A股量化趋势日报 - {today_str}</h2>
        <p><b>筛选策略：</b>20日均线上方 + 动量突围 + 放量1.5倍突破</p>
        {res_df.to_html(index=False)}
        </body></html>
        """

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"], msg["To"] = CONFIG["email_user"], CONFIG["email_to"]
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(CONFIG["smtp_host"], CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(CONFIG["email_user"], CONFIG["email_password"])
            server.sendmail(CONFIG["email_user"], [CONFIG["email_to"]], msg.as_string())
        log.info("📧 报告邮件发送成功。")
    except Exception as e:
        log.error(f"邮件发送失败: {e}")

# =========================================================
# 5. 主任务调度
# =========================================================
def main_task():
    log.info("=======================================================")
    log.info("   启动每日 A 股量化分析任务")
    log.info("=======================================================")
    
    odm = OneDriveManager(CONFIG)
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, CONFIG["db_filename"])
        
        # 下载
        odm.download_db(db_path)
        
        # 更新
        is_updated = incremental_update(db_path)
        
        # 如果更新了，回传备份
        if is_updated:
            odm.upload_db(db_path)
            
        # 筛选与发信
        run_strategy_and_email(db_path)
        
    log.info("=======================================================")
    log.info("   任务完成")
    log.info("=======================================================")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    
    if mode == "auth":
        OneDriveManager(CONFIG).ensure_auth()
        print("✅ 授权成功！")
    elif mode == "export":
        print(OneDriveManager(CONFIG).export_token_b64())
    elif mode == "run":
        main_task()
