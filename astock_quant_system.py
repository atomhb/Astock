#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, base64, logging, tempfile, gzip, shutil, requests, smtplib, json, re
import msal, duckdb
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from tqdm import tqdm

# =========================================================
# 1. 环境配置与日志初始化
# =========================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# 强制锁定北京时区
CN_TZ = timezone(timedelta(hours=8))
GRAPH_ENDPOINT = 'https://graph.microsoft.com/v1.0'
SCOPES = ['Files.ReadWrite.All', 'offline_access']

def _get_env(key, default=""):
    return os.getenv(key, default).strip()

CONFIG = {
    "azure_client_id":  _get_env("AZURE_CLIENT_ID"),
    "onedrive_folder":  _get_env("ONEDRIVE_FOLDER", "Stock"),
    "db_filename":      _get_env("DB_FILENAME", "A_stock.duckdb"),
    "email_user":       _get_env("EMAIL_USER"),
    "email_password":   _get_env("EMAIL_PASSWORD"),
    "email_to":         _get_env("EMAIL_TO"),
    "smtp_host":        _get_env("SMTP_HOST", "smtp.office365.com"),
    "smtp_port":        int(_get_env("SMTP_PORT", "587")),
}

# =========================================================
# 2. 微软 Graph API 管理器 (工业级实现)
# =========================================================
class GraphManager:
    def __init__(self):
        self.cache_file = "token_cache.bin"
        self.token_cache = msal.SerializableTokenCache()
        
        # 尝试从环境变量注入 Token (解决 Padding 报错的关键就在这)
        b64_token = os.getenv("ONEDRIVE_TOKEN_CACHE_B64")
        if b64_token:
            try:
                # 强效清洗：去除换行、空格，自动补齐等号
                clean_b64 = re.sub(r'[^A-Za-z0-9+/=]', '', b64_token)
                clean_b64 += '=' * (-len(clean_b64) % 4)
                decoded_token = base64.b64decode(clean_b64).decode('utf-8')
                self.token_cache.deserialize(decoded_token)
                log.info("✅ 已从环境变量恢复 Token 缓存")
            except Exception as e:
                log.error(f"❌ 环境变量 Token 解析失败: {e}")

        # 如果本地有缓存文件也加载
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r") as f:
                self.token_cache.deserialize(f.read())

        self.msal_app = msal.PublicClientApplication(
            CONFIG["azure_client_id"],
            authority="https://login.microsoftonline.com/common",
            token_cache=self.token_cache
        )

    def _save_cache(self):
        if self.token_cache.has_state_changed:
            with open(self.cache_file, "w") as f:
                f.write(self.token_cache.serialize())

    def get_token(self):
        accounts = self.msal_app.get_accounts()
        result = None
        if accounts:
            # 自动尝试使用 Refresh Token 刷新
            result = self.msal_app.acquire_token_silent(SCOPES, account=accounts[0])
        
        if not result:
            if os.getenv("GITHUB_ACTIONS"):
                raise RuntimeError("❌ GitHub 环境中 Token 失效且无法刷新，请在本地重新 export")
            # 本地环境下发起设备代码流登录
            flow = self.msal_app.initiate_device_flow(scopes=SCOPES)
            print(flow['message'])
            result = self.msal_app.acquire_token_by_device_flow(flow)
        
        if "access_token" in result:
            self._save_cache()
            return result["access_token"]
        else:
            raise RuntimeError(f"❌ 获取 Token 失败: {result.get('error_description')}")

    def download_db(self, local_path):
        """从云端下载并解压"""
        token = self.get_token()
        remote_path = f"{CONFIG['onedrive_folder']}/{CONFIG['db_filename']}.gz"
        url = f"{GRAPH_ENDPOINT}/me/drive/root:/{remote_path}:/content"
        
        log.info(f"⬇️ 正在从云端下载数据库...")
        r = requests.get(url, headers={'Authorization': f'Bearer {token}'}, stream=True)
        
        if r.status_code == 200:
            temp_gz = local_path + ".gz"
            with open(temp_gz, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            # 解压
            with gzip.open(temp_gz, 'rb') as f_in, open(local_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(temp_gz)
            log.info("✅ 数据库解压完成")
            return True
        else:
            log.info("ℹ️ 云端暂无备份，将初始化新数据库")
            return False

    def upload_db(self, local_path):
        """压缩并分块上传 (支持大文件)"""
        token = self.get_token()
        remote_path = f"{CONFIG['onedrive_folder']}/{CONFIG['db_filename']}.gz"
        
        # 1. 压缩
        local_gz = local_path + ".gz"
        with open(local_path, 'rb') as f_in, gzip.open(local_gz, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
        
        file_size = os.path.getsize(local_gz)
        
        # 2. 创建上传会话 (适用于大文件)
        create_session_url = f"{GRAPH_ENDPOINT}/me/drive/root:/{remote_path}:/createUploadSession"
        session_res = requests.post(
            create_session_url, 
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            json={'item': {'@microsoft.graph.conflictBehavior': 'replace'}}
        )
        
        if session_res.status_code != 200:
            raise RuntimeError(f"无法创建上传会话: {session_res.text}")
            
        upload_url = session_res.json()['uploadUrl']
        
        # 3. 分块上传
        log.info(f"⬆️ 正在同步至云端 ({file_size/1024/1024:.2f} MB)...")
        with open(local_gz, 'rb') as f:
            chunk_size = 3276800 # 约 3.1MB
            while True:
                start = f.tell()
                chunk = f.read(chunk_size)
                if not chunk: break
                end = f.tell()
                headers = {
                    'Content-Range': f'bytes {start}-{end-1}/{file_size}',
                    'Content-Length': str(len(chunk))
                }
                requests.put(upload_url, headers=headers, data=chunk)
        
        os.remove(local_gz)
        log.info("✅ 云端同步成功")

# =========================================================
# 3. 业务逻辑 (Baostock + DuckDB)
# =========================================================
def incremental_update(db_path):
    """增量拉取 K 线数据"""
    now_bj = datetime.now(CN_TZ).date()
    
    with duckdb.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_qfq (
                symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, 
                low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE, 
                PRIMARY KEY(symbol, date)
            )
        """)
        r = con.execute("SELECT MAX(date) FROM daily_qfq").fetchone()
        last_date = r[0] if r and r[0] else None

    # 判断今天是否已更新
    if last_date and last_date >= now_bj:
        log.info(f"✅ 数据已更新至 {last_date}，无需重复拉取")
        return False

    start_str = (last_date + timedelta(days=1)).strftime("%Y-%m-%d") if last_date else \
                (now_bj - timedelta(days=90)).strftime("%Y-%m-%d")
    end_str = now_bj.strftime("%Y-%m-%d")

    log.info(f"🚀 增量拉取范围: {start_str} ~ {end_str}")
    bs.login()
    stock_df = bs.query_stock_basic().get_data()
    symbols = [row[0] for row in stock_df.values if row[4] == '1' and row[5] == '1']
    
    batch_size = 200
    for i in tqdm(range(0, len(symbols), batch_size), desc="数据采集"):
        batch = symbols[i : i + batch_size]
        rows = []
        for sym in batch:
            rs = bs.query_history_k_data_plus(sym, "date,open,high,low,close,volume,amount", 
                                              start_date=start_str, end_date=end_str, 
                                              frequency="d", adjustflag="2")
            while rs.next(): rows.append([sym] + rs.get_row_data())
        if rows:
            df = pd.DataFrame(rows, columns=['symbol','date','open','high','low','close','vol','amount'])
            df[['open','high','low','close','vol','amount']] = df[['open','high','low','close','vol','amount']].apply(pd.to_numeric)
            df['date'] = pd.to_datetime(df['date']).dt.date
            with duckdb.connect(db_path) as con:
                con.execute("INSERT INTO daily_qfq SELECT * FROM df ON CONFLICT DO NOTHING")
    bs.logout()
    return True

def run_strategy_and_email(db_path):
    """选股策略：20日多头排列 + 放量突破 + 动量前 15 名"""
    sql = """
    WITH base AS (
        SELECT symbol, date, close, vol,
            AVG(close) OVER(PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as ma20,
            AVG(vol) OVER(PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as vma20,
            LAG(close, 20) OVER(PARTITION BY symbol ORDER BY date) as c20
        FROM daily_qfq
    )
    SELECT symbol as '代码', close as '收盘', 
           ROUND((close/NULLIF(c20,0)-1)*100, 2) as '20日涨幅%', 
           ROUND(vol/NULLIF(vma20,0), 2) as '量比'
    FROM base WHERE date = (SELECT MAX(date) FROM daily_qfq)
    AND close > ma20 AND vol > vma20 * 1.5 AND (close/NULLIF(c20,0)) > 1.05
    ORDER BY '20日涨幅%' DESC LIMIT 15
    """
    with duckdb.connect(db_path, read_only=True) as con:
        res_df = con.execute(sql).df()
    
    today_str = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    subject = f"📈 量化选股日报 - {today_str}"
    
    html = f"""
    <html>
    <body style="font-family: sans-serif;">
        <h2 style="color: #2c3e50;">A股量化趋势突破池 - {today_str}</h2>
        <p>策略：20日线支撑 + 1.5倍放量 + 动量突围</p>
        {res_df.to_html(index=False, border=0, classes='table')}
        <style>
            .table {{ border-collapse: collapse; width: 100%; }}
            .table th {{ background-color: #f2f2f2; padding: 8px; text-align: center; border: 1px solid #ddd; }}
            .table td {{ padding: 8px; text-align: center; border: 1px solid #ddd; }}
        </style>
    </body>
    </html>
    """
    
    msg = MIMEMultipart()
    msg["Subject"], msg["From"], msg["To"] = subject, CONFIG["email_user"], CONFIG["email_to"]
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(CONFIG["smtp_host"], CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(CONFIG["email_user"], CONFIG["email_password"])
            server.sendmail(CONFIG["email_user"], [CONFIG["email_to"]], msg.as_string())
        log.info("📧 邮件发送成功")
    except Exception as e:
        log.error(f"❌ 邮件发送失败: {e}")

# =========================================================
# 4. 主程序入口
# =========================================================
def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    gm = GraphManager()

    if mode == "auth":
        gm.get_token()
        print("\n✅ 授权成功！本地 token_cache.bin 已更新。")
    elif mode == "export":
        if not os.path.exists("token_cache.bin"):
            print("❌ 错误：请先运行 auth 模式")
            return
        with open("token_cache.bin", "rb") as f:
            b64 = base64.b64encode(f.read()).decode('utf-8')
            print("\n" + "="*60 + "\n👇 请复制下方内容到 GitHub Secret: ONEDRIVE_TOKEN_CACHE_B64 👇\n")
            print(b64)
            print("\n" + "="*60)
    elif mode == "run":
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, CONFIG["db_filename"])
            # 执行下载 -> 更新 -> 策略 -> 上传
            gm.download_db(db_path)
            if incremental_update(db_path):
                gm.upload_db(db_path)
            run_strategy_and_email(db_path)
            log.info("🏁 任务全部完成")

if __name__ == "__main__":
    main()
