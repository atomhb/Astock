# 📈 A 股每日简单量化分析系统

> 基于 **Baostock + DuckDB + OneDrive + GitHub Actions + Outlook SMTP** 的 A 股日常量化分析脚本。  
> 系统会自动同步数据库、增量更新行情、执行简单选股策略，并通过邮件发送日报。

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-自动运行-2088FF?logo=github-actions&logoColor=white)
![OneDrive](https://img.shields.io/badge/OneDrive-云端备份-0078D4?logo=microsoft-onedrive&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-本地数据库-FFF000?logo=duckdb&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green)

---

## ✨ 功能简介

本项目支持以下自动化流程：

- ☁️ 从 **OneDrive** 下载历史 DuckDB 数据库备份
- 📥 使用 **Baostock** 增量更新 A 股日线数据
- 🗄️ 同时维护两张行情表：
  - `daily_qfq`：前复权日线
  - `daily_hfq`：后复权日线
- 🔍 使用 DuckDB 执行简单量化筛选策略
- 📧 生成 HTML 格式日报并通过邮箱发送
- ⬆️ 更新后的数据库重新压缩上传到 OneDrive
- 💻 支持本地手动运行
- 🤖 支持 GitHub Actions 每个交易日自动运行
- 📝 自动生成运行日志，便于排错

---

## 📁 项目结构

```text
.
├── astock_quant_system.py      # 🐍 主程序
├── requirements.txt            # 📦 Python 依赖
├── .env                        # 🔐 本地环境变量（不要上传到 GitHub）
├── .gitignore                  # 🚫 Git 忽略规则
└── .github
    └── workflows
        └── stock_daily.yml     # ⚙️ GitHub Actions 定时任务
```

---

## 🧠 策略逻辑

当前内置策略基于 `daily_qfq` 前复权数据：

| 条件 | 说明 |
|------|------|
| 📊 收盘价 > 20 日均线 | 趋势向上过滤 |
| 🔊 今日成交量 > 20 日均量 × 1.5 | 放量突破过滤 |
| 💰 成交额 > 3000 万 | 流动性过滤 |
| 🚀 按 20 日涨幅从高到低排序 | 强势动量排序 |
| 🏆 取前 15 个标的 | 输出结果数量 |

> ⚠️ **注意：本项目仅用于学习和研究，不构成投资建议。**

---

## 🛠️ 环境要求

| 依赖 | 说明 |
|------|------|
| 🐍 Python 3.12 | 运行环境 |
| 🏢 Microsoft 账号 | 用于 OneDrive 云端备份 |
| 🔑 Azure App Registration | 用于 OneDrive API 授权 |
| 📬 Outlook / Microsoft 邮箱 | 用于 SMTP 发信 |
| 🐙 GitHub 仓库 | 用于 Actions 自动运行 |

---

## 🗄️ 数据库结构

两张主表结构相同，主键为 `(symbol, date)`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR | 股票代码 |
| `date` | DATE | 交易日期 |
| `open` | DOUBLE | 开盘价 |
| `high` | DOUBLE | 最高价 |
| `low` | DOUBLE | 最低价 |
| `close` | DOUBLE | 收盘价 |
| `volume` | DOUBLE | 成交量 |
| `amount` | DOUBLE | 成交额 |

---
---

## 🗺️ 后续可扩展方向

- 📐 增加更多技术指标（MACD、RSI、布林带）
- 🔔 增加涨停板、连板、换手率等过滤条件
- 📊 增加 K 线图表输出
- 📎 增加附件形式邮件
- 📋 增加多策略日报对比
- 🌏 增加港股 / 美股支持
- 🖥️ 增加 Web 面板查看选股结果

---

## ⚠️ 免责声明

本项目仅用于技术研究、自动化练习与量化策略测试。  
所有选股结果仅供参考，**不构成任何投资建议**。  
市场有风险，投资需谨慎。

---

## 📄 License

MIT License © 2026
