# 📈 A Stock

> 基于 **investment_data + DuckDB + OneDrive + GitHub Actions + Outlook SMTP** 的 A 股自动化量化分析脚本。  
> 系统每日自动同步数据库、增量更新行情、执行动量/量能/布林带选股策略，并通过邮件发送日报。


![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-自动运行-2088FF?logo=github-actions&logoColor=white)
![OneDrive](https://img.shields.io/badge/OneDrive-云端备份-0078D4?logo=microsoft-onedrive&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-本地数据库-FFF000?logo=duckdb&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-green)

---

## ✨ 功能简介

| 模块 | 说明 |
|------|------|
| 📦 数据源 | 每日唯一依赖 [chenditc/investment_data](https://github.com/chenditc/investment_data)（Qlib 格式）|
| 🗄️ 存储 | DuckDB 本地文件，gzip 压缩后上传 OneDrive，节省传输带宽 |
| 🔄 增量更新 | 比对最近 5 个交易日，仅拉取缺失数据 |
| 📐 复权处理 | 基于 `close / adjclose` 动态构建前复权（qfq）/ 后复权（hfq）视图 |
| 🎯 选股策略 | 动量因子 + 量能突破 + 布林带收口，信号使用前复权数据 |
| 💰 收益计算 | 买卖收益率、止盈止损判断均使用后复权数据 |
| 📧 日报推送 | GitHub Actions 定时触发，结果通过 Outlook SMTP 发送邮件 |

---
## 📁 项目结构

```text
.
├── astock.py      # 🐍 主程序
├── requirements.txt            # 📦 Python 依赖

---

## 🧠 策略逻辑

当前内置策略基于 `daily_qfq` 前复权数据：

| 条件 | 说明 |
|------|------|
| 📊 收盘价 > 20 日均线 | 趋势向上过滤 |
| 🔊 今日成交量 > 20 日均量 × 1.5 | 放量突破过滤 |
| 💰 成交额 > 3000 万 | 流动性过滤 |
| 🚀 按 20 日涨幅从高到低排序 | 强势动量排序 |
| 🏆 取前 20 个标的 | 输出结果数量 |

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

## 🗺️ 后续可扩展方向

- 📐 增加更多技术指标
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

## 🙏 致谢

本项目的实现离不开以下优秀的开源项目与服务，特此致谢：

- **[chenditc/investment_data](https://github.com/chenditc/investment_data)**  
  提供高质量、持续更新的 A 股 Qlib 格式行情数据，是本项目每日数据同步的**唯一数据源**。  
  感谢 [@chenditc](https://github.com/chenditc) 及所有贡献者的长期维护与无私分享。

- **[DuckDB](https://github.com/duckdb/duckdb)**  
  轻量级嵌入式 OLAP 数据库，让本地量化分析无需部署任何服务即可高效运行。

- **[Qlib](https://github.com/microsoft/qlib)**  
  微软开源量化投资平台，提供数据加载、因子计算与回测的完整基础设施。

- **[GitHub Actions](https://github.com/features/actions)**  
  免费的 CI/CD 平台，支持定时调度，让全自动化日报零成本运行于云端。

- **[MSAL Python](https://github.com/AzureAD/microsoft-authentication-library-for-python)**  
  Microsoft 官方 Python 认证库，为 OneDrive（Microsoft Graph API）提供稳定的 OAuth2 支持。

- **[python-dotenv](https://github.com/theskumar/python-dotenv)**  
  简洁的环境变量管理工具，支持本地开发与 CI 环境的配置切换。

## 📄 License

MIT License © 2026
