# 加密货币量化交易机器人

一个基于Python的加密货币量化交易系统，支持多交易所、实时数据获取和策略回测。

## 项目特性

- 🚀 **多交易所支持**: 基于ccxt统一接口，支持币安、OKX等主流交易所
- 📊 **实时数据**: 支持WebSocket实时数据和REST API历史数据
- 🔒 **安全第一**: API密钥通过环境变量管理，绝不硬编码
- 🧪 **策略回测**: 支持历史数据回测，验证策略有效性
- 📈 **技术指标**: 集成常用技术分析指标
- 🎯 **模块化设计**: 清晰的代码结构，易于扩展和维护

## 技术栈

- **Python 3.10+**
- **包管理器**: `uv`
- **核心依赖**:
  - `ccxt`: 统一的交易所API访问
  - `pandas`: 数据处理与分析
  - `python-dotenv`: 环境变量管理
  - `websocket-client`: WebSocket连接

## 快速开始

### 1. 环境准备

确保已安装Python 3.10+和uv包管理器。

### 2. 安装依赖

```bash
uv sync
```

### 3. 配置环境变量

复制环境变量模板文件：

```bash
cp env.example .env
```

编辑`.env`文件，配置你的API密钥：

```env
# 币安交易所 API 配置
BINANCE_API_KEY=your_api_key_here
BINANCE_SECRET_KEY=your_secret_key_here
BINANCE_SANDBOX=true

# 交易配置
DEFAULT_SYMBOL=BTC/USDT
DEFAULT_TIMEFRAME=1h
MAX_POSITION_SIZE=0.1
```

### 4. 运行Demo

```bash
uv run python main.py
```

## 项目结构

```
damn_rich/
├── src/
│   └── damn_rich/
│       ├── exchange/          # 交易所连接模块
│       │   ├── __init__.py
│       │   └── binance_client.py
│       ├── data/              # 数据获取模块
│       │   ├── __init__.py
│       │   └── market_data.py
│       ├── strategy/          # 交易策略模块
│       │   └── __init__.py
│       ├── utils/             # 工具模块
│       │   ├── __init__.py
│       │   └── config.py
│       └── __init__.py
├── main.py                    # 主程序入口
├── pyproject.toml            # 项目配置
├── env.example              # 环境变量模板
└── README.md                # 项目说明
```

## 功能模块

### 交易所连接 (`exchange/`)
- 支持币安交易所连接
- 统一的API接口
- 沙盒环境支持
- 连接状态检测

### 数据获取 (`data/`)
- 实时价格获取
- 历史K线数据
- 市场信息统计
- 数据格式标准化

### 配置管理 (`utils/`)
- 环境变量管理
- 配置验证
- 安全密钥存储

## 开发计划

- [ ] 交易策略开发
- [ ] 回测系统
- [ ] 风险管理
- [ ] 实时交易执行
- [ ] 性能监控
- [ ] 日志系统

## 安全提醒

⚠️ **重要**: 
- 永远不要将API密钥提交到代码仓库
- 使用沙盒环境进行测试
- 设置合理的交易限额
- 定期检查账户安全

## 许可证

MIT License
