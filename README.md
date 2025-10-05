# 加密货币量化交易机器人

一个基于 Python 的加密货币量化交易系统，支持多交易所、实时数据获取和策略回测。

## 项目特性

### 当前特性 ✅

- 🚀 **多交易所支持**: 基于 ccxt 统一接口，支持币安等主流交易所
- 📊 **数据同步**: 自动同步 K 线数据，支持历史数据和增量更新
- 🔒 **安全第一**: API 密钥通过环境变量管理，绝不硬编码
- 🎯 **模块化设计**: 清晰的代码结构，服务分离架构
- ⚡ **高性能**: 异步处理，数据库连接池，智能重试机制
- 📝 **完善日志**: 分级日志系统，滚动存储，服务独立日志

### 计划特性 🚧

- 🧪 **策略回测**: 支持历史数据回测，验证策略有效性
- 📈 **技术指标**: 集成常用技术分析指标
- 🎨 **Web 界面**: React/Vue.js 前端管理界面
- 🔄 **实时通信**: WebSocket 实时数据推送和状态更新
- 📱 **API 服务**: RESTful API 和 WebSocket 接口
- 🐳 **容器化**: Docker 部署和编排支持

## 技术栈

### 当前技术栈 ✅

- **Python 3.10+**: 主要开发语言
- **包管理器**: `uv` - 快速包管理
- **数据库**: PostgreSQL - 关系型数据库
- **任务调度**: APScheduler - Python 任务调度
- **核心依赖**:
  - `ccxt`: 统一的交易所 API 访问
  - `sqlalchemy`: ORM 数据库操作
  - `pandas`: 数据处理与分析
  - `python-dotenv`: 环境变量管理
  - `redis`: 消息队列和缓存

### 计划技术栈 🚧

- **API 框架**: FastAPI - 现代异步 Web 框架
- **前端框架**: React/Vue.js + TypeScript
- **实时通信**: WebSocket + Server-Sent Events
- **容器化**: Docker + Docker Compose
- **监控**: Prometheus + Grafana (可选)

## 快速开始

### 1. 环境准备

确保已安装 Python 3.10+和 uv 包管理器。

### 2. 安装依赖

```bash
uv sync
```

### 3. 配置环境变量

复制环境变量模板文件：

```bash
cp env.example .env
```

编辑`.env`文件，配置你的 API 密钥：

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

### 4. 配置数据库

运行命令

```shell
docker run --name damn_rich_postgres -e POSTGRES_DB=crypto_trading -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=your_password_here -p 5432:5432 -d postgres:15
```

确保 PostgreSQL 数据库已安装并运行，然后配置数据库连接：

```bash
# 复制环境变量模板
cp env.example .env

# 编辑.env文件，配置数据库连接
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=crypto_trading
# DB_USER=postgres
# DB_PASSWORD=your_password
```

### 5. 运行服务

```bash
# 运行数据同步服务
uv run python main.py data-sync

# 运行交易执行服务
uv run python main.py trading-bot

# 查看使用说明
uv run python main.py
```

### 6. 服务管理

```bash
# 启动数据同步服务 (后台运行)
nohup uv run python main.py data-sync > logs/data_sync.log 2>&1 &

# 启动交易执行服务 (后台运行)
nohup uv run python main.py trading-bot > logs/trading_bot.log 2>&1 &

# 查看服务状态
ps aux | grep python

# 停止服务
pkill -f "python main.py"
```

## 项目结构

### 当前结构

```
damn_rich/
├── src/
│   └── damn_rich/
│       ├── data/              # 数据获取模块
│       │   ├── __init__.py
│       │   └── historical_fetcher.py
│       ├── database/          # 数据库模块
│       │   ├── __init__.py
│       │   └── models.py
│       ├── exchange/          # 交易所连接模块
│       │   └── __init__.py
│       ├── infrastructure/    # 基础设施模块
│       │   ├── __init__.py
│       │   └── scheduler_service.py
│       ├── services/          # 服务模块
│       │   ├── __init__.py
│       │   ├── data_sync_service.py
│       │   └── trading_bot_service.py
│       ├── strategy/          # 交易策略模块
│       │   └── __init__.py
│       ├── task/              # 任务模块
│       │   ├── __init__.py
│       │   ├── base_task.py
│       │   └── kline_sync_task.py
│       ├── utils/             # 工具模块
│       │   ├── __init__.py
│       │   ├── config.py
│       │   └── logger.py
│       └── __init__.py
├── logs/                      # 日志目录
│   ├── data_sync/
│   └── trading_bot/
├── main.py                    # 主程序入口
├── pyproject.toml            # 项目配置
├── env.example              # 环境变量模板
└── README.md                # 项目说明
```

### 目标结构 (待实现)

```
damn_rich/
├── src/
│   └── damn_rich/
│       ├── api/                    # API 服务模块
│       │   ├── __init__.py
│       │   ├── api_server.py       # API 服务主类
│       │   ├── routes/             # 路由模块
│       │   │   ├── __init__.py
│       │   │   ├── data.py         # 数据相关路由
│       │   │   ├── trading.py      # 交易相关路由
│       │   │   └── system.py       # 系统相关路由
│       │   ├── websocket/          # WebSocket 处理
│       │   │   ├── __init__.py
│       │   │   └── handlers.py
│       │   └── middleware/         # 中间件
│       │       ├── __init__.py
│       │       ├── auth.py
│       │       └── cors.py
│       ├── data/                   # 数据获取模块
│       ├── database/               # 数据库模块
│       │   └── migrations/         # 数据库迁移
│       ├── infrastructure/         # 基础设施模块
│       │   └── message_queue.py    # 消息队列服务
│       ├── services/               # 服务模块
│       ├── strategy/               # 交易策略模块
│       ├── task/                   # 任务模块
│       └── utils/                  # 工具模块
├── frontend/                       # 前端代码
│   ├── public/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   ├── services/
│   │   └── utils/
│   ├── package.json
│   └── vite.config.js
├── docker-compose.yml              # 容器编排
├── Dockerfile                      # 容器构建
├── logs/                           # 日志目录
├── main.py                         # 主入口
├── pyproject.toml                  # 项目配置
├── env.example                     # 环境变量模板
└── README.md                       # 项目说明
```

## 功能模块

### 数据同步服务 (`services/data_sync_service.py`)

- **K 线数据同步**: 自动同步 4 小时 K 线数据
- **历史数据获取**: 支持一年历史数据批量获取
- **增量更新**: 智能增量数据更新机制
- **任务调度**: 基于 APScheduler 的定时任务
- **数据验证**: 数据完整性和一致性检查

### 交易执行服务 (`services/trading_bot_service.py`)

- **策略执行**: 交易策略运行引擎
- **风险管理**: 仓位控制和风险监控
- **订单管理**: 交易订单执行和管理
- **状态监控**: 实时交易状态跟踪
- **数据依赖**: 依赖 K 线数据进行交易决策

### 数据获取模块 (`data/historical_fetcher.py`)

- **多交易所支持**: 基于 ccxt 统一接口
- **历史数据抓取**: 支持多种时间周期
- **API 限制处理**: 智能限流和重试机制
- **数据格式化**: 统一数据格式输出
- **沙盒支持**: 测试环境支持

### 数据库模块 (`database/models.py`)

- **PostgreSQL 集成**: 高性能数据库连接
- **表结构设计**: 优化的数据库表结构
  - `exchanges`: 交易所信息表
  - `symbols`: 交易对信息表
  - `kline_data`: K 线数据表
- **索引优化**: 查询性能优化
- **连接池管理**: 数据库连接池

### 任务调度模块 (`infrastructure/scheduler_service.py`)

- **APScheduler 集成**: 强大的任务调度功能
- **Redis 支持**: 分布式任务调度
- **任务持久化**: 任务状态持久化存储
- **动态任务管理**: 运行时添加/删除任务
- **故障恢复**: 服务重启后任务恢复

### 日志系统 (`utils/logger.py`)

- **分级日志**: DEBUG/INFO/WARNING/ERROR/CRITICAL
- **滚动存储**: 按大小和时间滚动
- **服务分离**: 不同服务独立日志文件
- **配置灵活**: 环境变量配置
- **性能优化**: 异步日志写入

### 配置管理 (`utils/config.py`)

- **环境变量管理**: 集中配置管理
- **类型安全**: 类型提示和验证
- **默认值支持**: 合理的默认配置
- **安全存储**: API 密钥安全管理
- **多环境支持**: 开发/测试/生产环境

## 开发计划

### 已完成功能 ✅

- [x] **基础架构**: 模块化项目结构
- [x] **数据同步**: K 线数据自动同步系统
- [x] **数据库管理**: PostgreSQL 集成和表结构
- [x] **任务调度**: APScheduler 定时任务系统
- [x] **日志系统**: 分级日志和滚动存储
- [x] **服务分离**: 数据同步和交易执行服务分离
- [x] **环境配置**: 完整的配置管理系统

### 核心功能开发 🚧

- [ ] **交易策略开发**: 技术指标计算和信号生成
- [ ] **回测系统**: 历史数据策略验证
- [ ] **风险管理**: 仓位控制和止损机制
- [ ] **实时交易执行**: 订单管理和执行引擎
- [ ] **性能监控**: 策略表现和系统健康监控

### API 服务开发 📋

- [ ] **RESTful API**: FastAPI 接口服务
  - [ ] 数据查询接口 (`/api/v1/data/*`)
  - [ ] 交易控制接口 (`/api/v1/trading/*`)
  - [ ] 系统管理接口 (`/api/v1/system/*`)
  - [ ] 策略管理接口 (`/api/v1/strategies/*`)
- [ ] **WebSocket 服务**: 实时数据推送
  - [ ] 实时 K 线数据推送
  - [ ] 交易信号广播
  - [ ] 系统状态更新
- [ ] **消息队列**: Redis 进程间通信
  - [ ] 进程间消息传递
  - [ ] 状态同步机制
  - [ ] 命令执行队列

### 前端界面开发 🎨

- [ ] **管理界面**: React/Vue.js 前端应用
  - [ ] 交易对管理页面
  - [ ] 策略配置界面
  - [ ] 系统监控面板
  - [ ] 交易执行控制台
- [ ] **实时图表**: 交易数据可视化
  - [ ] K 线图表展示
  - [ ] 技术指标图表
  - [ ] 策略表现分析
  - [ ] 实时价格监控

### 高级功能 🔧

- [ ] **数据库迁移**: 版本管理和结构升级
  - [ ] 迁移脚本系统
  - [ ] 版本控制机制
  - [ ] 数据备份恢复
- [ ] **容器化部署**: Docker 和编排
  - [ ] 多阶段构建优化
  - [ ] Docker Compose 编排
  - [ ] 生产环境配置
- [ ] **监控告警**: 系统健康监控
  - [ ] 性能指标收集
  - [ ] 异常告警机制
  - [ ] 日志聚合分析

### 架构升级 🏗️

#### 当前架构

```
┌─────────────────┐    ┌─────────────────┐
│   Data Sync     │    │   Trading Bot   │
│   (Background)  │    │   (Background)  │
└─────────────────┘    └─────────────────┘
         │                       │
         └───────────────────────┘
                    │
         ┌─────────────────┐
         │   PostgreSQL    │
         └─────────────────┘
```

#### 目标架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   API Server    │    │   Background    │
│   (React/Vue)   │◄──►│   (FastAPI)     │◄──►│   Services      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                       │
                              ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Redis Queue   │    │   PostgreSQL    │
                       │   (Message Bus) │    │   (Database)    │
                       └─────────────────┘    └─────────────────┘
```

### 实施优先级 📅

#### 阶段 1: API 服务基础 (2-3 周)

1. FastAPI 框架集成
2. 基础 REST API 接口
3. Redis 消息队列通信
4. WebSocket 实时推送

#### 阶段 2: 前端界面开发 (3-4 周)

1. 前端框架搭建
2. 交易对管理界面
3. 系统监控面板
4. 实时数据展示

#### 阶段 3: 高级功能 (4-5 周)

1. 交易策略开发
2. 回测系统实现
3. 风险管理模块
4. 性能优化

#### 阶段 4: 生产部署 (2-3 周)

1. 容器化配置
2. 生产环境部署
3. 监控告警系统
4. 文档完善

### 技术选型 📚

#### 后端技术栈

- **API 框架**: FastAPI (异步支持、自动文档)
- **消息队列**: Redis (高性能、持久化)
- **数据库**: PostgreSQL (事务支持、扩展性)
- **任务调度**: APScheduler (灵活配置、持久化)

#### 前端技术栈

- **框架**: React + TypeScript 或 Vue 3 + TypeScript
- **UI 库**: Ant Design 或 Element Plus
- **图表**: ECharts 或 TradingView
- **状态管理**: Redux/Zustand 或 Pinia
- **构建工具**: Vite 或 Webpack

#### 部署技术栈

- **容器化**: Docker + Docker Compose
- **编排**: Kubernetes (可选)
- **监控**: Prometheus + Grafana (可选)
- **日志**: ELK Stack (可选)

## 安全提醒

⚠️ **重要**:

- 永远不要将 API 密钥提交到代码仓库
- 使用沙盒环境进行测试
- 设置合理的交易限额
- 定期检查账户安全

## 许可证

MIT License
