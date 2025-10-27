# 快速开始指南

本指南帮助你快速部署加密货币量化交易机器人到 AWS EC2。

## 🚀 10 分钟快速部署

### 前提条件

- ✅ AWS EC2 实例（已启动）
- ✅ GitHub 账户
- ✅ Docker Hub 账户
- ✅ 币安 API 密钥

### 步骤 1: 配置 GitHub Secrets (2 分钟)

在你的 GitHub 仓库中，进入 `Settings > Secrets and variables > Actions`，添加以下 Secrets：

```
DOCKERHUB_USERNAME=你的Docker Hub用户名
DOCKERHUB_TOKEN=你的Docker Hub令牌
EC2_HOST=你的EC2 IP地址
EC2_USER=ec2-user 或 ubuntu
EC2_SSH_KEY=你的SSH私钥内容（完整复制）
AWS_ACCESS_KEY_ID=你的AWS访问密钥ID
AWS_SECRET_ACCESS_KEY=你的AWS访问密钥
AWS_REGION=us-east-1
```

### 步骤 2: 初始化 EC2 服务器 (3 分钟)

```bash
# SSH 连接到你的 EC2 服务器
ssh -i your-key.pem ec2-user@your-ec2-ip

# 下载并运行初始化脚本
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/setup-server.sh
chmod +x setup-server.sh
./setup-server.sh

# 退出并重新登录
exit
ssh -i your-key.pem ec2-user@your-ec2-ip
```

### 步骤 3: 配置环境变量 (2 分钟)

```bash
# 创建项目目录
mkdir -p ~/crypto-bot
cd ~/crypto-bot

# 创建 .env 文件
cat > .env << 'EOF'
# Docker 镜像
DOCKER_IMAGE=你的Docker Hub用户名/crypto-bot:latest

# 币安 API
BINANCE_API_KEY=你的币安API密钥
BINANCE_SECRET_KEY=你的币安密钥
BINANCE_SANDBOX=true

# 数据库
DB_NAME=crypto_trading
DB_USER=postgres
DB_PASSWORD=你的强密码至少16字符

# Redis
REDIS_PASSWORD=你的Redis密码至少32字符
EOF
```

### 步骤 4: 部署应用 (3 分钟)

有两种方式：

#### 方式 A: 自动部署（推荐）

```bash
# 在本地开发机器上
git add .
git commit -m "Initial deployment"
git push origin main

# GitHub Actions 会自动构建和部署
# 访问 https://github.com/your-username/damn_rich/actions 查看进度
```

#### 方式 B: 手动部署

```bash
# 在 EC2 服务器上
cd ~/crypto-bot
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/docker-compose.prod.yml
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/deploy.sh
chmod +x deploy.sh
./deploy.sh latest
```

### 步骤 5: 验证部署 (1 分钟)

```bash
# 查看服务状态
docker-compose -f docker-compose.prod.yml ps

# 查看日志
docker-compose -f docker-compose.prod.yml logs -f

# 应该看到类似输出:
# crypto_postgres    running
# crypto_redis       running
# crypto_data_sync   running
# crypto_trading_bot running
```

## ✅ 部署完成！

现在你的交易机器人已经在云端运行了！

### 下一步

#### 查看日志

```bash
# 数据同步服务
docker-compose -f docker-compose.prod.yml logs -f data-sync

# 交易机器人
docker-compose -f docker-compose.prod.yml logs -f trading-bot
```

#### 监控服务

```bash
# 查看资源使用
docker stats

# 查看数据库
docker exec -it crypto_postgres psql -U postgres -d crypto_trading
```

#### 更新应用

```bash
# 方式1: 推送代码到 GitHub（自动部署）
git push origin main

# 方式2: 手动更新
./deploy.sh latest
```

#### 停止服务

```bash
docker-compose -f docker-compose.prod.yml down
```

## 🔧 常用命令

```bash
# 进入项目目录
cd ~/crypto-bot

# 查看服务状态
docker-compose -f docker-compose.prod.yml ps

# 重启服务
docker-compose -f docker-compose.prod.yml restart

# 查看日志（最近100行）
docker-compose -f docker-compose.prod.yml logs --tail=100

# 进入容器
docker-compose -f docker-compose.prod.yml exec data-sync bash

# 备份数据库
docker exec crypto_postgres pg_dump -U postgres crypto_trading > backup.sql
```

## 📊 健康检查

运行以下命令确认一切正常：

```bash
# 1. 检查所有容器都在运行
docker ps

# 2. 检查数据库连接
docker exec -it crypto_postgres psql -U postgres -d crypto_trading -c "SELECT NOW();"

# 3. 检查 Redis
docker exec -it crypto_redis redis-cli ping

# 4. 检查应用日志是否有错误
docker-compose -f docker-compose.prod.yml logs | grep ERROR
```

## 🐛 遇到问题？

### 容器无法启动

```bash
# 查看详细日志
docker-compose -f docker-compose.prod.yml logs

# 重新构建
docker-compose -f docker-compose.prod.yml up -d --force-recreate
```

### 数据库连接失败

```bash
# 检查数据库是否运行
docker ps | grep postgres

# 检查密码是否正确
cat .env | grep DB_PASSWORD

# 重启数据库
docker-compose -f docker-compose.prod.yml restart postgres
```

### GitHub Actions 失败

1. 检查 Secrets 是否都配置正确
2. 查看 Actions 日志找到具体错误
3. 确认 EC2 安全组允许 SSH 连接

### 更多帮助

- 详细部署文档: [DEPLOYMENT.md](./DEPLOYMENT.md)
- 项目文档: [README.md](./README.md)
- GitHub Issues: https://github.com/your-username/damn_rich/issues

---

**祝你交易顺利！** 🎉

