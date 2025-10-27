# 部署文档

本文档详细说明如何将加密货币量化交易机器人部署到 Amazon EC2 云服务器。

## 📋 目录

- [架构概览](#架构概览)
- [前置要求](#前置要求)
- [服务器初始化](#服务器初始化)
- [配置 GitHub Actions](#配置-github-actions)
- [本地部署](#本地部署)
- [生产环境部署](#生产环境部署)
- [监控和维护](#监控和维护)
- [故障排查](#故障排查)

## 🏗️ 架构概览

### 容器架构

```
┌─────────────────────────────────────────────────────┐
│                    EC2 服务器                         │
│                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  │
│  │  Data Sync   │  │ Trading Bot  │  │  Redis   │  │
│  │  Container   │  │  Container   │  │Container │  │
│  └──────────────┘  └──────────────┘  └──────────┘  │
│         │                  │                │        │
│         └──────────────────┴────────────────┘        │
│                          │                           │
│                  ┌──────────────┐                    │
│                  │  PostgreSQL  │                    │
│                  │  Container   │                    │
│                  └──────────────┘                    │
│                                                       │
└─────────────────────────────────────────────────────┘
```

### CI/CD 流程

```
代码推送到 GitHub
      │
      ▼
GitHub Actions 触发
      │
      ▼
构建 Docker 镜像
      │
      ▼
推送到 Docker Hub
      │
      ▼
SSH 连接到 EC2
      │
      ▼
拉取镜像并部署
      │
      ▼
健康检查
```

## 📦 前置要求

### 1. AWS 账户和 EC2 实例

- AWS 账户
- EC2 实例（推荐配置）：
  - 实例类型: `t3.medium` 或更高
  - 操作系统: Amazon Linux 2023 或 Ubuntu 22.04 LTS
  - 存储: 至少 20GB EBS
  - 安全组配置:
    - SSH (22): 你的 IP 地址
    - HTTP (80): 0.0.0.0/0 (可选，用于 API)
    - HTTPS (443): 0.0.0.0/0 (可选，用于 API)

### 2. GitHub 账户

- GitHub 仓库
- 配置 GitHub Actions 权限

### 3. Docker Hub 账户

- Docker Hub 用户名
- 访问令牌（Access Token）

### 4. 本地开发工具

- Git
- Docker 和 Docker Compose
- SSH 客户端

## 🚀 服务器初始化

### 步骤 1: 连接到 EC2 服务器

```bash
# 使用 SSH 连接到服务器
ssh -i your-key.pem ec2-user@your-ec2-ip

# 或 Ubuntu 系统
ssh -i your-key.pem ubuntu@your-ec2-ip
```

### 步骤 2: 下载并运行初始化脚本

```bash
# 下载初始化脚本
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/setup-server.sh

# 添加执行权限
chmod +x setup-server.sh

# 运行初始化脚本
./setup-server.sh
```

初始化脚本会自动完成以下任务：

- 更新系统包
- 安装 Docker 和 Docker Compose
- 配置用户权限
- 创建项目目录
- 配置防火墙
- 优化系统配置

### 步骤 3: 重新登录

```bash
# 退出当前会话
exit

# 重新登录以使 docker 组权限生效
ssh -i your-key.pem ec2-user@your-ec2-ip
```

### 步骤 4: 验证安装

```bash
# 验证 Docker
docker --version
docker ps

# 验证 Docker Compose
docker-compose --version
```

## ⚙️ 配置 GitHub Actions

### 步骤 1: 配置 GitHub Secrets

在 GitHub 仓库设置中添加以下 Secrets (`Settings > Secrets and variables > Actions`):

| Secret 名称             | 说明                 | 示例                                       |
| ----------------------- | -------------------- | ------------------------------------------ |
| `DOCKERHUB_USERNAME`    | Docker Hub 用户名    | `your-username`                            |
| `DOCKERHUB_TOKEN`       | Docker Hub 访问令牌  | `dckr_pat_xxxxx`                           |
| `AWS_ACCESS_KEY_ID`     | AWS 访问密钥 ID      | `AKIAIOSFODNN7EXAMPLE`                     |
| `AWS_SECRET_ACCESS_KEY` | AWS 访问密钥         | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `AWS_REGION`            | AWS 区域             | `us-east-1`                                |
| `EC2_HOST`              | EC2 服务器 IP 或域名 | `3.123.45.67`                              |
| `EC2_USER`              | EC2 登录用户名       | `ec2-user` 或 `ubuntu`                     |
| `EC2_SSH_KEY`           | EC2 SSH 私钥内容     | `-----BEGIN RSA PRIVATE KEY-----\n...`     |

### 步骤 2: 创建 Docker Hub 访问令牌

1. 登录 [Docker Hub](https://hub.docker.com/)
2. 点击右上角头像 > Account Settings
3. 选择 Security > New Access Token
4. 创建令牌并复制保存

### 步骤 3: 配置 AWS IAM

创建 IAM 用户并赋予适当权限（如需使用 AWS 服务）：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["ec2:DescribeInstances", "ec2:DescribeInstanceStatus"],
      "Resource": "*"
    }
  ]
}
```

## 🔧 本地部署（开发环境）

### 步骤 1: 克隆仓库

```bash
git clone https://github.com/your-username/damn_rich.git
cd damn_rich
```

### 步骤 2: 配置环境变量

```bash
# 复制环境变量模板
cp env.example .env

# 编辑 .env 文件
vim .env
```

### 步骤 3: 构建和启动服务

```bash
# 构建 Docker 镜像
docker-compose build

# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 查看服务状态
docker-compose ps
```

### 步骤 4: 测试服务

```bash
# 查看数据同步服务日志
docker-compose logs -f data-sync

# 查看交易机器人日志
docker-compose logs -f trading-bot

# 进入容器
docker-compose exec data-sync bash
```

## 🌐 生产环境部署

### 方式 1: 自动部署（推荐）

通过 GitHub Actions 自动部署：

```bash
# 1. 确保所有 GitHub Secrets 已配置

# 2. 推送代码到 main 分支
git add .
git commit -m "deploy: update production"
git push origin main

# 3. GitHub Actions 会自动:
#    - 构建 Docker 镜像
#    - 推送到 Docker Hub
#    - SSH 到 EC2 服务器
#    - 部署最新版本

# 4. 查看 GitHub Actions 状态
# 访问: https://github.com/your-username/damn_rich/actions
```

### 方式 2: 手动部署

如需手动部署或更新：

#### 在 EC2 服务器上:

```bash
# 1. 创建项目目录
mkdir -p ~/crypto-bot
cd ~/crypto-bot

# 2. 下载 docker-compose.prod.yml
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/docker-compose.prod.yml

# 3. 创建 .env 文件
vim .env
```

添加以下环境变量到 `.env`:

```env
# 币安交易所配置
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key
BINANCE_SANDBOX=true

# 交易配置
DEFAULT_SYMBOL=BTC/USDT
DEFAULT_TIMEFRAME=1h
MAX_POSITION_SIZE=0.1

# 数据库配置
DB_NAME=crypto_trading
DB_USER=postgres
DB_PASSWORD=your_secure_password

# Redis配置
REDIS_PASSWORD=your_redis_password

# Docker镜像
DOCKER_IMAGE=your-dockerhub-username/crypto-bot:latest
```

```bash
# 4. 下载并运行部署脚本
curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/deploy.sh
chmod +x deploy.sh

# 5. 执行部署
./deploy.sh latest
```

### 初次部署注意事项

首次部署时，需要初始化数据库：

```bash
# 进入数据库容器
docker exec -it crypto_postgres psql -U postgres -d crypto_trading

# 检查表是否创建（应用会自动创建）
\dt

# 退出
\q
```

## 📊 监控和维护

### 查看服务状态

```bash
# 查看所有服务状态
docker-compose -f docker-compose.prod.yml ps

# 查看服务资源使用
docker stats

# 查看特定服务日志
docker-compose -f docker-compose.prod.yml logs -f data-sync
docker-compose -f docker-compose.prod.yml logs -f trading-bot

# 查看最近100行日志
docker-compose -f docker-compose.prod.yml logs --tail=100
```

### 重启服务

```bash
# 重启所有服务
docker-compose -f docker-compose.prod.yml restart

# 重启特定服务
docker-compose -f docker-compose.prod.yml restart data-sync
docker-compose -f docker-compose.prod.yml restart trading-bot
```

### 更新服务

```bash
# 使用部署脚本更新到最新版本
./deploy.sh latest

# 或指定版本
./deploy.sh v1.2.3
```

### 备份数据

```bash
# 备份 PostgreSQL 数据库
docker exec crypto_postgres pg_dump -U postgres crypto_trading > backup_$(date +%Y%m%d).sql

# 备份数据卷
sudo tar -czf crypto_data_backup_$(date +%Y%m%d).tar.gz /var/lib/crypto-bot/

# 下载备份到本地
scp -i your-key.pem ec2-user@your-ec2-ip:~/backup_*.sql ./
```

### 恢复数据

```bash
# 恢复数据库
cat backup_20241008.sql | docker exec -i crypto_postgres psql -U postgres crypto_trading
```

### 清理资源

```bash
# 清理未使用的 Docker 镜像
docker image prune -a

# 清理未使用的容器
docker container prune

# 清理未使用的卷
docker volume prune

# 清理所有未使用的资源
docker system prune -a
```

## 🔄 回滚

如果新版本出现问题，可以快速回滚：

```bash
# 查看可用的镜像版本
docker images | grep crypto-bot

# 使用回滚脚本
./rollback.sh previous-version-tag
```

## 🐛 故障排查

### 问题 1: 容器无法启动

```bash
# 查看详细日志
docker-compose -f docker-compose.prod.yml logs

# 检查容器状态
docker ps -a

# 检查容器健康状态
docker inspect --format='{{.State.Health.Status}}' crypto_data_sync
```

### 问题 2: 数据库连接失败

```bash
# 检查 PostgreSQL 是否运行
docker ps | grep postgres

# 检查数据库日志
docker logs crypto_postgres

# 测试数据库连接
docker exec -it crypto_postgres psql -U postgres -d crypto_trading
```

### 问题 3: 内存不足

```bash
# 查看资源使用
docker stats

# 增加 swap 空间 (EC2)
sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### 问题 4: GitHub Actions 部署失败

1. 检查 GitHub Secrets 配置是否正确
2. 查看 Actions 日志: `https://github.com/your-username/damn_rich/actions`
3. 验证 SSH 连接:
   ```bash
   ssh -i your-key.pem ec2-user@your-ec2-ip "docker --version"
   ```

### 问题 5: API 密钥错误

```bash
# 检查环境变量
docker-compose -f docker-compose.prod.yml exec data-sync env | grep BINANCE

# 更新环境变量后重启
docker-compose -f docker-compose.prod.yml restart
```

## 📈 性能优化

### 1. 数据库优化

```sql
-- 连接到数据库
docker exec -it crypto_postgres psql -U postgres -d crypto_trading

-- 查看表大小
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 创建索引（如需要）
CREATE INDEX idx_kline_symbol_time ON kline_data(symbol_id, timestamp DESC);
```

### 2. 日志管理

```bash
# 限制日志大小（已在 docker-compose.prod.yml 配置）
# 手动清理旧日志
sudo find /var/lib/crypto-bot/logs -name "*.log" -mtime +7 -delete
```

### 3. 监控告警（可选）

使用 CloudWatch 监控 EC2 实例：

```bash
# 安装 CloudWatch 代理
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm
```

## 🔒 安全建议

1. **定期更新**

   ```bash
   # 更新系统包
   sudo yum update -y  # Amazon Linux
   sudo apt update && sudo apt upgrade -y  # Ubuntu
   ```

2. **配置防火墙**

   ```bash
   # 只允许必要的端口
   sudo firewall-cmd --permanent --add-service=ssh
   sudo firewall-cmd --reload
   ```

3. **使用强密码**

   - 数据库密码至少 16 字符
   - Redis 密码至少 32 字符
   - 定期更换密钥

4. **启用 SSH 密钥认证**

   ```bash
   # 禁用密码登录
   sudo vim /etc/ssh/sshd_config
   # 设置: PasswordAuthentication no
   sudo systemctl restart sshd
   ```

5. **监控日志**
   ```bash
   # 定期检查异常日志
   docker-compose -f docker-compose.prod.yml logs | grep ERROR
   ```

## 📚 相关文档

- [项目 README](./README.md)
- [Docker 官方文档](https://docs.docker.com/)
- [GitHub Actions 文档](https://docs.github.com/en/actions)
- [AWS EC2 文档](https://docs.aws.amazon.com/ec2/)

## 🆘 获取帮助

如遇到问题：

1. 查看本文档的故障排查章节
2. 检查项目 Issues: https://github.com/your-username/damn_rich/issues
3. 查看日志文件: `/var/lib/crypto-bot/logs/`

---

**最后更新时间:** 2024-10-08

