# 部署脚本说明

本目录包含用于部署和管理加密货币量化交易机器人的各种脚本。

## 📁 文件说明

### `setup-server.sh`

服务器初始化脚本，用于在全新的 EC2 服务器上安装和配置所有必要的依赖。

**功能：**

- 安装 Docker 和 Docker Compose
- 配置用户权限
- 创建必要的目录结构
- 配置防火墙规则
- 优化系统配置

**使用方式：**

```bash
chmod +x setup-server.sh
./setup-server.sh
```

### `deploy.sh`

手动部署脚本，用于在服务器上部署或更新应用。

**功能：**

- 拉取最新 Docker 镜像
- 备份当前日志
- 停止旧容器并启动新容器
- 验证部署状态
- 清理旧镜像

**使用方式：**

```bash
chmod +x deploy.sh
./deploy.sh [version]

# 示例
./deploy.sh latest          # 部署最新版本
./deploy.sh v1.2.3          # 部署特定版本
```

### `rollback.sh`

快速回滚脚本，用于在出现问题时回滚到之前的版本。

**功能：**

- 停止当前运行的容器
- 切换到指定的旧版本
- 重新启动服务
- 验证回滚状态

**使用方式：**

```bash
chmod +x rollback.sh
./rollback.sh <version>

# 示例
./rollback.sh v1.2.2
```

## 🚀 快速开始

### 首次部署

1. **在 EC2 服务器上运行初始化脚本**

   ```bash
   curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/setup-server.sh
   chmod +x setup-server.sh
   ./setup-server.sh
   ```

2. **退出并重新登录**

   ```bash
   exit
   ssh -i your-key.pem ec2-user@your-ec2-ip
   ```

3. **创建项目目录和配置文件**

   ```bash
   mkdir -p ~/crypto-bot
   cd ~/crypto-bot

   # 下载 docker-compose 文件
   curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/docker-compose.prod.yml

   # 创建 .env 文件
   vim .env
   ```

4. **运行部署脚本**
   ```bash
   curl -O https://raw.githubusercontent.com/your-username/damn_rich/main/deploy/deploy.sh
   chmod +x deploy.sh
   ./deploy.sh latest
   ```

### 日常更新

使用 GitHub Actions 自动部署（推荐）：

```bash
git push origin main
```

或手动部署：

```bash
cd ~/crypto-bot
./deploy.sh latest
```

### 紧急回滚

```bash
cd ~/crypto-bot
./rollback.sh <previous-version>
```

## ⚙️ 环境变量配置

在运行部署脚本之前，确保 `~/crypto-bot/.env` 文件包含以下变量：

```env
# Docker 镜像
DOCKER_IMAGE=your-dockerhub-username/crypto-bot:latest

# 币安 API
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key
BINANCE_SANDBOX=true

# 数据库
DB_NAME=crypto_trading
DB_USER=postgres
DB_PASSWORD=your_secure_password

# Redis
REDIS_PASSWORD=your_redis_password

# 其他配置...
```

## 📊 监控和管理

### 查看服务状态

```bash
cd ~/crypto-bot
docker-compose -f docker-compose.prod.yml ps
```

### 查看日志

```bash
# 所有服务
docker-compose -f docker-compose.prod.yml logs -f

# 特定服务
docker-compose -f docker-compose.prod.yml logs -f data-sync
docker-compose -f docker-compose.prod.yml logs -f trading-bot
```

### 重启服务

```bash
docker-compose -f docker-compose.prod.yml restart
```

### 停止服务

```bash
docker-compose -f docker-compose.prod.yml down
```

## 🔧 自定义配置

### 修改 Docker 镜像仓库

编辑部署脚本，修改 `DOCKER_IMAGE` 变量：

```bash
vim deploy.sh

# 修改为你的镜像仓库
DOCKER_IMAGE="your-registry/your-image"
```

### 修改项目目录

默认项目目录为 `~/crypto-bot`，可以修改脚本中的 `PROJECT_DIR` 变量：

```bash
vim deploy.sh

# 修改项目目录
PROJECT_DIR="/your/custom/path"
```

## 🐛 故障排查

### 脚本权限问题

```bash
chmod +x deploy/*.sh
```

### Docker 权限问题

```bash
# 将用户添加到 docker 组
sudo usermod -aG docker $USER

# 重新登录使权限生效
```

### 端口占用问题

```bash
# 查看端口占用
sudo netstat -tlnp | grep 5432
sudo netstat -tlnp | grep 6379

# 停止占用端口的进程
sudo kill -9 <PID>
```

## 📝 最佳实践

1. **定期备份数据**

   - 使用 `pg_dump` 备份数据库
   - 备份 `/var/lib/crypto-bot/` 目录

2. **测试后再部署**

   - 先在开发环境测试
   - 使用 `develop` 分支进行预发布测试

3. **保留多个版本**

   - 不要立即清理所有旧镜像
   - 至少保留最近 3 个版本以便回滚

4. **监控资源使用**

   - 定期检查 `docker stats`
   - 监控磁盘空间使用

5. **安全管理密钥**
   - 不要在脚本中硬编码密钥
   - 使用 `.env` 文件管理敏感信息
   - 确保 `.env` 文件权限为 600

## 📚 相关文档

- [完整部署文档](../DEPLOYMENT.md)
- [项目 README](../README.md)

---

**维护者:** Your Name  
**最后更新:** 2024-10-08

