#!/bin/bash
#
# 手动部署脚本
# 在EC2服务器上运行此脚本以手动部署或更新应用
#
# 使用方式: 
#   chmod +x deploy.sh
#   ./deploy.sh [version]
#
# 参数:
#   version - Docker镜像版本标签 (默认: latest)

set -e

# 配置
DOCKER_IMAGE="${DOCKER_IMAGE:-your-dockerhub-username/crypto-bot}"
VERSION="${1:-latest}"
PROJECT_DIR="$HOME/crypto-bot"

echo "🚀 开始部署..."
echo "📦 镜像: $DOCKER_IMAGE:$VERSION"

# 检查是否存在.env文件
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "❌ 错误: .env 文件不存在"
    echo "请先创建 $PROJECT_DIR/.env 文件并配置环境变量"
    exit 1
fi

# 进入项目目录
cd $PROJECT_DIR

# 拉取最新镜像
echo "📥 拉取Docker镜像..."
docker pull $DOCKER_IMAGE:$VERSION

# 备份当前运行的容器日志（可选）
echo "💾 备份日志..."
mkdir -p backups/logs
if [ -d "/var/lib/crypto-bot/logs" ]; then
    cp -r /var/lib/crypto-bot/logs backups/logs/$(date +%Y%m%d_%H%M%S) || true
fi

# 停止并移除旧容器
echo "🛑 停止旧容器..."
export DOCKER_IMAGE="$DOCKER_IMAGE:$VERSION"
docker-compose -f docker-compose.prod.yml down || true

# 启动新容器
echo "🚀 启动新容器..."
docker-compose -f docker-compose.prod.yml up -d

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "📊 检查服务状态..."
docker-compose -f docker-compose.prod.yml ps

# 显示最近的日志
echo ""
echo "📜 最近的日志:"
docker-compose -f docker-compose.prod.yml logs --tail=50

# 清理旧镜像
echo ""
echo "🧹 清理旧镜像..."
docker image prune -af --filter "until=72h"

echo ""
echo "✅ 部署完成！"
echo ""
echo "🔍 查看日志:"
echo "   docker-compose -f docker-compose.prod.yml logs -f [service-name]"
echo ""
echo "📊 查看状态:"
echo "   docker-compose -f docker-compose.prod.yml ps"
echo ""
echo "🛑 停止服务:"
echo "   docker-compose -f docker-compose.prod.yml down"


