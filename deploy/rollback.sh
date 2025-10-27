#!/bin/bash
#
# 回滚脚本
# 快速回滚到之前的版本
#
# 使用方式: 
#   chmod +x rollback.sh
#   ./rollback.sh <previous-version>

set -e

if [ -z "$1" ]; then
    echo "❌ 错误: 请提供要回滚到的版本"
    echo "使用方式: ./rollback.sh <version>"
    echo ""
    echo "查看可用的镜像版本:"
    docker images | grep crypto-bot
    exit 1
fi

VERSION=$1
DOCKER_IMAGE="${DOCKER_IMAGE:-your-dockerhub-username/crypto-bot}"
PROJECT_DIR="$HOME/crypto-bot"

echo "⚠️  准备回滚到版本: $VERSION"
read -p "确认继续? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 回滚已取消"
    exit 0
fi

cd $PROJECT_DIR

echo "🔄 执行回滚..."
export DOCKER_IMAGE="$DOCKER_IMAGE:$VERSION"
docker-compose -f docker-compose.prod.yml down
docker-compose -f docker-compose.prod.yml up -d

echo "⏳ 等待服务启动..."
sleep 10

echo "📊 检查服务状态..."
docker-compose -f docker-compose.prod.yml ps

echo "✅ 回滚完成！"


