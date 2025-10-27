#!/bin/bash
#
# EC2 服务器初始化脚本
# 在全新的 Amazon Linux 2023 或 Ubuntu 服务器上运行此脚本
#
# 使用方式: 
#   chmod +x setup-server.sh
#   ./setup-server.sh

set -e

echo "🚀 开始初始化服务器..."

# 检测操作系统
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    echo "❌ 无法检测操作系统"
    exit 1
fi

# 更新系统包
echo "📦 更新系统包..."
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    sudo apt-get update
    sudo apt-get upgrade -y
elif [ "$OS" = "amzn" ] || [ "$OS" = "centos" ] || [ "$OS" = "rhel" ]; then
    sudo yum update -y
fi

# 安装Docker
echo "🐳 安装 Docker..."
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    # Ubuntu/Debian
    sudo apt-get install -y \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    
    # 添加Docker官方GPG密钥
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    
    # 设置仓库
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # 安装Docker Engine
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
elif [ "$OS" = "amzn" ]; then
    # Amazon Linux
    sudo yum install -y docker
    sudo systemctl start docker
    sudo systemctl enable docker
fi

# 安装Docker Compose (独立版本，作为备份)
echo "🔧 安装 Docker Compose..."
DOCKER_COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d\" -f4)
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 将当前用户添加到docker组
echo "👤 配置用户权限..."
sudo usermod -aG docker $USER

# 创建项目目录
echo "📁 创建项目目录..."
mkdir -p ~/crypto-bot
mkdir -p /var/lib/crypto-bot/{postgres,redis,logs}

# 设置防火墙规则（可选）
echo "🔒 配置防火墙..."
if command -v ufw &> /dev/null; then
    # Ubuntu
    sudo ufw allow 22/tcp
    sudo ufw allow 80/tcp
    sudo ufw allow 443/tcp
elif command -v firewall-cmd &> /dev/null; then
    # CentOS/RHEL
    sudo firewall-cmd --permanent --add-service=ssh
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --permanent --add-service=https
    sudo firewall-cmd --reload
fi

# 配置系统优化
echo "⚙️  配置系统优化..."
# 增加文件描述符限制
sudo tee -a /etc/security/limits.conf > /dev/null <<EOF
* soft nofile 65536
* hard nofile 65536
EOF

# Docker日志轮转
sudo tee /etc/docker/daemon.json > /dev/null <<EOF
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
EOF

# 重启Docker使配置生效
sudo systemctl restart docker

echo ""
echo "✅ 服务器初始化完成！"
echo ""
echo "📝 后续步骤:"
echo "1. 退出并重新登录以使docker组权限生效"
echo "2. 在GitHub仓库设置以下Secrets:"
echo "   - DOCKERHUB_USERNAME: Docker Hub用户名"
echo "   - DOCKERHUB_TOKEN: Docker Hub访问令牌"
echo "   - AWS_ACCESS_KEY_ID: AWS访问密钥ID"
echo "   - AWS_SECRET_ACCESS_KEY: AWS访问密钥"
echo "   - AWS_REGION: AWS区域（如: us-east-1）"
echo "   - EC2_HOST: EC2服务器IP或域名"
echo "   - EC2_USER: EC2登录用户名"
echo "   - EC2_SSH_KEY: EC2 SSH私钥内容"
echo "3. 在服务器上创建 .env 文件并配置环境变量"
echo "4. 推送代码到GitHub main分支触发自动部署"
echo ""
echo "🔧 验证安装:"
echo "   docker --version"
echo "   docker-compose --version"


