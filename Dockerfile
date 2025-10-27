# 多阶段构建，优化镜像大小
FROM python:3.11-slim as builder

# 设置工作目录
WORKDIR /app

# 安装uv包管理器
RUN pip install --no-cache-dir uv

# 复制项目配置文件
COPY pyproject.toml ./
COPY uv.lock ./

# 使用uv安装依赖到虚拟环境
RUN uv sync --frozen --no-dev

# 最终运行阶段
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# 设置工作目录
WORKDIR /app

# 安装必要的系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# 从构建阶段复制虚拟环境
COPY --from=builder /app/.venv /app/.venv

# 复制项目源代码
COPY src/ /app/src/
COPY main.py /app/

# 创建日志目录
RUN mkdir -p /app/logs

# 创建非root用户运行应用
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# 健康检查（可根据实际服务调整）
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# 默认命令（可被docker-compose覆盖）
CMD ["python", "main.py", "data-sync"]


