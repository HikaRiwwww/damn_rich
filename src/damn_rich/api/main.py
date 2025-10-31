"""
Damn Rich 统一 API 服务
整合数据同步和交易机器人的 API 接口
"""

import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from damn_rich.api.data_sync_api import router as data_sync_router
from damn_rich.api.exceptions import (
    APIException,
    api_exception_handler,
    general_exception_handler,
    sqlalchemy_exception_handler,
    validation_exception_handler,
)
from damn_rich.api.logging_config import APILogger, setup_api_logging
from damn_rich.api.strategy_api import router as strategy_router
from damn_rich.api.trading_bot_api import router as trading_bot_router

# 设置日志
setup_api_logging()
logger = APILogger("main")

# 创建 FastAPI 应用
app = FastAPI(
    title="Damn Rich API",
    description="加密货币量化交易系统 API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册异常处理器
app.add_exception_handler(APIException, api_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(SQLAlchemyError, sqlalchemy_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# 注册路由
app.include_router(data_sync_router)
app.include_router(trading_bot_router)
app.include_router(strategy_router)


@app.get("/")
async def root():
    """根路径"""
    logger.info("API root endpoint accessed")
    return {"message": "Damn Rich API Server", "version": "1.0.0", "docs": "/docs"}


@app.get("/health")
async def health():
    """健康检查"""
    logger.info("Health check endpoint accessed")
    return {"status": "healthy", "service": "Damn Rich API"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
