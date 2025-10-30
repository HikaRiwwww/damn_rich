"""
交易执行服务 API
用于向前端提供交易记录和策略状态接口
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_
from sqlalchemy.orm import Session

from damn_rich.api.exceptions import DatabaseException
from damn_rich.api.logging_config import APILogger
from damn_rich.api.response_model import create_success_response
from damn_rich.database.models import DatabaseManager
from damn_rich.utils.config import Config

router = APIRouter(prefix="/api/trading-bot", tags=["trading-bot"])
logger = APILogger("trading_bot_api")


def get_db():
    """获取数据库会话"""
    try:
        database_manager = DatabaseManager(Config.get_database_url())
        with database_manager.get_session() as session:
            yield session
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise DatabaseException(f"数据库连接失败: {str(e)}")
    finally:
        if "database_manager" in locals():
            database_manager.close()


@router.get("/status")
async def get_trading_bot_status():
    """获取交易机器人状态"""
    try:
        logger.info("Fetching trading bot status")

        # 这里可以添加更多的状态检查逻辑
        status_data = {
            "service_name": "交易机器人服务",
            "status": "running",
            "last_check": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "features": {
                "auto_trading": False,  # 暂时关闭自动交易
                "strategy_running": False,
                "risk_management": True,
            },
        }

        logger.info("Successfully fetched trading bot status")
        return create_success_response(data=status_data, message="交易机器人状态正常")

    except Exception as e:
        logger.error(f"Failed to get trading bot status: {str(e)}")
        raise DatabaseException(f"获取交易机器人状态失败: {str(e)}")


@router.get("/trading-records")
async def get_trading_records(
    limit: int = Query(50, ge=1, le=500, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    db: Session = Depends(get_db),
):
    """获取交易记录"""
    try:
        logger.info(f"Fetching trading records with limit={limit}, offset={offset}")

        # 这里应该查询实际的交易记录表
        # 目前返回模拟数据
        trading_records = [
            {
                "id": 1,
                "symbol": "BTC/USDT",
                "side": "buy",
                "amount": 0.001,
                "price": 45000.0,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "completed",
                "strategy": "test_strategy",
            },
            {
                "id": 2,
                "symbol": "ETH/USDT",
                "side": "sell",
                "amount": 0.1,
                "price": 3000.0,
                "timestamp": datetime.utcnow().isoformat(),
                "status": "completed",
                "strategy": "test_strategy",
            },
        ]

        logger.info(f"Successfully fetched {len(trading_records)} trading records")
        return create_success_response(
            data=trading_records, message=f"成功获取 {len(trading_records)} 条交易记录"
        )

    except Exception as e:
        logger.error(f"Failed to fetch trading records: {str(e)}")
        raise DatabaseException(f"获取交易记录失败: {str(e)}")


@router.get("/strategies")
async def get_strategies():
    """获取交易策略列表"""
    try:
        logger.info("Fetching trading strategies")

        # 这里应该查询实际的策略表
        # 目前返回模拟数据
        strategies = [
            {
                "id": 1,
                "name": "简单移动平均策略",
                "description": "基于简单移动平均线的交易策略",
                "status": "inactive",
                "created_at": datetime.utcnow().isoformat(),
            },
            {
                "id": 2,
                "name": "RSI 超买超卖策略",
                "description": "基于 RSI 指标的超买超卖交易策略",
                "status": "inactive",
                "created_at": datetime.utcnow().isoformat(),
            },
        ]

        logger.info(f"Successfully fetched {len(strategies)} strategies")
        return create_success_response(
            data=strategies, message=f"成功获取 {len(strategies)} 个交易策略"
        )

    except Exception as e:
        logger.error(f"Failed to fetch strategies: {str(e)}")
        raise DatabaseException(f"获取交易策略失败: {str(e)}")
