"""
数据同步服务 API
用于向前端提供 K 线数据查询接口
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc
from sqlalchemy.orm import Session

from damn_rich.api.dependencies import get_db
from damn_rich.api.exceptions import DatabaseException, NotFoundException
from damn_rich.api.logging_config import APILogger
from damn_rich.api.response_model import create_success_response
from damn_rich.database.models import Exchange, KlineData, Symbol

router = APIRouter(prefix="/api/data-sync", tags=["data-sync"])
logger = APILogger("data_sync_api")


@router.get("/exchanges")
async def get_exchanges(db: Session = Depends(get_db)):
    """获取支持的交易所列表"""
    try:
        logger.info("Fetching exchanges list")
        exchanges = db.query(Exchange).all()

        exchange_data = [
            {
                "id": exchange.id,
                "name": exchange.name,
                "display_name": exchange.display_name,
                "is_active": exchange.is_active,
                "created_at": exchange.created_at.isoformat()
                if exchange.created_at
                else None,
            }
            for exchange in exchanges
        ]

        logger.info(f"Successfully fetched {len(exchange_data)} exchanges")
        return create_success_response(
            data=exchange_data, message=f"成功获取 {len(exchange_data)} 个交易所"
        )

    except Exception as e:
        logger.error(f"Failed to fetch exchanges: {str(e)}")
        raise DatabaseException(f"获取交易所列表失败: {str(e)}")


@router.get("/symbols")
async def get_symbols(
    is_trading: Optional[bool] = Query(None, description="是否可交易"),
    db: Session = Depends(get_db),
):
    """获取交易对列表"""
    try:
        logger.info(f"Fetching symbols with is_trading={is_trading}")

        query = db.query(Symbol)

        if is_trading is not None:
            query = query.filter(Symbol.is_trading == is_trading)

        symbols = query.all()

        symbol_data = [
            {
                "id": symbol.id,
                "symbol": symbol.symbol,
                "base_currency": symbol.base_asset,
                "quote_currency": symbol.quote_asset,
                "is_trading": symbol.is_trading,
                "is_active": symbol.is_active,
                "base_precision": symbol.base_precision,
                "quote_precision": symbol.quote_precision,
                "min_order_size": symbol.min_order_size,
                "max_order_size": symbol.max_order_size,
            }
            for symbol in symbols
        ]

        logger.info(f"Successfully fetched {len(symbol_data)} symbols")
        return create_success_response(
            data=symbol_data, message=f"成功获取 {len(symbol_data)} 个交易对"
        )

    except Exception as e:
        logger.error(f"Failed to fetch symbols: {str(e)}")
        raise DatabaseException(f"获取交易对列表失败: {str(e)}")


@router.get("/kline-data")
async def get_kline_data(
    symbol_id: int = Query(..., description="交易对ID"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    start_time: Optional[str] = Query(None, description="开始时间 (ISO格式)"),
    end_time: Optional[str] = Query(None, description="结束时间 (ISO格式)"),
    db: Session = Depends(get_db),
):
    """获取K线数据"""
    try:
        logger.info(
            f"Fetching kline data for symbol_id={symbol_id}, limit={limit}, offset={offset}"
        )

        # 验证交易对是否存在
        symbol = db.query(Symbol).filter(Symbol.id == symbol_id).first()
        if not symbol:
            raise NotFoundException(f"交易对 ID {symbol_id} 不存在")

        query = db.query(KlineData).filter(KlineData.symbol_id == symbol_id)

        # 时间过滤
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                query = query.filter(KlineData.datetime >= start_dt)
            except ValueError:
                raise ValueError("开始时间格式不正确，请使用 ISO 格式")

        if end_time:
            try:
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                query = query.filter(KlineData.datetime <= end_dt)
            except ValueError:
                raise ValueError("结束时间格式不正确，请使用 ISO 格式")

        # 排序和分页
        kline_data = (
            query.order_by(desc(KlineData.datetime)).offset(offset).limit(limit).all()
        )

        result_data = [
            {
                "id": kline.id,
                "timestamp": int(kline.timestamp),
                "datetime": kline.datetime.isoformat() if kline.datetime else None,
                "open": float(kline.open_price),
                "high": float(kline.high_price),
                "low": float(kline.low_price),
                "close": float(kline.close_price),
                "volume": float(kline.volume),
                "symbol_id": kline.symbol_id,
                "exchange_id": kline.exchange_id,
                "timeframe": kline.timeframe,
            }
            for kline in kline_data
        ]

        logger.info(f"Successfully fetched {len(result_data)} kline records")
        return create_success_response(
            data=result_data, message=f"成功获取 {len(result_data)} 条K线数据"
        )

    except ValueError as e:
        logger.error(f"Validation error in get_kline_data: {str(e)}")
        raise ValueError(str(e))
    except Exception as e:
        logger.error(f"Failed to fetch kline data: {str(e)}")
        raise DatabaseException(f"获取K线数据失败: {str(e)}")


@router.get("/service-status")
async def get_service_status():
    """获取数据同步服务状态"""
    try:
        logger.info("Fetching data sync service status")

        # 这里可以添加更多的服务状态检查
        status_data = {
            "service_name": "数据同步服务",
            "status": "running",
            "last_check": datetime.utcnow().isoformat(),
            "version": "1.0.0",
        }

        logger.info("Successfully fetched service status")
        return create_success_response(data=status_data, message="服务状态正常")

    except Exception as e:
        logger.error(f"Failed to get service status: {str(e)}")
        raise DatabaseException(f"获取服务状态失败: {str(e)}")
