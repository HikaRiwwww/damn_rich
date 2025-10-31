"""
策略管理服务 API
用于向前端提供策略状态和管理接口
"""

import json
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from damn_rich.api.dependencies import get_db, get_strategy_manager
from damn_rich.api.exceptions import DatabaseException, NotFoundException
from damn_rich.api.logging_config import APILogger
from damn_rich.api.response_model import create_success_response
from damn_rich.database.models import Strategy

router = APIRouter(prefix="/api/strategy", tags=["strategy"])
logger = APILogger("strategy_api")


@router.get("/list")
async def get_strategies(
    is_enabled: Optional[bool] = Query(None, description="是否启用"),
    is_active: Optional[bool] = Query(None, description="是否激活"),
    db: Session = Depends(get_db),
):
    """获取策略列表"""
    try:
        logger.info("Fetching strategies list")

        query = db.query(Strategy)

        # 过滤条件
        if is_enabled is not None:
            query = query.filter(Strategy.is_enabled == is_enabled)
        if is_active is not None:
            query = query.filter(Strategy.is_active == is_active)

        strategies = query.order_by(Strategy.created_at.desc()).all()

        # 获取已加载的策略实例（用于判断运行时状态）
        strategy_manager = get_strategy_manager()
        loaded_strategies = strategy_manager.get_all_strategies()

        # 构建响应数据
        strategy_data = []
        for strategy in strategies:
            # 判断策略是否已加载到内存
            is_loaded = strategy.name in loaded_strategies

            strategy_info = {
                "id": strategy.id,
                "name": strategy.name,
                "name_cn": strategy.name_cn,
                "description": strategy.description,
                "version": strategy.version,
                "author": strategy.author,
                "is_active": strategy.is_active,
                "is_enabled": strategy.is_enabled,
                "is_loaded": is_loaded,  # 是否已加载到内存
                "file_path": strategy.file_path,
                "class_name": strategy.class_name,
                "config": json.loads(strategy.config) if strategy.config else None,
                "created_at": strategy.created_at.isoformat()
                if strategy.created_at
                else None,
                "updated_at": strategy.updated_at.isoformat()
                if strategy.updated_at
                else None,
            }
            strategy_data.append(strategy_info)

        logger.info(f"Successfully fetched {len(strategy_data)} strategies")
        return create_success_response(
            data=strategy_data, message=f"成功获取 {len(strategy_data)} 个策略"
        )

    except Exception as e:
        logger.error(f"Failed to fetch strategies: {str(e)}")
        raise DatabaseException(f"获取策略列表失败: {str(e)}")


@router.get("/{strategy_id}")
async def get_strategy(
    strategy_id: int,
    db: Session = Depends(get_db),
):
    """获取单个策略详情"""
    try:
        logger.info(f"Fetching strategy with id={strategy_id}")

        strategy = db.query(Strategy).filter(Strategy.id == strategy_id).first()
        if not strategy:
            raise NotFoundException(f"策略 ID {strategy_id} 不存在")

        # 获取已加载的策略实例
        strategy_manager = get_strategy_manager()
        loaded_strategy = strategy_manager.get_strategy(strategy.name)

        strategy_data = {
            "id": strategy.id,
            "name": strategy.name,
            "name_cn": strategy.name_cn,
            "description": strategy.description,
            "version": strategy.version,
            "author": strategy.author,
            "is_active": strategy.is_active,
            "is_enabled": strategy.is_enabled,
            "is_loaded": loaded_strategy is not None,
            "file_path": strategy.file_path,
            "class_name": strategy.class_name,
            "config": json.loads(strategy.config) if strategy.config else None,
            "created_at": strategy.created_at.isoformat()
            if strategy.created_at
            else None,
            "updated_at": strategy.updated_at.isoformat()
            if strategy.updated_at
            else None,
        }

        logger.info(f"Successfully fetched strategy {strategy.name}")
        return create_success_response(data=strategy_data, message="成功获取策略信息")

    except NotFoundException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch strategy: {str(e)}")
        raise DatabaseException(f"获取策略信息失败: {str(e)}")


@router.get("/status/summary")
async def get_strategy_status_summary(db: Session = Depends(get_db)):
    """获取策略状态摘要"""
    try:
        logger.info("Fetching strategy status summary")

        total = db.query(Strategy).count()
        enabled = db.query(Strategy).filter(Strategy.is_enabled.is_(True)).count()
        active = db.query(Strategy).filter(Strategy.is_active.is_(True)).count()

        # 获取已加载的策略数量
        strategy_manager = get_strategy_manager()
        loaded_count = len(strategy_manager.get_all_strategies())

        summary = {
            "total": total,
            "enabled": enabled,
            "active": active,
            "loaded": loaded_count,
            "disabled": total - enabled,
            "inactive": total - active,
        }

        logger.info("Successfully fetched strategy status summary")
        return create_success_response(data=summary, message="成功获取策略状态摘要")

    except Exception as e:
        logger.error(f"Failed to fetch strategy status summary: {str(e)}")
        raise DatabaseException(f"获取策略状态摘要失败: {str(e)}")
