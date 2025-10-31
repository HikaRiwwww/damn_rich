"""
API 依赖注入模块
提供统一的依赖函数，供所有路由使用
"""

from typing import Generator

from sqlalchemy.orm import Session

from damn_rich.api.exceptions import DatabaseException
from damn_rich.api.logging_config import APILogger
from damn_rich.database.models import DatabaseManager
from damn_rich.services.strategy_manager import StrategyManager
from damn_rich.utils.config import Config

logger = APILogger("dependencies")


def get_db() -> Generator[Session, None, None]:
    """
    获取数据库会话依赖

    用法:
        @router.get("/example")
        async def example_endpoint(db: Session = Depends(get_db)):
            ...
    """
    database_manager = None
    try:
        database_manager = DatabaseManager(Config.get_database_url())
        with database_manager.get_session() as session:
            yield session
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise DatabaseException(f"数据库连接失败: {str(e)}")
    finally:
        if database_manager:
            database_manager.close()


def get_strategy_manager() -> StrategyManager:
    """
    获取策略管理器实例依赖

    由于 StrategyManager 使用单例模式，多次调用会返回同一个实例。
    策略管理器在首次创建时会自动初始化（在 __init__ 中）。

    用法:
        @router.get("/example")
        async def example_endpoint(manager: StrategyManager = Depends(get_strategy_manager)):
            ...
    """
    # StrategyManager 是单例，创建时会自动初始化
    return StrategyManager()
