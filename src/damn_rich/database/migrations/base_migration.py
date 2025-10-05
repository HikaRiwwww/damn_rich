"""
数据库迁移基类
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


class BaseMigration(ABC):
    """数据库迁移基类"""

    def __init__(self):
        self.version = self.get_version()
        self.description = self.get_description()
        self.created_at = datetime.utcnow()

    @abstractmethod
    def get_version(self) -> str:
        """
        获取迁移版本号

        Returns:
            str: 版本号，格式如 "001", "002" 等
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        获取迁移描述

        Returns:
            str: 迁移描述
        """
        pass

    @abstractmethod
    def upgrade(self, session: Session) -> bool:
        """
        执行升级操作

        Args:
            session: 数据库会话

        Returns:
            bool: 升级是否成功
        """
        pass

    @abstractmethod
    def downgrade(self, session: Session) -> bool:
        """
        执行降级操作

        Args:
            session: 数据库会话

        Returns:
            bool: 降级是否成功
        """
        pass

    def get_dependencies(self) -> List[str]:
        """
        获取依赖的迁移版本

        Returns:
            List[str]: 依赖的版本号列表
        """
        return []

    def validate_before_upgrade(self, session: Session) -> bool:
        """
        升级前验证

        Args:
            session: 数据库会话

        Returns:
            bool: 验证是否通过
        """
        return True

    def validate_after_upgrade(self, session: Session) -> bool:
        """
        升级后验证

        Args:
            session: 数据库会话

        Returns:
            bool: 验证是否通过
        """
        return True

    def get_rollback_sql(self) -> List[str]:
        """
        获取回滚SQL语句

        Returns:
            List[str]: 回滚SQL语句列表
        """
        return []
