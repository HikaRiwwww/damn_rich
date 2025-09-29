"""
数据库连接管理
"""

import os
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()


class DatabaseConnection:
    """数据库连接管理类"""

    def __init__(self):
        """初始化数据库连接"""
        self.engine = None
        self.SessionLocal = None
        self._initialize()

    def _initialize(self):
        """初始化数据库连接"""
        try:
            # 从环境变量获取数据库配置
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME", "crypto_trading")
            db_user = os.getenv("DB_USER", "postgres")
            db_password = os.getenv("DB_PASSWORD", "")

            # 构建数据库URL
            database_url = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )

            self.engine = create_engine(
                database_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=10,
                max_overflow=20,
            )

            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )

            print(f"数据库连接初始化成功: {db_host}:{db_port}/{db_name}")

        except Exception as e:
            print(f"数据库连接初始化失败: {e}")
            raise

    def get_session(self):
        """获取数据库会话"""
        return self.SessionLocal()

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"数据库连接测试失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            print("数据库连接已关闭")


# 全局数据库连接实例
db_connection = DatabaseConnection()
