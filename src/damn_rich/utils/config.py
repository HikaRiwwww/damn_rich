"""
配置管理模块
"""

import os
from typing import Optional

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class Config:
    """配置管理类"""

    # 交易所配置
    BINANCE_API_KEY: Optional[str] = os.getenv("BINANCE_API_KEY")
    BINANCE_SECRET_KEY: Optional[str] = os.getenv("BINANCE_SECRET_KEY")
    BINANCE_SANDBOX: bool = os.getenv("BINANCE_SANDBOX", "true").lower() == "true"

    # 交易配置
    DEFAULT_SYMBOL: str = os.getenv("DEFAULT_SYMBOL", "BTC/USDT")
    DEFAULT_TIMEFRAME: str = os.getenv("DEFAULT_TIMEFRAME", "1h")
    MAX_POSITION_SIZE: float = float(os.getenv("MAX_POSITION_SIZE", "0.1"))

    # 日志配置
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # 数据库配置
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    DB_NAME: str = os.getenv("DB_NAME", "crypto_trading")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # 数据抓取配置
    DEFAULT_EXCHANGE: str = os.getenv("DEFAULT_EXCHANGE", "binance")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "0.1"))

    # Redis配置
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
    REDIS_PREFIX: str = os.getenv("REDIS_PREFIX", "trading_bot:jobs:")
    USE_REDIS: bool = os.getenv("USE_REDIS", "false").lower() == "true"

    @classmethod
    def validate_config(cls) -> bool:
        """
        验证配置是否完整

        Returns:
            配置是否有效
        """
        if not cls.BINANCE_API_KEY or not cls.BINANCE_SECRET_KEY:
            print("警告: 未配置API密钥，将使用公共API")
            return True

        return True

    @classmethod
    def get_exchange_config(cls) -> dict:
        """
        获取交易所配置

        Returns:
            交易所配置字典
        """
        config = {
            "sandbox": cls.BINANCE_SANDBOX,
            "enableRateLimit": True,
        }

        if cls.BINANCE_API_KEY and cls.BINANCE_SECRET_KEY:
            config.update(
                {
                    "apiKey": cls.BINANCE_API_KEY,
                    "secret": cls.BINANCE_SECRET_KEY,
                }
            )

        return config

    @classmethod
    def get_database_url(cls) -> str:
        """
        获取数据库连接URL

        Returns:
            数据库连接URL
        """
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    @classmethod
    def get_redis_config(cls) -> dict:
        """
        获取Redis配置

        Returns:
            Redis配置字典
        """
        config = {
            "host": cls.REDIS_HOST,
            "port": cls.REDIS_PORT,
            "db": cls.REDIS_DB,
            "prefix": cls.REDIS_PREFIX,
        }

        if cls.REDIS_PASSWORD:
            config["password"] = cls.REDIS_PASSWORD

        return config
