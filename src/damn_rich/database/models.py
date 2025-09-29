"""
数据库模型定义
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Exchange(Base):
    """交易所信息模型"""

    __tablename__ = "exchanges"

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 交易所基本信息
    name = Column(String(50), nullable=False, unique=True, comment="交易所名称")
    display_name = Column(String(100), nullable=True, comment="交易所显示名称")
    country = Column(String(50), nullable=True, comment="所在国家")
    website = Column(String(200), nullable=True, comment="官方网站")

    # 技术信息
    api_base_url = Column(String(200), nullable=True, comment="API基础URL")
    websocket_url = Column(String(200), nullable=True, comment="WebSocket URL")
    rate_limit = Column(Integer, nullable=True, comment="API限制频率(毫秒)")

    # 状态信息
    is_active = Column(Boolean, default=True, comment="是否激活")
    is_sandbox_supported = Column(Boolean, default=False, comment="是否支持沙盒")
    sandbox_url = Column(String(200), nullable=True, comment="沙盒环境URL")

    # 元数据
    description = Column(Text, nullable=True, comment="交易所描述")

    # 无关联关系定义（通过业务逻辑保证数据一致性）

    # 索引
    __table_args__ = (
        Index("idx_exchange_name", "name"),
        Index("idx_exchange_active", "is_active"),
    )

    def __repr__(self):
        return f"<Exchange(name={self.name}, display_name={self.display_name}, is_active={self.is_active})>"


class Symbol(Base):
    """交易对信息模型"""

    __tablename__ = "symbols"

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 交易对基本信息
    symbol = Column(String(50), nullable=False, comment="交易对符号")
    base_asset = Column(String(20), nullable=False, comment="基础资产")
    quote_asset = Column(String(20), nullable=False, comment="计价资产")

    # 交易对状态
    is_active = Column(Boolean, default=True, comment="是否激活")
    is_trading = Column(Boolean, default=True, comment="是否可交易")

    # 精度信息
    base_precision = Column(Integer, nullable=True, comment="基础资产精度")
    quote_precision = Column(Integer, nullable=True, comment="计价资产精度")
    min_order_size = Column(Float, nullable=True, comment="最小订单数量")
    max_order_size = Column(Float, nullable=True, comment="最大订单数量")

    # 无关联关系定义（通过业务逻辑保证数据一致性）

    # 索引
    __table_args__ = (
        Index("idx_symbol", "symbol"),
        Index("idx_base_quote", "base_asset", "quote_asset"),
        Index("idx_symbol_active", "is_active"),
    )

    def __repr__(self):
        return f"<Symbol(symbol={self.symbol}, base={self.base_asset}, quote={self.quote_asset})>"


class KlineData(Base):
    """K线数据模型"""

    __tablename__ = "kline_data"

    # 主键
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 关联ID（无外键约束）
    exchange_id = Column(Integer, nullable=False, comment="交易所ID")
    symbol_id = Column(Integer, nullable=False, comment="交易对ID")
    timeframe = Column(String(10), nullable=False, comment="时间周期")

    # 时间信息
    timestamp = Column(BigInteger, nullable=False, comment="K线时间戳(毫秒)")
    datetime = Column(DateTime, nullable=False, comment="K线时间")

    # OHLCV数据
    open_price = Column(Float, nullable=False, comment="开盘价")
    high_price = Column(Float, nullable=False, comment="最高价")
    low_price = Column(Float, nullable=False, comment="最低价")
    close_price = Column(Float, nullable=False, comment="收盘价")
    volume = Column(Float, nullable=False, comment="成交量")

    # 其他数据
    quote_volume = Column(Float, nullable=True, comment="成交额")
    trades_count = Column(Integer, nullable=True, comment="成交笔数")
    taker_buy_base_volume = Column(Float, nullable=True, comment="主动买入成交量")
    taker_buy_quote_volume = Column(Float, nullable=True, comment="主动买入成交额")

    # 无关联关系定义（通过业务逻辑保证数据一致性）

    # 索引
    __table_args__ = (
        Index("idx_exchange_symbol_timeframe", "exchange_id", "symbol_id", "timeframe"),
        Index("idx_timestamp", "timestamp"),
        Index("idx_datetime", "datetime"),
        Index("idx_exchange_symbol_timestamp", "exchange_id", "symbol_id", "timestamp"),
    )

    def __repr__(self):
        return f"<KlineData(exchange_id={self.exchange_id}, symbol_id={self.symbol_id}, timeframe={self.timeframe}, datetime={self.datetime})>"


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, database_url: str):
        """
        初始化数据库管理器

        Args:
            database_url: 数据库连接URL
        """
        self.database_url = database_url
        self.engine = None
        self.SessionLocal = None
        self._initialize()

    def _initialize(self):
        """初始化数据库连接"""
        try:
            self.engine = create_engine(
                self.database_url,
                echo=False,  # 设置为True可以看到SQL语句
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )
            print("数据库连接初始化成功")
        except Exception as e:
            print(f"数据库连接初始化失败: {e}")
            raise

    def create_tables(self):
        """创建所有表"""
        try:
            Base.metadata.create_all(bind=self.engine)
            print("数据库表创建成功")
        except Exception as e:
            print(f"创建数据库表失败: {e}")
            raise

    def get_session(self):
        """获取数据库会话"""
        return self.SessionLocal()

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            print("数据库连接已关闭")
