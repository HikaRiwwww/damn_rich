"""
数据管理模块 - 统一的数据抓取和存储接口
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ..database.connection import db_connection
from ..database.init_data import DatabaseInitializer
from ..database.storage import DataStorage
from ..utils.config import Config
from .historical_fetcher import HistoricalDataFetcher


class DataManager:
    """数据管理器 - 统一的数据抓取和存储接口"""

    def __init__(self, exchange_name: str = None, sandbox: bool = None):
        """
        初始化数据管理器

        Args:
            exchange_name: 交易所名称，默认使用配置中的值
            sandbox: 是否使用沙盒环境，默认使用配置中的值
        """
        self.exchange_name = exchange_name or Config.DEFAULT_EXCHANGE
        self.sandbox = sandbox if sandbox is not None else Config.BINANCE_SANDBOX

        # 初始化组件
        self.fetcher = HistoricalDataFetcher(self.exchange_name, self.sandbox)
        self.storage = DataStorage()
        self.initializer = DatabaseInitializer()

        # 确保数据库表存在并初始化基础数据
        self.storage.create_tables()
        self.initializer.init_all()

    def sync_historical_data(
        self, symbol: str, timeframe: str = "1h", days: int = 30, batch_size: int = None
    ) -> Dict[str, Any]:
        """
        同步历史数据到数据库

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            days: 抓取天数
            batch_size: 批处理大小

        Returns:
            同步结果统计
        """
        batch_size = batch_size or Config.BATCH_SIZE

        print(f"🔄 开始同步 {symbol} {timeframe} 历史数据...")

        # 获取数据库中最新时间戳
        latest_timestamp = self.storage.get_latest_timestamp(
            self.exchange_name, symbol, timeframe
        )

        # 计算开始时间
        if latest_timestamp:
            start_date = datetime.fromtimestamp(latest_timestamp / 1000)
            print(f"📅 从最新数据时间开始: {start_date}")
        else:
            start_date = datetime.now() - timedelta(days=days)
            print(f"📅 从 {days} 天前开始: {start_date}")

        end_date = datetime.now()

        # 抓取数据
        kline_data = self.fetcher.fetch_historical_data(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            limit=1000,
        )

        if not kline_data:
            return {
                "success": False,
                "message": "没有抓取到数据",
                "fetched_count": 0,
                "stored_count": 0,
            }

        # 存储数据
        stored_count = self.storage.store_kline_data(
            exchange=self.exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            kline_data=kline_data,
            batch_size=batch_size,
        )

        result = {
            "success": True,
            "message": "数据同步完成",
            "fetched_count": len(kline_data),
            "stored_count": stored_count,
            "symbol": symbol,
            "timeframe": timeframe,
            "exchange": self.exchange_name,
        }

        print(f"✅ 同步完成: 抓取 {len(kline_data)} 条，存储 {stored_count} 条")
        return result

    def get_kline_data(
        self,
        symbol: str,
        timeframe: str = "1h",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        获取K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            start_date: 开始时间
            end_date: 结束时间
            limit: 限制条数

        Returns:
            K线数据DataFrame
        """
        start_timestamp = int(start_date.timestamp() * 1000) if start_date else None
        end_timestamp = int(end_date.timestamp() * 1000) if end_date else None

        return self.storage.get_kline_data(
            exchange=self.exchange_name,
            symbol=symbol,
            timeframe=timeframe,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            limit=limit,
        )

    def get_latest_data(
        self, symbol: str, timeframe: str = "1h", limit: int = 100
    ) -> pd.DataFrame:
        """
        获取最新K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            limit: 数据条数

        Returns:
            K线数据DataFrame
        """
        return self.get_kline_data(symbol, timeframe, limit=limit)

    def get_data_info(self, symbol: str, timeframe: str = "1h") -> Dict[str, Any]:
        """
        获取数据信息

        Args:
            symbol: 交易对符号
            timeframe: 时间周期

        Returns:
            数据信息字典
        """
        count = self.storage.get_data_count(self.exchange_name, symbol, timeframe)

        latest_timestamp = self.storage.get_latest_timestamp(
            self.exchange_name, symbol, timeframe
        )

        latest_datetime = None
        if latest_timestamp:
            latest_datetime = datetime.fromtimestamp(latest_timestamp / 1000)

        return {
            "exchange": self.exchange_name,
            "symbol": symbol,
            "timeframe": timeframe,
            "total_count": count,
            "latest_timestamp": latest_timestamp,
            "latest_datetime": latest_datetime,
        }

    def sync_multiple_symbols(
        self, symbols: List[str], timeframe: str = "1h", days: int = 30
    ) -> Dict[str, Any]:
        """
        同步多个交易对的数据

        Args:
            symbols: 交易对列表
            timeframe: 时间周期
            days: 抓取天数

        Returns:
            同步结果统计
        """
        results = {}

        for symbol in symbols:
            print(f"\n📊 同步 {symbol} 数据...")
            try:
                result = self.sync_historical_data(symbol, timeframe, days)
                results[symbol] = result
            except Exception as e:
                print(f"❌ 同步 {symbol} 失败: {e}")
                results[symbol] = {
                    "success": False,
                    "message": str(e),
                    "fetched_count": 0,
                    "stored_count": 0,
                }

        # 统计结果
        total_fetched = sum(r.get("fetched_count", 0) for r in results.values())
        total_stored = sum(r.get("stored_count", 0) for r in results.values())
        success_count = sum(1 for r in results.values() if r.get("success", False))

        summary = {
            "total_symbols": len(symbols),
            "success_count": success_count,
            "failed_count": len(symbols) - success_count,
            "total_fetched": total_fetched,
            "total_stored": total_stored,
            "results": results,
        }

        print(f"\n📊 批量同步完成:")
        print(f"   成功: {success_count}/{len(symbols)}")
        print(f"   总抓取: {total_fetched} 条")
        print(f"   总存储: {total_stored} 条")

        return summary

    def cleanup_old_data(
        self, symbol: str, timeframe: str = "1h", keep_days: int = 365
    ) -> int:
        """
        清理旧数据

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            keep_days: 保留天数

        Returns:
            删除的记录数
        """
        cutoff_timestamp = int(
            (datetime.now() - timedelta(days=keep_days)).timestamp() * 1000
        )

        deleted_count = self.storage.delete_old_data(
            self.exchange_name, symbol, timeframe, cutoff_timestamp
        )

        print(f"🗑️ 清理了 {deleted_count} 条旧数据")
        return deleted_count
