"""
历史K线数据抓取模块
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import ccxt
import pandas as pd


class HistoricalDataFetcher:
    """历史数据抓取器"""

    def __init__(self, exchange_name: str = "binance", sandbox: bool = True):
        """
        初始化历史数据抓取器

        Args:
            exchange_name: 交易所名称
            sandbox: 是否使用沙盒环境
        """
        self.exchange_name = exchange_name
        self.sandbox = sandbox
        self.exchange = self._initialize_exchange()

    def _initialize_exchange(self):
        """初始化交易所连接"""
        try:
            exchange_class = getattr(ccxt, self.exchange_name)
            exchange = exchange_class(
                {
                    "sandbox": self.sandbox,
                    "enableRateLimit": True,
                    "rateLimit": 1200,  # 限制请求频率
                }
            )
            print(f"{self.exchange_name} 交易所连接初始化成功")
            return exchange
        except Exception as e:
            print(f"交易所连接初始化失败: {e}")
            raise

    def fetch_historical_data(
        self,
        symbol: str,
        timeframe: str = "1h",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000,
    ) -> Optional[List[List]]:
        """
        抓取历史K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            start_date: 开始时间
            end_date: 结束时间
            limit: 每次请求的数据条数

        Returns:
            K线数据列表
        """
        try:
            # 如果没有指定开始时间，默认获取最近30天的数据
            if start_date is None:
                start_date = datetime.now() - timedelta(days=30)

            if end_date is None:
                end_date = datetime.now()

            # 转换时间格式
            since = int(start_date.timestamp() * 1000)
            until = int(end_date.timestamp() * 1000)

            all_data = []
            current_since = since

            print(f"开始抓取 {symbol} {timeframe} 历史数据...")
            print(f"时间范围: {start_date} 到 {end_date}")

            while current_since < until:
                try:
                    # 抓取数据
                    ohlcv = self.exchange.fetch_ohlcv(
                        symbol=symbol,
                        timeframe=timeframe,
                        since=current_since,
                        limit=limit,
                    )

                    if not ohlcv:
                        break

                    all_data.extend(ohlcv)

                    # 更新下次请求的时间戳
                    current_since = ohlcv[-1][0] + 1

                    # 如果最后一条数据的时间戳超过结束时间，则停止
                    if ohlcv[-1][0] >= until:
                        break

                    # 限制请求频率
                    time.sleep(0.1)

                    print(f"已抓取 {len(all_data)} 条数据...")

                except Exception as e:
                    print(f"抓取数据时出错: {e}")
                    time.sleep(1)
                    continue

            print(f"数据抓取完成，共 {len(all_data)} 条记录")
            return all_data

        except Exception as e:
            print(f"抓取历史数据失败: {e}")
            return None

    def fetch_latest_data(
        self, symbol: str, timeframe: str = "1h", limit: int = 100
    ) -> Optional[List[List]]:
        """
        抓取最新的K线数据

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            limit: 数据条数

        Returns:
            K线数据列表
        """
        try:
            ohlcv = self.exchange.fetch_ohlcv(
                symbol=symbol, timeframe=timeframe, limit=limit
            )

            print(f"抓取到 {len(ohlcv)} 条最新数据")
            return ohlcv

        except Exception as e:
            print(f"抓取最新数据失败: {e}")
            return None

    def get_supported_symbols(self) -> List[str]:
        """
        获取支持的交易对列表

        Returns:
            交易对列表
        """
        try:
            markets = self.exchange.load_markets()
            symbols = list(markets.keys())
            print(f"获取到 {len(symbols)} 个交易对")
            return symbols
        except Exception as e:
            print(f"获取交易对列表失败: {e}")
            return []

    def get_supported_timeframes(self) -> List[str]:
        """
        获取支持的时间周期列表

        Returns:
            时间周期列表
        """
        try:
            timeframes = list(self.exchange.timeframes.keys())
            print(f"支持的时间周期: {timeframes}")
            return timeframes
        except Exception as e:
            print(f"获取时间周期列表失败: {e}")
            return []

    def validate_symbol(self, symbol: str) -> bool:
        """
        验证交易对是否有效

        Args:
            symbol: 交易对符号

        Returns:
            是否有效
        """
        try:
            markets = self.exchange.load_markets()
            return symbol in markets
        except Exception as e:
            print(f"验证交易对失败: {e}")
            return False

    def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对市场信息

        Args:
            symbol: 交易对符号

        Returns:
            市场信息字典
        """
        try:
            markets = self.exchange.load_markets()
            if symbol in markets:
                return markets[symbol]
            return None
        except Exception as e:
            print(f"获取市场信息失败: {e}")
            return None
