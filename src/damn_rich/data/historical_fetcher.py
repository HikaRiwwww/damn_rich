"""
历史K线数据抓取模块
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import ccxt

from damn_rich.utils.config import Config


class HistoricalDataFetcher:
    """历史数据抓取器"""

    def __init__(self, exchange_name: str = "binance", sandbox: bool = None):
        """
        初始化历史数据抓取器

        Args:
            exchange_name: 交易所名称
            sandbox: 是否使用沙盒环境
        """
        self.exchange_name = exchange_name
        # 如果没有指定sandbox，则从环境变量读取
        if sandbox is None:
            self.sandbox = Config.BINANCE_SANDBOX
        else:
            self.sandbox = sandbox
        self.exchange = self._initialize_exchange()

    def _initialize_exchange(self):
        """初始化交易所连接"""
        try:
            exchange_class = getattr(ccxt, self.exchange_name)

            # 构建交易所配置
            exchange_config = {
                "sandbox": self.sandbox,
                "enableRateLimit": True,
                # 移除 rateLimit 参数，和测试脚本保持一致
            }

            # 如果配置了API密钥，则添加到配置中
            if Config.BINANCE_API_KEY and Config.BINANCE_SECRET_KEY:
                exchange_config.update(
                    {
                        "apiKey": Config.BINANCE_API_KEY,
                        "secret": Config.BINANCE_SECRET_KEY,
                    }
                )
                print(f"{self.exchange_name} 交易所连接初始化成功 (使用API密钥)")
            else:
                print(f"{self.exchange_name} 交易所连接初始化成功 (公共API)")

            exchange = exchange_class(exchange_config)
            return exchange
        except Exception as e:
            print(f"交易所连接初始化失败: {e}")
            raise

    def _align_to_4h_open(self, timestamp_ms):
        # 转换成秒
        ts = timestamp_ms // 1000
        # 对齐到4小时开盘
        aligned = (ts // (4 * 3600)) * (4 * 3600)
        return aligned * 1000

    def fetch_binance_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_date: str,
        end_date: str = None,
        limit: int = 500,
    ):
        """
        从币安拉取任意时间区间的历史K线，自动分页
        :param symbol: 交易对，例如 'BTC/USDT'
        :param timeframe: 时间周期，例如 '1m', '1h', '4h', '1d'
        :param start_date: 起始时间（ISO格式字符串），例如 '2024-10-05T00:00:00Z'
        :param end_date: 结束时间（ISO格式字符串），默认为当前UTC时间
        :param limit: 每次请求的最大根数，默认500
        :return: pandas.DataFrame
        """

        since = self.exchange.parse8601(start_date)

        if end_date:
            end_ts = self.exchange.parse8601(end_date)
        else:
            end_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

        all_ohlcv = []

        while True:
            # 拉数据
            ohlcv = self.exchange.fetch_ohlcv(
                symbol, timeframe, since=since, limit=limit
            )
            if not ohlcv:
                break

            # 如果这一批最后一根时间 >= 结束时间，就截断
            if ohlcv[-1][0] >= end_ts:
                ohlcv = [x for x in ohlcv if x[0] <= end_ts]
                all_ohlcv += ohlcv
                break

            all_ohlcv += ohlcv

            # 更新 since，避免重复
            since = ohlcv[-1][0] + 1

            # 防止触发限频
            time.sleep(self.exchange.rateLimit / 1000)

            # 保护机制：防止死循环
            if since > end_ts:
                break

        return all_ohlcv

    def _get_max_limit_for_timeframe(self, timeframe: str) -> int:
        """
        根据时间周期获取最大limit值

        Args:
            timeframe: 时间周期

        Returns:
            最大limit值
        """
        # 币安API对不同时间周期的实际限制（基于测试结果）
        limits = {
            "1m": 1000,  # 约16小时
            "3m": 1000,  # 约2天
            "5m": 1000,  # 约3.5天
            "15m": 1000,  # 约10天
            "30m": 1000,  # 约20天
            "1h": 1000,  # 约41天
            "2h": 1000,  # 约83天
            "4h": 1000,  # 约166天，实际只返回约4天
            "6h": 1000,  # 约250天，实际只返回约4天
            "8h": 1000,  # 约333天，实际只返回约4天
            "12h": 1000,  # 约500天，实际只返回约4天
            "1d": 1000,  # 约1000天，实际只返回约4天
            "3d": 1000,  # 约3000天，实际只返回约4天
            "1w": 1000,  # 约7000天，实际只返回约4天
            "1M": 1000,  # 约30000天，实际只返回约4天
        }

        return limits.get(timeframe, 1000)

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
