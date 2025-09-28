"""
市场数据获取模块
"""
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import ccxt


class MarketData:
    """市场数据获取类"""
    
    def __init__(self, exchange):
        """
        初始化市场数据获取器
        
        Args:
            exchange: 交易所客户端实例
        """
        self.exchange = exchange
    
    def get_historical_data(
        self, 
        symbol: str = 'BTC/USDT', 
        timeframe: str = '1h', 
        limit: int = 100
    ) -> Optional[pd.DataFrame]:
        """
        获取历史K线数据
        
        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            limit: 数据条数
            
        Returns:
            DataFrame格式的K线数据
        """
        try:
            # 获取OHLCV数据
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            
            # 转换为DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('datetime', inplace=True)
            
            return df
            
        except Exception as e:
            print(f"获取历史数据失败: {e}")
            return None
    
    def get_realtime_price(self, symbol: str = 'BTC/USDT') -> Optional[float]:
        """
        获取实时价格
        
        Args:
            symbol: 交易对符号
            
        Returns:
            当前价格
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            print(f"获取实时价格失败: {e}")
            return None
    
    def get_market_info(self, symbol: str = 'BTC/USDT') -> Optional[Dict[str, Any]]:
        """
        获取市场信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            市场信息字典
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return {
                'symbol': symbol,
                'price': ticker['last'],
                'bid': ticker['bid'],
                'ask': ticker['ask'],
                'volume': ticker['baseVolume'],
                'change': ticker['change'],
                'percentage': ticker['percentage'],
                'timestamp': ticker['timestamp']
            }
        except Exception as e:
            print(f"获取市场信息失败: {e}")
            return None
