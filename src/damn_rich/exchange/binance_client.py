"""
币安交易所客户端
"""
import ccxt
import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()


class BinanceClient:
    """币安交易所客户端"""
    
    def __init__(self, sandbox: bool = True):
        """
        初始化币安客户端
        
        Args:
            sandbox: 是否使用沙盒环境
        """
        self.sandbox = sandbox
        self.exchange = None
        self._initialize_exchange()
    
    def _initialize_exchange(self):
        """初始化交易所连接"""
        try:
            # 获取API密钥
            api_key = os.getenv('BINANCE_API_KEY')
            secret = os.getenv('BINANCE_SECRET_KEY')
            
            if not api_key or not secret:
                print("警告: 未找到API密钥，将使用公共API")
                self.exchange = ccxt.binance({
                    'sandbox': self.sandbox,
                    'enableRateLimit': True,
                })
            else:
                self.exchange = ccxt.binance({
                    'apiKey': api_key,
                    'secret': secret,
                    'sandbox': self.sandbox,
                    'enableRateLimit': True,
                })
            
            print(f"币安客户端初始化成功 (沙盒模式: {self.sandbox})")
            
        except Exception as e:
            print(f"币安客户端初始化失败: {e}")
            raise
    
    def get_ticker(self, symbol: str = 'BTC/USDT') -> Optional[Dict[str, Any]]:
        """
        获取交易对价格信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            价格信息字典
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker
        except Exception as e:
            print(f"获取价格信息失败: {e}")
            return None
    
    def get_balance(self) -> Optional[Dict[str, Any]]:
        """
        获取账户余额
        
        Returns:
            余额信息字典
        """
        try:
            balance = self.exchange.fetch_balance()
            return balance
        except Exception as e:
            print(f"获取余额失败: {e}")
            return None
    
    def is_connected(self) -> bool:
        """
        检查连接状态
        
        Returns:
            是否连接成功
        """
        try:
            # 尝试获取服务器时间
            self.exchange.fetch_time()
            return True
        except Exception:
            return False
