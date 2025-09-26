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
    BINANCE_API_KEY: Optional[str] = os.getenv('BINANCE_API_KEY')
    BINANCE_SECRET_KEY: Optional[str] = os.getenv('BINANCE_SECRET_KEY')
    BINANCE_SANDBOX: bool = os.getenv('BINANCE_SANDBOX', 'true').lower() == 'true'
    
    # 交易配置
    DEFAULT_SYMBOL: str = os.getenv('DEFAULT_SYMBOL', 'BTC/USDT')
    DEFAULT_TIMEFRAME: str = os.getenv('DEFAULT_TIMEFRAME', '1h')
    MAX_POSITION_SIZE: float = float(os.getenv('MAX_POSITION_SIZE', '0.1'))
    
    # 日志配置
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
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
            'sandbox': cls.BINANCE_SANDBOX,
            'enableRateLimit': True,
        }
        
        if cls.BINANCE_API_KEY and cls.BINANCE_SECRET_KEY:
            config.update({
                'apiKey': cls.BINANCE_API_KEY,
                'secret': cls.BINANCE_SECRET_KEY,
            })
        
        return config
