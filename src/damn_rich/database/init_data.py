"""
数据库初始化数据
"""

from typing import Any, Dict, List

from .connection import db_connection
from .models import Exchange, Symbol


class DatabaseInitializer:
    """数据库初始化器"""

    def __init__(self):
        """初始化数据库初始化器"""
        self.db = db_connection

    def init_exchanges(self):
        """初始化交易所数据"""
        session = self.db.get_session()

        try:
            # 检查是否已有数据
            existing_count = session.query(Exchange).count()
            if existing_count > 0:
                print(f"交易所数据已存在 ({existing_count} 条记录)")
                return

            # 预定义的交易所数据
            exchanges_data = [
                {
                    "name": "binance",
                    "display_name": "Binance",
                    "country": "Cayman Islands",
                    "website": "https://www.binance.com",
                    "api_base_url": "https://api.binance.com",
                    "websocket_url": "wss://stream.binance.com:9443",
                    "rate_limit": 1200,
                    "is_active": True,
                    "is_sandbox_supported": True,
                    "sandbox_url": "https://testnet.binance.vision",
                    "description": "全球最大的加密货币交易所",
                },
                {
                    "name": "okx",
                    "display_name": "OKX",
                    "country": "Seychelles",
                    "website": "https://www.okx.com",
                    "api_base_url": "https://www.okx.com",
                    "websocket_url": "wss://ws.okx.com:8443",
                    "rate_limit": 1000,
                    "is_active": True,
                    "is_sandbox_supported": True,
                    "sandbox_url": "https://www.okx.com",
                    "description": "全球领先的数字资产交易平台",
                },
                {
                    "name": "huobi",
                    "display_name": "Huobi",
                    "country": "Seychelles",
                    "website": "https://www.huobi.com",
                    "api_base_url": "https://api.huobi.pro",
                    "websocket_url": "wss://api.huobi.pro/ws",
                    "rate_limit": 1000,
                    "is_active": True,
                    "is_sandbox_supported": False,
                    "description": "全球知名的数字资产交易平台",
                },
                {
                    "name": "coinbase",
                    "display_name": "Coinbase Pro",
                    "country": "United States",
                    "website": "https://pro.coinbase.com",
                    "api_base_url": "https://api.pro.coinbase.com",
                    "websocket_url": "wss://ws-feed.pro.coinbase.com",
                    "rate_limit": 1000,
                    "is_active": True,
                    "is_sandbox_supported": True,
                    "sandbox_url": "https://api-public.sandbox.pro.coinbase.com",
                    "description": "美国领先的数字货币交易平台",
                },
            ]

            # 批量插入交易所数据
            for exchange_data in exchanges_data:
                exchange = Exchange(**exchange_data)
                session.add(exchange)

            session.commit()
            print(f"成功初始化 {len(exchanges_data)} 个交易所")

        except Exception as e:
            session.rollback()
            print(f"初始化交易所数据失败: {e}")
            raise
        finally:
            session.close()

    def init_common_symbols(self):
        """初始化常用交易对数据"""
        session = self.db.get_session()

        try:
            # 检查是否已有数据
            existing_count = session.query(Symbol).count()
            if existing_count > 0:
                print(f"交易对数据已存在 ({existing_count} 条记录)")
                return

            # 常用交易对数据
            symbols_data = [
                # BTC交易对
                {"symbol": "BTC/USDT", "base_asset": "BTC", "quote_asset": "USDT"},
                {"symbol": "BTC/USD", "base_asset": "BTC", "quote_asset": "USD"},
                {"symbol": "BTC/EUR", "base_asset": "BTC", "quote_asset": "EUR"},
                {"symbol": "BTC/BUSD", "base_asset": "BTC", "quote_asset": "BUSD"},
                # ETH交易对
                {"symbol": "ETH/USDT", "base_asset": "ETH", "quote_asset": "USDT"},
                {"symbol": "ETH/BTC", "base_asset": "ETH", "quote_asset": "BTC"},
                {"symbol": "ETH/USD", "base_asset": "ETH", "quote_asset": "USD"},
                {"symbol": "ETH/BUSD", "base_asset": "ETH", "quote_asset": "BUSD"},
                # BNB交易对
                {"symbol": "BNB/USDT", "base_asset": "BNB", "quote_asset": "USDT"},
                {"symbol": "BNB/BTC", "base_asset": "BNB", "quote_asset": "BTC"},
                {"symbol": "BNB/BUSD", "base_asset": "BNB", "quote_asset": "BUSD"},
                # 其他主流币种
                {"symbol": "ADA/USDT", "base_asset": "ADA", "quote_asset": "USDT"},
                {"symbol": "SOL/USDT", "base_asset": "SOL", "quote_asset": "USDT"},
                {"symbol": "DOT/USDT", "base_asset": "DOT", "quote_asset": "USDT"},
                {"symbol": "MATIC/USDT", "base_asset": "MATIC", "quote_asset": "USDT"},
                {"symbol": "AVAX/USDT", "base_asset": "AVAX", "quote_asset": "USDT"},
                {"symbol": "LINK/USDT", "base_asset": "LINK", "quote_asset": "USDT"},
                {"symbol": "UNI/USDT", "base_asset": "UNI", "quote_asset": "USDT"},
                {"symbol": "LTC/USDT", "base_asset": "LTC", "quote_asset": "USDT"},
                {"symbol": "XRP/USDT", "base_asset": "XRP", "quote_asset": "USDT"},
                {"symbol": "DOGE/USDT", "base_asset": "DOGE", "quote_asset": "USDT"},
            ]

            # 批量插入交易对数据
            for symbol_data in symbols_data:
                symbol = Symbol(
                    symbol=symbol_data["symbol"],
                    base_asset=symbol_data["base_asset"],
                    quote_asset=symbol_data["quote_asset"],
                    is_active=True,
                    is_trading=True,
                )
                session.add(symbol)

            session.commit()
            print(f"成功初始化 {len(symbols_data)} 个交易对")

        except Exception as e:
            session.rollback()
            print(f"初始化交易对数据失败: {e}")
            raise
        finally:
            session.close()

    def init_all(self):
        """初始化所有数据"""
        print("🚀 开始初始化数据库...")

        # 创建表
        from .models import Base

        Base.metadata.create_all(bind=self.db.engine)
        print("✅ 数据库表创建完成")

        # 初始化交易所数据
        print("📊 初始化交易所数据...")
        self.init_exchanges()

        # 初始化交易对数据
        print("💰 初始化交易对数据...")
        self.init_common_symbols()

        print("🎉 数据库初始化完成！")

    def get_exchange_by_name(self, name: str) -> Exchange:
        """根据名称获取交易所"""
        session = self.db.get_session()
        try:
            return session.query(Exchange).filter(Exchange.name == name).first()
        finally:
            session.close()

    def get_symbol_by_name(self, symbol: str) -> Symbol:
        """根据符号获取交易对"""
        session = self.db.get_session()
        try:
            return session.query(Symbol).filter(Symbol.symbol == symbol).first()
        finally:
            session.close()
