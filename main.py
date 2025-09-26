"""
加密货币量化交易机器人 - 主程序
"""
import sys
import os
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from damn_rich.exchange.binance_client import BinanceClient
from damn_rich.data.market_data import MarketData
from damn_rich.utils.config import Config


def main():
    """主程序入口"""
    print("🚀 加密货币量化交易机器人启动中...")
    
    # 验证配置
    if not Config.validate_config():
        print("❌ 配置验证失败")
        return
    
    try:
        # 初始化币安客户端
        print("📡 连接币安交易所...")
        binance_client = BinanceClient(sandbox=Config.BINANCE_SANDBOX)
        
        # 检查连接状态
        if not binance_client.is_connected():
            print("❌ 无法连接到币安交易所")
            return
        
        print("✅ 币安交易所连接成功")
        
        # 初始化市场数据获取器
        market_data = MarketData(binance_client.exchange)
        
        # 获取实时价格
        print(f"💰 获取 {Config.DEFAULT_SYMBOL} 实时价格...")
        price = market_data.get_realtime_price(Config.DEFAULT_SYMBOL)
        
        if price:
            print(f"📈 {Config.DEFAULT_SYMBOL} 当前价格: ${price:,.2f}")
        else:
            print("❌ 无法获取价格信息")
            return
        
        # 获取历史数据
        print(f"📊 获取 {Config.DEFAULT_SYMBOL} 历史数据...")
        historical_data = market_data.get_historical_data(
            symbol=Config.DEFAULT_SYMBOL,
            timeframe=Config.DEFAULT_TIMEFRAME,
            limit=10
        )
        
        if historical_data is not None and not historical_data.empty:
            print(f"📈 最近10根K线数据:")
            print(historical_data[['open', 'high', 'low', 'close', 'volume']].tail())
        else:
            print("❌ 无法获取历史数据")
            return
        
        # 获取市场信息
        print(f"📋 获取 {Config.DEFAULT_SYMBOL} 市场信息...")
        market_info = market_data.get_market_info(Config.DEFAULT_SYMBOL)
        
        if market_info:
            print(f"📊 市场信息:")
            print(f"   价格: ${market_info['price']:,.2f}")
            print(f"   买一价: ${market_info['bid']:,.2f}")
            print(f"   卖一价: ${market_info['ask']:,.2f}")
            print(f"   24h成交量: {market_info['volume']:,.2f}")
            print(f"   24h涨跌: {market_info['percentage']:+.2f}%")
        
        print("\n🎉 系统运行正常，所有模块测试通过！")
        print("💡 下一步可以开始开发交易策略和回测系统")
        
    except Exception as e:
        print(f"❌ 系统运行出错: {e}")
        return


if __name__ == "__main__":
    main()
