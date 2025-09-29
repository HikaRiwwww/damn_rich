"""
历史K线数据抓取和存储演示程序
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# 添加src目录到Python路径
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from damn_rich.data.historical_fetcher import HistoricalDataFetcher
from damn_rich.database.connection import db_connection
from damn_rich.database.storage import DataStorage
from damn_rich.utils.config import Config


def main():
    """主程序入口"""
    print("🚀 历史K线数据抓取和存储演示程序启动...")

    # 验证配置
    if not Config.validate_config():
        print("❌ 配置验证失败")
        return

    try:
        # 测试数据库连接
        print("📡 测试数据库连接...")
        if not db_connection.test_connection():
            print("❌ 数据库连接失败，请检查数据库配置")
            return
        print("✅ 数据库连接成功")

        # 创建数据库表
        print("🗄️ 创建数据库表...")
        storage = DataStorage()
        storage.create_tables()
        print("✅ 数据库表创建成功")

        # 初始化数据抓取器
        print("📊 初始化数据抓取器...")
        fetcher = HistoricalDataFetcher(
            exchange_name=Config.DEFAULT_EXCHANGE, sandbox=Config.BINANCE_SANDBOX
        )

        # 测试抓取最新数据
        symbol = Config.DEFAULT_SYMBOL
        timeframe = Config.DEFAULT_TIMEFRAME

        print(f"💰 抓取 {symbol} {timeframe} 最新数据...")
        latest_data = fetcher.fetch_latest_data(symbol, timeframe, limit=10)

        if latest_data:
            print(f"✅ 成功抓取 {len(latest_data)} 条最新数据")

            # 存储数据到数据库
            print("💾 存储数据到数据库...")
            stored_count = storage.store_kline_data(
                exchange=Config.DEFAULT_EXCHANGE,
                symbol=symbol,
                timeframe=timeframe,
                kline_data=latest_data,
                batch_size=Config.BATCH_SIZE,
            )

            print(f"✅ 成功存储 {stored_count} 条数据")

            # 验证数据存储
            print("🔍 验证数据存储...")
            data_count = storage.get_data_count(
                exchange=Config.DEFAULT_EXCHANGE, symbol=symbol, timeframe=timeframe
            )
            print(f"📊 数据库中 {symbol} {timeframe} 数据条数: {data_count}")

            # 获取存储的数据
            print("📈 获取存储的数据...")
            df = storage.get_kline_data(
                exchange=Config.DEFAULT_EXCHANGE,
                symbol=symbol,
                timeframe=timeframe,
                limit=5,
            )

            if not df.empty:
                print("📊 最近5条K线数据:")
                print(df[["open", "high", "low", "close", "volume"]].tail())
            else:
                print("❌ 未找到数据")

        else:
            print("❌ 抓取数据失败")
            return

        # 测试历史数据抓取
        print("\n📅 测试历史数据抓取...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # 抓取最近7天的数据

        print(f"时间范围: {start_date} 到 {end_date}")

        historical_data = fetcher.fetch_historical_data(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            end_date=end_date,
            limit=100,
        )

        if historical_data:
            print(f"✅ 成功抓取 {len(historical_data)} 条历史数据")

            # 存储历史数据
            print("💾 存储历史数据...")
            stored_count = storage.store_kline_data(
                exchange=Config.DEFAULT_EXCHANGE,
                symbol=symbol,
                timeframe=timeframe,
                kline_data=historical_data,
                batch_size=Config.BATCH_SIZE,
            )

            print(f"✅ 成功存储 {stored_count} 条历史数据")

            # 最终数据统计
            final_count = storage.get_data_count(
                exchange=Config.DEFAULT_EXCHANGE, symbol=symbol, timeframe=timeframe
            )
            print(f"📊 数据库中总数据条数: {final_count}")

        else:
            print("❌ 抓取历史数据失败")

        print("\n🎉 数据抓取和存储演示完成！")
        print("💡 现在你可以使用这些数据进行策略开发和回测")

    except Exception as e:
        print(f"❌ 程序运行出错: {e}")
        return
    finally:
        # 关闭数据库连接
        db_connection.close()


if __name__ == "__main__":
    main()
