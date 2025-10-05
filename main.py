"""
加密货币量化交易机器人 - 主程序

支持多种服务模式：
- data-sync: 数据同步服务
- trading-bot: 交易执行服务
"""

import asyncio
import sys

from damn_rich.services import DataSyncService, TradingBotService
from damn_rich.utils.logger import setup_logging


async def run_data_sync_service():
    """运行数据同步服务"""
    logger = setup_logging("data_sync")

    logger.info("启动数据同步服务...")
    service = DataSyncService()
    await service.run()


async def run_trading_bot_service():
    """运行交易执行服务"""
    logger = setup_logging("trading_bot")

    logger.info("启动交易执行服务...")
    service = TradingBotService()
    await service.run()


async def main():
    """主函数"""
    # 检查命令行参数
    if len(sys.argv) > 1:
        service_type = sys.argv[1].lower()

        if service_type == "data-sync":
            await run_data_sync_service()
        elif service_type == "trading-bot":
            await run_trading_bot_service()
        else:
            print("❌ 未知的服务类型")
            print_usage()
            sys.exit(1)
    else:
        print("❌ 请指定服务类型")
        print_usage()
        sys.exit(1)


def print_usage():
    """打印使用说明"""
    print("\n📖 使用说明:")
    print("  python main.py data-sync    # 启动数据同步服务")
    print("  python main.py trading-bot  # 启动交易执行服务")
    print("\n🐳 Docker 部署:")
    print("  docker run --name data-sync crypto-bot:latest python main.py data-sync")
    print(
        "  docker run --name trading-bot crypto-bot:latest python main.py trading-bot"
    )


if __name__ == "__main__":
    asyncio.run(main())
