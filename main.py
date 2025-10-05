"""
加密货币量化交易机器人 - 主程序
"""

import asyncio
import logging
import signal
import sys

from damn_rich.database.models import DatabaseManager
from damn_rich.task import KlineSyncTask, SchedulerService
from damn_rich.utils.config import Config


class TradingBot:
    """交易机器人主类"""

    def __init__(self):
        """初始化交易机器人"""
        self.database_manager = None
        self.scheduler_service = None
        self.is_running = False
        self.logger = logging.getLogger("trading_bot")

    async def initialize(self):
        """初始化系统组件"""
        try:
            # 初始化数据库管理器
            self.database_manager = DatabaseManager(Config.get_database_url())
            self.database_manager.create_tables()

            # 初始化调度服务
            self.scheduler_service = SchedulerService(
                self.database_manager, use_redis=Config.USE_REDIS
            )

            self.logger.info("系统组件初始化完成")
            return True

        except Exception as e:
            self.logger.error(f"系统初始化失败: {e}")
            return False

    async def start(self):
        """启动交易机器人"""
        try:
            self.is_running = True
            self.logger.info("🚀 交易机器人启动中...")

            # 启动调度服务
            if self.scheduler_service:
                # 启动调度器
                if not self.scheduler_service.start():
                    return False

                # 添加K线数据同步任务（每4小时执行一次）
                def sync_job():
                    import asyncio
                    import concurrent.futures

                    async def _sync():
                        task = KlineSyncTask(self.database_manager)
                        return await task.execute()

                    # 使用线程池执行异步函数，避免事件循环冲突
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(asyncio.run, _sync())
                        return future.result()

                self.scheduler_service.add_interval_job(
                    func=sync_job,
                    job_id="kline_sync",
                    hours=4,
                )

                # 立即执行一次K线同步任务
                self.scheduler_service.run_job_now("kline_sync")

            return True

        except Exception as e:
            self.logger.error(f"交易机器人启动失败: {e}")
            return False

    async def stop(self):
        """停止交易机器人"""
        try:
            self.is_running = False
            self.logger.info("正在停止交易机器人...")

            if self.scheduler_service:
                self.scheduler_service.stop()

            if self.database_manager:
                self.database_manager.close()

            self.logger.info("交易机器人已停止")

        except Exception as e:
            self.logger.error(f"停止交易机器人时出错: {e}")

    async def run_manual_sync(self):
        """手动执行K线数据同步"""
        try:
            if self.scheduler_service:
                # 启动调度器（如果未启动）
                if not self.scheduler_service.is_running():
                    if not self.scheduler_service.start():
                        self.logger.error("无法启动调度器")
                        return False

                # 添加K线同步任务（如果不存在）
                jobs = self.scheduler_service.get_all_jobs()
                if not any(job["id"] == "kline_sync" for job in jobs):
                    # 创建一个包装函数，避免闭包问题
                    def sync_job():
                        import asyncio
                        import concurrent.futures

                        async def _sync():
                            task = KlineSyncTask(self.database_manager)
                            return await task.execute()

                        # 使用线程池执行异步函数，避免事件循环冲突
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(asyncio.run, _sync())
                            return future.result()

                    self.scheduler_service.add_interval_job(
                        func=sync_job,
                        job_id="kline_sync",
                        hours=4,
                    )

                # 立即执行同步任务
                result = self.scheduler_service.run_job_now("kline_sync")
                if result:
                    self.logger.info("手动K线数据同步完成")
                else:
                    self.logger.error("手动K线数据同步失败")
                return result
            return False

        except Exception as e:
            self.logger.error(f"手动同步失败: {e}")
            return False


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("trading_bot.log", encoding="utf-8"),
        ],
    )


async def main():
    """主程序入口"""
    setup_logging()
    logger = logging.getLogger("main")

    # 验证配置
    if not Config.validate_config():
        logger.error("❌ 配置验证失败")
        return

    # 创建交易机器人实例
    bot = TradingBot()

    # 设置信号处理
    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在停止...")
        asyncio.create_task(bot.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 初始化系统
        if not await bot.initialize():
            logger.error("❌ 系统初始化失败")
            return

        # 检查命令行参数
        if len(sys.argv) > 1 and sys.argv[1] == "sync":
            # 手动同步模式
            logger.info("🔄 执行手动K线数据同步...")
            success = await bot.run_manual_sync()
            if success:
                logger.info("✅ 手动同步完成")
            else:
                logger.error("❌ 手动同步失败")
            return

        # 启动交易机器人
        if not await bot.start():
            logger.error("❌ 交易机器人启动失败")
            return

        logger.info("✅ 交易机器人运行中...")
        logger.info("💡 使用 Ctrl+C 停止程序")
        logger.info("💡 使用 'python main.py sync' 执行手动同步")

        # 保持程序运行
        while bot.is_running:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("接收到键盘中断信号")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
    finally:
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
