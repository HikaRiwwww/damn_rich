"""
数据同步服务

专注于K线数据同步、历史数据获取等功能
"""

import asyncio
import logging
import signal
from typing import Optional

from damn_rich.database.models import DatabaseManager
from damn_rich.infrastructure import SchedulerService
from damn_rich.task import KlineSyncTask
from damn_rich.utils.config import Config
from damn_rich.utils.logger import get_logger


class DataSyncService:
    """数据同步服务"""

    def __init__(self):
        """初始化数据同步服务"""
        self.database_manager: Optional[DatabaseManager] = None
        self.scheduler_service: Optional[SchedulerService] = None
        self.is_running = False
        self.logger = get_logger("data_sync")

    async def initialize(self) -> bool:
        """
        初始化系统组件

        Returns:
            bool: 初始化是否成功
        """
        try:
            # 初始化数据库管理器
            self.database_manager = DatabaseManager(Config.get_database_url())
            self.database_manager.create_tables()

            # 初始化调度服务
            self.scheduler_service = SchedulerService(
                database_manager=self.database_manager, use_redis=Config.USE_REDIS
            )

            self.logger.info("数据同步服务组件初始化完成")
            return True

        except Exception as e:
            self.logger.error(f"数据同步服务初始化失败: {e}")
            return False

    async def start(self) -> bool:
        """
        启动数据同步服务

        Returns:
            bool: 启动是否成功
        """
        try:
            self.is_running = True
            self.logger.info("🚀 数据同步服务启动中...")

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

            self.logger.info("✅ 数据同步服务启动成功")
            return True

        except Exception as e:
            self.logger.error(f"数据同步服务启动失败: {e}")
            return False

    async def stop(self):
        """停止数据同步服务"""
        try:
            self.is_running = False
            self.logger.info("正在停止数据同步服务...")

            if self.scheduler_service:
                self.scheduler_service.stop()

            if self.database_manager:
                self.database_manager.close()

            self.logger.info("数据同步服务已停止")

        except Exception as e:
            self.logger.error(f"停止数据同步服务时出错: {e}")

    async def run_manual_sync(self) -> bool:
        """
        手动执行K线数据同步

        Returns:
            bool: 同步是否成功
        """
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
                    return True
                else:
                    self.logger.error("手动K线数据同步失败")
                    return False
            else:
                self.logger.error("调度服务未初始化")
                return False

        except Exception as e:
            self.logger.error(f"手动同步失败: {e}")
            return False

    async def run(self):
        """运行数据同步服务"""

        # 设置信号处理
        def signal_handler(signum, frame):
            self.logger.info(f"接收到信号 {signum}")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # 初始化系统
            if not await self.initialize():
                self.logger.error("❌ 系统初始化失败")
                return

            # 启动服务
            if not await self.start():
                self.logger.error("❌ 数据同步服务启动失败")
                return

            self.logger.info("✅ 数据同步服务运行中...")
            self.logger.info("💡 使用 Ctrl+C 停止程序")

            # 保持程序运行
            while self.is_running:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("接收到键盘中断信号")
        except Exception as e:
            self.logger.error(f"程序运行出错: {e}")
        finally:
            await self.stop()
