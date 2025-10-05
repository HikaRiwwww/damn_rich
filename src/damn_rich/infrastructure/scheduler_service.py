"""
基于APScheduler的调度服务
"""

import logging
from typing import Any, Dict, List, Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from damn_rich.database.models import DatabaseManager
from damn_rich.utils.config import Config


class SchedulerService:
    """基于APScheduler的调度服务"""

    def __init__(self, database_manager: DatabaseManager, use_redis: bool):
        """
        初始化调度服务

        Args:
            database_manager: 数据库管理器
            use_redis: 是否使用Redis作为任务存储，None时从环境变量读取
        """
        self.database_manager = database_manager
        self.use_redis = use_redis
        self.logger = logging.getLogger("scheduler_service")
        self.scheduler: Optional[BackgroundScheduler] = None
        self.job_stores = {}
        self.executors = {}
        self.job_defaults = {}

        # 配置任务存储
        self._configure_job_stores()

        # 配置执行器
        self._configure_executors()

        # 配置默认参数
        self._configure_job_defaults()

    def _configure_job_stores(self):
        """配置任务存储"""
        if self.use_redis:
            # 使用Redis作为任务存储（支持分布式和持久化）
            redis_config = Config.get_redis_config()
            self.job_stores = {"default": RedisJobStore(**redis_config)}
        else:
            # 使用内存存储（适合单机部署）
            self.job_stores = {"default": {"type": "memory"}}

    def _configure_executors(self):
        """配置执行器"""
        self.executors = {
            "default": ThreadPoolExecutor(max_workers=10),
            "high_priority": ThreadPoolExecutor(max_workers=5),
        }

    def _configure_job_defaults(self):
        """配置任务默认参数"""
        self.job_defaults = {
            "coalesce": True,  # 合并错过的任务
            "max_instances": 1,  # 最大并发实例数
            "misfire_grace_time": 300,  # 错过任务的宽限时间（秒）
        }

    def start(self):
        """启动调度器"""
        try:
            # 创建调度器
            self.scheduler = BackgroundScheduler(
                jobstores=self.job_stores,
                executors=self.executors,
                job_defaults=self.job_defaults,
                timezone="Asia/Shanghai",
            )

            # 添加事件监听器
            self.scheduler.add_listener(
                self._job_listener,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
            )

            # 启动调度器
            self.scheduler.start()
            self.logger.info("调度服务启动成功")

            return True

        except Exception as e:
            self.logger.error(f"调度服务启动失败: {e}")
            return False

    def stop(self):
        """停止调度器"""
        try:
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=True)
                self.logger.info("调度服务已停止")
        except Exception as e:
            self.logger.error(f"停止调度服务时出错: {e}")

    def _job_listener(self, event):
        """任务事件监听器"""
        if event.exception:
            self.logger.error(f"任务执行失败: {event.job_id}, 错误: {event.exception}")

    def add_job(self, func, job_id: str, trigger, **kwargs) -> bool:
        """
        添加任务

        Args:
            func: 要执行的函数
            job_id: 任务ID
            trigger: 触发器
            **kwargs: 其他参数

        Returns:
            bool: 是否添加成功
        """
        try:
            if not self.scheduler:
                self.logger.error("调度器未启动")
                return False

            self.scheduler.add_job(
                func=func, trigger=trigger, id=job_id, replace_existing=True, **kwargs
            )
            return True

        except Exception as e:
            self.logger.error(f"添加任务失败: {job_id}, 错误: {e}")
            return False

    def remove_job(self, job_id: str) -> bool:
        """
        移除任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 是否移除成功
        """
        try:
            if not self.scheduler:
                self.logger.error("调度器未启动")
                return False

            self.scheduler.remove_job(job_id)
            return True

        except Exception as e:
            self.logger.error(f"移除任务失败: {job_id}, 错误: {e}")
            return False

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务信息

        Args:
            job_id: 任务ID

        Returns:
            Dict: 任务信息
        """
        try:
            if not self.scheduler:
                return None

            job = self.scheduler.get_job(job_id)
            if job:
                return {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time,
                    "trigger": str(job.trigger),
                    "func": job.func.__name__
                    if hasattr(job.func, "__name__")
                    else str(job.func),
                }
            return None

        except Exception as e:
            self.logger.error(f"获取任务信息失败: {job_id}, 错误: {e}")
            return None

    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """
        获取所有任务信息

        Returns:
            List[Dict]: 任务信息列表
        """
        try:
            if not self.scheduler:
                return []

            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append(
                    {
                        "id": job.id,
                        "name": job.name,
                        "next_run_time": job.next_run_time,
                        "trigger": str(job.trigger),
                        "func": job.func.__name__
                        if hasattr(job.func, "__name__")
                        else str(job.func),
                    }
                )
            return jobs

        except Exception as e:
            self.logger.error(f"获取所有任务信息失败: {e}")
            return []

    def add_interval_job(self, func, job_id: str, **interval_kwargs) -> bool:
        """
        添加间隔任务

        Args:
            func: 要执行的函数
            job_id: 任务ID
            **interval_kwargs: 间隔参数 (seconds, minutes, hours, days, weeks)

        Returns:
            bool: 是否添加成功
        """
        trigger = IntervalTrigger(**interval_kwargs)
        return self.add_job(func, job_id, trigger)

    def add_cron_job(self, func, job_id: str, **cron_kwargs) -> bool:
        """
        添加Cron任务

        Args:
            func: 要执行的函数
            job_id: 任务ID
            **cron_kwargs: Cron参数 (year, month, day, week, day_of_week, hour, minute, second)

        Returns:
            bool: 是否添加成功
        """
        trigger = CronTrigger(**cron_kwargs)
        return self.add_job(func, job_id, trigger)

    def run_job_now(self, job_id: str) -> bool:
        """
        立即运行任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 是否运行成功
        """
        try:
            if not self.scheduler:
                self.logger.error("调度器未启动")
                return False

            job = self.scheduler.get_job(job_id)
            if job:
                # 直接执行任务函数
                try:
                    job.func()
                    return True
                except Exception as e:
                    self.logger.error(f"任务执行失败: {job_id}, 错误: {e}")
                    return False
            else:
                self.logger.error(f"任务不存在: {job_id}")
                return False

        except Exception as e:
            self.logger.error(f"立即运行任务失败: {job_id}, 错误: {e}")
            return False

    def pause_job(self, job_id: str) -> bool:
        """
        暂停任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 是否暂停成功
        """
        try:
            if not self.scheduler:
                self.logger.error("调度器未启动")
                return False

            self.scheduler.pause_job(job_id)
            return True

        except Exception as e:
            self.logger.error(f"暂停任务失败: {job_id}, 错误: {e}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """
        恢复任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 是否恢复成功
        """
        try:
            if not self.scheduler:
                self.logger.error("调度器未启动")
                return False

            self.scheduler.resume_job(job_id)
            return True

        except Exception as e:
            self.logger.error(f"恢复任务失败: {job_id}, 错误: {e}")
            return False

    def is_running(self) -> bool:
        """
        检查调度器是否运行中

        Returns:
            bool: 调度器是否运行中
        """
        return self.scheduler is not None and self.scheduler.running
