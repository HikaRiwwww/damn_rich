"""
基础任务类和调度器
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from damn_rich.database.models import DatabaseManager


class BaseTask(ABC):
    """基础任务类"""

    def __init__(self, name: str, description: str = ""):
        """
        初始化基础任务

        Args:
            name: 任务名称
            description: 任务描述
        """
        self.name = name
        self.description = description
        self.logger = logging.getLogger(f"task.{name}")
        self.is_running = False
        self.last_run_time: Optional[datetime] = None
        self.next_run_time: Optional[datetime] = None
        self.run_count = 0
        self.error_count = 0
        self.last_error: Optional[str] = None

    @abstractmethod
    async def execute(self) -> bool:
        """
        执行任务逻辑

        Returns:
            bool: 任务是否执行成功
        """
        pass

    async def run(self) -> bool:
        """
        运行任务（包含错误处理和统计）

        Returns:
            bool: 任务是否执行成功
        """
        self.is_running = True
        start_time = datetime.now()

        try:
            self.logger.info(f"开始执行任务: {self.name}")
            result = await self.execute()

            if result:
                self.run_count += 1
                self.last_run_time = datetime.now()
                self.last_error = None
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"任务执行成功: {self.name}, 耗时: {duration:.2f}秒")
            else:
                self.error_count += 1
                self.last_error = "任务执行返回失败"
                self.logger.error(f"任务执行失败: {self.name}")

            return result

        except Exception as e:
            self.error_count += 1
            self.last_error = str(e)
            self.logger.error(f"任务执行异常: {self.name}, 错误: {e}")
            return False
        finally:
            self.is_running = False

    def get_status(self) -> Dict[str, Any]:
        """
        获取任务状态

        Returns:
            Dict: 任务状态信息
        """
        return {
            "name": self.name,
            "description": self.description,
            "is_running": self.is_running,
            "last_run_time": self.last_run_time.isoformat()
            if self.last_run_time
            else None,
            "next_run_time": self.next_run_time.isoformat()
            if self.next_run_time
            else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
        }


class TaskScheduler:
    """任务调度器"""

    def __init__(self, database_manager: DatabaseManager):
        """
        初始化任务调度器

        Args:
            database_manager: 数据库管理器
        """
        self.database_manager = database_manager
        self.tasks: Dict[str, BaseTask] = {}
        self.logger = logging.getLogger("task.scheduler")
        self.is_running = False
        self._stop_event = asyncio.Event()

    def register_task(self, task: BaseTask) -> None:
        """
        注册任务

        Args:
            task: 要注册的任务
        """
        self.tasks[task.name] = task
        self.logger.info(f"注册任务: {task.name}")

    def unregister_task(self, task_name: str) -> None:
        """
        注销任务

        Args:
            task_name: 任务名称
        """
        if task_name in self.tasks:
            del self.tasks[task_name]
            self.logger.info(f"注销任务: {task_name}")

    def get_task(self, task_name: str) -> Optional[BaseTask]:
        """
        获取任务

        Args:
            task_name: 任务名称

        Returns:
            BaseTask: 任务对象，如果不存在则返回None
        """
        return self.tasks.get(task_name)

    def get_all_tasks(self) -> List[BaseTask]:
        """
        获取所有任务

        Returns:
            List[BaseTask]: 任务列表
        """
        return list(self.tasks.values())

    def get_task_status(self) -> List[Dict[str, Any]]:
        """
        获取所有任务状态

        Returns:
            List[Dict]: 任务状态列表
        """
        return [task.get_status() for task in self.tasks.values()]

    async def run_task(self, task_name: str) -> bool:
        """
        运行指定任务

        Args:
            task_name: 任务名称

        Returns:
            bool: 任务是否执行成功
        """
        task = self.get_task(task_name)
        if not task:
            self.logger.error(f"任务不存在: {task_name}")
            return False

        return await task.run()

    async def run_all_tasks(self) -> Dict[str, bool]:
        """
        运行所有任务

        Returns:
            Dict[str, bool]: 任务执行结果
        """
        results = {}
        for task_name in self.tasks:
            results[task_name] = await self.run_task(task_name)
        return results

    async def start_scheduler(self, interval: int = 300) -> None:
        """
        启动调度器

        Args:
            interval: 调度间隔（秒），默认5分钟
        """
        self.is_running = True
        self.logger.info(f"启动任务调度器，间隔: {interval}秒")

        while not self._stop_event.is_set():
            try:
                # 运行所有任务
                await self.run_all_tasks()

                # 等待指定间隔
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                # 正常超时，继续下一轮
                continue
            except Exception as e:
                self.logger.error(f"调度器运行异常: {e}")
                await asyncio.sleep(60)  # 出错后等待1分钟再继续

        self.is_running = False
        self.logger.info("任务调度器已停止")

    async def stop_scheduler(self) -> None:
        """停止调度器"""
        self._stop_event.set()
        self.logger.info("正在停止任务调度器...")

    def is_scheduler_running(self) -> bool:
        """
        检查调度器是否运行中

        Returns:
            bool: 调度器是否运行中
        """
        return self.is_running
