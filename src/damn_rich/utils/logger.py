"""
日志工具模块

提供统一的日志配置和管理功能
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from damn_rich.utils.config import Config


def setup_logging(service_name: str) -> logging.Logger:
    """
    设置日志配置

    Args:
        service_name: 服务名称 (data_sync, trading_bot 等)

    Returns:
        logging.Logger: 配置好的日志器
    """
    # 获取项目根目录 (main.py 的上一层)
    project_root = Path(__file__).parent.parent.parent.parent

    # 构建日志目录路径
    log_dir = project_root / Config.LOG_DIR / service_name
    log_dir.mkdir(parents=True, exist_ok=True)

    # 创建日志器
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))

    # 清除已有的处理器，避免重复
    logger.handlers.clear()

    # 创建格式器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # 控制台只显示 INFO 及以上级别
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器 (滚动日志)
    log_file = log_dir / f"{service_name}.log"
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=Config.LOG_MAX_BYTES,
        backupCount=Config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 记录日志配置信息
    logger.info(f"日志系统初始化完成")
    logger.info(f"日志目录: {log_dir}")
    logger.info(f"日志级别: {Config.LOG_LEVEL}")
    logger.info(f"最大文件大小: {Config.LOG_MAX_BYTES / 1024 / 1024:.1f}MB")
    logger.info(f"备份文件数量: {Config.LOG_BACKUP_COUNT}")

    return logger


def get_logger(service_name: str) -> logging.Logger:
    """
    获取指定服务的日志器

    Args:
        service_name: 服务名称

    Returns:
        logging.Logger: 日志器
    """
    logger = logging.getLogger(service_name)
    if not logger.handlers:
        # 如果日志器还没有配置，则进行配置
        return setup_logging(service_name)
    return logger


def create_daily_logger(service_name: str) -> logging.Logger:
    """
    创建按日期滚动的日志器

    Args:
        service_name: 服务名称

    Returns:
        logging.Logger: 配置好的日志器
    """
    from logging.handlers import TimedRotatingFileHandler

    # 获取项目根目录
    project_root = Path(__file__).parent.parent.parent.parent
    log_dir = project_root / Config.LOG_DIR / service_name
    log_dir.mkdir(parents=True, exist_ok=True)

    # 创建日志器
    logger = logging.getLogger(f"{service_name}_daily")
    logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))

    # 清除已有的处理器
    logger.handlers.clear()

    # 创建格式器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 按日期滚动的文件处理器
    log_file = log_dir / f"{service_name}.log"
    daily_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1,
        backupCount=Config.LOG_BACKUP_COUNT,
        encoding="utf-8",
        utc=False,
    )
    daily_handler.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    daily_handler.setFormatter(formatter)
    daily_handler.suffix = "%Y-%m-%d"  # 备份文件后缀格式
    logger.addHandler(daily_handler)

    return logger
