"""
API 日志配置
提供统一的日志记录格式和配置
"""

import logging
import logging.config
import os
from datetime import datetime
from typing import Any, Dict


def setup_api_logging(log_level: str = "INFO", log_dir: str = "/app/logs") -> None:
    """设置 API 日志配置"""

    # 确保日志目录存在
    os.makedirs(log_dir, exist_ok=True)

    # 日志配置
    logging_config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {"format": "%(levelname)s - %(message)s"},
            "json": {
                "format": '{"timestamp": "%(asctime)s", "logger": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}',
                "datefmt": "%Y-%m-%dT%H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "detailed",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": log_level,
                "formatter": "detailed",
                "filename": os.path.join(log_dir, "api.log"),
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8",
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "detailed",
                "filename": os.path.join(log_dir, "api_error.log"),
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8",
            },
        },
        "loggers": {
            "damn_rich.api": {
                "level": log_level,
                "handlers": ["console", "file", "error_file"],
                "propagate": False,
            },
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console", "file"],
                "propagate": False,
            },
            "uvicorn.error": {
                "level": "INFO",
                "handlers": ["console", "file", "error_file"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console", "file"],
                "propagate": False,
            },
        },
        "root": {"level": log_level, "handlers": ["console", "file"]},
    }

    # 应用日志配置
    logging.config.dictConfig(logging_config)


def get_api_logger(name: str) -> logging.Logger:
    """获取 API 日志记录器"""
    return logging.getLogger(f"damn_rich.api.{name}")


class APILogger:
    """API 日志记录器"""

    def __init__(self, name: str):
        self.logger = get_api_logger(name)

    def info(self, message: str, **kwargs) -> None:
        """记录信息日志"""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """记录警告日志"""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs) -> None:
        """记录错误日志"""
        self.logger.error(message, extra=kwargs)

    def debug(self, message: str, **kwargs) -> None:
        """记录调试日志"""
        self.logger.debug(message, extra=kwargs)

    def log_request(self, method: str, path: str, **kwargs) -> None:
        """记录请求日志"""
        self.info(f"Request: {method} {path}", **kwargs)

    def log_response(self, method: str, path: str, status_code: int, **kwargs) -> None:
        """记录响应日志"""
        self.info(f"Response: {method} {path} -> {status_code}", **kwargs)

    def log_error(self, method: str, path: str, error: str, **kwargs) -> None:
        """记录错误日志"""
        self.error(f"Error: {method} {path} -> {error}", **kwargs)
