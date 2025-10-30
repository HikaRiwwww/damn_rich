"""
API 响应格式模型
提供统一的响应结构和错误处理
"""

from typing import Any, Optional

from pydantic import BaseModel


class APIResponse(BaseModel):
    """统一 API 响应格式"""

    success: bool
    message: str
    data: Optional[Any] = None
    error_code: Optional[str] = None
    timestamp: Optional[str] = None


class ErrorResponse(BaseModel):
    """错误响应格式"""

    success: bool = False
    message: str
    error_code: str
    data: Optional[Any] = None
    timestamp: Optional[str] = None


class SuccessResponse(BaseModel):
    """成功响应格式"""

    success: bool = True
    message: str = "操作成功"
    data: Optional[Any] = None
    timestamp: Optional[str] = None


def create_success_response(data: Any = None, message: str = "操作成功") -> dict:
    """创建成功响应"""
    from datetime import datetime

    return {
        "success": True,
        "message": message,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }


def create_error_response(
    message: str, error_code: str = "UNKNOWN_ERROR", data: Any = None
) -> dict:
    """创建错误响应"""
    from datetime import datetime

    return {
        "success": False,
        "message": message,
        "error_code": error_code,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }
