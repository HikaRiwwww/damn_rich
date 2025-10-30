"""
API 异常处理器
提供统一的异常捕获和错误响应
"""

import logging

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from damn_rich.api.response_model import create_error_response

logger = logging.getLogger(__name__)


class APIException(Exception):
    """自定义 API 异常"""

    def __init__(
        self, message: str, error_code: str = "API_ERROR", status_code: int = 400
    ):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        super().__init__(self.message)


class DatabaseException(APIException):
    """数据库异常"""

    def __init__(
        self, message: str = "数据库操作失败", error_code: str = "DATABASE_ERROR"
    ):
        super().__init__(message, error_code, 500)


class ValidationException(APIException):
    """验证异常"""

    def __init__(
        self, message: str = "参数验证失败", error_code: str = "VALIDATION_ERROR"
    ):
        super().__init__(message, error_code, 422)


class NotFoundException(APIException):
    """资源未找到异常"""

    def __init__(self, message: str = "资源未找到", error_code: str = "NOT_FOUND"):
        super().__init__(message, error_code, 404)


class UnauthorizedException(APIException):
    """未授权异常"""

    def __init__(self, message: str = "未授权访问", error_code: str = "UNAUTHORIZED"):
        super().__init__(message, error_code, 401)


class ForbiddenException(APIException):
    """禁止访问异常"""

    def __init__(self, message: str = "禁止访问", error_code: str = "FORBIDDEN"):
        super().__init__(message, error_code, 403)


class RateLimitException(APIException):
    """频率限制异常"""

    def __init__(self, message: str = "请求过于频繁", error_code: str = "RATE_LIMIT"):
        super().__init__(message, error_code, 429)


async def api_exception_handler(request: Request, exc: APIException) -> JSONResponse:
    """API 异常处理器"""
    logger.error(f"API Exception: {exc.error_code} - {exc.message}", exc_info=True)
    return JSONResponse(
        status_code=exc.status_code,
        content=create_error_response(exc.message, exc.error_code),
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """HTTP 异常处理器"""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content=create_error_response(str(exc.detail), f"HTTP_{exc.status_code}"),
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """请求验证异常处理器"""
    logger.error(f"Validation Error: {exc.errors()}")
    error_details = []
    for error in exc.errors():
        error_details.append(
            {
                "field": ".".join(str(x) for x in error["loc"]),
                "message": error["msg"],
                "type": error["type"],
            }
        )

    return JSONResponse(
        status_code=422,
        content=create_error_response(
            "请求参数验证失败", "VALIDATION_ERROR", {"errors": error_details}
        ),
    )


async def sqlalchemy_exception_handler(
    request: Request, exc: SQLAlchemyError
) -> JSONResponse:
    """SQLAlchemy 异常处理器"""
    logger.error(f"Database Error: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=create_error_response("数据库操作失败", "DATABASE_ERROR"),
    )


async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """通用异常处理器"""
    logger.error(f"Unexpected Error: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=create_error_response("服务器内部错误", "INTERNAL_ERROR"),
    )
