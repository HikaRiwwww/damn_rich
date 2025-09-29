"""
数据验证模块 - 通过业务逻辑保证数据相关性
"""

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from .models import Exchange, KlineData, Symbol


class DataValidator:
    """数据验证器"""

    def __init__(self, session: Session):
        """
        初始化数据验证器

        Args:
            session: 数据库会话
        """
        self.session = session

    def validate_exchange_exists(self, exchange_id: int) -> bool:
        """
        验证交易所是否存在

        Args:
            exchange_id: 交易所ID

        Returns:
            是否存在
        """
        try:
            exchange = (
                self.session.query(Exchange).filter(Exchange.id == exchange_id).first()
            )
            return exchange is not None
        except Exception:
            return False

    def validate_symbol_exists(self, symbol_id: int) -> bool:
        """
        验证交易对是否存在

        Args:
            symbol_id: 交易对ID

        Returns:
            是否存在
        """
        try:
            symbol = self.session.query(Symbol).filter(Symbol.id == symbol_id).first()
            return symbol is not None
        except Exception:
            return False

    def validate_kline_data_consistency(self, kline_data: KlineData) -> Dict[str, Any]:
        """
        验证K线数据的一致性

        Args:
            kline_data: K线数据对象

        Returns:
            验证结果字典
        """
        result = {"is_valid": True, "errors": [], "warnings": []}

        # 验证交易所是否存在
        if not self.validate_exchange_exists(kline_data.exchange_id):
            result["is_valid"] = False
            result["errors"].append(f"交易所ID {kline_data.exchange_id} 不存在")

        # 验证交易对是否存在
        if not self.validate_symbol_exists(kline_data.symbol_id):
            result["is_valid"] = False
            result["errors"].append(f"交易对ID {kline_data.symbol_id} 不存在")

        # 验证价格数据合理性
        if kline_data.open_price <= 0:
            result["is_valid"] = False
            result["errors"].append("开盘价必须大于0")

        if kline_data.high_price <= 0:
            result["is_valid"] = False
            result["errors"].append("最高价必须大于0")

        if kline_data.low_price <= 0:
            result["is_valid"] = False
            result["errors"].append("最低价必须大于0")

        if kline_data.close_price <= 0:
            result["is_valid"] = False
            result["errors"].append("收盘价必须大于0")

        # 验证价格逻辑关系
        if kline_data.high_price < kline_data.low_price:
            result["is_valid"] = False
            result["errors"].append("最高价不能小于最低价")

        if kline_data.high_price < max(kline_data.open_price, kline_data.close_price):
            result["warnings"].append("最高价可能不合理")

        if kline_data.low_price > min(kline_data.open_price, kline_data.close_price):
            result["warnings"].append("最低价可能不合理")

        # 验证成交量
        if kline_data.volume < 0:
            result["is_valid"] = False
            result["errors"].append("成交量不能为负数")

        # 验证时间戳
        if kline_data.timestamp <= 0:
            result["is_valid"] = False
            result["errors"].append("时间戳必须大于0")

        return result

    def get_exchange_by_id(self, exchange_id: int) -> Optional[Exchange]:
        """
        根据ID获取交易所

        Args:
            exchange_id: 交易所ID

        Returns:
            交易所对象或None
        """
        try:
            return (
                self.session.query(Exchange).filter(Exchange.id == exchange_id).first()
            )
        except Exception:
            return None

    def get_symbol_by_id(self, symbol_id: int) -> Optional[Symbol]:
        """
        根据ID获取交易对

        Args:
            symbol_id: 交易对ID

        Returns:
            交易对对象或None
        """
        try:
            return self.session.query(Symbol).filter(Symbol.id == symbol_id).first()
        except Exception:
            return None

    def get_exchange_by_name(self, name: str) -> Optional[Exchange]:
        """
        根据名称获取交易所

        Args:
            name: 交易所名称

        Returns:
            交易所对象或None
        """
        try:
            return self.session.query(Exchange).filter(Exchange.name == name).first()
        except Exception:
            return None

    def get_symbol_by_name(self, symbol: str) -> Optional[Symbol]:
        """
        根据名称获取交易对

        Args:
            symbol: 交易对符号

        Returns:
            交易对对象或None
        """
        try:
            return self.session.query(Symbol).filter(Symbol.symbol == symbol).first()
        except Exception:
            return None

    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        验证数据完整性

        Returns:
            验证结果字典
        """
        result = {
            "is_valid": True,
            "orphaned_kline_data": [],
            "missing_exchanges": [],
            "missing_symbols": [],
        }

        try:
            # 检查孤立的K线数据（引用了不存在的交易所或交易对）
            orphaned_data = (
                self.session.query(KlineData)
                .outerjoin(Exchange, KlineData.exchange_id == Exchange.id)
                .outerjoin(Symbol, KlineData.symbol_id == Symbol.id)
                .filter((Exchange.id.is_(None)) | (Symbol.id.is_(None)))
                .all()
            )

            if orphaned_data:
                result["is_valid"] = False
                result["orphaned_kline_data"] = [
                    {
                        "id": data.id,
                        "exchange_id": data.exchange_id,
                        "symbol_id": data.symbol_id,
                        "timestamp": data.timestamp,
                    }
                    for data in orphaned_data
                ]

            # 检查是否有K线数据引用了不存在的交易所
            missing_exchanges = (
                self.session.query(KlineData.exchange_id)
                .outerjoin(Exchange, KlineData.exchange_id == Exchange.id)
                .filter(Exchange.id.is_(None))
                .distinct()
                .all()
            )

            if missing_exchanges:
                result["missing_exchanges"] = [row[0] for row in missing_exchanges]

            # 检查是否有K线数据引用了不存在的交易对
            missing_symbols = (
                self.session.query(KlineData.symbol_id)
                .outerjoin(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(Symbol.id.is_(None))
                .distinct()
                .all()
            )

            if missing_symbols:
                result["missing_symbols"] = [row[0] for row in missing_symbols]

        except Exception as e:
            result["is_valid"] = False
            result["error"] = str(e)

        return result

    def cleanup_orphaned_data(self) -> int:
        """
        清理孤立的数据

        Returns:
            清理的记录数
        """
        try:
            # 删除引用了不存在交易所的K线数据
            deleted_count = (
                self.session.query(KlineData)
                .outerjoin(Exchange, KlineData.exchange_id == Exchange.id)
                .filter(Exchange.id.is_(None))
                .delete()
            )

            # 删除引用了不存在交易对的K线数据
            deleted_count += (
                self.session.query(KlineData)
                .outerjoin(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(Symbol.id.is_(None))
                .delete()
            )

            self.session.commit()
            return deleted_count

        except Exception as e:
            self.session.rollback()
            print(f"清理孤立数据失败: {e}")
            return 0
