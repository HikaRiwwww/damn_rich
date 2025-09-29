"""
数据存储模块
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .connection import db_connection
from .models import Base, Exchange, KlineData, Symbol
from .validators import DataValidator


class DataStorage:
    """数据存储类"""

    def __init__(self):
        """初始化数据存储"""
        self.db = db_connection

    def create_tables(self):
        """创建数据库表"""
        try:
            Base.metadata.create_all(bind=self.db.engine)
            print("数据库表创建成功")
        except Exception as e:
            print(f"创建数据库表失败: {e}")
            raise

    def get_or_create_exchange(self, exchange_name: str, session: Session) -> Exchange:
        """
        获取或创建交易所记录

        Args:
            exchange_name: 交易所名称
            session: 数据库会话

        Returns:
            交易所记录
        """
        # 尝试获取现有记录
        exchange = (
            session.query(Exchange).filter(Exchange.name == exchange_name).first()
        )

        if not exchange:
            # 创建新记录
            exchange = Exchange(
                name=exchange_name,
                display_name=exchange_name.title(),
                is_active=True,
                is_sandbox_supported=True,
            )
            session.add(exchange)
            session.flush()  # 获取ID但不提交

        return exchange

    def get_or_create_symbol(self, symbol: str, session: Session) -> Symbol:
        """
        获取或创建交易对记录

        Args:
            symbol: 交易对符号
            session: 数据库会话

        Returns:
            交易对记录
        """
        # 尝试获取现有记录
        symbol_record = session.query(Symbol).filter(Symbol.symbol == symbol).first()

        if not symbol_record:
            # 解析交易对
            if "/" in symbol:
                base_asset, quote_asset = symbol.split("/", 1)
            else:
                base_asset = symbol
                quote_asset = "USDT"

            # 创建新记录
            symbol_record = Symbol(
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                is_active=True,
                is_trading=True,
            )
            session.add(symbol_record)
            session.flush()  # 获取ID但不提交

        return symbol_record

    def store_kline_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        kline_data: List[List],
        batch_size: int = 1000,
    ) -> int:
        """
        存储K线数据到数据库

        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间周期
            kline_data: K线数据列表
            batch_size: 批处理大小

        Returns:
            成功存储的记录数
        """
        if not kline_data:
            print("没有数据需要存储")
            return 0

        stored_count = 0
        session = self.db.get_session()

        try:
            # 获取或创建交易所和交易对记录
            exchange_record = self.get_or_create_exchange(exchange, session)
            symbol_record = self.get_or_create_symbol(symbol, session)

            # 分批处理数据
            for i in range(0, len(kline_data), batch_size):
                batch_data = kline_data[i : i + batch_size]
                batch_records = []

                for kline in batch_data:
                    try:
                        # 解析K线数据 [timestamp, open, high, low, close, volume, ...]
                        timestamp = kline[0]
                        open_price = float(kline[1])
                        high_price = float(kline[2])
                        low_price = float(kline[3])
                        close_price = float(kline[4])
                        volume = float(kline[5])

                        # 创建K线记录
                        kline_record = KlineData(
                            exchange_id=exchange_record.id,
                            symbol_id=symbol_record.id,
                            timeframe=timeframe,
                            timestamp=timestamp,
                            datetime=datetime.fromtimestamp(timestamp / 1000),
                            open_price=open_price,
                            high_price=high_price,
                            low_price=low_price,
                            close_price=close_price,
                            volume=volume,
                            quote_volume=float(kline[6]) if len(kline) > 6 else None,
                            trades_count=int(kline[7]) if len(kline) > 7 else None,
                            taker_buy_base_volume=float(kline[8])
                            if len(kline) > 8
                            else None,
                            taker_buy_quote_volume=float(kline[9])
                            if len(kline) > 9
                            else None,
                        )

                        # 验证数据一致性
                        validator = DataValidator(session)
                        validation_result = validator.validate_kline_data_consistency(
                            kline_record
                        )

                        if not validation_result["is_valid"]:
                            print(f"数据验证失败: {validation_result['errors']}")
                            continue

                        if validation_result["warnings"]:
                            print(f"数据警告: {validation_result['warnings']}")

                        batch_records.append(kline_record)

                    except (ValueError, IndexError) as e:
                        print(f"解析K线数据失败: {e}, 数据: {kline}")
                        continue

                # 批量插入数据
                if batch_records:
                    try:
                        session.add_all(batch_records)
                        session.commit()
                        stored_count += len(batch_records)
                        print(f"成功存储 {len(batch_records)} 条记录")

                    except IntegrityError:
                        # 如果出现重复数据，则跳过
                        session.rollback()
                        print(f"跳过重复数据，批次大小: {len(batch_records)}")
                        continue

        except Exception as e:
            session.rollback()
            print(f"存储数据失败: {e}")
            raise
        finally:
            session.close()

        print(f"总共存储了 {stored_count} 条K线数据")
        return stored_count

    def get_latest_timestamp(
        self, exchange: str, symbol: str, timeframe: str
    ) -> Optional[int]:
        """
        获取指定交易对和时间周期的最新时间戳

        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间周期

        Returns:
            最新时间戳，如果没有数据则返回None
        """
        session = self.db.get_session()

        try:
            result = (
                session.query(KlineData.timestamp)
                .join(Exchange, KlineData.exchange_id == Exchange.id)
                .join(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(
                    Exchange.name == exchange,
                    Symbol.symbol == symbol,
                    KlineData.timeframe == timeframe,
                )
                .order_by(KlineData.timestamp.desc())
                .first()
            )

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"获取最新时间戳失败: {e}")
            return None
        finally:
            session.close()

    def get_kline_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        从数据库获取K线数据

        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间周期
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            limit: 限制条数

        Returns:
            K线数据DataFrame
        """
        session = self.db.get_session()

        try:
            query = (
                session.query(KlineData)
                .join(Exchange, KlineData.exchange_id == Exchange.id)
                .join(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(
                    Exchange.name == exchange,
                    Symbol.symbol == symbol,
                    KlineData.timeframe == timeframe,
                )
            )

            if start_timestamp:
                query = query.filter(KlineData.timestamp >= start_timestamp)

            if end_timestamp:
                query = query.filter(KlineData.timestamp <= end_timestamp)

            query = query.order_by(KlineData.timestamp)

            if limit:
                query = query.limit(limit)

            results = query.all()

            if not results:
                return pd.DataFrame()

            # 转换为DataFrame
            data = []
            for record in results:
                data.append(
                    {
                        "timestamp": record.timestamp,
                        "datetime": record.datetime,
                        "open": record.open_price,
                        "high": record.high_price,
                        "low": record.low_price,
                        "close": record.close_price,
                        "volume": record.volume,
                        "quote_volume": record.quote_volume,
                        "trades_count": record.trades_count,
                        "taker_buy_base_volume": record.taker_buy_base_volume,
                        "taker_buy_quote_volume": record.taker_buy_quote_volume,
                    }
                )

            df = pd.DataFrame(data)
            df.set_index("datetime", inplace=True)

            return df

        except Exception as e:
            print(f"获取K线数据失败: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def get_data_count(self, exchange: str, symbol: str, timeframe: str) -> int:
        """
        获取指定交易对和时间周期的数据条数

        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间周期

        Returns:
            数据条数
        """
        session = self.db.get_session()

        try:
            count = (
                session.query(KlineData)
                .join(Exchange, KlineData.exchange_id == Exchange.id)
                .join(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(
                    Exchange.name == exchange,
                    Symbol.symbol == symbol,
                    KlineData.timeframe == timeframe,
                )
                .count()
            )

            return count

        except Exception as e:
            print(f"获取数据条数失败: {e}")
            return 0
        finally:
            session.close()

    def delete_old_data(
        self, exchange: str, symbol: str, timeframe: str, before_timestamp: int
    ) -> int:
        """
        删除指定时间之前的数据

        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            timeframe: 时间周期
            before_timestamp: 删除此时间戳之前的数据

        Returns:
            删除的记录数
        """
        session = self.db.get_session()

        try:
            deleted_count = (
                session.query(KlineData)
                .join(Exchange, KlineData.exchange_id == Exchange.id)
                .join(Symbol, KlineData.symbol_id == Symbol.id)
                .filter(
                    Exchange.name == exchange,
                    Symbol.symbol == symbol,
                    KlineData.timeframe == timeframe,
                    KlineData.timestamp < before_timestamp,
                )
                .delete()
            )

            session.commit()
            print(f"删除了 {deleted_count} 条旧数据")
            return deleted_count

        except Exception as e:
            session.rollback()
            print(f"删除旧数据失败: {e}")
            return 0
        finally:
            session.close()

    def get_exchange_info(self, exchange_name: str) -> Optional[Dict[str, Any]]:
        """
        获取交易所信息

        Args:
            exchange_name: 交易所名称

        Returns:
            交易所信息字典
        """
        session = self.db.get_session()

        try:
            exchange = (
                session.query(Exchange).filter(Exchange.name == exchange_name).first()
            )
            if exchange:
                return {
                    "id": exchange.id,
                    "name": exchange.name,
                    "display_name": exchange.display_name,
                    "country": exchange.country,
                    "website": exchange.website,
                    "is_active": exchange.is_active,
                    "is_sandbox_supported": exchange.is_sandbox_supported,
                    "rate_limit": exchange.rate_limit,
                }
            return None
        except Exception as e:
            print(f"获取交易所信息失败: {e}")
            return None
        finally:
            session.close()

    def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对信息

        Args:
            symbol: 交易对符号

        Returns:
            交易对信息字典
        """
        session = self.db.get_session()

        try:
            symbol_record = (
                session.query(Symbol).filter(Symbol.symbol == symbol).first()
            )
            if symbol_record:
                return {
                    "id": symbol_record.id,
                    "symbol": symbol_record.symbol,
                    "base_asset": symbol_record.base_asset,
                    "quote_asset": symbol_record.quote_asset,
                    "is_active": symbol_record.is_active,
                    "is_trading": symbol_record.is_trading,
                    "base_precision": symbol_record.base_precision,
                    "quote_precision": symbol_record.quote_precision,
                }
            return None
        except Exception as e:
            print(f"获取交易对信息失败: {e}")
            return None
        finally:
            session.close()

    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        验证数据完整性

        Returns:
            验证结果字典
        """
        session = self.db.get_session()
        try:
            validator = DataValidator(session)
            return validator.validate_data_integrity()
        finally:
            session.close()

    def cleanup_orphaned_data(self) -> int:
        """
        清理孤立的数据

        Returns:
            清理的记录数
        """
        session = self.db.get_session()
        try:
            validator = DataValidator(session)
            return validator.cleanup_orphaned_data()
        finally:
            session.close()
