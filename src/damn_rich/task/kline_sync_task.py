"""
K线数据同步任务

用于同步symbols表中所有交易对的4小时K线数据
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import and_

from damn_rich.data.historical_fetcher import HistoricalDataFetcher
from damn_rich.database.models import DatabaseManager, Exchange, KlineData, Symbol
from damn_rich.task.base_task import BaseTask


class KlineSyncTask(BaseTask):
    """K线数据同步任务"""

    def __init__(
        self, database_manager: DatabaseManager, exchange_name: str = "binance"
    ):
        """
        初始化K线数据同步任务

        Args:
            database_manager: 数据库管理器
            exchange_name: 交易所名称
        """
        super().__init__(
            name="kline_sync", description="同步symbols表中所有交易对的4小时K线数据"
        )
        self.database_manager = database_manager
        self.exchange_name = exchange_name
        self.exchange_id: Optional[int] = None
        self.fetcher = HistoricalDataFetcher(exchange_name)

    async def execute(self) -> bool:
        """
        执行K线数据同步任务

        Returns:
            bool: 任务是否执行成功
        """
        try:
            # 获取交易所ID
            if not await self._get_exchange_id():
                return False

            # 获取所有活跃的交易对
            symbols = await self._get_active_symbols()
            if not symbols:
                self.logger.warning("没有找到活跃的交易对")
                return True

            self.logger.info(f"找到 {len(symbols)} 个活跃交易对")

            # 为每个交易对同步数据
            success_count = 0
            for symbol in symbols:
                try:
                    if await self._sync_symbol_data(symbol):
                        success_count += 1
                        self.logger.info(f"交易对 {symbol.symbol} 数据同步成功")
                    else:
                        self.logger.warning(f"交易对 {symbol.symbol} 数据同步失败")
                except Exception as e:
                    self.logger.error(f"交易对 {symbol.symbol} 同步异常: {e}")

                # 避免请求过于频繁
                await asyncio.sleep(0.5)

            self.logger.info(f"K线数据同步完成，成功: {success_count}/{len(symbols)}")
            return success_count > 0

        except Exception as e:
            self.logger.error(f"K线数据同步任务执行失败: {e}")
            return False

    async def _get_exchange_id(self) -> bool:
        """
        获取交易所ID

        Returns:
            bool: 是否成功获取交易所ID
        """
        try:
            with self.database_manager.get_session() as session:
                exchange = (
                    session.query(Exchange)
                    .filter(Exchange.name == self.exchange_name)
                    .first()
                )

                if not exchange:
                    self.logger.error(f"未找到交易所: {self.exchange_name}")
                    return False

                self.exchange_id = exchange.id
                self.logger.info(f"获取交易所ID: {self.exchange_id}")
                return True

        except Exception as e:
            self.logger.error(f"获取交易所ID失败: {e}")
            return False

    async def _get_active_symbols(self) -> List[Symbol]:
        """
        获取所有活跃的交易对

        Returns:
            List[Symbol]: 活跃交易对列表
        """
        try:
            with self.database_manager.get_session() as session:
                symbols = (
                    session.query(Symbol)
                    .filter(and_(Symbol.is_active, Symbol.is_trading))
                    .all()
                )
                return symbols

        except Exception as e:
            self.logger.error(f"获取活跃交易对失败: {e}")
            return []

    async def _sync_symbol_data(self, symbol: Symbol) -> bool:
        """
        同步单个交易对的数据

        Args:
            symbol: 交易对对象

        Returns:
            bool: 是否同步成功
        """
        try:
            # 检查历史数据完成状态
            historical_completed = await self._check_historical_completion(symbol.id)

            if historical_completed:
                # 历史数据已完成，进行增量更新
                return await self._incremental_update(symbol)
            else:
                # 历史数据未完成，进行快速历史数据获取
                return await self._fast_historical_update(symbol)

        except Exception as e:
            self.logger.error(f"同步交易对数据失败 {symbol.symbol}: {e}")
            return False

    async def _check_historical_completion(self, symbol_id: int) -> bool:
        """
        检查历史数据是否已完成（至少有一年的数据）

        Args:
            symbol_id: 交易对ID

        Returns:
            bool: 历史数据是否已完成
        """
        try:
            with self.database_manager.get_session() as session:
                # 检查数据量是否足够（一年约2190条4小时K线）
                count = (
                    session.query(KlineData)
                    .filter(
                        and_(
                            KlineData.exchange_id == self.exchange_id,
                            KlineData.symbol_id == symbol_id,
                            KlineData.timeframe == "4h",
                        )
                    )
                    .count()
                )

                # 如果数据量超过2000条，认为历史数据已完成
                if count >= 2000:
                    self.logger.info(f"历史数据已完成: {count} 条4小时K线")
                    return True

                # 检查时间跨度是否足够
                oldest_data = (
                    session.query(KlineData)
                    .filter(
                        and_(
                            KlineData.exchange_id == self.exchange_id,
                            KlineData.symbol_id == symbol_id,
                            KlineData.timeframe == "4h",
                        )
                    )
                    .order_by(KlineData.datetime.asc())
                    .first()
                )

                if oldest_data:
                    time_diff = datetime.now() - oldest_data.datetime
                    if time_diff.days >= 365:
                        self.logger.info(f"历史数据时间跨度足够: {time_diff.days} 天")
                        return True

                self.logger.info(f"历史数据未完成: {count} 条K线，时间跨度不足")
                return False

        except Exception as e:
            self.logger.error(f"检查历史数据完成状态失败: {e}")
            return False

    async def _check_historical_data(self, symbol_id: int) -> bool:
        """
        检查是否已有历史数据

        Args:
            symbol_id: 交易对ID

        Returns:
            bool: 是否已有历史数据
        """
        try:
            with self.database_manager.get_session() as session:
                count = (
                    session.query(KlineData)
                    .filter(
                        and_(
                            KlineData.exchange_id == self.exchange_id,
                            KlineData.symbol_id == symbol_id,
                            KlineData.timeframe == "4h",
                        )
                    )
                    .count()
                )
                return count > 0

        except Exception as e:
            self.logger.error(f"检查历史数据失败: {e}")
            return False

    async def _full_update(self, symbol: Symbol) -> bool:
        """
        全量更新：获取过去一年的4小时K线数据

        Args:
            symbol: 交易对对象

        Returns:
            bool: 是否更新成功
        """
        try:
            # 计算过去一年的时间范围
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)

            self.logger.info(f"全量更新 {symbol.symbol}: {start_date} 到 {end_date}")

            # 获取历史数据
            kline_data = self.fetcher.fetch_binance_ohlcv(
                symbol=symbol.symbol,
                timeframe="4h",
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat() if end_date else None,
                limit=500,
            )

            if not kline_data:
                self.logger.warning(f"未获取到 {symbol.symbol} 的历史数据")
                return False

            # 处理和保存数据
            return await self._process_and_save_data(
                symbol, kline_data, is_full_update=True
            )

        except Exception as e:
            self.logger.error(f"全量更新失败 {symbol.symbol}: {e}")
            return False

    async def _fast_historical_update(self, symbol: Symbol) -> bool:
        """
        快速历史数据更新：高频请求获取历史数据

        Args:
            symbol: 交易对对象

        Returns:
            bool: 是否更新成功
        """
        try:
            self.logger.info(f"快速历史数据更新 {symbol.symbol}: 高频请求获取历史数据")

            # 获取当前数据库中最老的数据时间
            oldest_time = await self._get_oldest_data_time(symbol.id)

            # 如果没有历史数据，从一年前开始
            if not oldest_time:
                oldest_time = datetime.now() - timedelta(days=365)

            # 计算需要获取的时间范围
            current_time = datetime.now()
            time_diff = current_time - oldest_time

            self.logger.info(f"需要获取 {time_diff.days} 天的历史数据")

            # 分批次高频获取数据
            success = await self._batch_fetch_historical_data(
                symbol, oldest_time, current_time
            )

            return success

        except Exception as e:
            self.logger.error(f"快速历史数据更新失败 {symbol.symbol}: {e}")
            return False

    async def _get_oldest_data_time(self, symbol_id: int) -> Optional[datetime]:
        """
        获取数据库中该交易对最老的数据时间

        Args:
            symbol_id: 交易对ID

        Returns:
            Optional[datetime]: 最老数据时间，如果没有数据则返回None
        """
        try:
            with self.database_manager.get_session() as session:
                oldest_data = (
                    session.query(KlineData)
                    .filter(
                        and_(
                            KlineData.exchange_id == self.exchange_id,
                            KlineData.symbol_id == symbol_id,
                            KlineData.timeframe == "4h",
                        )
                    )
                    .order_by(KlineData.datetime.asc())
                    .first()
                )

                return oldest_data.datetime if oldest_data else None

        except Exception as e:
            self.logger.error(f"获取最老数据时间失败: {e}")
            return None

    async def _batch_fetch_historical_data(
        self, symbol: Symbol, start_time: datetime, end_time: datetime
    ) -> bool:
        """
        分批次获取历史数据

        Args:
            symbol: 交易对对象
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            bool: 是否获取成功
        """
        try:
            # 从指定的开始时间开始获取数据
            current_start = start_time
            batch_count = 0
            total_fetched = 0
            retry_count = 0
            max_retries = 100  # 最大重试次数

            while current_start < end_time and retry_count < max_retries:
                self.logger.info(
                    f"批次 {batch_count + 1}: 从 {current_start} 开始获取4小时数据"
                )

                # 获取从当前开始时间的数据
                batch_data = self.fetcher.fetch_binance_ohlcv(
                    symbol=symbol.symbol,
                    timeframe="4h",
                    start_date=current_start.isoformat(),
                    end_date=None,  # 不指定结束时间，让API返回从开始时间的数据
                    limit=500,
                )

                if batch_data:
                    # 保存数据
                    save_success = await self._process_and_save_data(
                        symbol, batch_data, is_full_update=True
                    )
                    if save_success:
                        total_fetched += len(batch_data)

                    self.logger.info(
                        f"批次 {batch_count + 1}: 获取到 {len(batch_data)} 条数据，保存成功: {save_success}"
                    )

                    # 更新下一批次的开始时间：从最后一条数据的时间继续
                    last_timestamp = batch_data[-1][0]
                    current_start = datetime.fromtimestamp(
                        last_timestamp / 1000
                    ) + timedelta(hours=4)

                    batch_count += 1
                    retry_count = 0  # 成功获取数据后重置重试计数

                    # 如果获取的数据少于预期，说明可能接近API限制
                    if len(batch_data) < 50:
                        self.logger.warning(
                            f"批次 {batch_count}: 数据量较少({len(batch_data)}条)，可能接近API限制，等待3秒后继续"
                        )
                        await asyncio.sleep(3.0)
                    else:
                        await asyncio.sleep(1.0)  # 正常情况每秒请求一次

                else:
                    retry_count += 1
                    self.logger.warning(
                        f"批次 {batch_count + 1}: 数据为空，等待10秒后重试 (重试 {retry_count}/{max_retries})"
                    )
                    await asyncio.sleep(10.0)  # 等待10秒后重试

            if retry_count >= max_retries:
                self.logger.error(f"历史数据获取失败: 达到最大重试次数 {max_retries}")
                return False

            self.logger.info(
                f"历史数据获取完成: 共 {batch_count} 个批次，总计 {total_fetched} 条数据"
            )
            return total_fetched > 0

        except Exception as e:
            self.logger.error(f"分批次获取历史数据失败: {e}")
            return False

    async def _incremental_update(self, symbol: Symbol) -> bool:
        """
        增量更新：获取最近200根4小时K线

        Args:
            symbol: 交易对对象

        Returns:
            bool: 是否更新成功
        """
        try:
            self.logger.info(f"增量更新 {symbol.symbol}: 获取最近200根4小时K线")

            # 获取最新数据
            kline_data = self.fetcher.fetch_latest_data(
                symbol=symbol.symbol, timeframe="4h", limit=200
            )

            if not kline_data:
                self.logger.warning(f"未获取到 {symbol.symbol} 的最新数据")
                return False

            # 处理和保存数据
            return await self._process_and_save_data(
                symbol, kline_data, is_full_update=False
            )

        except Exception as e:
            self.logger.error(f"增量更新失败 {symbol.symbol}: {e}")
            return False

    async def _process_and_save_data(
        self, symbol: Symbol, kline_data: List[List], is_full_update: bool = False
    ) -> bool:
        """
        处理和保存K线数据

        Args:
            symbol: 交易对对象
            kline_data: K线数据列表
            is_full_update: 是否全量更新

        Returns:
            bool: 是否保存成功
        """
        try:
            if not kline_data:
                return False

            # 过滤和处理数据
            processed_data = self._filter_and_process_kline_data(kline_data)
            if not processed_data:
                self.logger.warning(f"没有有效的K线数据需要保存: {symbol.symbol}")
                return True

            # 保存数据到数据库
            saved_count = await self._save_kline_data(
                symbol, processed_data, is_full_update
            )

            self.logger.info(f"保存了 {saved_count} 条K线数据: {symbol.symbol}")
            return saved_count > 0

        except Exception as e:
            self.logger.error(f"处理和保存数据失败 {symbol.symbol}: {e}")
            return False

    def _filter_and_process_kline_data(self, kline_data: List[List]) -> List[Tuple]:
        """
        过滤和处理K线数据

        Args:
            kline_data: 原始K线数据

        Returns:
            List[Tuple]: 处理后的K线数据
        """
        processed_data = []
        current_time = datetime.now()

        for kline in kline_data:
            try:
                # 解析K线数据 [timestamp, open, high, low, close, volume]
                timestamp_ms = int(kline[0])
                datetime_obj = datetime.fromtimestamp(timestamp_ms / 1000)

                # 过滤进行中的K线（最后一根K线可能还在进行中）
                # 4小时K线在收盘后才会定型，这里保留最后一根用于增量更新
                if datetime_obj >= current_time:
                    continue

                # 验证数据完整性
                if len(kline) < 6:
                    continue

                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])

                # 基本数据验证
                if not all(
                    [open_price > 0, high_price > 0, low_price > 0, close_price > 0]
                ):
                    continue

                if not (
                    low_price <= open_price <= high_price
                    and low_price <= close_price <= high_price
                ):
                    continue

                # 添加额外字段（如果有的话）
                quote_volume = float(kline[6]) if len(kline) > 6 else None
                trades_count = int(kline[7]) if len(kline) > 7 else None
                taker_buy_base_volume = float(kline[8]) if len(kline) > 8 else None
                taker_buy_quote_volume = float(kline[9]) if len(kline) > 9 else None

                processed_data.append(
                    (
                        timestamp_ms,
                        datetime_obj,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        quote_volume,
                        trades_count,
                        taker_buy_base_volume,
                        taker_buy_quote_volume,
                    )
                )

            except (ValueError, TypeError, IndexError) as e:
                self.logger.warning(f"跳过无效的K线数据: {kline}, 错误: {e}")
                continue

        return processed_data

    async def _save_kline_data(
        self, symbol: Symbol, processed_data: List[Tuple], is_full_update: bool = False
    ) -> int:
        """
        保存K线数据到数据库

        Args:
            symbol: 交易对对象
            processed_data: 处理后的K线数据
            is_full_update: 是否全量更新

        Returns:
            int: 保存的数据条数
        """
        try:
            saved_count = 0

            with self.database_manager.get_session() as session:
                for data in processed_data:
                    (
                        timestamp_ms,
                        datetime_obj,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        quote_volume,
                        trades_count,
                        taker_buy_base_volume,
                        taker_buy_quote_volume,
                    ) = data

                    # 检查数据是否已存在（去重）
                    existing = (
                        session.query(KlineData)
                        .filter(
                            and_(
                                KlineData.exchange_id == self.exchange_id,
                                KlineData.symbol_id == symbol.id,
                                KlineData.timeframe == "4h",
                                KlineData.timestamp == timestamp_ms,
                            )
                        )
                        .first()
                    )

                    if existing:
                        # 更新现有数据
                        existing.open_price = open_price
                        existing.high_price = high_price
                        existing.low_price = low_price
                        existing.close_price = close_price
                        existing.volume = volume
                        existing.quote_volume = quote_volume
                        existing.trades_count = trades_count
                        existing.taker_buy_base_volume = taker_buy_base_volume
                        existing.taker_buy_quote_volume = taker_buy_quote_volume
                        existing.datetime = datetime_obj
                    else:
                        # 创建新数据
                        new_kline = KlineData(
                            exchange_id=self.exchange_id,
                            symbol_id=symbol.id,
                            timeframe="4h",
                            timestamp=timestamp_ms,
                            datetime=datetime_obj,
                            open_price=open_price,
                            high_price=high_price,
                            low_price=low_price,
                            close_price=close_price,
                            volume=volume,
                            quote_volume=quote_volume,
                            trades_count=trades_count,
                            taker_buy_base_volume=taker_buy_base_volume,
                            taker_buy_quote_volume=taker_buy_quote_volume,
                        )
                        session.add(new_kline)

                    saved_count += 1

                # 提交事务
                session.commit()

            return saved_count

        except Exception as e:
            self.logger.error(f"保存K线数据失败: {e}")
            return 0
