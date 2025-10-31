"""
示例策略文件

这个文件展示了如何创建一个交易策略。
每个策略应该：
1. 继承StrategyBase类
2. 实现on_data方法
3. 可以定义类属性：name_cn, description, version, author（可选）
"""

import pandas as pd

from damn_rich.strategy.base import StrategyBase


class ExampleStrategy(StrategyBase):
    """示例策略

    这是一个简单的示例策略，展示了策略的基本结构。
    可以在这里添加策略的详细说明。
    """

    # 可选：定义策略的中文名称（也可以通过数据库配置）
    name_cn = "示例策略"

    # 可选：定义策略描述（也可以通过数据库配置）
    description = "这是一个示例策略，用于演示策略开发"

    # 可选：定义策略版本
    version = "1.0.0"

    # 可选：定义策略作者
    author = "Your Name"

    def __init__(self, config):
        """
        初始化策略

        Args:
            config: 策略配置字典，从数据库读取
            name_cn: 策略中文名称
            description: 策略描述
        """
        super().__init__(config)
    

    def on_data(self, df: pd.DataFrame):
        """
        每次新数据推送时调用

        Args:
            df: pandas DataFrame，包含OHLCV数据
                列名通常包括：open, high, low, close, volume

        Returns:
            dict: 交易信号
                - action: 'buy' | 'sell' | 'hold'
                - confidence: float (0.0-1.0) 信号置信度
        """
        if df.empty:
            return {"action": "hold", "confidence": 0.0}

        # 这里实现你的策略逻辑
        # 示例：简单的移动平均策略
        if len(df) < 20:
            return {"action": "hold", "confidence": 0.0}

        # 计算移动平均
        df["ma_short"] = df["close"].rolling(window=5).mean()
        df["ma_long"] = df["close"].rolling(window=20).mean()

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        # 买入信号：短期均线上穿长期均线
        if (
            latest["ma_short"] > latest["ma_long"]
            and prev["ma_short"] <= prev["ma_long"]
        ):
            return {"action": "buy", "confidence": 0.7}

        # 卖出信号：短期均线下穿长期均线
        if (
            latest["ma_short"] < latest["ma_long"]
            and prev["ma_short"] >= prev["ma_long"]
        ):
            return {"action": "sell", "confidence": 0.7}

        return {"action": "hold", "confidence": 0.0}

    def on_order_filled(self, order):
        """
        每次订单成交时调用（可选实现）

        Args:
            order: 订单信息字典
        """
        # 可以在这里处理订单成交后的逻辑
        # 例如：更新持仓、记录交易历史等
        pass
