class StrategyBase:
    """策略基类"""

    # 可选：定义策略的中文名称（也可以通过数据库配置）
    name_cn = ""

    # 可选：定义策略描述（也可以通过数据库配置）
    description = ""

    # 可选：定义策略版本
    version = ""

    # 可选：定义策略作者
    author = ""

    def __init__(self, config):
        self.name = self.__class__.__name__
        # 如果没有传入description或为空，尝试使用类变量中的默认值
        self.config = config

    def on_data(self, df):
        """
        每次新数据推送时调用
        输入：pandas DataFrame，包含OHLCV
        输出：signal = {'action': 'buy'/'sell'/'hold', 'confidence': float}
        """
        raise NotImplementedError("Subclasses must implement this method")

    def on_order_filled(self, order):
        """
        每次订单成交时调用(可选)
        """
        pass
