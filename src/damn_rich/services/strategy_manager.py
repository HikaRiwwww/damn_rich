import importlib.util
import inspect
import json
from pathlib import Path
from typing import Dict, Optional

from damn_rich.database.models import DatabaseManager, Strategy
from damn_rich.strategy.base import StrategyBase
from damn_rich.utils.config import Config
from damn_rich.utils.logger import get_logger

logger = get_logger(__name__)


class StrategyManager:
    """策略管理器（单例模式）

    负责管理所有策略的加载、初始化、运行和停止

    提供策略的注册、注销、获取等功能

    提供策略的运行状态、错误信息、日志记录等功能

    提供策略的配置、参数、状态等管理功能

    使用单例模式，确保全局只有一个实例
    """

    _instance: Optional["StrategyManager"] = None
    _initialized: bool = False

    def __new__(cls):
        """单例模式：确保只创建一个实例"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """
        初始化策略管理器

        注意：由于单例模式，__init__ 可能会被多次调用，
        因此使用 _initialized 标志确保只初始化一次
        """
        if StrategyManager._initialized:
            return

        try:
            self.database_manager = DatabaseManager(Config.get_database_url())
            self.strategies: Dict[str, StrategyBase] = {}
            self.strategy_dir = Path(__file__).parent.parent / "strategy"
            self._load_strategies()
            StrategyManager._initialized = True
            logger.info("策略管理器初始化完成")
        except Exception as e:
            logger.error(f"策略管理器初始化失败: {e}", exc_info=True)
            # 重置标志，允许下次重试
            StrategyManager._initialized = False
            raise

    def has_initialized(self) -> bool:
        """
        检查策略管理器是否已初始化
        """
        return StrategyManager._initialized

    def _load_strategies(self):
        """
        扫描策略目录，加载所有策略类，并自动注册到数据库
        """
        if not self.database_manager:
            logger.error("数据库管理器未初始化，无法加载策略")
            return

        logger.info(f"开始扫描策略目录: {self.strategy_dir}")

        # 获取策略目录下的所有Python文件（排除__init__.py和base.py）
        strategy_files = [
            f
            for f in self.strategy_dir.glob("*.py")
            if f.name not in ["__init__.py", "base.py"]
        ]

        if not strategy_files:
            logger.warning("未找到策略文件")
            return

        with self.database_manager.get_session() as session:
            for strategy_file in strategy_files:
                try:
                    self._load_and_register_strategy(strategy_file, session)
                except Exception as e:
                    logger.error(
                        f"加载策略文件 {strategy_file} 失败: {e}", exc_info=True
                    )

            session.commit()

        logger.info(f"策略加载完成，共加载 {len(self.strategies)} 个策略")

    def _load_and_register_strategy(
        self, strategy_file: Path, session
    ) -> Optional[StrategyBase]:
        """
        加载单个策略文件并注册到数据库

        Args:
            strategy_file: 策略文件路径
            session: 数据库会话

        Returns:
            策略实例（如果加载成功且启用）
        """
        module_name = strategy_file.stem  # 文件名（不含扩展名）
        file_path_str = str(strategy_file)

        logger.debug(f"正在加载策略文件: {strategy_file}")

        # 动态导入策略模块
        spec = importlib.util.spec_from_file_location(module_name, strategy_file)
        if spec is None or spec.loader is None:
            logger.error(f"无法创建模块规范: {strategy_file}")
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # 查找所有继承自StrategyBase的类
        strategy_classes = []
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                obj != StrategyBase
                and issubclass(obj, StrategyBase)
                and obj.__module__ == module.__name__
            ):
                strategy_classes.append((name, obj))

        if not strategy_classes:
            logger.debug(f"文件 {strategy_file} 中未找到策略类")
            return None

        # 处理每个策略类
        for class_name, strategy_class in strategy_classes:
            try:
                # 检查数据库中是否已存在该策略
                db_strategy = (
                    session.query(Strategy).filter(Strategy.name == class_name).first()
                )

                # 尝试从类中获取元数据
                class_doc = inspect.getdoc(strategy_class) or ""
                class_name_cn = getattr(strategy_class, "name_cn", "")
                class_description = getattr(strategy_class, "description", "")
                # 如果类中没有description，尝试使用文档字符串的第一行
                if not class_description and class_doc:
                    class_description = class_doc.split("\n")[0].strip()

                if db_strategy:
                    # 更新策略信息（如果文件路径或类名发生变化）
                    if db_strategy.file_path != file_path_str:
                        db_strategy.file_path = file_path_str
                    if db_strategy.class_name != class_name:
                        db_strategy.class_name = class_name
                    # 如果数据库中没有名称或描述，尝试更新
                    if not db_strategy.name_cn and class_name_cn:
                        db_strategy.name_cn = class_name_cn
                    if not db_strategy.description and class_description:
                        db_strategy.description = class_description
                    logger.debug(f"更新策略: {class_name}")
                else:
                    # 创建新策略记录
                    db_strategy = Strategy(
                        name=class_name,
                        name_cn=class_name_cn,
                        description=class_description,
                        file_path=file_path_str,
                        class_name=class_name,
                        is_active=False,
                        is_enabled=False,
                        config=None,
                        version=getattr(strategy_class, "version", "1.0.0"),
                        author=getattr(strategy_class, "author", None),
                    )
                    session.add(db_strategy)
                    logger.info(f"注册新策略: {class_name}")

                # 如果策略已启用，则初始化策略实例
                if db_strategy.is_enabled:
                    try:
                        # 解析配置
                        config = {}
                        if db_strategy.config:
                            config = json.loads(db_strategy.config)

                        # 初始化策略实例
                        strategy_instance = strategy_class(
                            config=config,
                            name_cn=db_strategy.name_cn or "",
                            description=db_strategy.description or "",
                        )

                        self.strategies[class_name] = strategy_instance
                        logger.info(f"策略 {class_name} 已启用并加载")
                    except Exception as e:
                        logger.error(
                            f"初始化策略 {class_name} 失败: {e}", exc_info=True
                        )
                        db_strategy.is_enabled = False

            except Exception as e:
                logger.error(f"处理策略类 {class_name} 失败: {e}", exc_info=True)
                continue

        return None

    def get_strategy(self, name: str) -> Optional[StrategyBase]:
        """
        获取策略实例

        Args:
            name: 策略名称

        Returns:
            策略实例，如果不存在返回None
        """
        return self.strategies.get(name)

    def get_all_strategies(self) -> Dict[str, StrategyBase]:
        """
        获取所有已加载的策略

        Returns:
            策略字典
        """
        return self.strategies.copy()

    def reload_strategy(self, name: str) -> bool:
        """
        重新加载指定策略

        Args:
            name: 策略名称

        Returns:
            是否成功
        """
        if not self.database_manager:
            logger.error("数据库管理器未初始化")
            return False

        with self.database_manager.get_session() as session:
            db_strategy = session.query(Strategy).filter(Strategy.name == name).first()
            if not db_strategy:
                logger.error(f"策略 {name} 不存在于数据库中")
                return False

            strategy_file = Path(db_strategy.file_path)
            if not strategy_file.exists():
                logger.error(f"策略文件不存在: {strategy_file}")
                return False

            try:
                # 从已加载的策略中移除
                if name in self.strategies:
                    del self.strategies[name]

                # 重新加载
                self._load_and_register_strategy(strategy_file, session)
                session.commit()
                logger.info(f"策略 {name} 重新加载成功")
                return True
            except Exception as e:
                logger.error(f"重新加载策略 {name} 失败: {e}", exc_info=True)
                session.rollback()
                return False
