"""
StandX交易所适配器

基于StandX交易所API实现的适配器，使用模块化设计
官方端点：
- HTTP: https://perps.standx.com
- WebSocket: wss://perps.standx.com/ws-stream/v1

注意：实现基于StandX官方API文档
"""

import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Union, Callable
from decimal import Decimal
from datetime import datetime
import base58, secrets
from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    ExchangeType, OrderBookData, TradeData, TickerData, BalanceData, OrderData,
    OrderSide, OrderType, OrderStatus, PositionData, ExchangeInfo, OHLCVData
)
from ....services.events import Event

# 导入分离的模块
from .standx_base import StandXBase
from .standx_rest import StandXRest
from .standx_websocket import StandXWebSocket
from ..subscription_manager import SubscriptionManager, DataType, create_subscription_manager


class StandXAdapter(ExchangeAdapter):
    """StandX交易所适配器 - 基于MESA架构的统一接口实现"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        """初始化StandX适配器"""
        super().__init__(config, event_bus)

        # 初始化组件模块
        self.base = StandXBase(config)
        self.rest = StandXRest(config, self.logger)
        self.websocket = StandXWebSocket(config, self.logger)

        # 复制基础配置到实例
        self.base_url = self.base.DEFAULT_BASE_URL
        self.ws_url = self.base.DEFAULT_WS_URL
        self.symbols_info = {}

        # 设置日志器
        self.base.logger = self.logger
        self.rest.logger = self.logger
        self.websocket.logger = self.logger

        # 🚀 初始化订阅管理器 - 加载StandX配置文件
        try:
            # 尝试加载YAML配置文件
            config_dict = self._load_standx_config()

            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

            if self.logger:
                self.logger.info(
                    f"✅ StandX订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}")

        except Exception as e:
            self.logger.warning(f"创建StandX订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'standx',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['BTC_USDT_PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                    }
                }
            }
            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

    def _load_standx_config(self) -> Dict[str, Any]:
        """加载StandX配置文件"""
        import yaml
        import os

        # 尝试多个可能的配置文件路径
        config_paths = [
            'config/exchanges/standx_config.yaml',
            'config/exchanges/standx.yaml',
            os.path.join(os.path.dirname(__file__), '../../../../config/exchanges/standx_config.yaml')
        ]

        for config_path in config_paths:
            try:
                if os.path.exists(config_path):
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = yaml.safe_load(f)

                    # 提取StandX配置
                    standx_config = config_data.get('standx', {})
                    standx_config['exchange_id'] = 'standx'

                    if self.logger:
                        self.logger.info(f"📁 成功加载StandX配置文件: {config_path}")

                    return standx_config

            except Exception as e:
                if self.logger:
                    self.logger.debug(f"加载配置文件失败 {config_path}: {e}")
                continue

        # 如果所有路径都失败，返回默认配置
        if self.logger:
            self.logger.warning("未找到StandX配置文件，使用默认配置")

        return {
            'exchange_id': 'standx',
            'subscription_mode': {
                'mode': 'predefined',
                'predefined': {
                    'symbols': ['BTC_USDT_PERP', 'ETH_USDT_PERP', 'SOL_USDT_PERP'],
                    'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                }
            },
            'custom_subscriptions': {
                'active_combination': 'major_coins',
                'combinations': {
                    'major_coins': {
                        'description': '主流币种永续合约订阅',
                        'symbols': ['BTC_USDT_PERP', 'ETH_USDT_PERP', 'SOL_USDT_PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False}
                    }
                }
            }
        }

    # === 生命周期管理实现 ===

    async def _do_connect(self) -> bool:
        """执行具体的连接逻辑"""
        try:
            # 建立REST连接
            await self.rest.setup_session()

            # 建立WebSocket连接
            await self.websocket.connect()

            # 获取支持的交易对
            # await self.websocket.fetch_supported_symbols()
            #
            # # 同步支持的交易对到其他模块
            # self.base._supported_symbols = self.websocket._supported_symbols
            # self.base._contract_mappings = self.websocket._contract_mappings
            # self.base._symbol_contract_mappings = self.websocket._symbol_contract_mappings

            self.logger.info("StandX连接成功")
            return True

        except Exception as e:
            self.logger.warning(f"StandX连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        try:
            # 关闭WebSocket连接
            await self.websocket.disconnect()

            # 关闭REST会话
            await self.rest.close_session()

            # 清理订阅管理器
            self._subscription_manager.clear_subscriptions()

            self.logger.info("StandX连接已断开")

        except Exception as e:
            self.logger.warning(f"断开StandX连接时出错: {e}")

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        try:
            # 使用REST模块进行认证
            await self.rest.auth.authenticate('bsc', self.config.wallet_address, self.config.wallet_private_key)
            return await self.rest.authenticate()
        except Exception as e:
            self.logger.warning(f"StandX认证失败: {str(e)}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查"""
        try:
            # 使用REST模块进行健康检查
            return await self.rest.health_check()
        except Exception as e:
            health_data = {
                'exchange_time': datetime.now(),
                'market_count': len(self.base._supported_symbols),
                'api_accessible': False,
                'error': str(e)
            }
            return health_data

    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        pass

    # === 市场数据接口实现 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            # 获取支持的交易对列表
            supported_symbols = await self.get_supported_symbols()

            # 构建markets字典
            markets = {}
            for symbol in supported_symbols:
                # 解析symbol获取base和quote
                if '_' in symbol:
                    base, quote = symbol.split('_', 1)
                else:
                    # 回退处理
                    if symbol.endswith('USDT'):
                        base = symbol[:-4]
                        quote = 'USDT'
                    else:
                        base = symbol
                        quote = 'USDT'

                markets[symbol] = {
                    'id': symbol,
                    'symbol': symbol,
                    'base': base,
                    'quote': quote,
                    'baseId': base,
                    'quoteId': quote,
                    'active': True,
                    'type': 'swap',
                    'spot': False,
                    'margin': False,
                    'future': False,
                    'swap': True,
                    'option': False,
                    'contract': True,
                    'contractSize': 1,
                    'linear': True,
                    'inverse': False,
                    'expiry': None,
                    'expiryDatetime': None,
                    'strike': None,
                    'optionType': None,
                    'precision': {
                        'amount': 8,
                        'price': 8,
                        'cost': 8,
                        'base': 8,
                        'quote': 8
                    },
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': 10000000},
                        'leverage': {'min': 1, 'max': 100}
                    },
                    'info': {
                        'symbol': symbol,
                        'exchange': 'standx',
                        'type': 'perpetual'
                    }
                }

            self.logger.info(f"✅ StandX交易所信息: {len(markets)}个市场")

            return ExchangeInfo(
                name="StandX",
                id="standx",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "spot_trading", "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets=markets,
                status="operational",
                timestamp=datetime.now()
            )

        except Exception as e:
            self.logger.error(f"❌ 获取StandX交易所信息失败: {e}")
            # 返回空markets的基本信息
            return ExchangeInfo(
                name="StandX",
                id="standx",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "spot_trading", "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets={},
                status="operational",
                timestamp=datetime.now()
            )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            ticker_data = await self.rest.fetch_ticker(mapped_symbol)
            return self.base._parse_ticker(ticker_data, symbol)
        except Exception as e:
            self.logger.warning(f"获取ticker数据失败: {e}")
            raise

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            orderbook_data = await self.rest.fetch_orderbook(mapped_symbol, limit)
            return self.base._parse_orderbook(orderbook_data, symbol)
        except Exception as e:
            self.logger.warning(f"获取orderbook数据失败: {e}")
            raise

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照 - 通过REST API

        Args:
            symbol: 交易对符号
            limit: 深度限制

        Returns:
            Dict: 完整的订单簿快照数据
        """
        try:
            return await self.rest.get_orderbook_snapshot(symbol, limit)
        except Exception as e:
            self.logger.warning(f"获取订单簿快照失败: {e}")
            return {
                "data": [{
                    "asks": [],
                    "bids": [],
                    "depthType": "SNAPSHOT"
                }]
            }

    async def get_trades(self, symbol: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[
        TradeData]:
        """获取最近成交记录"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            since_timestamp = int(since.timestamp() * 1000) if since else None
            trades_data = await self.rest.fetch_trades(mapped_symbol, since_timestamp, limit)
            return [self.base._parse_trade(trade, symbol) for trade in trades_data]
        except Exception as e:
            self.logger.warning(f"获取trades数据失败: {e}")
            return []

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        try:
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 并发获取所有ticker数据
            tasks = [self.get_ticker(symbol) for symbol in symbols]
            tickers = await asyncio.gather(*tasks, return_exceptions=True)

            # 过滤掉异常结果
            valid_tickers = [ticker for ticker in tickers if isinstance(ticker, TickerData)]
            return valid_tickers

        except Exception as e:
            self.logger.warning(f"获取多个行情数据失败: {e}")
            return []

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        return await self.websocket.get_supported_symbols()

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        return await self.rest.get_balances()

    async def get_ohlcv(self, symbol: str, timeframe: str, since: Optional[datetime] = None,
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)

            # 映射时间框架
            interval_map = {
                '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1h', '4h': '4h', '1d': '1d'
            }
            interval = interval_map.get(timeframe, '1h')

            return await self.rest.get_klines(mapped_symbol, interval, since, limit)
        except Exception as e:
            self.logger.warning(f"获取K线数据失败: {e}")
            return []

    # === 交易接口实现 ===

    async def place_order(self, symbol: str, side: OrderSide, order_type: OrderType, quantity: Decimal,
                          price: Decimal = None, time_in_force: str = "GTC", client_order_id: str = None) -> OrderData:
        """下单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.place_order(mapped_symbol, side, order_type, quantity, price, time_in_force,
                                               client_order_id)
        except Exception as e:
            self.logger.warning(f"下单失败: {e}")
            raise

    async def cancel_order(self, symbol: str, order_id: str = None, client_order_id: str = None) -> bool:
        """取消订单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.cancel_order_by_id(mapped_symbol, order_id, client_order_id)
        except Exception as e:
            self.logger.warning(f"取消订单失败: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: str = None, client_order_id: str = None) -> OrderData:
        """查询订单状态"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.get_order_status(mapped_symbol, order_id, client_order_id)
        except Exception as e:
            self.logger.warning(f"查询订单状态失败: {e}")
            raise

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            return await self.rest.get_open_orders(mapped_symbol)
        except Exception as e:
            self.logger.warning(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(self, symbol: Optional[str] = None, since: Optional[datetime] = None,
                                limit: Optional[int] = None) -> List[OrderData]:
        """获取订单历史"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            return await self.rest.get_order_history(mapped_symbol, since, limit)
        except Exception as e:
            self.logger.warning(f"获取订单历史失败: {e}")
            return []

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            orders_data = await self.rest.cancel_all_orders(mapped_symbol)
            return [self.base._parse_order(order) for order in orders_data]
        except Exception as e:
            self.logger.warning(f"取消所有订单失败: {e}")
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """获取持仓信息"""
        try:
            mapped_symbols = [self.base._map_symbol(s) for s in symbols] if symbols else None
            return await self.rest.get_positions(mapped_symbols)
        except Exception as e:
            self.logger.warning(f"获取持仓信息失败: {e}")
            return []

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.set_leverage(mapped_symbol, leverage)
        except Exception as e:
            self.logger.warning(f"设置杠杆失败: {e}")
            return {'symbol': symbol, 'leverage': leverage, 'error': str(e)}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.set_margin_mode(mapped_symbol, margin_mode)
        except Exception as e:
            self.logger.warning(f"设置保证金模式失败: {e}")
            return {'symbol': symbol, 'margin_mode': margin_mode, 'error': str(e)}

    async def execute_trading_cycle(
        self,
        symbol: str,
        quantity: Decimal,
        buy_price: Optional[Decimal] = None,
        sell_price: Optional[Decimal] = None,
        price_spread: Optional[Decimal] = None,
        order_type: OrderType = OrderType.MARKET,
        wait_for_fill: bool = True,
        max_wait_seconds: int = 30
    ) -> Dict[str, Any]:
        """
        执行一个完整的交易周期：先开 buy 单，然后立即开 sell 单
        
        Args:
            symbol: 交易对符号
            quantity: 交易数量
            buy_price: 买入价格（限价单时使用，如果为 None 则使用市价）
            sell_price: 卖出价格（限价单时使用，如果为 None 则使用 buy_price + price_spread 或市价）
            price_spread: 价格价差（sell_price = buy_price + price_spread，仅在 sell_price 为 None 时使用）
            order_type: 订单类型（MARKET 或 LIMIT）
            wait_for_fill: 是否等待 buy 单成交后再下 sell 单（限价单时有效）
            max_wait_seconds: 最大等待时间（秒）
        
        Returns:
            Dict: 包含 buy_order 和 sell_order 的字典
        """
        import asyncio
        from datetime import datetime
        
        result = {
            'success': False,
            'buy_order': None,
            'sell_order': None,
            'error': None,
            'timestamp': datetime.now()
        }
        
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            
            # 生成客户端订单ID
            buy_cl_ord_id = base58.b58encode(secrets.token_bytes(26)).decode("utf-8")
            sell_cl_ord_id = base58.b58encode(secrets.token_bytes(26)).decode("utf-8")
            
            # === 步骤1: 下 buy 订单 ===
            if self.logger:
                self.logger.info(f"🔄 [交易周期] 开始执行交易周期: {symbol}, 数量: {quantity}")
            
            try:
                buy_order = await self.place_order(
                    symbol=mapped_symbol,
                    side=OrderSide.BUY,
                    order_type=order_type,
                    quantity=quantity,
                    price=buy_price,
                    time_in_force="GTC" if order_type == OrderType.MARKET else "IOC",
                    client_order_id=buy_cl_ord_id
                )
                result['buy_order'] = buy_order
                
                if self.logger:
                    self.logger.info(f"✅ [交易周期] Buy 订单已提交: ID={buy_order.id}, 状态={buy_order.status.value}")
                
            except Exception as e:
                error_msg = f"下 buy 订单失败: {str(e)}"
                if self.logger:
                    self.logger.error(f"❌ [交易周期] {error_msg}")
                result['error'] = error_msg
                return result
            
            # === 步骤2: 等待 buy 订单成交（如果是限价单且需要等待） ===
            if order_type == OrderType.LIMIT and wait_for_fill:
                if self.logger:
                    self.logger.info(f"⏳ [交易周期] 等待 buy 订单成交...")
                
                start_time = datetime.now()
                buy_filled = False
                
                while (datetime.now() - start_time).total_seconds() < max_wait_seconds:
                    try:
                        # 查询订单状态
                        order_status = await self.get_order_status(
                            symbol=mapped_symbol,
                            order_id=buy_order.id
                        )
                        
                        if order_status.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]:
                            buy_filled = True
                            buy_order = order_status  # 更新订单信息
                            
                            if self.logger:
                                self.logger.info(
                                    f"✅ [交易周期] Buy 订单已成交: "
                                    f"成交数量={order_status.filled}, "
                                    f"平均价格={order_status.average}"
                                )
                            break
                        elif order_status.status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                            error_msg = f"Buy 订单被取消或拒绝: {order_status.status.value}"
                            if self.logger:
                                self.logger.error(f"❌ [交易周期] {error_msg}")
                            result['error'] = error_msg
                            return result
                        
                        # 等待一小段时间再查询
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        if self.logger:
                            self.logger.warning(f"⚠️ [交易周期] 查询 buy 订单状态失败: {e}")
                        await asyncio.sleep(1)
                
                if not buy_filled:
                    if self.logger:
                        self.logger.warning(f"⚠️ [交易周期] Buy 订单在 {max_wait_seconds} 秒内未完全成交，继续执行 sell 订单")
            elif order_type == OrderType.MARKET:
                # 市价单通常立即成交，但我们也等待一小段时间确保订单已处理
                await asyncio.sleep(0.5)
            
            # === 步骤3: 计算 sell 价格 ===
            if order_type == OrderType.LIMIT:
                if sell_price is None:
                    # 使用 buy 订单的成交均价或买入价格
                    base_price = buy_order.average if buy_order.average else buy_price
                    if base_price is None:
                        # 如果都没有，获取当前市场价格
                        ticker = await self.get_ticker(mapped_symbol)
                        base_price = ticker.last
                    
                    # 计算 sell 价格
                    if price_spread is not None:
                        sell_price = base_price + price_spread
                    else:
                        # 默认使用相同价格（可能亏损，但可以快速完成交易周期）
                        sell_price = base_price
                        if self.logger:
                            self.logger.warning(f"⚠️ [交易周期] 未指定 sell_price 和 price_spread，使用 buy 价格（可能亏损）")
            else:
                # 市价单不需要价格
                sell_price = None
            
            # === 步骤4: 下 sell 订单 ===
            try:
                # 如果 buy 订单部分成交，使用实际成交数量
                sell_quantity = buy_order.filled if buy_order.filled > 0 else quantity
                
                sell_order = await self.place_order(
                    symbol=mapped_symbol,
                    side=OrderSide.SELL,
                    order_type=order_type,
                    quantity=sell_quantity,
                    price=sell_price,
                    time_in_force="GTC" if order_type == OrderType.MARKET else "IOC",
                    client_order_id=sell_cl_ord_id
                )
                result['sell_order'] = sell_order
                
                if self.logger:
                    self.logger.info(
                        f"✅ [交易周期] Sell 订单已提交: "
                        f"ID={sell_order.id}, "
                        f"状态={sell_order.status.value}, "
                        f"价格={sell_price if sell_price else '市价'}"
                    )
                
            except Exception as e:
                error_msg = f"下 sell 订单失败: {str(e)}"
                if self.logger:
                    self.logger.error(f"❌ [交易周期] {error_msg}")
                result['error'] = error_msg
                # 即使 sell 订单失败，buy 订单已经成功，所以返回部分结果
                return result
            
            # === 交易周期完成 ===
            result['success'] = True
            
            if self.logger:
                buy_status = buy_order.status.value
                sell_status = sell_order.status.value
                self.logger.info(
                    f"🎉 [交易周期] 交易周期完成: "
                    f"Buy订单={buy_status}, "
                    f"Sell订单={sell_status}"
                )
            
            return result
            
        except Exception as e:
            error_msg = f"执行交易周期失败: {str(e)}"
            if self.logger:
                self.logger.error(f"❌ [交易周期] {error_msg}")
                import traceback
                self.logger.error(traceback.format_exc())
            result['error'] = error_msg
            return result

    # === WebSocket订阅接口实现 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        await self.websocket.subscribe_ticker(symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        await self.websocket.subscribe_orderbook(symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        await self.websocket.subscribe_trades(symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        await self.websocket.subscribe_user_data(callback)

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None,
                                      callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅ticker数据（支持硬编码和动态两种模式）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            callback: ticker数据回调函数 (symbol, ticker_data)
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅ticker数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.TICKER):
                if self.logger:
                    self.logger.info("配置中禁用了ticker数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.TICKER,
                    callback=callback
                )

            # 委托给websocket模块执行实际订阅
            await self.websocket.batch_subscribe_tickers(symbols, callback)

            if self.logger:
                self.logger.info(f"✅ StandX批量订阅ticker完成: {len(symbols)}个交易对")

        except Exception as e:
            if self.logger:
                self.logger.error(f"StandX批量订阅ticker失败: {str(e)}")
            raise

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None, depth: int = 15,
                                         callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅订单簿数据（支持硬编码和动态两种模式）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            depth: 订单簿深度
            callback: 订单簿数据回调函数
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX动态模式：发现 {len(symbols)} 个交易对")

            # 检查是否应该订阅orderbook数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK):
                if self.logger:
                    self.logger.info("配置中禁用了orderbook数据订阅，跳过")
                return

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.ORDERBOOK,
                    callback=callback
                )

            # 委托给websocket模块执行实际订阅
            await self.websocket.batch_subscribe_orderbooks(symbols, depth, callback)

            if self.logger:
                self.logger.info(f"✅ StandX批量订阅orderbook完成: {len(symbols)}个交易对")

        except Exception as e:
            if self.logger:
                self.logger.error(f"StandX批量订阅orderbook失败: {str(e)}")
            raise

    async def batch_subscribe_mixed(self,
                                    symbols: Optional[List[str]] = None,
                                    ticker_callback: Optional[Callable[[str, TickerData], None]] = None,
                                    orderbook_callback: Optional[Callable[[str, OrderBookData], None]] = None,
                                    trades_callback: Optional[Callable[[str, TradeData], None]] = None,
                                    user_data_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
                                    depth: int = 15) -> None:
        """批量订阅混合数据类型（支持任意组合）

        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            ticker_callback: ticker数据回调函数
            orderbook_callback: orderbook数据回调函数
            trades_callback: trades数据回调函数
            user_data_callback: user_data回调函数
            depth: 订单簿深度
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                if self._subscription_manager.mode.value == "predefined":
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 StandX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 StandX动态模式：发现 {len(symbols)} 个交易对")

            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return

            # 根据配置决定订阅哪些数据类型
            subscription_count = 0

            # 订阅ticker数据
            if (ticker_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.TICKER)):
                await self.batch_subscribe_tickers(symbols, ticker_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅ticker数据: {len(symbols)}个交易对")

            # 订阅orderbook数据
            if (orderbook_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK)):
                await self.batch_subscribe_orderbooks(symbols, depth, orderbook_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅orderbook数据: {len(symbols)}个交易对")

            # 订阅trades数据
            if (trades_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.TRADES)):
                for symbol in symbols:
                    await self.subscribe_trades(symbol, trades_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅trades数据: {len(symbols)}个交易对")

            # 订阅user_data数据
            if (user_data_callback is not None and
                    self._subscription_manager.should_subscribe_data_type(DataType.USER_DATA)):
                await self.subscribe_user_data(user_data_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ 已订阅user_data数据")

            # 获取订阅统计信息
            stats = self._subscription_manager.get_subscription_stats()
            if self.logger:
                self.logger.info(f"🎯 StandX混合订阅完成: {subscription_count}种数据类型, {len(symbols)}个交易对")
                self.logger.info(f"📊 订阅统计: {stats}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"StandX批量混合订阅失败: {e}")
            raise

    def get_subscription_manager(self) -> SubscriptionManager:
        """获取订阅管理器实例"""
        return self._subscription_manager

    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return self._subscription_manager.get_subscription_stats()

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        await self.websocket.unsubscribe(symbol)

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        await self.websocket.unsubscribe_all()

    # === 向后兼容的接口 ===

    async def subscribe_order_book(self, symbol: str, callback, depth: int = 20):
        """订阅订单簿数据 - 向后兼容"""
        await self.websocket.subscribe_order_book(symbol, callback, depth)

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[TradeData]:
        """获取最近成交记录 - 向后兼容"""
        return await self.rest.get_recent_trades(symbol, limit)

    async def create_order(self, symbol: str, side: OrderSide, order_type: OrderType, amount: Decimal,
                           price: Optional[Decimal] = None, params: Optional[Dict[str, Any]] = None) -> OrderData:
        """创建订单 - 向后兼容"""
        return await self.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=amount,
            price=price,
            time_in_force=params.get('timeInForce', 'GTC') if params else 'GTC',
            client_order_id=params.get('clientOrderId') if params else None
        )

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取单个订单信息 - 向后兼容"""
        return await self.get_order_status(symbol, order_id)

    async def connect(self) -> bool:
        """连接 - 向后兼容"""
        return await self._do_connect()

    async def disconnect(self) -> None:
        """断开连接 - 向后兼容"""
        return await self._do_disconnect()

    async def authenticate(self) -> bool:
        """进行身份认证 - 向后兼容"""
        return await self._do_authenticate()

    async def health_check(self) -> Dict[str, Any]:
        """健康检查 - 向后兼容"""
        return await self._do_health_check()

    async def get_exchange_status(self) -> Dict[str, Any]:
        """获取交易所状态 - 向后兼容"""
        return {
            'status': 'online' if self.connected else 'offline',
            'timestamp': int(time.time() * 1000)
        }

    async def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取交易对信息 - 向后兼容"""
        return self.symbols_info.get(symbol)

    async def get_points(self):
        return await self.rest.auth.get_points()

    # === 工具方法 ===

    def format_quantity(self, symbol: str, quantity: Decimal) -> Decimal:
        """格式化数量精度"""
        symbol_info = self.symbols_info.get(symbol)
        return self.base.format_quantity(symbol, quantity, symbol_info)

    def format_price(self, symbol: str, price: Decimal) -> Decimal:
        """格式化价格精度"""
        symbol_info = self.symbols_info.get(symbol)
        return self.base.format_price(symbol, price, symbol_info)

    # === 事件处理方法 ===

    async def _handle_ticker_update(self, ticker: TickerData) -> None:
        """处理ticker更新事件"""
        try:
            # 在新架构中简化事件处理
            self.logger.debug(f"Ticker更新: {ticker.symbol}@standx, 价格: {ticker.last}")
        except Exception as e:
            self.logger.warning(f"处理ticker更新事件失败: {e}")

    async def _handle_orderbook_update(self, orderbook: OrderBookData) -> None:
        """处理orderbook更新事件"""
        try:
            # 在新架构中简化事件处理
            self.logger.debug(f"订单簿更新: {orderbook.symbol}@standx")
        except Exception as e:
            self.logger.warning(f"处理orderbook更新事件失败: {e}")

    # === 属性代理 ===

    @property
    def api_key(self) -> str:
        """获取API密钥"""
        return self.rest.api_key

    @property
    def api_secret(self) -> str:
        """获取API密钥"""
        return self.rest.api_secret

    def is_authenticated(self) -> bool:
        """获取认证状态"""
        return self.rest.is_authenticated

    @property
    def symbol_mapping(self) -> Dict[str, str]:
        """获取符号映射"""
        return getattr(self.config, 'symbol_mapping', {})

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化符号 - 向后兼容"""
        return self.base._normalize_symbol(symbol)

    def _map_symbol(self, symbol: str) -> str:
        """映射符号 - 向后兼容"""
        return self.base._map_symbol(symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射符号 - 向后兼容"""
        return self.base._reverse_map_symbol(exchange_symbol)

    def _safe_decimal(self, value: Any) -> Decimal:
        """安全转换为Decimal - 向后兼容"""
        return self.base._safe_decimal(value)

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """订阅所有交易对的ticker数据（使用ticker.all频道）"""
        try:
            self.logger.info("开始订阅所有交易对的ticker数据")

            # 建立WebSocket连接
            if not self.websocket._ws_connection:
                await self.websocket.connect()

            # 订阅所有ticker
            subscribe_msg = {
                "type": "subscribe",
                "channel": "ticker.all"
            }

            if self.websocket._ws_connection:
                await self.websocket._ws_connection.send_str(json.dumps(subscribe_msg))
                self.logger.info("已订阅所有交易对的ticker数据")

            # 如果提供了回调函数，保存它
            if callback:
                self.websocket.ticker_callback = callback

        except Exception as e:
            self.logger.warning(f"订阅所有ticker时出错: {e}")

    async def _fetch_supported_symbols(self) -> None:
        """通过WebSocket获取支持的交易对 - 向后兼容"""
        await self.websocket.fetch_supported_symbols()

    async def _process_metadata_response(self, data: Dict[str, Any]) -> None:
        """处理metadata响应数据 - 向后兼容"""
        await self.websocket._process_metadata_response(data)

    async def _close_websocket(self) -> None:
        """关闭WebSocket连接 - 向后兼容"""
        await self.websocket.disconnect()

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数 - 向后兼容"""
        await self.websocket._safe_callback(callback, data)

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None,
                       data: Optional[Dict] = None, signed: bool = False) -> Dict[str, Any]:
        """执行HTTP请求 - 向后兼容"""
        return await self.rest._request(method, endpoint, params, data, signed)

    async def _fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对行情数据 - 向后兼容"""
        return await self.rest.fetch_ticker(symbol)

    async def _fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿数据 - 向后兼容"""
        return await self.rest.fetch_orderbook(symbol, limit)

    async def _fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[
        Dict[str, Any]]:
        """获取交易记录 - 向后兼容"""
        return await self.rest.fetch_trades(symbol, since, limit)

    async def _fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额数据 - 向后兼容"""
        return await self.rest.fetch_balances()

    # === 数据解析方法 - 向后兼容 ===

    def _parse_ticker(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据 - 向后兼容"""
        return self.base._parse_ticker(data, symbol)

    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据 - 向后兼容"""
        return self.base._parse_orderbook(data, symbol)

    def _parse_trade(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析交易数据 - 向后兼容"""
        return self.base._parse_trade(data, symbol)

    def _parse_balance(self, data: Dict[str, Any]) -> BalanceData:
        """解析余额数据 - 向后兼容"""
        return self.base._parse_balance(data)

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        """解析订单数据 - 向后兼容"""
        return self.base._parse_order(data)

    def _parse_order_status(self, status: str) -> OrderStatus:
        """解析订单状态 - 向后兼容"""
        return self.base._parse_order_status(status)

    def _normalize_contract_symbol(self, symbol: str) -> str:
        """将StandX合约symbol转换为标准格式 - 向后兼容"""
        return self.base._normalize_contract_symbol(symbol)

    def _get_auth_headers(self) -> Dict[str, str]:
        """获取认证请求头 - 向后兼容"""
        return self.base.get_auth_headers(self.api_key)

    # === 属性访问 - 向后兼容 ===

    @property
    def _supported_symbols(self) -> List[str]:
        """获取支持的交易对 - 向后兼容"""
        return self.base._supported_symbols

    @property
    def _contract_mappings(self) -> Dict[str, str]:
        """获取合约映射 - 向后兼容"""
        return self.base._contract_mappings

    @property
    def _symbol_contract_mappings(self) -> Dict[str, str]:
        """获取符号到合约映射 - 向后兼容"""
        return self.base._symbol_contract_mappings

    @property
    def _ws_connection(self):
        """获取WebSocket连接 - 向后兼容"""
        # 如果websocket已经初始化，返回其连接
        if hasattr(self, 'websocket') and self.websocket is not None:
            return self.websocket._ws_connection
        # 否则返回临时存储的值或None
        else:
            return getattr(self, '_ws_connection_value', None)

    @_ws_connection.setter
    def _ws_connection(self, value):
        """设置WebSocket连接 - 向后兼容"""
        # 检查websocket属性是否已经初始化
        if hasattr(self, 'websocket') and self.websocket is not None:
            self.websocket._ws_connection = value
        # 如果websocket还未初始化，直接设置为实例属性
        else:
            object.__setattr__(self, '_ws_connection_value', value)

    @property
    def _ws_subscriptions(self):
        """获取WebSocket订阅 - 向后兼容"""
        return self.websocket._ws_subscriptions

    @property
    def session(self):
        """获取HTTP会话 - 向后兼容"""
        return self.rest.session

    @property
    def ws_connections(self):
        """获取WebSocket连接字典 - 向后兼容"""
        return getattr(self.websocket, 'ws_connections', {})

    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            # 尝试从依赖注入容器获取符号缓存服务
            from ....di.container import get_container
            from ....services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService

            container = get_container()
            symbol_cache_service = container.get(ISymbolCacheService)

            if self.logger:
                self.logger.info("✅ 获取符号缓存服务成功")
            return symbol_cache_service

        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 获取符号缓存服务失败: {e}，返回None")
            return None
