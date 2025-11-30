"""
统一的WebSocket管理器
为交易所适配器提供WebSocket数据订阅功能

专门处理公共市场数据：
- 价格行情 (ticker)
- 订单簿 (orderbook)
- 交易记录 (trades)
- K线数据 (ohlcv)

私有数据（账户、订单）继续使用REST API
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Set
from datetime import datetime

from ...logging import get_logger

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException


class WSDataType(Enum):
    """WebSocket数据类型"""
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    TRADES = "trades"
    OHLCV = "ohlcv"
    USER_DATA = "user_data"  # 用户数据（需要认证）


@dataclass
class WSSubscription:
    """WebSocket订阅信息"""
    data_type: WSDataType
    symbol: str
    callback: Callable
    params: Dict[str, Any] = field(default_factory=dict)
    stream_id: Optional[str] = None
    
    def __post_init__(self):
        """生成唯一的流ID"""
        if not self.stream_id:
            self.stream_id = f"{self.data_type.value}_{self.symbol}_{id(self.callback)}"


@dataclass  
class WSConnectionState:
    """WebSocket连接状态"""
    connected: bool = False
    connecting: bool = False
    last_ping: Optional[float] = None
    last_pong: Optional[float] = None
    reconnect_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class WebSocketManager(ABC):
    """
    WebSocket管理器基类
    
    提供统一的WebSocket连接管理和数据订阅功能
    """
    
    def __init__(self, base_url: str, logger = None):
        self.base_url = base_url
        self.logger = logger or get_logger(self.__class__.__name__)
        
        # 连接管理
        self.connection: Optional[Any] = None
        self.state = WSConnectionState()
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 订阅管理
        self.subscriptions: Dict[str, WSSubscription] = {}
        self.pending_subscriptions: Set[str] = set()
        
        # 配置参数
        self.reconnect_interval = 5.0  # 重连间隔
        self.max_reconnect_attempts = 10  # 最大重连次数
        self.ping_interval = 30.0  # 心跳间隔
        self.ping_timeout = 10.0  # 心跳超时
        
        # 控制标志
        self.is_running = False
        self.tasks: Set[asyncio.Task] = set()
    
    @abstractmethod
    async def _build_websocket_url(self, subscription: WSSubscription) -> str:
        """构建WebSocket连接URL"""
        pass
    
    @abstractmethod
    async def _build_subscribe_message(self, subscription: WSSubscription) -> Dict[str, Any]:
        """构建订阅消息"""
        pass
    
    @abstractmethod
    async def _parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """解析WebSocket消息"""
        pass
    
    @abstractmethod
    async def _handle_error_message(self, message: Dict[str, Any]) -> None:
        """处理错误消息"""
        pass
    
    async def start(self) -> bool:
        """启动WebSocket管理器"""
        if self.is_running:
            return True
        
        try:
            self.is_running = True
            self.session = aiohttp.ClientSession()
            
            # 启动连接任务
            self.tasks.add(asyncio.create_task(self._connection_manager()))
            
            self.logger.info("🚀 WebSocket管理器启动成功")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ WebSocket管理器启动失败: {e}")
            await self.stop()
            return False
    
    async def stop(self) -> None:
        """停止WebSocket管理器"""
        self.is_running = False
        
        # 取消所有任务
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务完成
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        
        # 关闭连接
        await self._close_connection()
        
        # 关闭会话
        if self.session:
            await self.session.close()
            self.session = None
        
        self.logger.info("⏹️ WebSocket管理器已停止")
    
    async def subscribe(self, data_type: WSDataType, symbol: str, 
                       callback: Callable, **params) -> bool:
        """
        订阅WebSocket数据
        
        Args:
            data_type: 数据类型
            symbol: 交易对符号
            callback: 回调函数
            **params: 额外参数
        """
        try:
            subscription = WSSubscription(
                data_type=data_type,
                symbol=symbol,
                callback=callback,
                params=params
            )
            
            self.subscriptions[subscription.stream_id] = subscription
            self.pending_subscriptions.add(subscription.stream_id)
            
            # 如果已连接，立即发送订阅消息
            if self.state.connected:
                await self._send_subscribe_message(subscription)
            
            self.logger.info(f"📡 添加订阅: {data_type.value} {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 订阅失败: {e}")
            return False
    
    async def unsubscribe(self, data_type: WSDataType, symbol: str) -> bool:
        """取消订阅"""
        try:
            # 查找匹配的订阅
            to_remove = []
            for stream_id, subscription in self.subscriptions.items():
                if (subscription.data_type == data_type and 
                    subscription.symbol == symbol):
                    to_remove.append(stream_id)
            
            # 移除订阅
            for stream_id in to_remove:
                if stream_id in self.subscriptions:
                    del self.subscriptions[stream_id]
                if stream_id in self.pending_subscriptions:
                    self.pending_subscriptions.remove(stream_id)
            
            self.logger.info(f"🚫 取消订阅: {data_type.value} {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 取消订阅失败: {e}")
            return False
    
    async def _connection_manager(self) -> None:
        """连接管理器"""
        while self.is_running:
            try:
                if not self.state.connected and not self.state.connecting:
                    await self._establish_connection()
                
                # 检查连接状态
                if self.state.connected:
                    await self._check_connection_health()
                
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"连接管理器错误: {e}")
                await asyncio.sleep(5)
    
    async def _establish_connection(self) -> None:
        """建立WebSocket连接"""
        if self.state.connecting:
            return
        
        try:
            self.state.connecting = True
            
            # 构建WebSocket URL (使用第一个订阅作为示例)
            if not self.subscriptions:
                await asyncio.sleep(1)
                return
            
            first_subscription = next(iter(self.subscriptions.values()))
            ws_url = await self._build_websocket_url(first_subscription)
            
            # 建立连接
            if self.session:
                self.connection = await self.session.ws_connect(
                    ws_url,
                    timeout=aiohttp.ClientTimeout(total=30)
                )
            else:
                self.connection = await websockets.connect(ws_url)
            
            self.state.connected = True
            self.state.connecting = False
            self.state.reconnect_count = 0
            self.state.error_count = 0
            
            self.logger.info(f"✅ WebSocket连接建立: {ws_url}")
            
            # 发送待处理的订阅
            await self._send_pending_subscriptions()
            
            # 启动消息处理任务
            self.tasks.add(asyncio.create_task(self._message_handler()))
            self.tasks.add(asyncio.create_task(self._ping_handler()))
            
        except Exception as e:
            self.logger.error(f"❌ WebSocket连接失败: {e}")
            self.state.connecting = False
            self.state.connected = False
            self.state.error_count += 1
            self.state.last_error = str(e)
            
            # 等待重连
            await asyncio.sleep(self.reconnect_interval)
    
    async def _message_handler(self) -> None:
        """消息处理器"""
        try:
            if isinstance(self.connection, aiohttp.ClientWebSocketResponse):
                # aiohttp WebSocket
                async for msg in self.connection:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._process_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        self.logger.error(f"WebSocket错误: {self.connection.exception()}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        self.logger.info("WebSocket连接关闭")
                        break
            else:
                # websockets库
                async for message in self.connection:
                    await self._process_message(message)
                    
        except ConnectionClosed:
            self.logger.warning("WebSocket连接意外关闭")
        except Exception as e:
            self.logger.error(f"消息处理错误: {e}")
        finally:
            self.state.connected = False
            await self._close_connection()
    
    async def _process_message(self, message: str) -> None:
        """处理收到的消息"""
        try:
            data = json.loads(message)
            
            # 处理心跳响应
            if self._is_pong_message(data):
                self.state.last_pong = time.time()
                return
            
            # 处理错误消息
            if self._is_error_message(data):
                await self._handle_error_message(data)
                return
            
            # 解析并分发数据
            parsed_data = await self._parse_message(data)
            if parsed_data:
                await self._dispatch_data(parsed_data)
                
        except Exception as e:
            self.logger.error(f"消息处理失败: {e}")
    
    async def _dispatch_data(self, data: Dict[str, Any]) -> None:
        """分发数据给订阅者"""
        try:
            data_type = data.get('type')
            symbol = data.get('symbol')
            
            if not data_type or not symbol:
                return
            
            # 查找匹配的订阅
            for subscription in self.subscriptions.values():
                if (subscription.data_type.value == data_type and 
                    subscription.symbol == symbol):
                    try:
                        await self._safe_callback(subscription.callback, data)
                    except Exception as e:
                        self.logger.error(f"回调函数执行失败: {e}")
                        
        except Exception as e:
            self.logger.error(f"数据分发失败: {e}")
    
    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全执行回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error(f"回调函数执行异常: {e}")
    
    async def _ping_handler(self) -> None:
        """心跳处理器"""
        while self.is_running and self.state.connected:
            try:
                # 发送心跳
                await self._send_ping()
                self.state.last_ping = time.time()
                
                await asyncio.sleep(self.ping_interval)
                
                # 检查心跳响应
                if (self.state.last_pong and 
                    time.time() - self.state.last_pong > self.ping_timeout):
                    self.logger.warning("心跳超时，重新连接")
                    await self._close_connection()
                    break
                    
            except Exception as e:
                self.logger.error(f"心跳处理错误: {e}")
                break
    
    async def _send_pending_subscriptions(self) -> None:
        """发送待处理的订阅"""
        for stream_id in list(self.pending_subscriptions):
            if stream_id in self.subscriptions:
                subscription = self.subscriptions[stream_id]
                await self._send_subscribe_message(subscription)
                self.pending_subscriptions.remove(stream_id)
    
    async def _send_subscribe_message(self, subscription: WSSubscription) -> None:
        """发送订阅消息"""
        try:
            message = await self._build_subscribe_message(subscription)
            await self._send_message(message)
            
        except Exception as e:
            self.logger.error(f"发送订阅消息失败: {e}")
    
    async def _send_message(self, message: Dict[str, Any]) -> None:
        """发送消息"""
        if not self.state.connected or not self.connection:
            return
        
        try:
            message_str = json.dumps(message)
            
            if isinstance(self.connection, aiohttp.ClientWebSocketResponse):
                await self.connection.send_str(message_str)
            else:
                await self.connection.send(message_str)
                
        except Exception as e:
            self.logger.error(f"发送消息失败: {e}")
            self.state.connected = False
    
    async def _send_ping(self) -> None:
        """发送心跳"""
        ping_message = {"op": "ping", "timestamp": int(time.time() * 1000)}
        await self._send_message(ping_message)
    
    async def _close_connection(self) -> None:
        """关闭连接"""
        if self.connection:
            try:
                if isinstance(self.connection, aiohttp.ClientWebSocketResponse):
                    await self.connection.close()
                else:
                    await self.connection.close()
            except Exception as e:
                self.logger.error(f"关闭连接失败: {e}")
            finally:
                self.connection = None
        
        self.state.connected = False
    
    async def _check_connection_health(self) -> None:
        """检查连接健康状态"""
        if not self.state.connected:
            return
        
        # 检查错误次数
        if self.state.error_count > 5:
            self.logger.warning("错误次数过多，重新连接")
            await self._close_connection()
    
    def _is_pong_message(self, data: Dict[str, Any]) -> bool:
        """判断是否为心跳响应"""
        return data.get('op') == 'pong' or data.get('type') == 'pong'
    
    def _is_error_message(self, data: Dict[str, Any]) -> bool:
        """判断是否为错误消息"""
        return 'error' in data or data.get('type') == 'error'
    
    def get_connection_info(self) -> Dict[str, Any]:
        """获取连接信息"""
        return {
            'connected': self.state.connected,
            'connecting': self.state.connecting,
            'reconnect_count': self.state.reconnect_count,
            'error_count': self.state.error_count,
            'last_error': self.state.last_error,
            'subscriptions_count': len(self.subscriptions),
            'pending_subscriptions_count': len(self.pending_subscriptions)
        }


# class WSTest:

#     def __init__(self, ws_manager: WSSubscriptionManager):
#         import websocket
#         self.ws = websocket.WebSocketApp(
#             f"wss:{host}/ws/query?token={access_token}",
#             on_reconnect=lambda _ws: logger.debug('[WebSocket] Reconnect to WS server.'),
#             on_close=lambda ws, code, msg: logging.debug(
#                 f"[WebSocket] Connection closed with status code: {code}, message: {msg}"),
#             on_error=lambda ws, err: logger.error(f'[WebSocket] {err}'),
#             on_pong=lambda ws, data: logger.debug(f"[WebSocket] Receive pong message"),
#             on_message=self.__on_message
#         )
#         threading.Thread(target=self.ws.run_forever, kwargs={'ping_interval': 30, 'ping_timeout': 10, 'reconnect': 5},
#                          daemon=True).start()
#         start = time.time()
#         while not self.ws.sock or not self.ws.sock.sock:
#             time.sleep(1)
#             if time.time() - start >= 60:
#                 msg = (
#                     f"[WebSocket] Timeout to connect to MO for instance of {decoded_token.get('name').split(':')[0]}. "
#                     f"Uid is: {decoded_token.get('uid')}.")
#                 logger.error(msg)
#                 raise websocket.WebSocketConnectionClosedException(msg)

#     def __on_message(self, ws, message):
#         msg = json.loads(message)
#         # logger.debug(f"[WebSocket] Receive message: {msg}")
#         if msg.get('message_type') == 'query_id':
#             if not self.ws_msg_queue.empty():
#                 with self.ws_msg_queue.mutex:
#                     self.ws_msg_queue.queue.clear()
#             self.ws_msg_queue.put(msg)
#         elif msg.get('message_type') == 'query_result':
#             self.ws_msg_queue.put(msg)
#         else:
#             logger.warning(f"[WebSocket] Receive message not recognized.")