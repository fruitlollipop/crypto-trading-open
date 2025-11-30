"""
StandX REST API模块

包含HTTP请求、认证、私有数据获取、交易操作等功能
基于官方文档: https://docs.standx.com/standx-api/perps-http
"""

import time
import json
import aiohttp
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime

from .standx_base import StandXBase
from .standx_auth import StandXAuth
from ..models import (
    BalanceData, OrderData, OrderStatus, OrderSide, OrderType, PositionData, TradeData
)


class StandXRest(StandXBase):
    """StandX REST API接口"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        self.logger = logger
        self.session = None
        self.base_url = getattr(config, 'base_url', self.DEFAULT_BASE_URL) if config else self.DEFAULT_BASE_URL
        self.is_authenticated = False
        
        # 初始化认证模块
        self.auth = StandXAuth(logger=logger)
        
        # JWT token（从认证模块获取）
        self.jwt_token = None

    async def setup_session(self):
        """设置HTTP会话"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    'User-Agent': 'StandX-Adapter/1.0',
                    'Content-Type': 'application/json'
                }
            )

    async def close_session(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        signed: bool = False,
        body_signature: bool = False,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        执行HTTP请求

        Args:
            method: HTTP 方法 (GET, POST, DELETE)
            endpoint: API 端点路径
            params: URL 查询参数
            data: 请求体数据
            signed: 是否需要 JWT 认证
            body_signature: 是否需要请求体签名
            session_id: 会话 ID（用于订单响应流）
        """
        await self.setup_session()

        # 正确处理URL拼接
        base_url = self.base_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        url = f"{base_url}/{endpoint}"
        headers = {'Content-Type': 'application/json'}

        # JWT 认证
        if signed:
            if self.auth.jwt_token:
                headers['Authorization'] = f'Bearer {self.auth.jwt_token}'
            else:
                if self.logger:
                    self.logger.warning("需要 JWT token 但未设置，请求可能失败")

        # 请求体签名
        if body_signature and data:
            payload_str = json.dumps(data, separators=(',', ':'))
            signature_headers = self.auth.sign_request(payload_str)
            headers.update(signature_headers)

        # Session ID（用于订单响应流）
        if session_id:
            headers['x-session-id'] = session_id

        try:
            if method.upper() == 'GET':
                async with self.session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        raise Exception(f"StandX API错误 [{response.status}]: {error_text}")
            elif method.upper() == 'POST':
                async with self.session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        raise Exception(f"StandX API错误 [{response.status}]: {error_text}")
            elif method.upper() == 'DELETE':
                async with self.session.delete(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_text = await response.text()
                        raise Exception(f"StandX API错误 [{response.status}]: {error_text}")
            else:
                raise Exception(f"不支持的HTTP方法: {method}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"StandX HTTP请求失败: {e}")
            raise

    # === 公共数据接口 ===

    async def query_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        查询交易对信息
        
        GET /api/query_symbol_info
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_symbol_info', params=params)

    async def query_symbol_market(self, symbol: str) -> Dict[str, Any]:
        """
        查询交易对市场数据
        
        GET /api/query_symbol_market
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_symbol_market', params=params)

    async def query_symbol_price(self, symbol: str) -> Dict[str, Any]:
        """
        查询交易对价格
        
        GET /api/query_symbol_price
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_symbol_price', params=params)

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对行情数据（使用 query_symbol_market）"""
        return await self.query_symbol_market(symbol)

    async def query_depth_book(self, symbol: str) -> Dict[str, Any]:
        """
        查询订单簿深度
        
        GET /api/query_depth_book
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_depth_book', params=params)

    async def fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿数据（使用 query_depth_book）"""
        return await self.query_depth_book(symbol)

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照
        
        使用 query_depth_book API
        """
        return await self.query_depth_book(symbol)

    async def query_recent_trades(self, symbol: str) -> List[Dict[str, Any]]:
        """
        查询最近成交记录
        
        GET /api/query_recent_trades
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_recent_trades', params=params)

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取交易记录（使用 query_recent_trades）"""
        return await self.query_recent_trades(symbol)

    async def get_kline_history(
        self,
        symbol: str,
        resolution: str,
        from_time: int,
        to_time: int,
        count_back: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取K线历史数据
        
        GET /api/kline/history
        
        Args:
            symbol: 交易对符号
            resolution: 时间分辨率 (1m, 5m, 15m, 30m, 1h, 4h, 1d 等)
            from_time: 开始时间（Unix 时间戳，秒）
            to_time: 结束时间（Unix 时间戳，秒）
            count_back: 需要加载的K线数量（可选）
        """
        params = {
            'symbol': symbol,
            'from': from_time,
            'to': to_time,
            'resolution': resolution
        }
        if count_back:
            params['countBack'] = count_back
        return await self._request('GET', '/api/kline/history', params=params)

    async def fetch_klines(self, symbol: str, interval: str, since: Optional[int] = None,
                           limit: Optional[int] = None) -> List[List]:
        """获取K线数据（兼容接口）"""
        # 转换时间格式
        from_time = int(since / 1000) if since else int(time.time()) - 86400  # 默认24小时前
        to_time = int(time.time())
        
        # 映射时间间隔
        resolution_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1d'
        }
        resolution = resolution_map.get(interval, '1h')
        
        result = await self.get_kline_history(symbol, resolution, from_time, to_time, limit)
        
        # 转换格式为列表格式 [timestamp, open, high, low, close, volume]
        klines = []
        if 't' in result and 'o' in result and 'h' in result and 'l' in result and 'c' in result and 'v' in result:
            timestamps = result['t']
            opens = result['o']
            highs = result['h']
            lows = result['l']
            closes = result['c']
            volumes = result['v']
            
            for i in range(len(timestamps)):
                klines.append([
                    timestamps[i] * 1000,  # 转换为毫秒
                    float(opens[i]),
                    float(highs[i]),
                    float(lows[i]),
                    float(closes[i]),
                    float(volumes[i])
                ])
        
        return klines

    # === 私有数据接口 ===

    async def query_balance(self) -> Dict[str, Any]:
        """
        查询账户余额
        
        GET /api/query_balance
        """
        return await self._request('GET', '/api/query_balance', signed=True)

    async def fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额数据（兼容接口）"""
        return await self.query_balance()

    async def query_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        查询用户持仓
        
        GET /api/query_positions
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        return await self._request('GET', '/api/query_positions', params=params, signed=True)

    async def fetch_positions(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取持仓信息（兼容接口）"""
        if symbols and len(symbols) > 0:
            # 查询第一个符号的持仓
            positions = await self.query_positions(symbols[0])
        else:
            positions = await self.query_positions()
        return {"positions": positions}

    async def query_open_orders(self, symbol: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        查询用户所有开放订单
        
        GET /api/query_open_orders
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        if limit:
            params['limit'] = min(limit, 1200)  # StandX 最大支持 1200
        return await self._request('GET', '/api/query_open_orders', params=params, signed=True)

    async def fetch_open_orders(self, symbol: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取开放订单（兼容接口）"""
        result = await self.query_open_orders(symbol, limit)
        return result.get('result', [])

    async def query_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        order_type: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        last_id: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        查询用户订单
        
        GET /api/query_orders
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        if status:
            params['status'] = status
        if order_type:
            params['order_type'] = order_type
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        if last_id:
            params['last_id'] = last_id
        if limit:
            params['limit'] = min(limit, 500)  # StandX 最大支持 500
        return await self._request('GET', '/api/query_orders', params=params, signed=True)

    async def fetch_order_history(self, symbol: Optional[str] = None, since: Optional[int] = None,
                                  limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取订单历史（兼容接口）"""
        start = None
        if since:
            start_dt = datetime.fromtimestamp(since / 1000)
            start = start_dt.isoformat() + 'Z'
        
        result = await self.query_orders(symbol=symbol, start=start, limit=limit)
        return result.get('result', [])

    async def query_order(self, order_id: Optional[int] = None, cl_ord_id: Optional[str] = None) -> Dict[str, Any]:
        """
        查询订单
        
        GET /api/query_order
        """
        params = {}
        if order_id:
            params['order_id'] = order_id
        if cl_ord_id:
            params['cl_ord_id'] = cl_ord_id
        return await self._request('GET', '/api/query_order', params=params, signed=True)

    async def fetch_order_status(self, symbol: str, order_id: Optional[str] = None,
                                 client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """获取订单状态（兼容接口）"""
        order_id_int = int(order_id) if order_id else None
        return await self.query_order(order_id=order_id_int, cl_ord_id=client_order_id)

    # === 交易操作接口 ===

    async def new_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        qty: str,
        time_in_force: str,
        reduce_only: bool = False,
        price: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
        margin_mode: Optional[str] = None,
        leverage: Optional[int] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        创建新订单
        
        POST /api/new_order
        
        Args:
            symbol: 交易对符号
            side: 订单方向 (buy/sell)
            order_type: 订单类型 (limit/market)
            qty: 订单数量（字符串格式）
            time_in_force: 订单有效期 (gtc/ioc/fok)
            reduce_only: 是否只减仓
            price: 订单价格（限价单必需，字符串格式）
            cl_ord_id: 客户端订单ID
            margin_mode: 保证金模式 (cross/isolated)
            leverage: 杠杆倍数
            session_id: 会话ID（用于订单响应流）
        """
        data = {
            'symbol': symbol,
            'side': side,
            'order_type': order_type,
            'qty': qty,
            'time_in_force': time_in_force,
            'reduce_only': reduce_only
        }
        
        if price:
            data['price'] = price
        if cl_ord_id:
            data['cl_ord_id'] = cl_ord_id
        if margin_mode:
            data['margin_mode'] = margin_mode
        if leverage:
            data['leverage'] = leverage

        return await self._request(
            'POST',
            '/api/new_order',
            data=data,
            signed=True,
            body_signature=True,
            session_id=session_id
        )

    async def create_order(self, symbol: str, side: str, order_type: str, quantity: Decimal,
                           price: Optional[Decimal] = None, time_in_force: str = "GTC",
                           client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """创建订单（兼容接口）"""
        # 转换参数格式
        side_lower = side.lower()
        order_type_lower = order_type.lower()
        time_in_force_lower = time_in_force.lower()
        
        return await self.new_order(
            symbol=symbol,
            side=side_lower,
            order_type=order_type_lower,
            qty=str(quantity),
            time_in_force=time_in_force_lower,
            price=str(price) if price else None,
            cl_ord_id=client_order_id
        )

    async def cancel_order(
        self,
        order_id: Optional[int] = None,
        cl_ord_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        取消订单
        
        POST /api/cancel_order
        """
        data = {}
        if order_id:
            data['order_id'] = order_id
        if cl_ord_id:
            data['cl_ord_id'] = cl_ord_id
        
        if not data:
            raise ValueError("至少需要提供 order_id 或 cl_ord_id 之一")

        return await self._request(
            'POST',
            '/api/cancel_order',
            data=data,
            signed=True,
            body_signature=True,
            session_id=session_id
        )

    async def cancel_orders(
        self,
        order_id_list: Optional[List[int]] = None,
        cl_ord_id_list: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        取消多个订单
        
        POST /api/cancel_orders
        """
        data = {}
        if order_id_list:
            data['order_id_list'] = order_id_list
        if cl_ord_id_list:
            data['cl_ord_id_list'] = cl_ord_id_list
        
        if not data:
            raise ValueError("至少需要提供 order_id_list 或 cl_ord_id_list 之一")

        return await self._request(
            'POST',
            '/api/cancel_orders',
            data=data,
            signed=True,
            body_signature=True
        )

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """取消所有订单"""
        # 获取所有开放订单
        open_orders = await self.query_open_orders(symbol=symbol)
        orders = open_orders.get('result', [])
        
        if not orders:
            return []
        
        # 提取订单ID列表
        order_id_list = [order['id'] for order in orders if 'id' in order]
        
        if order_id_list:
            return await self.cancel_orders(order_id_list=order_id_list)
        else:
            return []

    # === 账户设置接口 ===

    async def change_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """
        修改杠杆倍数
        
        POST /api/change_leverage
        """
        data = {
            'symbol': symbol,
            'leverage': leverage
        }
        return await self._request('POST', '/api/change_leverage', data=data, signed=True, body_signature=True)

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数（兼容接口）"""
        return await self.change_leverage(symbol, leverage)

    async def change_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """
        修改保证金模式
        
        POST /api/change_margin_mode
        """
        data = {
            'symbol': symbol,
            'margin_mode': margin_mode.lower()
        }
        return await self._request('POST', '/api/change_margin_mode', data=data, signed=True, body_signature=True)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式（兼容接口）"""
        return await self.change_margin_mode(symbol, margin_mode)

    async def transfer_margin(self, symbol: str, amount_in: str) -> Dict[str, Any]:
        """
        转移保证金
        
        POST /api/transfer_margin
        """
        data = {
            'symbol': symbol,
            'amount_in': amount_in
        }
        return await self._request('POST', '/api/transfer_margin', data=data, signed=True, body_signature=True)

    async def query_position_config(self, symbol: str) -> Dict[str, Any]:
        """
        查询持仓配置
        
        GET /api/query_position_config
        """
        params = {'symbol': symbol}
        return await self._request('GET', '/api/query_position_config', params=params, signed=True)

    async def query_trades(
        self,
        symbol: Optional[str] = None,
        last_id: Optional[int] = None,
        side: Optional[str] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        查询用户成交记录
        
        GET /api/query_trades
        """
        params = {}
        if symbol:
            params['symbol'] = symbol
        if last_id:
            params['last_id'] = last_id
        if side:
            params['side'] = side
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        if limit:
            params['limit'] = min(limit, 500)  # StandX 最大支持 500
        return await self._request('GET', '/api/query_trades', params=params, signed=True)

    # === 数据解析接口 ===

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            balance_data = await self.fetch_balances()
            return [
                self._parse_balance(balance)
                for balance in balance_data.get('balances', [])
                if Decimal(balance.get('free', '0')) > 0 or Decimal(balance.get('locked', '0')) > 0
            ]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取账户余额失败: {e}")
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """获取持仓信息"""
        try:
            positions_data = await self.fetch_positions(symbols)
            positions = []
            for pos in positions_data.get('positions', []):
                positions.append({
                    'symbol': pos.get('symbol', ''),
                    'size': Decimal(str(pos.get('positionAmt', '0'))),
                    'side': 'long' if float(pos.get('positionAmt', '0')) > 0 else 'short',
                    'entry_price': Decimal(str(pos.get('entryPrice', '0'))),
                    'mark_price': Decimal(str(pos.get('markPrice', '0'))),
                    'unrealized_pnl': Decimal(str(pos.get('unRealizedProfit', '0'))),
                    'percentage': float(pos.get('percentage', '0')),
                    'timestamp': datetime.now()
                })
            return positions
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取持仓信息失败: {e}")
            return []

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            orders_data = await self.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in orders_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(self, symbol: Optional[str] = None, since: Optional[datetime] = None,
                                limit: Optional[int] = None) -> List[OrderData]:
        """获取订单历史"""
        try:
            since_timestamp = int(since.timestamp() * 1000) if since else None
            orders_data = await self.fetch_order_history(symbol, since_timestamp, limit)
            return [self._parse_order(order) for order in orders_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取订单历史失败: {e}")
            return []

    async def place_order(self, symbol: str, side: OrderSide, order_type: OrderType, quantity: Decimal,
                          price: Optional[Decimal] = None, time_in_force: str = "GTC",
                          client_order_id: Optional[str] = None) -> OrderData:
        """下单"""
        try:
            side_str = 'BUY' if side == OrderSide.BUY else 'SELL'
            type_str = 'LIMIT' if order_type == OrderType.LIMIT else 'MARKET'

            order_data = await self.create_order(
                symbol=symbol,
                side=side_str,
                order_type=type_str,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                client_order_id=client_order_id
            )
            return self._parse_order(order_data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"下单失败: {e}")
            raise

    async def cancel_order_by_id(self, symbol: str, order_id: Optional[str] = None,
                                 client_order_id: Optional[str] = None) -> bool:
        """取消订单"""
        try:
            await self.cancel_order(symbol, order_id, client_order_id)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消订单失败: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None,
                               client_order_id: Optional[str] = None) -> OrderData:
        """获取订单状态"""
        try:
            order_data = await self.fetch_order_status(symbol, order_id, client_order_id)
            return self._parse_order(order_data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取订单状态失败: {e}")
            raise

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[TradeData]:
        """获取最近成交记录"""
        try:
            trades_data = await self.fetch_trades(symbol, limit=limit)
            return [self._parse_trade(trade, symbol) for trade in trades_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取最近成交记录失败: {e}")
            return []

    async def get_klines(self, symbol: str, interval: str, since: Optional[datetime] = None,
                         limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        try:
            since_timestamp = int(since.timestamp() * 1000) if since else None
            klines_data = await self.fetch_klines(symbol, interval, since_timestamp, limit)

            # 转换数据格式
            klines = []
            for kline in klines_data:
                if len(kline) >= 6:
                    klines.append({
                        'timestamp': kline[0],
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[5])
                    })
            return klines
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取K线数据失败: {e}")
            return []

    async def authenticate(self) -> bool:
        """进行身份认证"""
        try:
            # StandX 使用 JWT token 认证
            # 如果已经有有效的 token，则认证成功
            if self.auth.jwt_token and self.auth.is_token_valid():
                self.is_authenticated = True
                self.jwt_token = self.auth.jwt_token
                if self.logger:
                    self.logger.info("StandX 使用已有 JWT token 认证成功")
                return True
            else:
                if self.logger:
                    self.logger.warning("StandX 需要 JWT token，请先调用 auth.authenticate() 获取 token")
                self.is_authenticated = False
                return False
        except Exception as e:
            if self.logger:
                self.logger.warning(f"StandX 认证失败: {e}")
            self.is_authenticated = False
            return False

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # GET /api/health
            result = await self._request('GET', '/api/health')
            api_accessible = True
            error = None
        except Exception as e:
            api_accessible = False
            error = str(e)

        return {
            "status": "ok" if api_accessible else "error",
            "api_accessible": api_accessible,
            "authentication": "enabled" if self.is_authenticated else "disabled",
            "timestamp": time.time(),
            "error": error
        }
