#!/usr/bin/env python3
"""
StandX 交易周期示例

演示如何使用 execute_trading_cycle 方法执行完整的交易周期
"""

import asyncio
import logging
import os
import hashlib
import base64
import hmac
import json
import requests
import time
import argparse
import re
import math
from pathlib import Path
from decimal import Decimal
from types import SimpleNamespace
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime, timezone
from eth_account import Account
import yaml
import logging.config
import pandas as pd
from core.adapters.exchanges.adapter import ExchangeAdapter
from core.adapters.exchanges.interface import ExchangeConfig, ExchangeType
from core.adapters.exchanges.factory import get_exchange_factory
from core.adapters.exchanges.models import OrderType, OrderData


def init_logging(keyword):
    logging_config = yaml.safe_load(f"""
    version: 1
    disable_existing_loggers: false
    formatters:
      basic:
        datefmt: '%Y-%m-%d %H:%M:%S'
        format: '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d:%(funcName)s] - %(message)s'
      colored:
        (): colorlog.ColoredFormatter
        log_colors:
          DEBUG: white
          INFO: green
          WARNING: yellow
          ERROR: red
          CRITICAL: bold_red
        datefmt: '%Y-%m-%d %H:%M:%S'
        format: '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d:%(funcName)s] - %(message)s'
    handlers:
      console:
        class: logging.StreamHandler
        stream: ext://sys.stdout
        level: INFO
        formatter: colored
      file:
        class: logging.FileHandler
        filename: logs/{keyword}.log
        mode: w
        encoding: utf-8
        level: DEBUG
        formatter: basic
    loggers:
      root:
        level: DEBUG
        handlers:
          - file
          - console
      sqlalchemy.engine.Engine:
        level: DEBUG
        propagate: false
        handlers:
          - file
        filters: []
      sqlalchemy.pool.impl.QueuePool:
        level: DEBUG
        propagate: false
        handlers:
          - file
        filters: []
    """)
    logging.config.dictConfig(logging_config)
    return logging.getLogger(__name__)

def parse_args(name='standx'):
    parser = argparse.ArgumentParser(
        prog=name,
        description='StandX 交易示例',
        epilog="""
示例:
    # 使用默认数量 0.001
    python trade standx_trading_cycle_example.py

    # 指定交易数量
    python trade standx_trading_cycle_example.py --quantity 0.01

    # 指定交易对和数量
    python trade standx_trading_cycle_example.py --symbol ETH-USD --quantity 0.1

    # 使用限价单
    python trade standx_trading_cycle_example.py --quantity 0.01 --order-type limit
    
    # 统计账号积分和交易数据
    python stats standx_trading_cycle_example.py
    """
    )
    parser.add_argument('--config', action='store', required=False, type=str,
                        dest='config_file', help='Name of config file')
    parser.add_argument('--verbose', action='store_true', required=False, default=False, dest='verbose',
                        help='Show the verbose information, off by default')
    subparsers = parser.add_subparsers(title='Actions', description='Available actions', help='chose action to perform')
    trade_parser = subparsers.add_parser('trade', description='Make trades',
                                         help='usage: %(prog)s trade [options] args')
    stats_parser = subparsers.add_parser('stats', description='Get statistics',
                                         help='usage: %(prog)s stats [options] args')
    stats_parser.add_argument(
        '--tmux-config-dir',
        type=str,
        default='~/.tmuxp/standx',
        help='配置文件目录 (默认: ~/.tmuxp/standx)'
    )
    stats_parser.add_argument(
        '--file-pattern',
        type=str,
        default=r'^standx-.*\.yaml$',
        help='文件名正则表达式模式 (默认: ^standx-.*\\.yaml$)'
    )
    stats_parser.add_argument(
        '--excel-path',
        type=str,
        default='data/standx_stats.xlsx',
        help='Excel 文件路径 (默认: data/standx_stats.xlsx)'
    )
    trade_parser.add_argument(
        '--quantity',
        type=str,
        default='0.001',
        help='交易数量 (默认: 0.001)'
    )

    trade_parser.add_argument(
        '--symbol',
        type=str,
        default='BTC-USD',
        help='交易对符号 (默认: BTC-USD)'
    )

    trade_parser.add_argument(
        '--order-type',
        type=str,
        choices=['market', 'limit'],
        default='market',
        help='订单类型: market (市价) 或 limit (限价) (默认: market)'
    )

    trade_parser.add_argument(
        '--buy-price',
        type=str,
        default=None,
        help='买入价格 (限价单时使用)'
    )

    trade_parser.add_argument(
        '--price-spread',
        type=str,
        default=None,
        help='价格价差 (卖出价格 = 买入价格 + 价差)'
    )
    trade_parser.set_defaults(action='trade', func=make_trades)
    stats_parser.set_defaults(action='stats', func=get_stats)
    parser.set_defaults()
    return parser.parse_args()

def send_feishu_alert(args):
    ts = int(time.time())
    string_to_sign = '{}\n{}'.format(ts, os.getenv('FEISHU_WEBHOOK_SECRET'))
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    sign = base64.b64encode(hmac_code).decode('utf-8')
    feishu_msg_template = f"""
{{
    "timestamp": "{ts}",
    "sign": "{sign}",
    "msg_type": "post",
    "content": {{
        "post": {{
            "zh_cn": {{
                "title": "Standx Alert v1",
                "content": [
                    [
                        {{
                            "tag": "text",
                            "text": "编号："
                        {{,
                        {{
                            "tag": "text",
                            "text": "{args.account_name}"
                        }}
                    ],
                    [
                        {{
                            "tag": "text",
                            "text": "交易所："
                        }},
                        {{
                            "tag": "text",
                            "text": "StandX"
                        }}
                    ],
                    [
                        {{
                            "tag": "text",
                            "text": "货币代号："
                        }},
                        {{
                            "tag": "text",
                            "text": "{args.ticker}"
                        }}
                    ],
                    [
                        {{
                            "tag": "text",
                            "text": "告警信息："
                        }},
                        {{
                            "tag": "text",
                            "text": "有持仓，请确认和处理！"
                        }}
                    ],
                    [
                        {{
                            "tag": "at",
                            "user_id": "all",
                            "user_name": "所有人"
                        }}
                    ]
                ]
            }}
        }}
    }}
}}
    """
    feishu_body = json.loads(feishu_msg_template)
    res = requests.post(os.getenv('FEISHU_WEBHOOK_URL'), headers={'Content-Type': 'APPLICATION_JSON_UTF8'}, json=feishu_body)
    assert res.status_code == 200, f"Send Feishu alert failed: {res.text}"

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='StandX 交易周期示例',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认数量 0.001
  python standx_trading_cycle_example.py

  # 指定交易数量
  python standx_trading_cycle_example.py --quantity 0.01

  # 指定交易对和数量
  python standx_trading_cycle_example.py --symbol ETH-USD --quantity 0.1

  # 使用限价单
  python standx_trading_cycle_example.py --quantity 0.01 --order-type limit
        """
    )
    
    parser.add_argument(
        '--quantity',
        type=str,
        default='0.001',
        help='交易数量 (默认: 0.001)'
    )
    
    parser.add_argument(
        '--symbol',
        type=str,
        default='BTC-USD',
        help='交易对符号 (默认: BTC-USD)'
    )
    
    parser.add_argument(
        '--order-type',
        type=str,
        choices=['market', 'limit'],
        default='market',
        help='订单类型: market (市价) 或 limit (限价) (默认: market)'
    )
    
    parser.add_argument(
        '--buy-price',
        type=str,
        default=None,
        help='买入价格 (限价单时使用)'
    )
    
    parser.add_argument(
        '--price-spread',
        type=str,
        default=None,
        help='价格价差 (卖出价格 = 买入价格 + 价差)'
    )
    
    return parser.parse_args()


async def check_balance(adapter, symbol: str, quantity: Decimal, order_type: OrderType, buy_price: Optional[Decimal] = None) -> Tuple[bool, str, Decimal]:
    """
    检查账户余额是否足够
    
    Returns:
        (是否足够, 错误信息, 可用余额)
    """
    try:
        # 获取账户余额
        balances = await adapter.get_balances()
        
        if not balances:
            return False, "无法获取账户余额", Decimal('0')
        
        # StandX 使用 DUSD 作为保证金资产
        # 查找 DUSD 或 USDT 余额
        balance = None
        for b in balances:
            if b.currency.upper() in ['DUSD', 'USDT', 'USD']:
                balance = b
                break
        
        if not balance:
            return False, "未找到可用的余额（需要 DUSD/USDT/USD）", Decimal('0')
        
        available_balance = balance.free
        print(f"💰 账户余额检查:")
        print(f"   币种: {balance.currency}")
        print(f"   可用余额: {available_balance}")
        print(f"   冻结余额: {balance.used}")
        print(f"   总余额: {balance.total}")
        
        # 计算所需金额
        if order_type == OrderType.MARKET:
            # 市价单：获取当前价格估算所需金额
            try:
                ticker = await adapter.get_ticker(symbol)
                current_price = ticker.last
                required_amount = quantity * current_price
            except Exception as e:
                return False, f"无法获取当前价格: {e}", available_balance
        else:
            # 限价单：使用指定价格
            if buy_price is None:
                return False, "限价单需要指定买入价格", available_balance
            required_amount = quantity * buy_price
        
        # 添加一些缓冲（10%）
        required_amount_with_buffer = required_amount * Decimal('1.1')
        
        print(f"   交易数量: {quantity}")
        if order_type == OrderType.MARKET:
            print(f"   当前价格: {current_price}")
        else:
            print(f"   买入价格: {buy_price}")
        print(f"   所需金额: {required_amount}")
        print(f"   所需金额(含10%%缓冲): {required_amount_with_buffer}")
        
        if available_balance < required_amount_with_buffer:
            return False, f"余额不足！需要 {required_amount_with_buffer}，但只有 {available_balance}", available_balance
        
        print(f"✅ 余额充足，可以执行交易")
        return True, "", available_balance
        
    except Exception as e:
        return False, f"检查余额时出错: {e}", Decimal('0')


async def make_trades(logger: logging.Logger, args: argparse.Namespace):
    # 解析交易数量
    try:
        quantity = Decimal(str(args.quantity))
    except Exception as e:
        logger.error(f"❌ 无效的交易数量: {args.quantity}, 错误: {e}")
        return
    
    # 解析订单类型
    order_type = OrderType.MARKET if args.order_type == 'market' else OrderType.LIMIT
    
    # 解析价格
    buy_price = None
    if args.buy_price:
        try:
            buy_price = Decimal(str(args.buy_price))
        except Exception as e:
            logger.error(f"❌ 无效的买入价格: {args.buy_price}, 错误: {e}")
            return
    
    price_spread = None
    if args.price_spread:
        try:
            price_spread = Decimal(str(args.price_spread))
        except Exception as e:
            logger.error(f"❌ 无效的价格价差: {args.price_spread}, 错误: {e}")
            return

    adapter: ExchangeAdapter | None = await init_exchange(logger)

    # 验证认证状态
    if not adapter or not adapter.is_authenticated():
        logger.error("❌ 认证状态验证失败，无法继续执行")
        return
    
    try:
        symbol = args.symbol
        logger.info(f"📊 交易周期执行")
        logger.info(f"交易对: {symbol}")
        logger.info(f"交易数量: {quantity}")
        logger.info(f"订单类型: {args.order_type}")
        if buy_price:
            logger.info(f"买入价格: {buy_price}")
        if price_spread:
            logger.info(f"价格价差: {price_spread}")
        
        # === 步骤2: 执行交易周期 ===
        logger.info("🚀 开始执行交易周期...")
        result = await adapter.execute_trading_cycle(
            symbol=symbol,
            quantity=quantity,
            buy_price=buy_price,
            price_spread=price_spread,
            order_type=order_type,
            wait_for_fill=(order_type == OrderType.LIMIT),
            max_wait_seconds=30
        )
        if result['success']:
            print("✅ 交易周期完成")
            logger.info(f"Buy订单ID: {result['buy_order'].id}")
            logger.info(f"Buy订单状态: {result['buy_order'].status.value}")
            if result['buy_order'].average:
                logger.info(f"Buy平均成交价: {result['buy_order'].average}")
            logger.info(f"Buy成交数量: {result['buy_order'].filled}")
            logger.info(f"Sell订单ID: {result['sell_order'].id}")
            logger.info(f"Sell订单状态: {result['sell_order'].status.value}")
            if result['sell_order'].average:
                logger.info(f"Sell平均成交价: {result['sell_order'].average}")
            logger.info(f"Sell成交数量: {result['sell_order'].filled}")
        else:
            logger.error("❌ 交易周期失败")
            logger.error(f"错误信息: {result.get('error')}")
            if result.get('buy_order'):
                logger.error(f"Buy订单ID: {result['buy_order'].id}")
                logger.error(f"Buy订单状态: {result['buy_order'].status.value}")
            if result.get('sell_order'):
                logger.error(f"Sell订单ID: {result['sell_order'].id}")
                logger.error(f"Sell订单状态: {result['sell_order'].status.value}")
        
    except Exception as e:
        logger.error(f"❌ 执行出错: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        logger.info("🔧 断开连接...")
        try:
            await adapter.disconnect()
            logger.info("✅ 已断开")
        except Exception as e:
            logger.error(f"⚠️  断开连接时出错: {e}")
    logger.info("👋 交易完成")

def load_yaml_files_from_directory(directory: str, pattern: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    从指定目录读取所有匹配正则表达式的 YAML 文件
    
    Args:
        directory: 目录路径
        pattern: 文件名正则表达式模式
        logger: 日志记录器
    
    Returns:
        包含所有匹配文件内容的列表，每个元素是 (文件路径, 文件内容) 的元组
    """
    configs = []
    dir_path = Path(directory)
    
    if not dir_path.exists():
        logger.warning(f"目录不存在: {directory}")
        return configs
    
    if not dir_path.is_dir():
        logger.warning(f"路径不是目录: {directory}")
        return configs
    
    try:
        # 编译正则表达式
        regex = re.compile(pattern, re.IGNORECASE)
        
        # 遍历目录中的所有文件
        for file_path in dir_path.iterdir():
            if file_path.is_file() and file_path.suffix in ['.yaml', '.yml']:
                # 检查文件名是否匹配正则表达式
                if regex.search(file_path.name):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            configs.append({
                                'file_path': str(file_path),
                                'file_name': file_path.name,
                                'content': content
                            })
                            logger.info(f"✅ 成功加载配置文件: {file_path.name}")
                    except Exception as e:
                        logger.warning(f"⚠️  加载配置文件失败 {file_path.name}: {e}")
        
        logger.info(f"📁 从目录 {directory} 加载了 {len(configs)} 个匹配模式 '{pattern}' 的配置文件")
        
    except re.error as e:
        logger.error(f"❌ 正则表达式错误: {pattern}, 错误: {e}")
    except Exception as e:
        logger.error(f"❌ 读取目录文件时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return configs


def sanitize_sheet_name(name: str) -> str:
    """
    清理 sheet 名称，使其符合 Excel 要求
    
    Args:
        name: 原始名称
    
    Returns:
        清理后的名称（最多31个字符，移除非法字符）
    """
    # Excel sheet 名称限制：
    # - 最多31个字符
    # - 不能包含: \ / ? * [ ]
    # - 不能以单引号开头或结尾
    illegal_chars = ['\\', '/', '?', '*', '[', ']', ':', "'"]
    for char in illegal_chars:
        name = name.replace(char, '_')
    
    # 移除首尾空格和单引号
    name = name.strip().strip("'")
    
    # 限制长度
    if len(name) > 31:
        name = name[:31]
    
    # 如果为空，使用默认名称
    if not name:
        name = 'orders'
    
    return name


def write_orders_to_excel(orders: List[OrderData], excel_path: str, account_name: str, logger: logging.Logger) -> None:
    """
    将订单数据写入 Excel 文件（每次覆盖写入最新的数据，以日期为主键）
    每个账号使用独立的 sheet
    
    Args:
        orders: 订单列表
        excel_path: Excel 文件路径
        account_name: 账号名称（用于 sheet 名称）
        logger: 日志记录器
    """
    try:
        # 创建输出目录（如果不存在）
        excel_file = Path(excel_path)
        excel_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 将订单数据转换为字典列表
        orders_data = []
        # 获取当前 UTC 日期作为备用
        current_utc_date = datetime.now(timezone.utc).date()
        
        for order in orders:
            # 日期列：使用订单创建时间的日期，如果没有则使用当前 UTC 日期
            if order.timestamp:
                order_date = order.timestamp.date()
            else:
                order_date = current_utc_date
            
            # 处理客户端订单ID：拆分成链和钱包地址
            client_id = order.client_id or ''
            chain = ''
            wallet_address = ''
            if client_id and '_' in client_id:
                parts = client_id.split('_', 1)  # 只分割第一个下划线
                chain = parts[0]  # 链名（下划线前的部分）
                wallet_address = parts[1] if len(parts) > 1 else ''  # 钱包地址（下划线后的部分）
            elif client_id:
                # 如果没有下划线，整个值作为钱包地址
                wallet_address = client_id
            
            orders_data.append({
                '日期': order_date.strftime('%Y-%m-%d'),  # 只取日期部分
                '订单ID': order.id,
                '链': chain,
                '钱包地址': wallet_address,
                '交易对': order.symbol,
                '方向': order.side.value if hasattr(order.side, 'value') else str(order.side),
                '订单类型': order.type.value if hasattr(order.type, 'value') else str(order.type),
                '订单数量': float(order.amount),
                '订单价格': float(order.price) if order.price else None,
                '已成交数量': float(order.filled),
                '剩余数量': float(order.remaining),
                '成交金额': float(order.cost),
                '平均成交价': float(order.average) if order.average else None,
                '订单状态': order.status.value if hasattr(order.status, 'value') else str(order.status),
                '创建时间': order.timestamp.strftime('%Y-%m-%d %H:%M:%S') if order.timestamp else '',  # 保持原来的格式
                '更新时间': order.updated.strftime('%Y-%m-%d %H:%M:%S') if order.updated else '',
                '手续费': float(order.fee['fee']) if order.fee and isinstance(order.fee, dict) and 'fee' in order.fee else (float(order.fee) if order.fee else 0.0),
            })
        
        if not orders_data:
            logger.warning("⚠️  没有订单数据可写入")
            return
        
        # 创建订单详情 DataFrame（所有订单的详细数据）
        df_orders_detail = pd.DataFrame(orders_data)
        # 按创建时间排序（最新的在前）
        df_orders_detail = df_orders_detail.sort_values('创建时间', ascending=False)

        symbol_stats = df_orders_detail.groupby('交易对').agg({
            '订单ID': 'count',
        }).reset_index()
        symbol_stats.columns = ['交易对', '订单数量']
        
        # 检查每个交易对的订单数量是否为偶数
        odd_count_symbols = symbol_stats[symbol_stats['订单数量'] % 2 != 0]
        if not odd_count_symbols.empty:
            logger.warning("⚠️  以下交易对的订单数量不是偶数（可能存在未平仓订单）：")
            for _, row in odd_count_symbols.iterrows():
                logger.warning(f"   交易对: {row['交易对']}, 订单数量: {row['订单数量']}")
                send_feishu_alert(SimpleNamespace(account_name=account_name, tiker=row['交易对']))
        
        # 按日期分组，汇总每天的订单数据（统计数据）
        # 需要从创建时间中提取日期部分进行分组
        df_orders_detail['日期_分组'] = df_orders_detail['日期'].str[:10]  # 提取日期部分（YYYY-MM-DD）
        
        df_daily_stats = df_orders_detail.groupby('日期_分组').agg({
            '订单ID': 'count',  # 订单数量
            '已成交数量': 'sum', # 总成交数量
            '成交金额': 'sum',  # 总成交金额
            '手续费': 'sum',   # 总手续费
        }).reset_index()
        
        df_daily_stats.columns = ['日期', '订单数量', '已成交数量', '成交金额', '手续费']
        
        # 添加其他统计信息
        df_daily_stats['平均成交价'] = df_daily_stats['成交金额'] / df_daily_stats['已成交数量'].replace(0, 1)
        
        # 按日期排序（最新的在前）
        df_daily_stats = df_daily_stats.sort_values('日期', ascending=False)
        
        # 删除临时列
        df_orders_detail = df_orders_detail.drop(columns=['日期_分组'])
        
        # 添加一个空行作为分隔
        empty_row = pd.DataFrame([{col: '' for col in df_orders_detail.columns}])
        
        # 合并订单详情和统计数据
        df_daily_stats_expanded = df_daily_stats.copy()
        # 为统计数据添加与订单详情相同的列结构（其他列填充为空）
        for col in df_orders_detail.columns:
            if col not in df_daily_stats_expanded.columns:
                df_daily_stats_expanded[col] = ''
        
        # 确保列顺序一致
        df_daily_stats_expanded = df_daily_stats_expanded[df_orders_detail.columns]
        
        # 合并数据：先显示订单详情，然后添加空行，最后显示统计数据
        df_combined = pd.concat([
            df_orders_detail,
            empty_row,
            df_daily_stats_expanded
        ], ignore_index=True)
        
        # 清理账号名称作为 sheet 名称
        sheet_name = sanitize_sheet_name(account_name)
        
        # 读取现有的 Excel 文件（如果存在），保留其他 sheet
        existing_sheets = {}
        sheet_exists = False
        if excel_file.exists():
            try:
                excel_file_obj = pd.ExcelFile(excel_path, engine='openpyxl')
                for existing_sheet_name in excel_file_obj.sheet_names:
                    if existing_sheet_name == sheet_name:
                        # 当前账号的 sheet 已存在，标记为存在（将直接覆盖）
                        sheet_exists = True
                    else:
                        # 保留其他账号的 sheet 和积分 sheet（兼容 'points' 和 '积分'）
                        if existing_sheet_name not in ['积分', 'points']:
                            existing_sheets[existing_sheet_name] = pd.read_excel(excel_path, sheet_name=existing_sheet_name, engine='openpyxl')
                        else:
                            # 保留积分 sheet
                            existing_sheets[existing_sheet_name] = pd.read_excel(excel_path, sheet_name=existing_sheet_name, engine='openpyxl')
            except Exception as e:
                logger.warning(f"读取现有 Excel 文件失败: {e}")
        
        # 写入所有 sheet
        with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
            # 写入当前账号的订单数据（包含订单详情和统计数据，如果存在则覆盖，不存在则新建）
            df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
            # 写入其他保留的 sheet（包括其他账号的 orders 和积分 sheet）
            for existing_sheet_name, df_sheet in existing_sheets.items():
                df_sheet.to_excel(writer, sheet_name=existing_sheet_name, index=False)
        
        if sheet_exists:
            logger.info(f"✅ 已覆盖账号 {account_name} 的订单数据到 Excel: {excel_path} (sheet: {sheet_name})")
        else:
            logger.info(f"✅ 已新建账号 {account_name} 的订单数据到 Excel: {excel_path} (sheet: {sheet_name})")
        logger.info(f"   共写入 {len(df_orders_detail)} 笔订单详情和 {len(df_daily_stats)} 天的统计数据")
        
    except Exception as e:
        logger.error(f"❌ 写入订单数据到 Excel 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


def write_points_to_excel(points_data: Dict[str, Any], excel_path: str, account_name: str, logger: logging.Logger) -> None:
    """
    将积分数据写入 Excel 文件（所有账号放在一个 sheet 中，以账号名称为主键）
    只保存每个账号最新的积分数据，如果没有获取到某个账号的数据则保留该账号旧数据
    
    Args:
        points_data: 积分数据字典
        excel_path: Excel 文件路径
        account_name: 账号名称（作为主键）
        logger: 日志记录器
    """
    try:
        # 创建输出目录（如果不存在）
        excel_file = Path(excel_path)
        excel_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 准备积分数据行（以账号名称为主键）
        # 先添加账号名称
        points_row = {
            '账号名称': account_name,
        }
        
        # 从积分数据中获取 updated_at 作为更新时间
        if isinstance(points_data, dict) and 'updated_at' in points_data:
            updated_at = points_data['updated_at']
            if updated_at:
                # 如果 updated_at 是字符串，尝试格式化
                if isinstance(updated_at, str):
                    # 尝试解析 ISO 格式时间，如 "2025-12-03T10:30:39.537Z"
                    try:
                        # 去掉 Z 后缀，尝试解析
                        updated_at_str = updated_at.replace('Z', '').split('.')[0]  # 去掉毫秒和Z
                        dt = datetime.strptime(updated_at_str, '%Y-%m-%dT%H:%M:%S')
                        points_row['更新时间'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        # 如果解析失败，直接使用原值
                        points_row['更新时间'] = updated_at
                else:
                    points_row['更新时间'] = str(updated_at)
            else:
                points_row['更新时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            # 如果没有 updated_at 字段，使用当前时间
            points_row['更新时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 只保留需要的字段：rank, total_point, total_amount
        # 定义字段顺序和中文映射
        field_order = ['rank', 'total_point', 'total_amount']
        field_chinese_map = {
            'rank': '排名',
            'total_point': '总积分',
            'total_amount': '持有金额'
        }
        
        # 将积分数据的所有字段添加到行中
        if isinstance(points_data, dict):
            # 只处理需要的字段
            for key in field_order:
                if key in points_data:
                    value = points_data[key]
                    # 使用中文列名
                    chinese_key = field_chinese_map.get(key, key)
                    
                    # 处理 total_point 和 total_amount：除以 1000000，保留2位小数（不四舍五入）
                    if key in ['total_point', 'total_amount'] and value is not None:
                        try:
                            # 转换为数字
                            num_value = float(str(value))
                            # 除以 1000000
                            num_value = num_value / 1000000.0
                            # 保留2位小数（不四舍五入，向下截断）
                            num_value = math.floor(num_value * 100) / 100.0
                            points_row[chinese_key] = num_value
                        except (ValueError, TypeError):
                            # 如果转换失败，保持原值
                            points_row[chinese_key] = value
                    # 处理 rank
                    elif key == 'rank':
                        if value is None:
                            points_row[chinese_key] = ''
                        else:
                            points_row[chinese_key] = value
                    # 处理 null 值
                    else:
                        if value is None:
                            points_row[chinese_key] = ''
                        else:
                            points_row[chinese_key] = value
        else:
            points_row['积分数据'] = str(points_data)
        
        # 创建新账号数据的 DataFrame
        df_new = pd.DataFrame([points_row])
        
        # 使用统一的 points sheet（中文名称：积分）
        sheet_name = '积分'
        
        # 读取现有的 Excel 文件（如果存在），保留其他 sheet
        existing_sheets = {}
        df_existing_points = None
        
        if excel_file.exists():
            try:
                excel_file_obj = pd.ExcelFile(excel_path, engine='openpyxl')
                for existing_sheet_name in excel_file_obj.sheet_names:
                    # 兼容旧的 'points' sheet 名称和新的 '积分' sheet 名称
                    if existing_sheet_name == sheet_name or existing_sheet_name == 'points':
                        # 读取现有的积分数据（包含所有账号）
                        df_existing_points = pd.read_excel(excel_path, sheet_name=existing_sheet_name, engine='openpyxl')
                    elif existing_sheet_name != sheet_name and existing_sheet_name != 'points':  # 保留其他 sheet（如各账号的 orders）
                        existing_sheets[existing_sheet_name] = pd.read_excel(excel_path, sheet_name=existing_sheet_name, engine='openpyxl')
            except Exception as e:
                logger.warning(f"读取现有 Excel 文件失败: {e}")
        
        # 处理积分数据
        if df_existing_points is not None and not df_existing_points.empty:
            # 检查账号是否已存在
            if '账号名称' in df_existing_points.columns:
                # 如果账号已存在，则更新；否则追加
                account_exists = df_existing_points['账号名称'].astype(str) == account_name
                if account_exists.any():
                    # 更新该账号的记录
                    # 确保 df_new 的列顺序与 df_existing_points 一致
                    df_new_aligned = df_new.reindex(columns=df_existing_points.columns, fill_value='')
                    # 确保数据类型匹配：将 df_new_aligned 的每列转换为与 df_existing_points 相同的类型
                    for col in df_existing_points.columns:
                        if col in df_new_aligned.columns:
                            # 尝试将新数据的列转换为现有列的数据类型
                            try:
                                if df_existing_points[col].dtype != 'object':
                                    # 对于数值类型，使用 pd.to_numeric 转换
                                    df_new_aligned[col] = pd.to_numeric(df_new_aligned[col], errors='coerce')
                                    # 如果是整数类型，转换为整数
                                    if pd.api.types.is_integer_dtype(df_existing_points[col]):
                                        df_new_aligned[col] = df_new_aligned[col].astype('Int64')  # 使用可空整数类型
                                    else:
                                        # 保持与现有列相同的数据类型
                                        df_new_aligned[col] = df_new_aligned[col].astype(df_existing_points[col].dtype)
                                else:
                                    # 对于对象类型，转换为字符串
                                    df_new_aligned[col] = df_new_aligned[col].astype(str)
                            except (ValueError, TypeError):
                                # 如果转换失败，保持原类型
                                pass
                    # 现在可以安全地赋值了
                    # 如果匹配多行，删除所有匹配的行，然后添加新的一行（通常一个账号应该只有一行）
                    matching_indices = df_existing_points.index[account_exists]
                    if len(matching_indices) > 0:
                        # 如果有多行匹配，先删除所有匹配的行
                        if len(matching_indices) > 1:
                            df_existing_points = df_existing_points.drop(matching_indices)
                            logger.info(f"⚠️  发现 {len(matching_indices)} 行匹配账号 {account_name}，已删除所有重复行")
                        else:
                            # 只有一行匹配，删除它
                            df_existing_points = df_existing_points.drop(matching_indices[0])
                        # 添加新的一行数据
                        df_existing_points = pd.concat([df_existing_points, df_new_aligned], ignore_index=True)
                    df_points = df_existing_points
                    logger.info(f"✅ 已更新账号 {account_name} 的积分数据")
                else:
                    # 追加新账号的记录
                    df_points = pd.concat([df_existing_points, df_new], ignore_index=True)
                    logger.info(f"✅ 已添加新账号 {account_name} 的积分数据")
            else:
                # 如果没有账号名称列，直接追加
                df_points = pd.concat([df_existing_points, df_new], ignore_index=True)
                logger.info(f"✅ 已追加积分数据（无账号名称列）")
        else:
            # 创建新的积分数据
            df_points = df_new
            logger.info(f"✅ 已创建新账号 {account_name} 的积分数据")
        
        # 按账号名称排序
        if '账号名称' in df_points.columns:
            df_points = df_points.sort_values('账号名称', ascending=True)
        
        # 写入所有 sheet
        with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
            # 写入积分数据（包含所有账号）
            df_points.to_excel(writer, sheet_name=sheet_name, index=False)
            # 写入其他保留的 sheet（包括各账号的 orders）
            for existing_sheet_name, df_sheet in existing_sheets.items():
                df_sheet.to_excel(writer, sheet_name=existing_sheet_name, index=False)
        
        logger.info(f"✅ 已更新积分数据到 Excel: {excel_path} (sheet: {sheet_name})")
        logger.info(f"   当前共有 {len(df_points)} 个账号的积分数据")
        
    except Exception as e:
        logger.error(f"❌ 写入积分数据到 Excel 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def get_stats(logger: logging.Logger, args: argparse.Namespace):
    """
    获取 StandX 账号积分和交易数据
    
    1. 读取匹配正则表达式的 YAML 配置文件
    2. 获取订单历史
    3. 获取账号积分
    """
    logger.info("🎯 StandX 积分和订单查询")
    
    # === 步骤1: 读取配置文件 ===
    config_dir = args.tmux_config_dir if hasattr(args, 'tmux_config_dir') else '~/.tmuxp/standx'
    file_pattern = args.file_pattern if hasattr(args, 'file_pattern') else r'^standx-.*\.yaml$'
    excel_path = args.excel_path if hasattr(args, 'excel_path') else 'data/standx_stats.xlsx'
    
    # 展开用户目录路径
    if config_dir.startswith('~'):
        config_dir = os.path.expanduser(config_dir)
    
    logger.info(f"📁 读取配置文件目录: {config_dir}")
    logger.info(f"🔍 文件名匹配模式: {file_pattern}")
    logger.info(f"📊 Excel 文件路径: {excel_path}")
    
    configs = load_yaml_files_from_directory(config_dir, file_pattern, logger)
    
    if configs:
        logger.info(f"✅ 成功加载 {len(configs)} 个配置文件:")
        for config in configs:
            logger.info(f"   - {config['file_name']}")
    else:
        logger.warning(f"⚠️  未找到匹配模式 '{file_pattern}' 的配置文件")
        return

    for config in configs:
        logger.info(f"\n处理配置文件: {config['file_name']}")
        wallet_private_key = config['content'].get('environment', {}).get('STANDX_WALLET_PRIVATE_KEY')
        server_proxy = config['content'].get('environment', {}).get('server_proxy')
        if not wallet_private_key:
            logger.warning(f"⚠️  配置文件 {config['file_name']} 中未找到 STANDX_WALLET_PRIVATE_KEY，跳过")
            continue
        
        # 从配置文件名提取账号名称（去掉扩展名和前缀）
        account_name = config['file_name']
        # 去掉 .yaml 或 .yml 扩展名
        if account_name.endswith('.yaml'):
            account_name = account_name[:-5]
        elif account_name.endswith('.yml'):
            account_name = account_name[:-4]
        # 如果为空，使用文件名
        if not account_name:
            account_name = config['file_name']
        
        logger.info(f"📝 账号标识: {account_name}")
            
        # === 步骤2: 初始化交易所适配器 ===
        adapter: ExchangeAdapter | None = await init_exchange(logger, wallet_private_key=wallet_private_key, proxy=server_proxy)

        # 验证认证状态
        if not adapter or not adapter.is_authenticated():
            logger.error("❌ 认证状态验证失败，无法继续执行")
            continue

        try:
            # === 步骤3: 获取订单历史 ===
            logger.info("\n📊 获取订单历史...")
            orders = await adapter.get_order_history()
            logger.info(f"✅ 获取到 {len(orders)} 条订单记录")

            # 统计订单信息
            if orders:
                filled_orders = [o for o in orders if o.status.value in ['filled', 'partially_filled']]
                logger.info(f"   已成交订单: {len(filled_orders)} 条")

                # 计算总交易量
                total_volume = sum(float(o.filled) for o in filled_orders if o.filled)
                logger.info(f"   总交易量: {total_volume}")
                
                # === 写入订单数据到 Excel ===
                logger.info("\n💾 写入订单数据到 Excel...")
                write_orders_to_excel(orders, excel_path, account_name, logger)
            else:
                logger.warning("   没有订单数据")

            # === 步骤4: 获取账号积分 ===
            logger.info("\n🎁 获取账号积分...")
            points_data = None
            try:
                # 通过适配器获取积分
                points_data = await adapter.get_points()
                logger.info("✅ 积分数据获取成功")
                if points_data:
                    logger.info(f"   积分数据: {points_data}")
                else:
                    logger.warning("   未获取到积分数据")
            except Exception as e:
                logger.error(f"❌ 获取积分失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # === 写入积分数据到 Excel ===
            if points_data:
                logger.info("\n💾 写入积分数据到 Excel...")
                write_points_to_excel(points_data, excel_path, account_name, logger)

            logger.info("\n✅ 数据统计完成")

        except Exception as e:
            logger.error(f"❌ 执行出错: {e}")
            import traceback
            logger.error(traceback.format_exc())

        finally:
            logger.info("🔧 断开连接...")
            try:
                if adapter:
                    await adapter.disconnect()
                    logger.info("✅ 已断开")
            except Exception as e:
                logger.error(f"⚠️  断开连接时出错: {e}")


async def init_exchange(logger: logging.Logger, wallet_private_key=None, proxy=None):
    # 加载 StandX 配置
    try:
        import yaml

        config_file = Path("config/exchanges/standx_config.yaml")
        if not config_file.exists():
            logger.error(f"⚠️  配置文件不存在: {config_file}，将使用环境变量或默认配置")
            exchange_conf = {}
            auth_conf = {}
            api_conf = {}
        else:
            with open(config_file, 'r', encoding='utf-8') as f:
                exchange_data = yaml.safe_load(f)

            exchange_conf = exchange_data.get('standx', {})
            auth_conf = exchange_conf.get('authentication', {})
            api_conf = exchange_conf.get('api', {})

        # 如果环境变量没有，则从配置文件读取
        api_key = os.getenv(f"{'standx'.upper()}_API_KEY")
        api_secret = os.getenv(f"{'standx'.upper()}_API_SECRET")
        # wallet_address = os.getenv(f"{'standx'.upper()}_WALLET_ADDRESS")
        if not wallet_private_key:
            wallet_private_key = os.getenv(f"{'standx'.upper()}_WALLET_PRIVATE_KEY")
        api_key = api_key or auth_conf.get('api_key', '')
        api_secret = api_secret or auth_conf.get('api_secret', "")
        # wallet_address = wallet_address or auth_conf.get('wallet_address', "")
        wallet_private_key = wallet_private_key or auth_conf.get('wallet_private_key', "")
        if not wallet_private_key:
            raise ValueError("❌ 缺少钱包地址或钱包私钥")
        else:
            wallet_address = Account.from_key(wallet_private_key).address

        # 创建交易所配置
        exchange_config = ExchangeConfig(
            exchange_id='standx',
            name='StandX',
            exchange_type=ExchangeType.PERPETUAL,
            api_key=api_key,
            api_secret=api_secret,
            wallet_address=wallet_address,
            wallet_private_key=wallet_private_key,
            base_url=api_conf.get('base_url', 'https://perps.standx.com'),
            ws_url=api_conf.get('ws_url', 'wss://perps.standx.com/ws-stream/v1'),
            proxy=proxy,
            default_leverage=exchange_conf.get('trading', {}).get('default_leverage', 10),
            default_margin_mode=exchange_conf.get('trading', {}).get('margin_mode', 'cross')
        )
    except Exception as e:
        logger.error(f"❌ 加载配置失败: {e}")
        return

    # 创建 StandX 适配器
    logger.info("🔧 创建 StandX 适配器...")
    factory = get_exchange_factory()
    adapter = factory.create_adapter(exchange_id='standx', config=exchange_config)

    # 连接交易所
    logger.info("🔧 连接 StandX...")
    try:
        await adapter.connect()
        logger.info("✅ 连接成功")
    except Exception as e:
        logger.error(f"❌ 连接失败: {e}")
        return

    # 认证（必须成功才能继续）
    logger.info("🔧 进行认证...")
    try:
        authenticated = await adapter.authenticate()
        if authenticated:
            logger.info("✅ 认证成功")
        else:
            logger.error("""
            ❌ 认证失败
            ⚠️  认证失败，无法继续执行。请检查：
               1. 钱包地址是否正确
               2. 钱包私钥是否正确
               3. 网络连接是否正常
               """)
            return
    except Exception as e:
        logger.error(f"❌ 认证过程出错: {e}")
        import traceback
        traceback.print_exc()
        return
    return adapter


if __name__ == "__main__":
    parser_args = parse_args()
    _logger = init_logging(f"standx-{os.getenv('ACCOUNT_NAME', 'unknown')}")
    try:
        asyncio.run(parser_args.func(_logger, parser_args))
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序异常退出: {e}")
        import traceback
        traceback.print_exc()
