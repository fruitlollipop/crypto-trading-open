#!/usr/bin/env python3
"""
StandX 交易周期示例

演示如何使用 execute_trading_cycle 方法执行完整的交易周期
"""

import asyncio
import sys
import os
import argparse
from pathlib import Path
from decimal import Decimal
from typing import Optional, Tuple

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.adapters.exchanges.interface import ExchangeConfig, ExchangeType
from core.adapters.exchanges.factory import get_exchange_factory
from core.adapters.exchanges.models import OrderType


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


async def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()
    
    print("=" * 60)
    print("🎯 StandX 交易周期示例")
    print("=" * 60)
    print()
    
    # 解析交易数量
    try:
        quantity = Decimal(str(args.quantity))
    except Exception as e:
        print(f"❌ 无效的交易数量: {args.quantity}, 错误: {e}")
        return
    
    # 解析订单类型
    order_type = OrderType.MARKET if args.order_type == 'market' else OrderType.LIMIT
    
    # 解析价格
    buy_price = None
    if args.buy_price:
        try:
            buy_price = Decimal(str(args.buy_price))
        except Exception as e:
            print(f"❌ 无效的买入价格: {args.buy_price}, 错误: {e}")
            return
    
    price_spread = None
    if args.price_spread:
        try:
            price_spread = Decimal(str(args.price_spread))
        except Exception as e:
            print(f"❌ 无效的价格价差: {args.price_spread}, 错误: {e}")
            return

    # 加载 StandX 配置
    try:
        import yaml
        
        config_file = Path("config/exchanges/standx_config.yaml")
        if not config_file.exists():
            print(f"⚠️  配置文件不存在: {config_file}，将使用环境变量或默认配置")
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
        wallet_address = os.getenv(f"{'standx'.upper()}_WALLET_ADDRESS")
        wallet_private_key = os.getenv(f"{'standx'.upper()}_WALLET_PRIVATE_KEY")
        api_key = api_key or auth_conf.get('api_key', '')
        api_secret = api_secret or auth_conf.get('api_secret', "")
        wallet_address = wallet_address or auth_conf.get('wallet_address', "")
        wallet_private_key = wallet_private_key or auth_conf.get('wallet_private_key', "")
        if not wallet_address or not wallet_private_key:
            raise ValueError("❌ 缺少钱包地址或钱包私钥")
        
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
            default_leverage=exchange_conf.get('trading', {}).get('default_leverage', 10),
            default_margin_mode=exchange_conf.get('trading', {}).get('margin_mode', 'cross')
        )
        
    except Exception as e:
        print(f"❌ 加载配置失败: {e}")
        return
    
    # 创建 StandX 适配器
    print("🔧 创建 StandX 适配器...")
    factory = get_exchange_factory()
    adapter = factory.create_adapter(exchange_id='standx', config=exchange_config)
    
    # 连接交易所
    print("🔧 连接 StandX...")
    try:
        await adapter.connect()
        print("✅ 连接成功")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return
    
    # 认证（必须成功才能继续）
    print("🔧 进行认证...")
    authenticated = False
    try:
        authenticated = await adapter.authenticate()
        if authenticated:
            print("✅ 认证成功")
        else:
            print("❌ 认证失败")
            print("⚠️  认证失败，无法继续执行。请检查：")
            print("   1. 钱包地址是否正确")
            print("   2. 钱包私钥是否正确")
            print("   3. 网络连接是否正常")
            return
    except Exception as e:
        print(f"❌ 认证过程出错: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 验证认证状态
    if not adapter.is_authenticated():
        print("❌ 认证状态验证失败，无法继续执行")
        return
    
    try:
        symbol = args.symbol
        
        print("\n" + "=" * 60)
        print(f"📊 交易周期执行")
        print("=" * 60)
        print(f"交易对: {symbol}")
        print(f"交易数量: {quantity}")
        print(f"订单类型: {args.order_type}")
        if buy_price:
            print(f"买入价格: {buy_price}")
        if price_spread:
            print(f"价格价差: {price_spread}")
        print()
        
        # # === 步骤1: 检查账户余额（认证成功后） ===
        # print("🔍 检查账户余额...")
        #
        # # 再次确认认证状态
        # if not adapter.is_authenticated():
        #     print("❌ 认证状态已失效，无法查询余额")
        #     return
        #
        # balance_ok, balance_error, available_balance = await check_balance(
        #     adapter, symbol, quantity, order_type, buy_price
        # )
        #
        # if not balance_ok:
        #     print(f"❌ {balance_error}")
        #     print(f"   可用余额: {available_balance}")
        #     print("\n⚠️  余额不足，无法执行交易。请充值后重试。")
        #     return
        #
        # print()
        
        # === 步骤2: 执行交易周期 ===
        print("🚀 开始执行交易周期...")
        result = await adapter.execute_trading_cycle(
            symbol=symbol,
            quantity=quantity,
            buy_price=buy_price,
            price_spread=price_spread,
            order_type=order_type,
            wait_for_fill=(order_type == OrderType.LIMIT),
            max_wait_seconds=30
        )
        
        print()
        if result['success']:
            print("=" * 60)
            print("✅ 交易周期完成")
            print("=" * 60)
            print(f"Buy订单ID: {result['buy_order'].id}")
            print(f"Buy订单状态: {result['buy_order'].status.value}")
            if result['buy_order'].average:
                print(f"Buy平均成交价: {result['buy_order'].average}")
            print(f"Buy成交数量: {result['buy_order'].filled}")
            print()
            print(f"Sell订单ID: {result['sell_order'].id}")
            print(f"Sell订单状态: {result['sell_order'].status.value}")
            if result['sell_order'].average:
                print(f"Sell平均成交价: {result['sell_order'].average}")
            print(f"Sell成交数量: {result['sell_order'].filled}")
        else:
            print("=" * 60)
            print("❌ 交易周期失败")
            print("=" * 60)
            print(f"错误信息: {result.get('error')}")
            if result.get('buy_order'):
                print(f"Buy订单ID: {result['buy_order'].id}")
                print(f"Buy订单状态: {result['buy_order'].status.value}")
            if result.get('sell_order'):
                print(f"Sell订单ID: {result['sell_order'].id}")
                print(f"Sell订单状态: {result['sell_order'].status.value}")
        
        # 等待一段时间
        # await asyncio.sleep(2)
        
        # # 示例2: 限价单交易周期（带价差）
        # print("\n" + "=" * 60)
        # print("📊 示例2: 限价单交易周期（带价差）")
        # print("=" * 60)
        
        # # 获取当前价格
        # ticker = await adapter.get_ticker(symbol)
        # current_price = ticker.last
        
        # # 设置买入价格（略低于当前价）
        # buy_price = current_price * Decimal("0.999")  # 低0.1%
        # # 设置价差（卖出价格 = 买入价格 + 价差）
        # price_spread = current_price * Decimal("0.001")  # 0.1% 价差
        
        # print(f"   当前价格: {current_price}")
        # print(f"   买入价格: {buy_price}")
        # print(f"   预期卖出价格: {buy_price + price_spread}")
        
        # result = await adapter.execute_trading_cycle(
        #     symbol=symbol,
        #     quantity=quantity,
        #     buy_price=buy_price,
        #     price_spread=price_spread,
        #     order_type=OrderType.LIMIT,
        #     wait_for_fill=True,
        #     max_wait_seconds=30
        # )
        
        # if result['success']:
        #     print(f"✅ 交易周期完成")
        #     print(f"   Buy订单ID: {result['buy_order'].id}, 状态: {result['buy_order'].status.value}")
        #     print(f"   Sell订单ID: {result['sell_order'].id}, 状态: {result['sell_order'].status.value}")
        # else:
        #     print(f"❌ 交易周期失败: {result.get('error')}")
        
    except Exception as e:
        print(f"❌ 执行出错: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 断开连接
        print("\n🔧 断开连接...")
        try:
            await adapter.disconnect()
            print("✅ 已断开")
        except Exception as e:
            print(f"⚠️  断开连接时出错: {e}")
    
    print("\n" + "=" * 60)
    print("👋 示例完成")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序异常退出: {e}")
        import traceback
        traceback.print_exc()

