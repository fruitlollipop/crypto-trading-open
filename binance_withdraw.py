"""
@ Author:   Mr.Hat
@ Date:     2024/4/7 19:45
@ Description: Binance 提现脚本
@ History:  增强：参数化、重试机制、环境变量读取密钥、dry-run、安全校验、中文注释
"""

import os
import time
import random
import argparse
from typing import List, Optional

import ccxt
from loguru import logger


def load_wallets(file_path: str) -> List[str]:
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"找不到地址文件: {file_path}")
    with open(file_path, "r") as f:
        wallets = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]
    if not wallets:
        raise ValueError("地址文件为空或仅包含注释")
    return wallets


def withdraw_with_retry(
    client: ccxt.binance,
    address: str,
    amount: float,
    symbol: str,
    network: str,
    max_retries: int = 3,
    backoff_s: int = 3,
) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            client.proxies = {
                "http": "http://127.0.0.1:7890",
                "https": "http://127.0.0.1:7890",
            }
            client.withdraw(
                code=symbol,
                amount=amount,
                address=address,
                tag=None,
                params={"network": network},
            )
            logger.success(f">>> 提现成功 | {symbol} {amount} | {network} | {address}")
            return True
        except ccxt.BaseError as e:
            logger.warning(f"第{attempt}次尝试失败：{e}")
        except Exception as e:
            logger.error(f"未知错误（第{attempt}次）：{e}")
        if attempt < max_retries:
            sleep_s = backoff_s * attempt
            logger.info(f"{sleep_s}s 后重试...")
            time.sleep(sleep_s)
    return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Binance 提现批处理脚本")
    p.add_argument(
        "--wallets",
        dest="wallets_file",
        default=None,
        help="地址文件，默认 ./data/wallets.txt",
    )
    p.add_argument("--symbol", default="BNB", help="币种，如 BNB/ETH")
    p.add_argument(
        "--network", default="BSC", help="网络，如 ETH/BSC/ARBITRUM/OPTIMISM ..."
    )
    p.add_argument(
        "--amount-from", dest="amount_from", type=float, default=0.011, help="最小金额"
    )
    p.add_argument(
        "--amount-to", dest="amount_to", type=float, default=0.015, help="最大金额"
    )
    p.add_argument(
        "--delay-from", dest="delay_from", type=int, default=10, help="最小延时秒"
    )
    p.add_argument(
        "--delay-to", dest="delay_to", type=int, default=30, help="最大延时秒"
    )
    p.add_argument(
        "--api-key", dest="api_key", default=None, help="API Key（或用环境变量）"
    )
    p.add_argument(
        "--api-secret",
        dest="api_secret",
        default=None,
        help="API Secret（或用环境变量）",
    )
    p.add_argument("--dry-run", action="store_true", help="仅打印计划，不实际提现")
    return p.parse_args()


def main(
    wallets_file: Optional[str],
    symbol: str,
    network: str,
    amount_from: float,
    amount_to: float,
    delay_from: int,
    delay_to: int,
    api_key: Optional[str],
    api_secret: Optional[str],
    dry_run: bool,
):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    wallets_file = wallets_file or os.path.join(
        current_directory, "data", "wallets.txt"
    )
    logger.info(f"钱包地址文件: {wallets_file}")
    wallets = load_wallets(wallets_file)

    api_key = (api_key or os.getenv("BINANCE_API_KEY") or "").strip()
    api_secret = (api_secret or os.getenv("BINANCE_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise ValueError("未提供 Binance API_KEY/SECRET（参数或环境变量）")

    if amount_from <= 0 or amount_to <= 0 or amount_from > amount_to:
        raise ValueError("金额区间不合法：确保 0 < amount_from <= amount_to")
    if delay_from < 0 or delay_to < 0 or delay_from > delay_to:
        raise ValueError("延时区间不合法：确保 0 <= delay_from <= delay_to")

    client = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        }
    )

    logger.info("...............开始转账...............")
    total = len(wallets)
    for i, addr in enumerate(wallets, 1):
        if not isinstance(addr, str) or len(addr) < 10:
            logger.error(f"地址不合法，已跳过：{addr}")
            continue

        amount = round(random.uniform(amount_from, amount_to), 2)
        if dry_run:
            logger.info(
                f"[DRY-RUN] {i}/{total} | 计划提现 {symbol} {amount} | {network} -> {addr}"
            )
        else:
            if not withdraw_with_retry(client, addr, amount, symbol, network):
                logger.error(f"提现最终失败：{addr}")

        if delay_to > 0:
            sleep_s = random.randint(delay_from, delay_to)
            logger.info(f"等待 {sleep_s}s 后处理下一个地址...")
            time.sleep(sleep_s)


if __name__ == "__main__":
    args = parse_args()
    main(
        args.wallets_file,
        args.symbol,
        args.network,
        args.amount_from,
        args.amount_to,
        args.delay_from,
        args.delay_to,
        args.api_key,
        args.api_secret,
        args.dry_run,
    )
