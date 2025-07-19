# --- START OF FILE trader.py (CORRECTED) ---

import asyncio
import logging
import time
from typing import Optional, Any, Dict
import os

# Import client libraries and types
from kalshi.clients import KalshiHttpClient
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

# Configure logging
logger = logging.getLogger(__name__)

# --- Base Trade Execution Functions ---

async def execute_polymarket_trade(
    client: ClobClient, market_id: str, price: float, size: int, side: str, order_type: OrderType, proxies: dict
) -> Optional[Dict[str, Any]]:
    """
    Executes a trade on Polymarket with a specified order type.

    Args:
        client: The initialized ClobClient.
        market_id: The token ID of the Polymarket market.
        price: The limit price for the order.
        size: The number of contracts to trade.
        side: BUY or SELL (string constant).
        order_type: The type of order (Enum: FOK, FAK).
        proxies: The proxy configuration to use for the request.

    Returns:
        The API response dictionary on success, None on failure.
    """
    try:
        os.environ['HTTP_PROXY'] = proxies['http']
        os.environ['HTTPS_PROXY'] = proxies['https']
    
        order_args = OrderArgs(side=side, token_id=market_id, price=price, size=float(size))
        logger.info(f"Creating Polymarket order: {side} {size} of {market_id} @ {price}")
        signed_order = client.create_order(order_args)
        
        logger.info(f"Posting Polymarket order")
        response = client.post_order(signed_order, order_type)
        logger.info(f"Polymarket Response: {response}")

        if response and response.get("success"):
            return response
        else:
            logger.error(f"Polymarket order failed: {response.get('errorMsg', 'No error message')}")
            return None
            
    except Exception as e:
        logger.error(f"An error occurred during the Polymarket transaction: {e}", exc_info=True)
        return None
    finally:
        if 'HTTP_PROXY' in os.environ:
            del os.environ['HTTP_PROXY']
        if 'HTTPS_PROXY' in os.environ:
            del os.environ['HTTPS_PROXY']


async def execute_kalshi_trade(
    client: KalshiHttpClient, ticker: str, action: str, price: float, count: int, is_fok: bool, is_yes: bool
) -> Optional[Dict[str, Any]]:
    """
    Executes a limit order on Kalshi. For complimentary arbitrage, both 'yes' and 'no'
    markets are bought, so is_yes determines which side of the contract is being purchased.

    Args:
        client: The initialized KalshiHttpClient.
        ticker: The market ticker.
        action: 'buy' or 'sell'.
        price: The limit price for the order (e.g., 0.50 for 50 cents).
        count: The number of contracts.
        is_fok: True for a Fill-Or-Kill order, False for a standard limit order.
        is_yes: True to place the order on the 'yes' side, False for the 'no' side.

    Returns:
        The API response dictionary on success, None on failure.
    """
    try:
        limit_price_cents = int(price * 100)
        
        body = {
            "action": action,
            "client_order_id": f"comp-arb-{int(time.time())}",
            "count": count,
            "ticker": ticker,
            "type": 'limit',
        }
        
        # In Kalshi's API, you specify 'yes' or 'no' by which price field you set.
        if is_yes:
            body["side"] = 'yes'
            body["yes_price"] = limit_price_cents
        else:
            body["side"] = 'no'
            body["no_price"] = limit_price_cents

        if is_fok:
            body["time_in_force"] = "fill_or_kill"
        else:
            # For reversal (sell) orders, we can use a standard limit order that expires quickly
            # to avoid it sitting on the books if not filled immediately.
            body["expiration_ts"] = int(time.time() + 120) # 2 minute expiration

        logger.info(f"Posting Kalshi order: {body}")
        path = "/trade-api/v2/portfolio/orders"
        response = client.post(path=path, body=body)
        logger.info(f"Kalshi Response: {response}")

        # A successful order submission returns a response with an 'order' key.
        if response and "order" in response:
            return response
        else:
            logger.error(f"Kalshi order failed: {response}")
            return None

    except Exception as e:
        logger.error(f"An error occurred during the Kalshi transaction: {e}", exc_info=True)
        return None

# --- New Complimentary Arbitrage Execution Logic ---

async def execute_complimentary_buy_trade(
    poly_client: Optional[ClobClient], kalshi_client: Optional[KalshiHttpClient],
    canonical_name: str,
    book1_platform: str, book1_market_id: str, book1_price: float,
    book2_platform: str, book2_market_id: str, book2_price: float,
    trade_size: int, proxies: dict
):
    """
    Coordinates the two BUY legs of the complimentary arbitrage trade and handles failures.
    """
    logger.info(
        f"--- ATTEMPTING COMPLIMENTARY ARBITRAGE for {canonical_name} ---\n"
        f"  - BUY {trade_size} on {book1_platform} @ {book1_price:.4f}\n"
        f"  - BUY {trade_size} on {book2_platform} @ {book2_price:.4f}"
    )

    tasks = {}

    # Define the tasks for the two buy orders
    platforms = [book1_platform, book2_platform]
    market_ids = [book1_market_id, book2_market_id]
    prices = [book1_price, book2_price]
    
    for i, platform in enumerate(platforms):
        if platform == "Polymarket":
            tasks[i] = execute_polymarket_trade(poly_client, market_ids[i], prices[i], trade_size, BUY, OrderType.FOK, proxies)
        elif platform == "Kalshi":
            # For complimentary markets, we are always buying a "yes" contract on one outcome and a "yes" on the other.
            # If your market mapping involves 'no' markets on Kalshi, you would need to adjust the is_yes flag based on that.
            tasks[i] = execute_kalshi_trade(kalshi_client, market_ids[i], 'buy', prices[i], trade_size, is_fok=True, is_yes=True)

    # Execute both buy orders concurrently
    results = await asyncio.gather(*tasks.values())
    result1 = results[0]
    result2 = results[1]

    def is_trade_successful(platform, result):
        if not result:
            return False
        if platform == "Polymarket":
            return result.get('success') is True
        elif platform == "Kalshi":
            # Successful Kalshi order has an 'order' object and status is not 'CANCELED' or 'FAILED'
            return "order" in result and result['order'].get('status') not in ['CANCELED', 'FAILED']
        return False

    success1 = is_trade_successful(book1_platform, result1)
    success2 = is_trade_successful(book2_platform, result2)

    # --- Reversal Logic ---
    # Important: If one trade succeeds and the other fails, we must reverse the successful trade to avoid exposure.
    if success1 and not success2:
        logger.warning(f"[{canonical_name}] Buy on {book1_platform} SUCCEEDED but buy on {book2_platform} FAILED. Reversing first leg.")
        if book1_platform == "Polymarket":
            # Sell the contracts we just bought. Using a non-FOK order to increase chance of fill.
            await execute_polymarket_trade(poly_client, book1_market_id, book1_price, trade_size, SELL, OrderType.FAK, proxies)
        else: # Kalshi
            await execute_kalshi_trade(kalshi_client, book1_market_id, 'sell', book1_price, trade_size, is_fok=False, is_yes=True)

    elif not success1 and success2:
        logger.warning(f"[{canonical_name}] Buy on {book2_platform} SUCCEEDED but buy on {book1_platform} FAILED. Reversing second leg.")
        if book2_platform == "Polymarket":
            await execute_polymarket_trade(poly_client, book2_market_id, book2_price, trade_size, SELL, OrderType.FAK, proxies)
        else: # Kalshi
            await execute_kalshi_trade(kalshi_client, book2_market_id, 'sell', book2_price, trade_size, is_fok=False, is_yes=True)
            
    elif success1 and success2:
        logger.info(f"[{canonical_name}] Complimentary arbitrage trade successfully executed on both legs.")
    
    else:
        logger.error(f"[{canonical_name}] Both legs of the complimentary arbitrage trade failed.")