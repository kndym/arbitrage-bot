import asyncio
import logging
import pprint
from typing import Dict, Any, Optional, Tuple
import time
from datetime import datetime, timezone
import json
# Import your classes and functions
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI
from kalshi.wss import KalshiWSS, env, KEYID, private_key
from order_book import OrderBook
from polymarket_updates import update_polymarket_order_book
from kalshi_updates import update_kalshi_order_book

# --- Configuration for Market Mapping ---
MARKET_MAPPING: Dict[str, Dict[str, str]] = {

    "San Francisco vs Arizona":{ # Canonical name for the event
        "polymarket": "12649485682490623018879562550119924159605905047320602342489836819152443033042",
        "kalshi": "KXMLBGAME-25JUL03SFARI-ARI" # Example Kalshi ticker
    },

    "Chicago WS vs Los Angeles D":{ # Canonical name for the event
        "polymarket": "95017555500384179370741091753782700532812486436763739462972733407190887564342",
        "kalshi": "KXMLBGAME-25JUL03CWSLAD-CWS" # Example Kalshi ticker
    }, 
    "Kansas City vs Seattle":{ # Canonical name for the event
        "polymarket": "82997507803868156196594079733549911693678727367768394503054297414739649154218",
        "kalshi": "KXMLBGAME-25JUL03KCSEA-KC" # Example Kalshi ticker
    }
}

RUN_DURATION_MINUTES = 5
OUTPUT_FILE_NAME = "order_book_snapshot.txt"
JSON_OUTPUT_FILE_NAME = "order_book_updates.json" 
PRINT_INTERVAL_SECONDS = 10 # New constant for print interval

# --- Global Storage for Order Books and Comparison Data ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {}
MARKET_COMPARISON_DATA: Dict[str, Dict[str, Any]] = {}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def initialize_market_data():
    """Initializes all OrderBook instances and comparison data structures."""
    for canonical_name, market_ids in MARKET_MAPPING.items():
        logger.info(f"Initializing for canonical market: {canonical_name}")
        
        if "polymarket" in market_ids:
            poly_id = market_ids["polymarket"]
            ALL_ORDER_BOOKS[poly_id] = OrderBook(poly_id)
            REVERSE_MARKET_LOOKUP[poly_id] = canonical_name
            logger.info(f"  Added Polymarket book for {poly_id}")

        if "kalshi" in market_ids:
            kalshi_id = market_ids["kalshi"]
            ALL_ORDER_BOOKS[kalshi_id] = OrderBook(kalshi_id)
            REVERSE_MARKET_LOOKUP[kalshi_id] = canonical_name
            logger.info(f"  Added Kalshi book for {kalshi_id}")

        MARKET_COMPARISON_DATA[canonical_name] = {
            'cheapest_buy_yes': {'platform': None, 'price': float('inf')},
            'highest_sell_yes': {'platform': None, 'price': 0.0}
        }
    logger.info("All market data structures initialized.")


def get_paired_books(canonical_name: str) -> Tuple[Optional[OrderBook], Optional[OrderBook]]:
    """
    Retrieves the Polymarket and Kalshi OrderBook instances for a given canonical market name.
    """
    poly_book = None
    kalshi_book = None
    market_ids = MARKET_MAPPING.get(canonical_name)
    if market_ids:
        if "polymarket" in market_ids:
            poly_book = ALL_ORDER_BOOKS.get(market_ids["polymarket"])
        if "kalshi" in market_ids:
            kalshi_book = ALL_ORDER_BOOKS.get(market_ids["kalshi"])
    return poly_book, kalshi_book

def perform_cross_market_comparison(canonical_name: str):
    """
    Compares prices for a given canonical market across Polymarket and Kalshi
    and updates MARKET_COMPARISON_DATA.
    Also calculates and prints liquidity for arbitrage opportunities.
    """
    poly_book, kalshi_book = get_paired_books(canonical_name)

    current_comparison = MARKET_COMPARISON_DATA[canonical_name]

    cheapest_buy_price = float('inf')
    cheapest_buy_platform = None
    cheapest_buy_book: Optional[OrderBook] = None # New: store the actual book
    
    highest_sell_price = 0.0
    highest_sell_platform = None
    highest_sell_book: Optional[OrderBook] = None # New: store the actual book

    # Compare for cheapest buy (lowest ask)
    if poly_book and poly_book.lowest_ask is not None:
        if poly_book.lowest_ask < cheapest_buy_price:
            cheapest_buy_price = poly_book.lowest_ask
            cheapest_buy_platform = "Polymarket"
            cheapest_buy_book = poly_book
        elif poly_book.lowest_ask == cheapest_buy_price and cheapest_buy_platform is None:
            cheapest_buy_platform = "Polymarket"
            cheapest_buy_book = poly_book

    if kalshi_book and kalshi_book.lowest_ask is not None:
        if kalshi_book.lowest_ask < cheapest_buy_price:
            cheapest_buy_price = kalshi_book.lowest_ask
            cheapest_buy_platform = "Kalshi"
            cheapest_buy_book = kalshi_book
        elif kalshi_book.lowest_ask == cheapest_buy_price and cheapest_buy_platform == "Polymarket":
            pass # Keep Polymarket if already set and price is equal
        elif kalshi_book.lowest_ask == cheapest_buy_price and cheapest_buy_platform is None:
            cheapest_buy_platform = "Kalshi"
            cheapest_buy_book = kalshi_book

    # Compare for highest sell (highest bid)
    if poly_book and poly_book.highest_bid is not None:
        if poly_book.highest_bid > highest_sell_price:
            highest_sell_price = poly_book.highest_bid
            highest_sell_platform = "Polymarket"
            highest_sell_book = poly_book
        elif poly_book.highest_bid == highest_sell_price and highest_sell_platform is None:
            highest_sell_platform = "Polymarket"
            highest_sell_book = poly_book
    
    if kalshi_book and kalshi_book.highest_bid is not None:
        if kalshi_book.highest_bid > highest_sell_price:
            highest_sell_price = kalshi_book.highest_bid
            highest_sell_platform = "Kalshi"
            highest_sell_book = kalshi_book
        elif kalshi_book.highest_bid == highest_sell_price and highest_sell_platform == "Polymarket":
            pass # Keep Polymarket if already set and price is equal
        elif kalshi_book.highest_bid == highest_sell_price and highest_sell_platform is None:
            highest_sell_platform = "Kalshi"
            highest_sell_book = kalshi_book

    # Update global comparison data
    if cheapest_buy_price != float('inf'):
        current_comparison['cheapest_buy_yes'] = {'platform': cheapest_buy_platform, 'price': cheapest_buy_price}
    else:
        current_comparison['cheapest_buy_yes'] = {'platform': None, 'price': float('inf')}
    
    if highest_sell_price != 0.0:
        current_comparison['highest_sell_yes'] = {'platform': highest_sell_platform, 'price': highest_sell_price}
    else:
        current_comparison['highest_sell_yes'] = {'platform': None, 'price': 0.0}

    # Optionally print current arbitrage opportunities with liquidity
    if cheapest_buy_book and highest_sell_book and \
       cheapest_buy_platform != highest_sell_platform: # Must be different platforms for arb

        buy_price = current_comparison['cheapest_buy_yes']['price']
        sell_price = current_comparison['highest_sell_yes']['price']

        # Only consider if there's a profitable spread greater than 0.01 (1 cent)
        if sell_price > buy_price + 0.01:
            profit_per_share = sell_price - buy_price

            # --- Calculate Arbitrage Liquidity ---
            buy_liquidity = 0.0
            # Iterate through asks on the 'cheapest_buy_book' until price exceeds 'sell_price'
            for price, size in cheapest_buy_book.asks:
                if price <= sell_price: # We can buy at this price or cheaper
                    buy_liquidity += size
                else:
                    break # Prices are sorted, no more opportunities at better prices
            
            sell_liquidity = 0.0
            # Iterate through bids on the 'highest_sell_book' until price falls below 'buy_price'
            for price, size in highest_sell_book.bids:
                if price >= buy_price: # We can sell at this price or higher
                    sell_liquidity += size
                else:
                    break # Prices are sorted, no more opportunities at better prices
            
            arbitrage_liquidity = min(buy_liquidity, sell_liquidity)

            logger.info(f"Arbitrage Opportunity for {canonical_name}:")
            logger.info(f"  Buy Yes on {cheapest_buy_platform} at {buy_price:.4f}")
            logger.info(f"  Sell Yes on {highest_sell_platform} at {sell_price:.4f}")
            logger.info(f"  Potential Profit per Share: {profit_per_share:.4f}")
            logger.info(f"  Arbitrage Liquidity: {arbitrage_liquidity:.2f} shares (at current prices)")

async def process_websocket_message(source: str, message: Dict[str, Any]):
    """
    Processes a message from the WebSocket queue, updates the relevant order book,
    performs cross-market comparison if applicable, and logs the state to JSON.
    """
    market_id = None
    canonical_market_name = None

    if source == 'polymarket':
        market_id = message.get("asset_id")
        if market_id:
            canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
            if not canonical_market_name:
                logger.warning(f"Polymarket message for unmapped market_id: {market_id}")
                return
            order_book = ALL_ORDER_BOOKS.get(market_id)
            if order_book:
                update_polymarket_order_book(order_book, message)
                logger.debug(f"Polymarket book for {market_id} updated.")
            else:
                logger.error(f"OrderBook instance not found for Polymarket market_id: {market_id}")
                return

    elif source == 'kalshi':
        msg_content = message.get("msg", {})
        market_id = msg_content.get("market_ticker")
        if market_id:
            canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
            if not canonical_market_name:
                logger.warning(f"Kalshi message for unmapped market_id: {market_id}")
                return
            order_book = ALL_ORDER_BOOKS.get(market_id)
            if order_book:
                update_kalshi_order_book(order_book, message)
                logger.debug(f"Kalshi book for {market_id} updated.")
            else:
                logger.error(f"OrderBook instance not found for Kalshi market_id: {market_id}")
                return
    else:
        logger.warning(f"Unknown message source: {source}")
        return
    if canonical_market_name:
        logger.debug(f"Performing cross-market comparison for {canonical_market_name}")
        perform_cross_market_comparison(canonical_market_name)
        await log_order_book_state_to_json(canonical_market_name)


async def log_order_book_state_to_json(canonical_name: str):
    """
    Logs the current state of a specific canonical market's order books
    and comparison data to a JSONL file.
    """
    poly_book, kalshi_book = get_paired_books(canonical_name)
    comparison_data = MARKET_COMPARISON_DATA.get(canonical_name)

    log_entry = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_market": canonical_name,
        "polymarket_book": {},
        "kalshi_book": {},
        "comparison_data": comparison_data
    }

    if poly_book:
        log_entry["polymarket_book"] = {
            "market_id": poly_book.market_id,
            "last_updated_timestamp": poly_book.last_updated_timestamp,
            "bids": poly_book.bids, # These are sorted lists of (price, size)
            "asks": poly_book.asks,
            "highest_bid": poly_book.highest_bid,
            "lowest_ask": poly_book.lowest_ask,
            "bid_ask_spread": poly_book.bid_ask_spread,
            "mid_price": poly_book.mid_price,
            "total_bid_liquidity": poly_book.total_bid_liquidity,
            "total_ask_liquidity": poly_book.total_ask_liquidity,
        }
    
    if kalshi_book:
        log_entry["kalshi_book"] = {
            "market_id": kalshi_book.market_id,
            "last_updated_timestamp": kalshi_book.last_updated_timestamp,
            "bids": kalshi_book.bids,
            "asks": kalshi_book.asks,
            "highest_bid": kalshi_book.highest_bid,
            "lowest_ask": kalshi_book.lowest_ask,
            "bid_ask_spread": kalshi_book.bid_ask_spread,
            "mid_price": kalshi_book.mid_price,
            "total_bid_liquidity": kalshi_book.total_bid_liquidity,
            "total_ask_liquidity": kalshi_book.total_ask_liquidity,
        }

    try:
        # Open in append mode, write a single JSON line, then a newline
        with open(JSON_OUTPUT_FILE_NAME, "a") as f:
            f.write(json.dumps(log_entry) + "\n")
        logger.debug(f"Logged state for {canonical_name} to {JSON_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error writing to JSON log file: {e}")


def save_output_to_file():
    """Saves the current state of order books and comparison data to a text file."""
    logger.info(f"Saving output to {OUTPUT_FILE_NAME}...")
    with open(OUTPUT_FILE_NAME, "w") as f:
        f.write(f"--- Order Book Snapshot (as of {datetime.now().isoformat()}) ---\n\n")
        
        f.write("--- Individual Order Books ---\n")
        for market_id, order_book in ALL_ORDER_BOOKS.items():
            f.write(str(order_book)) # Uses the __str__ method of OrderBook
            f.write("\n" + "="*80 + "\n\n")

        f.write("--- Cross-Market Comparison Data ---\n")
        for canonical_name, data in MARKET_COMPARISON_DATA.items():
            f.write(f"Market: {canonical_name}\n")
            f.write(f"  Cheapest to Buy 'Yes': Platform={data['cheapest_buy_yes']['platform']}, Price={data['cheapest_buy_yes']['price']:.4f}\n")
            f.write(f"  Highest to Sell 'Yes': Platform={data['highest_sell_yes']['platform']}, Price={data['highest_sell_yes']['price']:.4f}\n")
            f.write("-" * 40 + "\n")
        
        f.write("\n--- Raw Market Mapping ---\n")
        pprint.pprint(MARKET_MAPPING, stream=f)
        
        f.write("\n--- Raw Reverse Market Lookup ---\n")
        pprint.pprint(REVERSE_MARKET_LOOKUP, stream=f)
        
    logger.info("Output saved successfully.")



async def print_prices_periodically():
    """Periodically prints the current best bid and ask for each platform and market."""
    while True:
        await asyncio.sleep(PRINT_INTERVAL_SECONDS)
        logger.info(f"\n--- Current Market Snapshot ({datetime.now().strftime('%H:%M:%S')}) ---")

        for canonical_name, comparison_data in MARKET_COMPARISON_DATA.items():
            logger.info(f"\nMarket: {canonical_name}")

            # Get the individual order books for this market pair
            poly_book, kalshi_book = get_paired_books(canonical_name)

            # Print Polymarket's Best Bid/Ask
            if poly_book:
                poly_highest_bid = poly_book.highest_bid
                poly_lowest_ask = poly_book.lowest_ask
                logger.info(f"  Polymarket:")
                logger.info(f"    Bid: {poly_highest_bid:.4f}" if poly_highest_bid is not None else "    Bid: N/A")
                logger.info(f"    Ask: {poly_lowest_ask:.4f}" if poly_lowest_ask is not None else "    Ask: N/A")
                logger.info(f"    Spread: {poly_book.bid_ask_spread:.4f}" if poly_book.bid_ask_spread is not None else "    Spread: N/A")
            else:
                logger.info(f"  Polymarket: N/A (Order Book not available)")

            # Print Kalshi's Best Bid/Ask
            if kalshi_book:
                kalshi_highest_bid = kalshi_book.highest_bid
                kalshi_lowest_ask = kalshi_book.lowest_ask
                logger.info(f"  Kalshi:")
                logger.info(f"    Bid: {kalshi_highest_bid:.4f}" if kalshi_highest_bid is not None else "    Bid: N/A")
                logger.info(f"    Ask: {kalshi_lowest_ask:.4f}" if kalshi_lowest_ask is not None else "    Ask: N/A")
                logger.info(f"    Spread: {kalshi_book.bid_ask_spread:.4f}" if kalshi_book.bid_ask_spread is not None else "    Spread: N/A")
            else:
                logger.info(f"  Kalshi: N/A (Order Book not available)")
            
            # Print Global Best Prices (from cross-market comparison)
            global_buy_platform = comparison_data['cheapest_buy_yes']['platform'] or 'N/A'
            global_buy_price = comparison_data['cheapest_buy_yes']['price'] 
            global_buy_price_str = f"{global_buy_price:.4f}" if global_buy_price != float('inf') else 'N/A'
            
            global_sell_platform = comparison_data['highest_sell_yes']['platform'] or 'N/A'
            global_sell_price = comparison_data['highest_sell_yes']['price']
            global_sell_price_str = f"{global_sell_price:.4f}" if global_sell_price != 0.0 else 'N/A'

            logger.info(f"  --- Cross-Market Best ---")
            logger.info(f"    Cheapest Buy 'Yes': {global_buy_platform} @ {global_buy_price_str}")
            logger.info(f"    Highest Sell 'Yes': {global_sell_platform} @ {global_sell_price_str}")

        logger.info("\n" + "=" * 80 + "\n") # Separator for next interval


async def main():
    message_queue = asyncio.Queue()

    await initialize_market_data()

    # Get specific asset IDs/tickers from the initialized MARKET_MAPPING
    poly_asset_ids_to_subscribe = [
        ids["polymarket"] for ids in MARKET_MAPPING.values() if "polymarket" in ids
    ]
    kalshi_tickers_to_subscribe = [
        ids["kalshi"] for ids in MARKET_MAPPING.values() if "kalshi" in ids
    ]

    polymarket_wss = PolymarketWSS(
        POLYMARKET_MARKET_WSS_URI, 
        poly_asset_ids_to_subscribe,
        message_queue
    )

    kalshi_wss = KalshiWSS(
        key_id=KEYID,
        private_key=private_key,
        environment=env,
        message_queue=message_queue,
        ticker_list=kalshi_tickers_to_subscribe
    )
    
    # Connect to websockets
    await kalshi_wss.connect()
    await polymarket_wss.connect()

    if kalshi_wss.ws and polymarket_wss.websocket:
        # Start listening in the background
        kalshi_listener_task = asyncio.create_task(kalshi_wss.listen())
        polymarket_listener_task = asyncio.create_task(polymarket_wss.listen())
        
        # Task to consume messages from the queue
        async def message_consumer():
            while True:
                source, message = await message_queue.get()
                logger.debug(f"\n--- Main received message from {source} ---")
                # This will now trigger the JSON logging as well
                asyncio.create_task(process_websocket_message(source, message))
                message_queue.task_done()

        consumer_task = asyncio.create_task(message_consumer())
        
        # Start the periodic printing task
        printer_task = asyncio.create_task(print_prices_periodically())

        logger.info(f"WebSocket listeners started. Running for {RUN_DURATION_MINUTES} minutes...")
        start_time = time.time()
        try:
            await asyncio.sleep(RUN_DURATION_MINUTES * 60)
            logger.info(f"Run duration of {RUN_DURATION_MINUTES} minutes completed.")
        except asyncio.CancelledError:
            logger.info("Program cancelled (e.g., Ctrl+C detected or explicit stop).")
        finally:
            logger.info("Shutting down...")
            kalshi_listener_task.cancel()
            polymarket_listener_task.cancel()
            consumer_task.cancel()
            printer_task.cancel()

            await asyncio.gather(
                kalshi_listener_task, 
                polymarket_listener_task, 
                consumer_task, 
                printer_task,
                return_exceptions=True
            )
            
            await kalshi_wss.disconnect()
            await polymarket_wss.disconnect()
            
            # Not saving to file on exit anymore, as it's continuously logged
            # Instead, we just confirm closure of the JSON file stream (implicitly done by `with open`)
            logger.info(f"Data has been continuously logged to {JSON_OUTPUT_FILE_NAME}")

    else:
        logger.error("Could not start listener, connection to one or more WebSockets failed.")


if __name__ == "__main__":
    try:
        # Remove the old text output file if it exists, to start fresh JSONL
        try:
            import os
            if os.path.exists(JSON_OUTPUT_FILE_NAME):
                os.remove(JSON_OUTPUT_FILE_NAME)
                logger.info(f"Removed old {JSON_OUTPUT_FILE_NAME} to start fresh.")
        except Exception as e:
            logger.warning(f"Could not remove old JSON log file: {e}")

        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program manually interrupted (Ctrl+C). Check JSON log file for data.")
    except Exception as e:
        logger.exception("An unexpected error occurred. Check JSON log file for data.")