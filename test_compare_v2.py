import asyncio
import logging
import pprint
from typing import Dict, Any, Optional, Tuple
import time
from datetime import datetime, timezone
import json
import os # For file operations

# Import your classes and functions
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI
from kalshi.wss import KalshiWSS, env, KEYID, private_key
from order_book import OrderBook
# Assuming your updates are in polymarket/updates.py and kalshi/updates.py relative to main.py
from polymarket.updates import update_polymarket_order_book
from kalshi.updates import update_kalshi_order_book

# --- Configuration Constants (Consolidated from config.py) ---
RUN_DURATION_MINUTES = 5 # Set to None for infinite run until Ctrl+C
JSON_OUTPUT_FILE_NAME = "order_book_updates.json" # Using .jsonl for JSON Lines format
PRINT_INTERVAL_SECONDS = 60

# File names for market mappings
MARKETS_FILE = 'markets.json'
COMP_FILE = 'compliment.json'

# --- Load Market Mappings from Files ---
try:
    with open(MARKETS_FILE) as json_file:
        MARKET_MAPPING: Dict[str, Dict[str, str]] = json.load(json_file)
    logging.info(f"Loaded MARKET_MAPPING from {MARKETS_FILE}")
except FileNotFoundError:
    logging.error(f"Error: {MARKETS_FILE} not found. Please create it with your market definitions.")
    MARKET_MAPPING = {} # Initialize empty to prevent further errors
except json.JSONDecodeError:
    logging.error(f"Error: Could not decode JSON from {MARKETS_FILE}. Check file format.")
    MARKET_MAPPING = {}

try:
    with open(COMP_FILE) as json_file:
        COMPLEMENTARY_MARKET_PAIRS: Dict[str, str] = json.load(json_file)
    logging.info(f"Loaded COMPLEMENTARY_MARKET_PAIRS from {COMP_FILE}")
except FileNotFoundError:
    logging.warning(f"Warning: {COMP_FILE} not found. Cross-outcome arbitrage will not be performed.")
    COMPLEMENTARY_MARKET_PAIRS = {} # Initialize empty
except json.JSONDecodeError:
    logging.error(f"Error: Could not decode JSON from {COMP_FILE}. Check file format.")
    COMPLEMENTARY_MARKET_PAIRS = {}


# --- Global Storage for Order Books and Comparison Data ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {} # Maps native_id -> canonical_name
MARKET_COMPARISON_DATA: Dict[str, Dict[str, Any]] = {} # Stores comparison for each canonical market


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def initialize_market_data():
    """Initializes all OrderBook instances and comparison data structures."""
    if not MARKET_MAPPING:
        logger.warning("MARKET_MAPPING is empty. No markets to track.")
        return

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

        # Initialize comparison data for each canonical market
        MARKET_COMPARISON_DATA[canonical_name] = {
            'cheapest_buy_yes': {'platform': None, 'price': float('inf')},
            'highest_sell_yes': {'platform': None, 'price': 0.0},
            'same_outcome_arbitrage_liquidity': 0.0 # Liquidity for (buy Yes / sell Yes) arb
        }
    logger.info("All market data structures initialized.")


def get_paired_books(canonical_name: str) -> Tuple[Optional[OrderBook], Optional[OrderBook]]:
    """
    Retrieves the Polymarket and Kalshi OrderBook instances for a given canonical market name.
    Returns (polymarket_book, kalshi_book).
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
    and updates MARKET_COMPARISON_DATA for the SAME outcome.
    Calculates and prints liquidity for arbitrage opportunities.
    """
    poly_book, kalshi_book = get_paired_books(canonical_name)

    current_comparison = MARKET_COMPARISON_DATA[canonical_name]

    cheapest_buy_price = float('inf')
    cheapest_buy_platform = None
    cheapest_buy_book: Optional[OrderBook] = None
    
    highest_sell_price = 0.0
    highest_sell_platform = None
    highest_sell_book: Optional[OrderBook] = None

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

    # Calculate and store SAME outcome arbitrage liquidity
    same_outcome_arbitrage_liquidity = 0.0 
    if cheapest_buy_book and highest_sell_book and \
       cheapest_buy_platform != highest_sell_platform: # Must be different platforms for arb

        buy_price = current_comparison['cheapest_buy_yes']['price']
        sell_price = current_comparison['highest_sell_yes']['price']

        # Only consider if there's a profitable spread greater than 0.01 (1 cent)
        if sell_price > buy_price + 0.01:
            profit_per_share = sell_price - buy_price

            buy_liquidity_depth = 0.0
            # Iterate through asks on the 'cheapest_buy_book' until price exceeds 'sell_price'
            for price, size in cheapest_buy_book.asks:
                if price <= sell_price: # We can buy at this price or cheaper
                    buy_liquidity_depth += size
                else:
                    break # Prices are sorted, no more opportunities at better prices
            
            sell_liquidity_depth = 0.0
            # Iterate through bids on the 'highest_sell_book' until price falls below 'buy_price'
            for price, size in highest_sell_book.bids:
                if price >= buy_price: # We can sell at this price or higher
                    sell_liquidity_depth += size
                else:
                    break # Prices are sorted, no more opportunities at better prices
            
            same_outcome_arbitrage_liquidity = min(buy_liquidity_depth, sell_liquidity_depth)

            logger.info(f"Same-Outcome Arbitrage Opportunity for {canonical_name}:")
            logger.info(f"  Buy Yes on {cheapest_buy_platform} at {buy_price:.4f}")
            logger.info(f"  Sell Yes on {highest_sell_platform} at {sell_price:.4f}")
            logger.info(f"  Potential Profit per Share: {profit_per_share:.4f}")
            logger.info(f"  Arbitrage Liquidity: {same_outcome_arbitrage_liquidity:.2f} shares")
    
    current_comparison['same_outcome_arbitrage_liquidity'] = same_outcome_arbitrage_liquidity


def find_cross_outcome_arbitrage(market_a_canonical_name: str, market_b_canonical_name: str):
    """
    Finds arbitrage opportunity by buying 'Yes' on market A and buying 'Yes' on market B,
    where A and B are complementary outcomes of the same event.
    For example, Buy Yes (Detroit Wins) + Buy Yes (Washington Wins) < 1.00.
    """
    # Get the best available 'Yes' ask for Market A across all platforms
    lowest_ask_a = float('inf')
    platform_a_for_buy = None
    book_a_for_buy: Optional[OrderBook] = None
    
    market_a_ids = MARKET_MAPPING.get(market_a_canonical_name, {})
    for platform_name, market_id in market_a_ids.items(): # platform_name can be "polymarket" or "kalshi"
        book = ALL_ORDER_BOOKS.get(market_id)
        if book and book.lowest_ask is not None:
            if book.lowest_ask < lowest_ask_a:
                lowest_ask_a = book.lowest_ask
                platform_a_for_buy = platform_name
                book_a_for_buy = book

    # Get the best available 'Yes' ask for Market B across all platforms
    lowest_ask_b = float('inf')
    platform_b_for_buy = None
    book_b_for_buy: Optional[OrderBook] = None
    
    market_b_ids = MARKET_MAPPING.get(market_b_canonical_name, {})
    for platform_name, market_id in market_b_ids.items(): # platform_name can be "polymarket" or "kalshi"
        book = ALL_ORDER_BOOKS.get(market_id)
        if book and book.lowest_ask is not None:
            if book.lowest_ask < lowest_ask_b:
                lowest_ask_b = book.lowest_ask
                platform_b_for_buy = platform_name
                book_b_for_buy = book

    # Proceed only if we found valid asks for both markets
    if lowest_ask_a == float('inf') or lowest_ask_b == float('inf') or \
       book_a_for_buy is None or book_b_for_buy is None:
        return # Not enough data to form an arb

    combined_buy_price = lowest_ask_a + lowest_ask_b
    profit_if_combined_less_than_1 = 1.0 - combined_buy_price # Payout is 1.0 for buying both outcomes

    # Check for profitable arb (e.g., combined cost < 0.99 for a > $0.01 profit)
    if profit_if_combined_less_than_1 > 0.01:
        
        # --- Calculate Arbitrage Liquidity ---
        # We're buying 'Yes' on market A and 'Yes' on market B.
        # Liquidity is limited by the available asks on each side, considering the counterparty price.
        
        buy_a_liquidity_depth = 0.0
        # Iterate through asks on market A. The arb is profitable as long as
        # A's price + B's best ask is still < 1.0 minus profit margin.
        for price_a, size_a in book_a_for_buy.asks:
            if price_a + lowest_ask_b < 1.0 - 0.01: # Check if this specific level for A is still profitable with best B
                buy_a_liquidity_depth += size_a
            else:
                break
        
        buy_b_liquidity_depth = 0.0
        # Iterate through asks on market B.
        for price_b, size_b in book_b_for_buy.asks:
            if price_b + lowest_ask_a < 1.0 - 0.01: # Check if this specific level for B is still profitable with best A
                buy_b_liquidity_depth += size_b
            else:
                break
        
        cross_outcome_arbitrage_liquidity = min(buy_a_liquidity_depth, buy_b_liquidity_depth)

        if cross_outcome_arbitrage_liquidity > 0: # Only log if there's actual liquidity
            logger.info(f"Cross-Outcome Arbitrage Opportunity: {market_a_canonical_name} + {market_b_canonical_name}:")
            logger.info(f"  Buy Yes on {market_a_canonical_name} ({platform_a_for_buy}) at {lowest_ask_a:.4f}")
            logger.info(f"  Buy Yes on {market_b_canonical_name} ({platform_b_for_buy}) at {lowest_ask_b:.4f}")
            logger.info(f"  Combined Cost: {combined_buy_price:.4f}")
            logger.info(f"  Potential Profit per Share: {profit_if_combined_less_than_1:.4f}")
            logger.info(f"  Arbitrage Liquidity: {cross_outcome_arbitrage_liquidity:.2f} shares")


async def process_websocket_message(source: str, message: Dict[str, Any]):
    """
    Processes a message from the WebSocket queue, updates the relevant order book,
    performs cross-market comparison if applicable, and logs the state to JSON.
    """
    market_id = None
    canonical_market_name = None

    if source == 'polymarket':
        # Polymarket 'book' and 'price_change' messages use 'market' hash (e.g., "0x...")
        # for their market identifier. The subscription uses 'asset_id'.
        # Ensure your MARKET_MAPPING uses the 'market' hash for Polymarket entries.
        market_id = message.get("asset_id") # Use 'market' for consistency with how data flows
        if market_id:
            canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
            if not canonical_market_name:
                # This could happen if the asset_id in config is for a market hash not explicitly mapped
                # or if the message structure changes slightly.
                logger.warning(f"Polymarket message for unmapped market_id: {market_id}. Message: {message}")
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

    # After updating, perform cross-market comparison if it's a mapped market
    if canonical_market_name:
        logger.debug(f"Performing cross-market comparison for {canonical_market_name}")
        perform_cross_market_comparison(canonical_market_name)
        # Log the current state to JSON after every relevant update
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


async def print_prices_periodically():
    """Periodically prints the current best bid and ask for each platform and market,
       and checks for cross-outcome arbitrage opportunities."""
    while True:
        await asyncio.sleep(PRINT_INTERVAL_SECONDS)
        logger.info(f"\n--- Current Market Snapshot ({datetime.now().strftime('%H:%M:%S')}) ---")

        # --- Print Same-Outcome Best Prices and Arb ---
        for canonical_name, comparison_data in MARKET_COMPARISON_DATA.items():
            logger.info(f"\nMarket: {canonical_name}")

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
            
            # Print Global Best Prices (from cross-market comparison for the SAME outcome)
            global_buy_platform = comparison_data['cheapest_buy_yes']['platform'] or 'N/A'
            global_buy_price = comparison_data['cheapest_buy_yes']['price'] 
            global_buy_price_str = f"{global_buy_price:.4f}" if global_buy_price != float('inf') else 'N/A'
            
            global_sell_platform = comparison_data['highest_sell_yes']['platform'] or 'N/A'
            global_sell_price = comparison_data['highest_sell_yes']['price']
            global_sell_price_str = f"{global_sell_price:.4f}" if global_sell_price != 0.0 else 'N/A'

            logger.info(f"  --- Cross-Platform Same-Outcome Best ---")
            logger.info(f"    Cheapest Buy 'Yes': {global_buy_platform} @ {global_buy_price_str}")
            logger.info(f"    Highest Sell 'Yes': {global_sell_platform} @ {global_sell_price_str}")
            
            arb_liquidity = comparison_data.get('same_outcome_arbitrage_liquidity', 0.0)
            if arb_liquidity > 0:
                logger.info(f"    Arbitrage Liquidity: {arb_liquidity:.2f} shares")
        
        # --- Check for Cross-Outcome Arbitrage Opportunities ---
        logger.info("\n--- Checking Cross-Outcome Arbitrage Opportunities ---")
        # Iterate through defined complementary pairs
        for market_a_name, market_b_name in COMPLEMENTARY_MARKET_PAIRS.items():
            # Ensure the complementary market is actually mapped in MARKET_MAPPING
            if market_a_name in MARKET_MAPPING and market_b_name in MARKET_MAPPING:
                # To avoid redundant checks (A-B and B-A if mapping is bidirectional)
                # Ensure we only check each unique pair once. Simple string comparison works.
                if market_a_name < market_b_name:
                    find_cross_outcome_arbitrage(market_a_name, market_b_name)
            else:
                logger.debug(f"Skipping cross-outcome check for {market_a_name} <-> {market_b_name} as one or both not fully mapped.")


        logger.info("\n" + "=" * 80 + "\n")


async def main():
    message_queue = asyncio.Queue()

    await initialize_market_data()

    # Get specific asset IDs/tickers from the initialized MARKET_MAPPING
    # Consolidate subscription lists, ensuring no duplicates
    poly_asset_ids_to_subscribe = list(set([
        market_ids["polymarket"] for market_ids in MARKET_MAPPING.values() if "polymarket" in market_ids
    ]))
    kalshi_tickers_to_subscribe = list(set([
        market_ids["kalshi"] for market_ids in MARKET_MAPPING.values() if "kalshi" in market_ids
    ]))
    
    # Check if we have any markets to subscribe to
    if not poly_asset_ids_to_subscribe and not kalshi_tickers_to_subscribe:
        logger.critical("No markets found in MARKET_MAPPING to subscribe to. Exiting.")
        return # Exit if no markets are defined

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

    # Only proceed if at least one connection is successful
    if kalshi_wss.ws or polymarket_wss.websocket:
        # Start listening in the background
        tasks = []
        if kalshi_wss.ws:
            tasks.append(asyncio.create_task(kalshi_wss.listen()))
        if polymarket_wss.websocket:
            tasks.append(asyncio.create_task(polymarket_wss.listen()))
        
        # Task to consume messages from the queue
        async def message_consumer():
            while True:
                source, message = await message_queue.get()
                logger.debug(f"\n--- Main received message from {source} ---")
                asyncio.create_task(process_websocket_message(source, message))
                message_queue.task_done()

        consumer_task = asyncio.create_task(message_consumer())
        tasks.append(consumer_task)
        
        # Start the periodic printing task
        #printer_task = asyncio.create_task(print_prices_periodically())
        #tasks.append(printer_task)

        logger.info(f"WebSocket listeners started. Running for {RUN_DURATION_MINUTES} minutes...")
        start_time = time.time()
        try:
            if RUN_DURATION_MINUTES is not None:
                await asyncio.sleep(RUN_DURATION_MINUTES * 60)
                logger.info(f"Run duration of {RUN_DURATION_MINUTES} minutes completed.")
            else:
                # If RUN_DURATION_MINUTES is None, run indefinitely until Ctrl+C
                await asyncio.Future() # An awaitable that never completes
        except asyncio.CancelledError:
            logger.info("Program cancelled (e.g., Ctrl+C detected or explicit stop).")
        finally:
            logger.info("Shutting down...")
            for task in tasks:
                task.cancel()
            
            # Gather all tasks to ensure they are properly cancelled and cleaned up
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Disconnect from WebSockets
            if kalshi_wss.ws:
                await kalshi_wss.disconnect()
            if polymarket_wss.websocket:
                await polymarket_wss.disconnect()
            
            logger.info(f"Data has been continuously logged to {JSON_OUTPUT_FILE_NAME}")

    else:
        logger.error("Could not start listener, connection to ALL WebSockets failed. Exiting.")


if __name__ == "__main__":
    try:
        # Remove the old JSON log file if it exists, to start fresh
        try:
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