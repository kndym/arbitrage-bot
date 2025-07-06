import asyncio
import logging
import pprint
from typing import Dict, Any, Optional, Tuple, List
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

# --- Configuration Constants ---
RUN_DURATION_MINUTES = None # Set to None for infinite run until Ctrl+C
INITIAL_STATE_FILE_NAME = "initial_order_books.json" # For the full initial snapshot
ORDER_BOOK_CHANGES_FILE_NAME = "order_book_deltas_jul_5_v2.jsonl" # For subsequent raw updates (JSON Lines)
PRINT_INTERVAL_SECONDS = 1000000000 # Keep this high as we log changes on event now

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

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def initialize_market_data():
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


# Helper to clean and round order book data for JSON logging (for initial state only)
def process_book_data_for_initial(book: OrderBook) -> Dict[str, Any]:
    if not book:
        return {}
    
    bids_rounded = [[round(p, 4), round(s, 2)] for p, s in book.bids]
    asks_rounded = [[round(p, 4), round(s, 2)] for p, s in book.asks]

    lowest_ask_val = round(book.lowest_ask, 4) if book.lowest_ask is not None and book.lowest_ask != float('inf') else None
    highest_bid_val = round(book.highest_bid, 4) if book.highest_bid is not None and book.highest_bid != 0.0 else None

    return {
        "id": book.market_id,
        "b": bids_rounded, # bids
        "a": asks_rounded, # asks
        "hb": highest_bid_val, # highest_bid
        "la": lowest_ask_val, # lowest_ask
        "s": round(book.bid_ask_spread, 4) if book.bid_ask_spread is not None else None, # spread
        "m": round(book.mid_price, 4) if book.mid_price is not None else None, # mid_price
        "tbq": round(book.total_bid_liquidity, 2), # total_bid_liquidity
        "taq": round(book.total_ask_liquidity, 2), # total_ask_liquidity
    }

# Helper to clean and round comparison data for JSON logging
def process_comparison_data(data: Dict[str, Any]) -> Dict[str, Any]:
    if not data:
        return {}

    cheapest_buy_price = data['cheapest_buy_yes']['price']
    cheapest_buy_platform = data['cheapest_buy_yes']['platform']

    highest_sell_price = data['highest_sell_yes']['price']
    highest_sell_platform = data['highest_sell_yes']['platform']

    cb_price = round(cheapest_buy_price, 4) if cheapest_buy_price != float('inf') else None
    hs_price = round(highest_sell_price, 4) if highest_sell_price != 0.0 else None 

    return {
        "cb": {"p": cheapest_buy_platform, "pr": cb_price}, # cheapest_buy_yes: platform, price
        "hs": {"p": highest_sell_platform, "pr": hs_price}, # highest_sell_yes: platform, price
        "sal": round(data['same_outcome_arbitrage_liquidity'], 2) # same_outcome_arbitrage_liquidity
    }


async def log_initial_state_to_json():
    """
    Logs the complete current state of all order books and comparison data to a single JSON file.
    This is called once at the start.
    """
    initial_state_data = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "markets": []
    }

    # Ensure all order books have a chance to populate before logging initial state
    # This might require some initial messages to arrive and be processed
    # before calling this function. For a robust system, you might wait for
    # a certain number of markets to have bids/asks.
    for canonical_name in sorted(MARKET_MAPPING.keys()): # Sort for consistent output
        poly_book, kalshi_book = get_paired_books(canonical_name)
        comparison_data = MARKET_COMPARISON_DATA.get(canonical_name)

        market_entry = {
            "cn_mkt": canonical_name,
            "pm": process_book_data_for_initial(poly_book),
            "ks": process_book_data_for_initial(kalshi_book),
            "cmp": process_comparison_data(comparison_data)
        }
        initial_state_data["markets"].append(market_entry)

    try:
        with open(INITIAL_STATE_FILE_NAME, "w") as f: # Use "w" to overwrite
            json.dump(initial_state_data, f, indent=2) # Use indent for readability in initial file
        logger.info(f"Logged initial state to {INITIAL_STATE_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error writing initial state to JSON file: {e}")


async def log_order_book_update_to_deltas_json(
    canonical_name: str, 
    source_platform: str, 
    native_market_id: str, 
    update_payload: Dict[str, Any] # This is the raw message content relevant to the update
):
    """
    Logs an order book update event for a specific market to the deltas JSONL file.
    Includes the raw update payload and current comparison data.
    """
    comparison_data = MARKET_COMPARISON_DATA.get(canonical_name)

    log_entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "name": canonical_name
    }

    # The 'update_payload' is the raw message specific to the order book change.
    # We choose a key based on the source platform.
    if source_platform == "polymarket":
        event=update_payload.get("event_type") 
        if event=="price_change":
            log_entry["pm_delta"] = {"changes": update_payload["changes"],
                                     "event_type": "delta",}
        elif event =="book":
            log_entry["pm_delta"] = {"changes": {"asks": update_payload["asks"],
                                                 "bids": update_payload["bids"],},
                                     "event_type": "book",}
        else:
            logger.debug(f"NOVEL MESSAGE POLY")
    elif source_platform == "kalshi":
        if update_payload.get("price"):
            log_entry["ks_delta"] = {"price": update_payload["price"],
                                     "delta": update_payload["delta"],
                                     "side": update_payload["side"]}
        elif update_payload.get("yes"):
            log_entry["ks_delta"] = {"yes": update_payload["yes"],
                                     "no": update_payload["no"],}
        else:
            logger.debug(f"NOVEL MESSAGE KALSHI")
    elif source_platform == "system_closure":
        # For closures, no delta, just record the event
        pass # No "pm_delta" or "ks_delta" for closure events

    if log_entry.get("ks_delta") or log_entry.get("pm_delta"):
        try:
            # Open in append mode, write a single JSON line, then a newline
            # Using separators to remove whitespace for maximum compactness
            with open(ORDER_BOOK_CHANGES_FILE_NAME, "a") as f:
                f.write(json.dumps(log_entry, separators=(',', ':')) + "\n")
            logger.debug(f"Logged delta for {canonical_name} (from {source_platform}) to {ORDER_BOOK_CHANGES_FILE_NAME}")
        except Exception as e:
            logger.error(f"Error writing to JSON deltas log file: {e}")


async def process_websocket_message(source: str, message: Dict[str, Any], polymarket_wss: PolymarketWSS, kalshi_wss: KalshiWSS):
    """
    Processes a message from the WebSocket queue, updates the relevant order book,
    performs cross-market comparison if applicable, and logs the state to JSON.
    Handles market closure/resolution by unsubscribing.
    """
    market_id = None
    canonical_market_name = None
    
    # Store the relevant part of the message to log as a delta
    update_payload = None 

    if source == 'polymarket':
        market_id = message.get("asset_id")
        if not market_id:
            logger.warning(f"Polymarket message missing 'asset_id'. Message: {message}")
            return
        
        canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
        if not canonical_market_name:
            logger.warning(f"Polymarket message for unmapped market_id: {market_id}. Message: {message}")
            return
        
        order_book = ALL_ORDER_BOOKS.get(market_id)
        if order_book:
            # Assume update_polymarket_order_book consumes the relevant message part
            # and that 'message' itself contains the delta info
            update_polymarket_order_book(order_book, message)
            update_payload = message # Log the raw Polymarket message
            logger.debug(f"Polymarket book for {market_id} updated.")
        else:
            logger.error(f"OrderBook instance not found for Polymarket market_id: {market_id}. Perhaps it was already closed/unsubscribed.")
            return

    elif source == 'kalshi':
        msg_content = message.get("msg", {})
        market_id = msg_content.get("market_ticker")
        
        if not market_id:
            logger.warning(f"Kalshi message missing 'market_ticker'. Message: {message}")
            return
        
        canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
        if not canonical_market_name:
            logger.warning(f"Kalshi message for unmapped market_id: {market_id}")
            return
        
        order_book = ALL_ORDER_BOOKS.get(market_id)
        if order_book:
            update_kalshi_order_book(order_book, message)
            # For Kalshi, the 'msg' part usually contains the event details, not the top-level message.
            update_payload = message.get("msg", message) # Log the relevant Kalshi message part
            logger.debug(f"Kalshi book for {market_id} updated.")
        else:
            logger.error(f"OrderBook instance not found for Kalshi market_id: {market_id}.")
            return
    
    elif source == "update" and False: # This source is specifically for Kalshi market status updates
        msg_content = message.get("msg", {})
        market_id = msg_content.get("market_ticker")
        
        result_true = ("result" in msg_content and msg_content["result"] is not None)
        closed_true = msg_content.get("is_deactivated", False)

        if market_id and (result_true or closed_true):
            canonical_market_name = REVERSE_MARKET_LOOKUP.get(market_id)
            if not canonical_market_name:
                logger.warning(f"Kalshi 'update' message for unmapped market_id: {market_id}. Skipping unsubscribe.")
                return

            logger.info(f"Market {canonical_market_name} (Kalshi: {market_id}) resolved (Result: {msg_content.get('result')}) or closed (Deactivated: {msg_content.get('is_deactivated')}). Attempting to unsubscribe from both platforms.")

            polymarket_id_for_canonical = MARKET_MAPPING.get(canonical_market_name, {}).get("polymarket")

            if market_id in kalshi_wss.ticker_list: 
                await kalshi_wss.unsubscribe(market_id)
                logger.info(f"Successfully unsubscribed from Kalshi market: {market_id}")
            else:
                logger.debug(f"Kalshi market {market_id} was not in active subscription list for KalshiWSS, skipping unsubscribe via WSS object.")

            if polymarket_id_for_canonical and polymarket_id_for_canonical in polymarket_wss.asset_ids: 
                await polymarket_wss.unsubscribe(polymarket_id_for_canonical)
                logger.info(f"Successfully unsubscribed from Polymarket market: {polymarket_id_for_canonical} (corresponding to {canonical_market_name})")
            elif polymarket_id_for_canonical:
                logger.debug(f"Polymarket market {polymarket_id_for_canonical} was not in active subscription list for PolymarketWSS, skipping unsubscribe via WSS object.")
            else:
                logger.debug(f"No corresponding Polymarket market found for {canonical_market_name} in mapping, skipping Polymarket unsubscribe.")
            
            # Clean up global data structures
            if market_id in ALL_ORDER_BOOKS: del ALL_ORDER_BOOKS[market_id]
            if market_id in REVERSE_MARKET_LOOKUP: del REVERSE_MARKET_LOOKUP[market_id]
            if polymarket_id_for_canonical and polymarket_id_for_canonical in ALL_ORDER_BOOKS: del ALL_ORDER_BOOKS[polymarket_id_for_canonical]
            if polymarket_id_for_canonical and polymarket_id_for_canonical in REVERSE_MARKET_LOOKUP: del REVERSE_MARKET_LOOKUP[polymarket_id_for_canonical]
            if canonical_market_name in MARKET_COMPARISON_DATA: del MARKET_COMPARISON_DATA[canonical_market_name]
                
            # Log this market closure/resolution event without an update_payload
            await log_order_book_update_to_deltas_json(canonical_market_name, "system_closure", market_id, {}) 

            return 
        else:
            logger.debug(f"Kalshi 'update' message received (not resolved/closed): {message}")
            return
    else:
        logger.warning(f"Unknown message source: {source}")
        return

    # After an order book was updated by a relevant message, perform comparison and log the raw message
    if update_payload is not None and canonical_market_name and canonical_market_name in MARKET_COMPARISON_DATA:
        logger.debug(f"Performing cross-market comparison for {canonical_market_name}")
        perform_cross_market_comparison(canonical_market_name)
        await log_order_book_update_to_deltas_json(canonical_market_name, source, market_id, update_payload)


async def print_prices_periodically():
    """Periodically prints the current best bid and ask for each platform and market,
       and checks for cross-outcome arbitrage opportunities. This function *only prints*."""
    while True:
        await asyncio.sleep(PRINT_INTERVAL_SECONDS)
        logger.info(f"\n--- Current Market Snapshot ({datetime.now().strftime('%H:%M:%S')}) ---")

        # --- Print Same-Outcome Best Prices and Arb ---
        for canonical_name in list(MARKET_COMPARISON_DATA.keys()): 
            if canonical_name not in MARKET_COMPARISON_DATA:
                continue

            comparison_data = MARKET_COMPARISON_DATA[canonical_name]
            logger.info(f"\nMarket: {canonical_name}")

            poly_book, kalshi_book = get_paired_books(canonical_name)

            if poly_book:
                poly_highest_bid = poly_book.highest_bid
                poly_lowest_ask = poly_book.lowest_ask
                logger.info(f"  Polymarket:")
                logger.info(f"    Bid: {poly_highest_bid:.4f}" if poly_highest_bid is not None else "    Bid: N/A")
                logger.info(f"    Ask: {poly_lowest_ask:.4f}" if poly_lowest_ask is not None else "    Ask: N/A")
                logger.info(f"    Spread: {poly_book.bid_ask_spread:.4f}" if poly_book.bid_ask_spread is not None else "    Spread: N/A")
            else:
                logger.info(f"  Polymarket: N/A (Order Book not available)")

            if kalshi_book:
                kalshi_highest_bid = kalshi_book.highest_bid
                kalshi_lowest_ask = kalshi_book.lowest_ask
                logger.info(f"  Kalshi:")
                logger.info(f"    Bid: {kalshi_highest_bid:.4f}" if kalshi_highest_bid is not None else "    Bid: N/A")
                logger.info(f"    Ask: {kalshi_lowest_ask:.4f}" if kalshi_lowest_ask is not None else "    Ask: N/A")
                logger.info(f"    Spread: {kalshi_book.bid_ask_spread:.4f}" if kalshi_book.bid_ask_spread is not None else "    Spread: N/A")
            else:
                logger.info(f"  Kalshi: N/A (Order Book not available)")
            
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
        
        logger.info("\n--- Checking Cross-Outcome Arbitrage Opportunities ---")
        for market_a_name, market_b_name in COMPLEMENTARY_MARKET_PAIRS.items():
            if market_a_name in MARKET_MAPPING and market_b_name in MARKET_MAPPING and \
               market_a_name in MARKET_COMPARISON_DATA and market_b_name in MARKET_COMPARISON_DATA:
                if market_a_name < market_b_name:
                    find_cross_outcome_arbitrage(market_a_name, market_b_name)
            else:
                logger.debug(f"Skipping cross-outcome check for {market_a_name} <-> {market_b_name} as one or both not fully mapped or actively tracked.")

        logger.info("\n" + "=" * 80 + "\n")


async def main():
    message_queue = asyncio.Queue()

    await initialize_market_data()

    poly_asset_ids_to_subscribe = list(set([
        market_ids["polymarket"] for market_ids in MARKET_MAPPING.values() if "polymarket" in market_ids
    ]))
    kalshi_tickers_to_subscribe = list(set([
        market_ids["kalshi"] for market_ids in MARKET_MAPPING.values() if "kalshi" in market_ids
    ]))
    
    if not poly_asset_ids_to_subscribe and not kalshi_tickers_to_subscribe:
        logger.critical("No markets found in MARKET_MAPPING to subscribe to. Exiting.")
        return

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
    
    await kalshi_wss.connect()
    await polymarket_wss.connect()

    if kalshi_wss.ws or polymarket_wss.websocket:
        tasks = []
        if kalshi_wss.ws:
            tasks.append(asyncio.create_task(kalshi_wss.listen()))
        else:
            logger.warning("Kalshi WebSocket connection not established.")
        if polymarket_wss.websocket:
            tasks.append(asyncio.create_task(polymarket_wss.listen()))
        else:
            logger.warning("Polymarket WebSocket connection not established.")

        async def message_consumer(pm_wss: PolymarketWSS, k_wss: KalshiWSS):
            # Give a small delay to allow initial messages to populate some order books
            # This is not guaranteed, but gives a better chance for initial state.
            await asyncio.sleep(5) 
            # Log the initial state *after* connections are established and some data might have flowed
            while True:
                source, message = await message_queue.get()
                logger.debug(f"\n--- Main received message from {source} ---")
                asyncio.create_task(process_websocket_message(source, message, pm_wss, k_wss))
                message_queue.task_done()

        consumer_task = asyncio.create_task(message_consumer(polymarket_wss, kalshi_wss))
        tasks.append(consumer_task)
        
        printer_task = asyncio.create_task(print_prices_periodically())
        tasks.append(printer_task)

        logger.info(f"WebSocket listeners started. Running for {RUN_DURATION_MINUTES} minutes...")
        try:
            if RUN_DURATION_MINUTES is not None:
                await asyncio.sleep(RUN_DURATION_MINUTES * 60)
                logger.info(f"Run duration of {RUN_DURATION_MINUTES} minutes completed.")
            else:
                await asyncio.Future() # Run indefinitely
        except asyncio.CancelledError:
            logger.info("Program cancelled (e.g., Ctrl+C detected or explicit stop).")
        finally:
            logger.info("Shutting down...")
            for task in tasks:
                task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if kalshi_wss.ws:
                await kalshi_wss.disconnect()
            if polymarket_wss.websocket:
                await polymarket_wss.disconnect()
            
            #logger.info(f"Initial state saved to {INITIAL_STATE_FILE_NAME}")
            logger.info(f"Order book changes saved to {ORDER_BOOK_CHANGES_FILE_NAME}")

    else:
        logger.error("Could not start listener, connection to ALL WebSockets failed. Exiting.")


if __name__ == "__main__":
    try:
        # Remove old JSON log files to start fresh
        for filename in [INITIAL_STATE_FILE_NAME, ORDER_BOOK_CHANGES_FILE_NAME]:
            try:
                if os.path.exists(filename):
                    os.remove(filename)
                    logger.info(f"Removed old {filename} to start fresh.")
            except Exception as e:
                logger.warning(f"Could not remove old file {filename}: {e}")

        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program manually interrupted (Ctrl+C). Check JSON log files for data.")
    except Exception as e:
        logger.exception("An unexpected error occurred. Check JSON log files for data.")