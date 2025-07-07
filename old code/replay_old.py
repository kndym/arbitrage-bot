import json
import csv
import logging
import math
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
import re
import os

# --- Custom Imports ---
from order_book import OrderBook
from polymarket.recreate import update_polymarket_order_book
from kalshi.recreate import update_kalshi_order_book

# --- Configuration ---
LOG_LEVEL = logging.INFO
JSONL_FILE_PATH = 'order_book_deltas_jul_5_v2.jsonl'
MARKETS_FILE = 'markets.json'
COMP_FILE = 'compliment.json'

# --- << DEBUGGING CONTROL >> ---
# Set to True to print detailed order book states for every attempted trade.
# This helps diagnose why this flawed script found trades.
DEBUG_MODE = True
# ---

# Output file name
EXECUTED_TRADES_CSV = 'executed_arbitrage_trades_flipped_kalshi_v3.csv'

# Profitability and Execution Configuration
PROFIT_THRESHOLD = 0.015 

# --- Global State ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {}
MARKET_MAPPING: Dict[str, Dict[str, str]] = {}
COMPLEMENTARY_MARKET_PAIRS: Dict[str, str] = {}
CSV_WRITER = None
CSV_FILE = None
TRADE_ID_COUNTER = 0

# --- Setup Functions ---
def setup_logging():
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
    global logger
    logger = logging.getLogger(__name__)

def load_market_data():
    global MARKET_MAPPING, COMPLEMENTARY_MARKET_PAIRS
    try:
        with open(MARKETS_FILE) as f: MARKET_MAPPING = json.load(f)
        with open(COMP_FILE) as f: COMPLEMENTARY_MARKET_PAIRS = json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Error: Could not find a required file: {e.filename}"); exit(1)

def initialize_order_books():
    for canonical_name, platforms in MARKET_MAPPING.items():
        if "polymarket" in platforms: ALL_ORDER_BOOKS[platforms["polymarket"]] = OrderBook(platforms["polymarket"]); REVERSE_MARKET_LOOKUP[platforms["polymarket"]] = canonical_name
        if "kalshi" in platforms: ALL_ORDER_BOOKS[platforms["kalshi"]] = OrderBook(platforms["kalshi"]); REVERSE_MARKET_LOOKUP[platforms["kalshi"]] = canonical_name

def setup_csv_writer():
    global CSV_FILE, CSV_WRITER
    if os.path.exists(EXECUTED_TRADES_CSV): os.remove(EXECUTED_TRADES_CSV)
    CSV_FILE = open(EXECUTED_TRADES_CSV, 'w', newline='')
    header = ['trade_id','timestamp','arbitrage_type','net_profit_per_share','trade_size','total_net_profit','fees_paid','market_a','platform_a','side_a','avg_price_a','market_b','platform_b','side_b','avg_price_b']
    CSV_WRITER = csv.writer(CSV_FILE); CSV_WRITER.writerow(header)

def cleanup():
    if CSV_FILE: CSV_FILE.close()

# --- Utility and Fee Calculation ---
def calculate_kalshi_fee(trade_size: float, price: float) -> float:
    if trade_size <= 0 or price <= 0 or price >= 1: return 0.0
    return math.ceil(0.07 * trade_size * price * (1.0 - price) * 100) / 100.0

def get_paired_books(canonical_name: str) -> Tuple[Optional[OrderBook], Optional[OrderBook]]:
    market_ids = MARKET_MAPPING.get(canonical_name, {}); poly_book = ALL_ORDER_BOOKS.get(market_ids.get("polymarket")); kalshi_book = ALL_ORDER_BOOKS.get(market_ids.get("kalshi")); return poly_book, kalshi_book

def _format_book_for_debug(book: OrderBook, name: str) -> str:
    """Helper to format an order book for pretty printing in debug mode."""
    if not book: return f"  {name}: [Book Not Available]\n"
    bids = list(book.bids)[:5]
    asks = list(book.asks)[:5]
    output = f"  {name} ({book.market_id}):\n"
    output += f"    Asks: {asks}\n"
    output += f"    Bids: {bids}\n"
    return output

# --- Trade Execution ---
def execute_trade_on_book(book: OrderBook, side_to_hit: str, size_to_trade: float) -> Tuple[float, float]:
    if size_to_trade <= 0 or not book: return 0.0, 0.0
    levels_to_hit = book.asks if side_to_hit == 'ask' else book.bids
    if not levels_to_hit: return 0.0, 0.0
    size_executed, total_cost, remaining_size = 0.0, 0.0, size_to_trade
    for price, available_size in list(levels_to_hit):
        if remaining_size <= 1e-9: break
        size_at_this_level = min(remaining_size, available_size)
        book._update_book_level(side_to_hit, price, available_size - size_at_this_level)
        size_executed += size_at_this_level; total_cost += size_at_this_level * price; remaining_size -= size_at_this_level
    avg_price, executed_size = ((total_cost / size_executed, size_executed) if size_executed > 0 else (0.0, 0.0))
    return avg_price, executed_size

# --- Main Execution Logic ---
def process_log_entry(log_entry: Dict[str, Any]):
    canonical_name = log_entry.get("name")
    if not canonical_name: return
    if "pm_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("polymarket")
        if market_id in ALL_ORDER_BOOKS: update_polymarket_order_book(ALL_ORDER_BOOKS[market_id], log_entry["pm_delta"])
    elif "ks_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("kalshi")
        if market_id in ALL_ORDER_BOOKS: update_kalshi_order_book(ALL_ORDER_BOOKS[market_id], log_entry["ks_delta"])

def run_normal_mode():
    logger.info("Running in NORMAL mode (with old, flawed logic).")
    global TRADE_ID_COUNTER

    with open(JSONL_FILE_PATH, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if (i+1) % 50000 == 0: logger.info(f"Processed {i+1}/{len(lines)} lines...")
            
            try: log_entry = json.loads(line)
            except json.JSONDecodeError: continue

            process_log_entry(log_entry)
            
            timestamp = log_entry.get('ts')
            canonical_name = log_entry.get('name')
            if not (timestamp and canonical_name): continue

            poly_book, kalshi_book = get_paired_books(canonical_name)
            if not (poly_book and kalshi_book): continue

            # --- Opp 1: Buy Poly, Sell Kalshi ---
            best_poly_ask = poly_book.lowest_ask
            best_kalshi_bid = kalshi_book.highest_bid
            if best_poly_ask is not None and best_kalshi_bid is not None and (best_kalshi_bid - best_poly_ask) > 0:
                # FLAWED LOGIC: This assumes you can fill across multiple price levels on one book
                # based on the best price of the other book (look-ahead bias).
                buy_liquidity = sum(size for price, size in poly_book.asks if price == best_kalshi_bid)
                sell_liquidity = sum(size for price, size in kalshi_book.bids if price == best_poly_ask)
                trade_size = min(buy_liquidity, sell_liquidity)
                if trade_size > 0:
                    estimated_fees = calculate_kalshi_fee(trade_size, best_kalshi_bid)
                    est_net_profit_per_share = ((best_kalshi_bid - best_poly_ask) * trade_size - estimated_fees) / trade_size if trade_size > 0 else 0
                    if est_net_profit_per_share > PROFIT_THRESHOLD:
                        
                        if DEBUG_MODE:
                            print("\n" + "="*80)
                            print(f"DEBUG (OLD LOGIC): Attempting 'Buy Poly, Sell Kalshi' on {canonical_name} at {timestamp}")
                            print(f"  Theoretical Spread: Buy Poly @ {best_poly_ask:.4f}, Sell Kalshi @ {best_kalshi_bid:.4f}")
                            print(f"  Flawed Liquidity Calc Trade Size: {trade_size:.2f}")
                            print("  --- ORDER BOOKS BEFORE EXECUTION ---")
                            print(_format_book_for_debug(poly_book, "Polymarket"))
                            print(_format_book_for_debug(kalshi_book, "Kalshi"))
                            print("-" * 80)

                        poly_avg_price, _ = execute_trade_on_book(poly_book, 'ask', trade_size)
                        kalshi_avg_price, _ = execute_trade_on_book(kalshi_book, 'bid', trade_size)
                        final_fees = calculate_kalshi_fee(trade_size, kalshi_avg_price)
                        final_net_profit = (kalshi_avg_price - poly_avg_price) * trade_size - final_fees
                        if final_net_profit > 0:
                            TRADE_ID_COUNTER += 1
                            CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(final_net_profit/trade_size):.4f}", f"{trade_size:.2f}", f"{final_net_profit:.2f}", f"{final_fees:.2f}", canonical_name, 'Polymarket', 'BUY', f"{poly_avg_price:.4f}", canonical_name, 'Kalshi', 'SELL', f"{kalshi_avg_price:.4f}"])
                            logger.info(f"TRADE {TRADE_ID_COUNTER}: Same-outcome arb on {canonical_name}, Size: {trade_size:.2f}, Net Profit: ${final_net_profit:.2f}")

            # --- Opp 2: Buy Kalshi, Sell Poly ---
            best_kalshi_ask = kalshi_book.lowest_ask
            best_poly_bid = poly_book.highest_bid
            if best_kalshi_ask is not None and best_poly_bid is not None and (best_poly_bid - best_kalshi_ask) > 0:
                buy_liquidity = sum(size for price, size in kalshi_book.asks if price == best_poly_bid)
                sell_liquidity = sum(size for price, size in poly_book.bids if price == best_kalshi_ask)
                trade_size = min(buy_liquidity, sell_liquidity)
                if trade_size > 0:
                    estimated_fees = calculate_kalshi_fee(trade_size, best_kalshi_ask)
                    est_net_profit_per_share = ((best_poly_bid - best_kalshi_ask) * trade_size - estimated_fees) / trade_size if trade_size > 0 else 0
                    if est_net_profit_per_share > PROFIT_THRESHOLD:

                        if DEBUG_MODE:
                            print("\n" + "="*80)
                            print(f"DEBUG (OLD LOGIC): Attempting 'Buy Kalshi, Sell Poly' on {canonical_name} at {timestamp}")
                            print(f"  Theoretical Spread: Buy Kalshi @ {best_kalshi_ask:.4f}, Sell Poly @ {best_poly_bid:.4f}")
                            print(f"  Flawed Liquidity Calc Trade Size: {trade_size:.2f}")
                            print("  --- ORDER BOOKS BEFORE EXECUTION ---")
                            print(_format_book_for_debug(kalshi_book, "Kalshi"))
                            print(_format_book_for_debug(poly_book, "Polymarket"))
                            print("-" * 80)
                            
                        kalshi_avg_price, _ = execute_trade_on_book(kalshi_book, 'ask', trade_size)
                        poly_avg_price, _ = execute_trade_on_book(poly_book, 'bid', trade_size)
                        final_fees = calculate_kalshi_fee(trade_size, kalshi_avg_price)
                        final_net_profit = (poly_avg_price - kalshi_avg_price) * trade_size - final_fees
                        if final_net_profit > 0:
                            TRADE_ID_COUNTER += 1
                            CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(final_net_profit/trade_size):.4f}", f"{trade_size:.2f}", f"{final_net_profit:.2f}", f"{final_fees:.2f}", canonical_name, 'Kalshi', 'BUY', f"{kalshi_avg_price:.4f}", canonical_name, 'Polymarket', 'SELL', f"{poly_avg_price:.4f}"])
                            logger.info(f"TRADE {TRADE_ID_COUNTER}: Same-outcome arb on {canonical_name}, Size: {trade_size:.2f}, Net Profit: ${final_net_profit:.2f}")

    logger.info(f"Normal run complete. Found {TRADE_ID_COUNTER} profitable trades.")

def main():
    setup_logging()
    load_market_data()
    initialize_order_books()
    setup_csv_writer() # Always setup writer for normal mode
    
    try:
        run_normal_mode()
    except Exception as e:
        logger.exception("An unexpected error occurred during replay.")
    finally:
        cleanup()

if __name__ == "__main__":
    main()