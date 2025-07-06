import json
import csv
import logging
import math
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
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

# --- << MODE SELECTION >> ---
# 'normal' -> Runs a single backtest with zero delay, outputs detailed trade log.
# 'delay'  -> Runs multiple backtests with different delays, outputs a summary report.
ANALYSIS_MODE = 'delay' # <-- CHANGE THIS TO 'normal' TO RUN THE ORIGINAL SCRIPT

# --- Delay Analysis Configuration (only used if ANALYSIS_MODE is 'delay') ---
DELAY_LEVELS_MS = [0, 50, 100, 250, 500, 1000, 2000, 3000, 4000, 5000, 10000, 50000, 100000] # Delays in milliseconds to test
DELAY_ANALYSIS_CSV_FILE = 'delay_analysis_summary.csv'


# --- << DEBUGGING CONTROL >> ---
DEBUG_MODE = False
DEBUG_OPPORTUNITIES_TO_ANALYZE = 20
# ---

# Output file name
EXECUTED_TRADES_CSV = 'executed_arbitrage_trades_flipped_kalshi_v3.csv'
if DEBUG_MODE:
    pass 


# Profitability and Execution Configuration
PROFIT_THRESHOLD = 0.015 

# --- Global State ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {}
MARKET_MAPPING: Dict[str, Dict[str, str]] = {}
COMPLEMENTARY_MARKET_PAIRS: Dict[str, str] = {}
BIDIRECTIONAL_COMPLEMENTS: Dict[str, str] = {}
CSV_WRITER = None
CSV_FILE = None
TRADE_ID_COUNTER = 0
DEBUG_OPPS_ANALYZED = 0

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
    global ALL_ORDER_BOOKS, REVERSE_MARKET_LOOKUP
    ALL_ORDER_BOOKS.clear()
    REVERSE_MARKET_LOOKUP.clear()
    for canonical_name, platforms in MARKET_MAPPING.items():
        if "polymarket" in platforms: ALL_ORDER_BOOKS[platforms["polymarket"]] = OrderBook(platforms["polymarket"]); REVERSE_MARKET_LOOKUP[platforms["polymarket"]] = canonical_name
        if "kalshi" in platforms: ALL_ORDER_BOOKS[platforms["kalshi"]] = OrderBook(platforms["kalshi"]); REVERSE_MARKET_LOOKUP[platforms["kalshi"]] = canonical_name

def setup_complementary_pairs():
    for key, value in COMPLEMENTARY_MARKET_PAIRS.items():
        BIDIRECTIONAL_COMPLEMENTS[key] = value
        BIDIRECTIONAL_COMPLEMENTS[value] = key

def setup_csv_writer(is_delay_mode=False):
    global CSV_FILE, CSV_WRITER
    if is_delay_mode:
        if os.path.exists(DELAY_ANALYSIS_CSV_FILE): os.remove(DELAY_ANALYSIS_CSV_FILE)
        CSV_FILE = open(DELAY_ANALYSIS_CSV_FILE, 'w', newline='')
        header = ['delay_ms', 'total_trades', 'total_net_profit', 'avg_profit_per_trade', 'same_outcome_trades', 'cross_outcome_trades']
    else:
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

def clean_complementary_key(key: str) -> str:
    return re.sub(r'_\w+$', '', key)

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


# --- Arbitrage Checkers ---

def check_and_execute_cross_outcome_arb(timestamp: str, market_a_name: str, market_b_name: str):
    """
    Checks for and executes a cross-outcome arbitrage trade.
    This involves buying YES on two complementary markets (A and B).
    """
    global TRADE_ID_COUNTER

    # 1. Find best ask for Market A across its platforms
    best_ask_a, platform_a, book_a = float('inf'), None, None
    for p_name, p_id in MARKET_MAPPING.get(market_a_name, {}).items():
        b = ALL_ORDER_BOOKS.get(p_id)
        if b and b.lowest_ask is not None and b.lowest_ask < best_ask_a:
            best_ask_a, platform_a, book_a = b.lowest_ask, p_name.capitalize(), b
            
    # 2. Find best ask for Market B across its platforms
    best_ask_b, platform_b, book_b = float('inf'), None, None
    for p_name, p_id in MARKET_MAPPING.get(market_b_name, {}).items():
        b = ALL_ORDER_BOOKS.get(p_id)
        if b and b.lowest_ask is not None and b.lowest_ask < best_ask_b:
            best_ask_b, platform_b, book_b = b.lowest_ask, p_name.capitalize(), b

    if not all([book_a, book_b]): return

    # 3. Check for opportunity: total cost must be less than 1.0
    if best_ask_a + best_ask_b >= 1.0 - PROFIT_THRESHOLD: return

    # 4. Calculate available liquidity (conservative estimate)
    liquidity_a = sum(size for price, size in book_a.asks if price + best_ask_b < 1.0)
    liquidity_b = sum(size for price, size in book_b.asks if best_ask_a + price < 1.0)
    theoretical_trade_size = min(liquidity_a, liquidity_b)

    if theoretical_trade_size <= 0: return

    # 5. Simulate execution to get actual filled size and prices
    avg_price_a, executed_a = execute_trade_on_book(book_a, 'ask', theoretical_trade_size)
    avg_price_b, executed_b = execute_trade_on_book(book_b, 'ask', theoretical_trade_size)
    
    # 6. The actual trade size is the minimum of what was filled on both legs
    actual_trade_size = min(executed_a, executed_b)

    # 7. Only proceed if we actually traded something on both sides
    if actual_trade_size > 0:
        # Calculate fees and profit based on the *actual* trade size
        fees_a = calculate_kalshi_fee(actual_trade_size, avg_price_a) if platform_a == 'Kalshi' else 0.0
        fees_b = calculate_kalshi_fee(actual_trade_size, avg_price_b) if platform_b == 'Kalshi' else 0.0
        total_fees = fees_a + fees_b
        
        # NOTE: The avg_prices are for the originally requested size, not the final actual_trade_size.
        # This is an acceptable approximation for backtesting. A perfectly accurate model
        # would require re-calculating the avg_price for the smaller actual_trade_size.
        net_profit = (actual_trade_size * 1.0) - (actual_trade_size * (avg_price_a + avg_price_b)) - total_fees
        
        # 8. Log to CSV if profitable
        if net_profit > 0:
            TRADE_ID_COUNTER += 1
            CSV_WRITER.writerow([
                TRADE_ID_COUNTER, timestamp, 'cross_outcome', f"{(net_profit/actual_trade_size):.4f}", f"{actual_trade_size:.2f}",
                f"{net_profit:.2f}", f"{total_fees:.2f}", market_a_name, platform_a, 'BUY', f"{avg_price_a:.4f}",
                market_b_name, platform_b, 'BUY', f"{avg_price_b:.4f}"
            ])
            logger.info(f"TRADE {TRADE_ID_COUNTER}: Cross-outcome arb {market_a_name} & {market_b_name}, Size: {actual_trade_size:.2f}, Profit: ${net_profit:.2f}")


# --- Debugging Function ---
def debug_first_arbitrage_opportunities():
    global DEBUG_OPPS_ANALYZED
    
    with open(JSONL_FILE_PATH, 'r') as f:
        logger.info(f"Starting FOCUSED DEBUG analysis with pre-fee estimation...")
        for i, line in enumerate(f):
            if DEBUG_OPPS_ANALYZED >= DEBUG_OPPORTUNITIES_TO_ANALYZE:
                logger.info(f"Analyzed {DEBUG_OPPORTUNITIES_TO_ANALYZE} promising opportunities. Stopping."); return

            try:
                log_entry = json.loads(line)
                process_log_entry(log_entry)
            except json.JSONDecodeError: continue

            for canonical_name in MARKET_MAPPING.keys():
                if DEBUG_OPPS_ANALYZED >= DEBUG_OPPORTUNITIES_TO_ANALYZE: break
                
                poly_book, kalshi_book = get_paired_books(canonical_name)
                if not (poly_book and kalshi_book and poly_book.bids and poly_book.asks and kalshi_book.bids and kalshi_book.asks): continue

                # --- Opportunity 1: Buy Poly, Sell Kalshi ---
                best_poly_ask = poly_book.lowest_ask
                best_kalshi_bid = kalshi_book.highest_bid
                if best_poly_ask is not None and best_kalshi_bid is not None and (best_kalshi_bid - best_poly_ask) > 0:
                    buy_liquidity = sum(size for price, size in poly_book.asks if price <= best_kalshi_bid)
                    sell_liquidity = sum(size for price, size in kalshi_book.bids if price >= best_poly_ask)
                    trade_size = min(buy_liquidity, sell_liquidity)
                    if trade_size > 0:
                        estimated_gross_profit = (best_kalshi_bid - best_poly_ask) * trade_size
                        estimated_fees = calculate_kalshi_fee(trade_size, best_kalshi_bid)
                        est_net_profit_per_share = (estimated_gross_profit - estimated_fees) / trade_size if trade_size > 0 else 0
                        if est_net_profit_per_share > PROFIT_THRESHOLD:
                            DEBUG_OPPS_ANALYZED += 1; print(f"\n{'='*80}\nDEBUG #{DEBUG_OPPS_ANALYZED}: Same-Outcome Arb (Buy Poly, Sell Kalshi)\nTimestamp: {log_entry.get('ts')} | Market: {canonical_name}\n{'-'*80}")
                            print(f"Initial Find: Buy Poly @ {best_poly_ask:.4f}, Sell Kalshi @ {best_kalshi_bid:.4f}. Spread: {(best_kalshi_bid - best_poly_ask):.4f}")
                            print(f"Liquidity & Pre-Fee Check: Trade Size={trade_size:.2f}, Est. Net Profit/Share=${est_net_profit_per_share:.4f} -> PASSED")
                            print(f"{'-'*20} SIMULATING EXECUTION {'-'*20}")
                            temp_poly_book_state, temp_kalshi_book_state = (list(poly_book.bids), list(poly_book.asks)), (list(kalshi_book.bids), list(kalshi_book.asks))
                            poly_avg_price, _ = execute_trade_on_book(poly_book, 'ask', trade_size); kalshi_avg_price, _ = execute_trade_on_book(kalshi_book, 'bid', trade_size)
                            final_fees = calculate_kalshi_fee(trade_size, kalshi_avg_price); final_net_profit = (kalshi_avg_price - poly_avg_price) * trade_size - final_fees
                            print(f"Executed Buy on Poly: Avg Price={poly_avg_price:.4f} (Slippage: {(poly_avg_price - best_poly_ask):.4f})")
                            print(f"Executed Sell on Kalshi: Avg Price={kalshi_avg_price:.4f} (Slippage: {(best_kalshi_bid - kalshi_avg_price):.4f})")
                            print(f"Post-Execution: Final Fees=${final_fees:.2f}, Final Net Profit=${final_net_profit:.2f}")
                            print(f"CONCLUSION: This trade would have been {'PROFITABLE' if final_net_profit > 0 else 'UNPROFITABLE'}")
                            poly_book._bids, poly_book._asks = dict(temp_poly_book_state[0]), dict(temp_poly_book_state[1]); kalshi_book._bids, kalshi_book._asks = dict(temp_kalshi_book_state[0]), dict(temp_kalshi_book_state[1])
                if DEBUG_OPPS_ANALYZED >= DEBUG_OPPORTUNITIES_TO_ANALYZE: break
                
                # --- Opportunity 2: Buy Kalshi, Sell Poly ---
                best_kalshi_ask = kalshi_book.lowest_ask
                best_poly_bid = poly_book.highest_bid
                if best_kalshi_ask is not None and best_poly_bid is not None and (best_poly_bid - best_kalshi_ask) > 0:
                    buy_liquidity = sum(size for price, size in kalshi_book.asks if price <= best_poly_bid)
                    sell_liquidity = sum(size for price, size in poly_book.bids if price >= best_kalshi_ask)
                    trade_size = min(buy_liquidity, sell_liquidity)
                    if trade_size > 0:
                        estimated_gross_profit = (best_poly_bid - best_kalshi_ask) * trade_size
                        estimated_fees = calculate_kalshi_fee(trade_size, best_kalshi_ask)
                        est_net_profit_per_share = (estimated_gross_profit - estimated_fees) / trade_size if trade_size > 0 else 0
                        if est_net_profit_per_share > PROFIT_THRESHOLD:
                            DEBUG_OPPS_ANALYZED += 1; print(f"\n{'='*80}\nDEBUG #{DEBUG_OPPS_ANALYZED}: Same-Outcome Arb (Buy Kalshi, Sell Poly)\nTimestamp: {log_entry.get('ts')} | Market: {canonical_name}\n{'-'*80}")
                            print(f"Initial Find: Buy Kalshi @ {best_kalshi_ask:.4f}, Sell Poly @ {best_poly_bid:.4f}. Spread: {(best_poly_bid - best_kalshi_ask):.4f}")
                            print(f"Liquidity & Pre-Fee Check: Trade Size={trade_size:.2f}, Est. Net Profit/Share=${est_net_profit_per_share:.4f} -> PASSED")
                            print(f"{'-'*20} SIMULATING EXECUTION {'-'*20}")
                            temp_poly_book_state, temp_kalshi_book_state = (list(poly_book.bids), list(poly_book.asks)), (list(kalshi_book.bids), list(kalshi_book.asks))
                            kalshi_avg_price, _ = execute_trade_on_book(kalshi_book, 'ask', trade_size); poly_avg_price, _ = execute_trade_on_book(poly_book, 'bid', trade_size)
                            final_fees = calculate_kalshi_fee(trade_size, kalshi_avg_price); final_net_profit = (poly_avg_price - kalshi_avg_price) * trade_size - final_fees
                            print(f"Executed Buy on Kalshi: Avg Price={kalshi_avg_price:.4f} (Slippage: {(kalshi_avg_price - best_kalshi_ask):.4f})")
                            print(f"Executed Sell on Poly: Avg Price={poly_avg_price:.4f} (Slippage: {(best_poly_bid - poly_avg_price):.4f})")
                            print(f"Post-Execution: Final Fees=${final_fees:.2f}, Final Net Profit=${final_net_profit:.2f}")
                            print(f"CONCLUSION: This trade would have been {'PROFITABLE' if final_net_profit > 0 else 'UNPROFITABLE'}")
                            poly_book._bids, poly_book._asks = dict(temp_poly_book_state[0]), dict(temp_poly_book_state[1]); kalshi_book._bids, kalshi_book._asks = dict(temp_kalshi_book_state[0]), dict(temp_kalshi_book_state[1])
            if DEBUG_OPPS_ANALYZED >= DEBUG_OPPORTUNITIES_TO_ANALYZE: break


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
    logger.info("Running in NORMAL mode.")
    global TRADE_ID_COUNTER

    with open(JSONL_FILE_PATH, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if (i+1) % 50000 == 0: logger.info(f"Processed {i+1}/{len(lines)} lines...")
            
            try: log_entry = json.loads(line)
            except json.JSONDecodeError: continue

            process_log_entry(log_entry)
            
            timestamp = log_entry.get('ts')
            updated_market_name = log_entry.get('name')
            if not (timestamp and updated_market_name): continue

            # --- Opp 1 & 2: Same-Outcome Arbitrage ---
            poly_book, kalshi_book = get_paired_books(updated_market_name)
            if poly_book and kalshi_book:
                # Buy Poly, Sell Kalshi
                best_poly_ask = poly_book.lowest_ask
                best_kalshi_bid = kalshi_book.highest_bid
                if best_poly_ask is not None and best_kalshi_bid is not None and (best_kalshi_bid - best_poly_ask) > 0:
                    buy_liquidity = sum(size for price, size in poly_book.asks if price <= best_kalshi_bid)
                    sell_liquidity = sum(size for price, size in kalshi_book.bids if price >= best_poly_ask)
                    theoretical_trade_size = min(buy_liquidity, sell_liquidity)
                    if theoretical_trade_size > 0:
                        est_fees = calculate_kalshi_fee(theoretical_trade_size, best_kalshi_bid)
                        est_net_profit_per_share = ((best_kalshi_bid - best_poly_ask) * theoretical_trade_size - est_fees) / theoretical_trade_size if theoretical_trade_size > 0 else 0
                        if est_net_profit_per_share > PROFIT_THRESHOLD:
                            poly_avg_price, executed_poly = execute_trade_on_book(poly_book, 'ask', theoretical_trade_size)
                            kalshi_avg_price, executed_kalshi = execute_trade_on_book(kalshi_book, 'bid', theoretical_trade_size)
                            actual_trade_size = min(executed_poly, executed_kalshi)
                            if actual_trade_size > 0:
                                final_fees = calculate_kalshi_fee(actual_trade_size, kalshi_avg_price)
                                final_net_profit = (kalshi_avg_price - poly_avg_price) * actual_trade_size - final_fees
                                if final_net_profit > 0:
                                    TRADE_ID_COUNTER += 1
                                    CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(final_net_profit/actual_trade_size):.4f}", f"{actual_trade_size:.2f}", f"{final_net_profit:.2f}", f"{final_fees:.2f}", updated_market_name, 'Polymarket', 'BUY', f"{poly_avg_price:.4f}", updated_market_name, 'Kalshi', 'SELL', f"{kalshi_avg_price:.4f}"])
                                    logger.info(f"TRADE {TRADE_ID_COUNTER}: Same-outcome arb on {updated_market_name}, Size: {actual_trade_size:.2f}, Net Profit: ${final_net_profit:.2f}")

                # Buy Kalshi, Sell Poly
                best_kalshi_ask = kalshi_book.lowest_ask
                best_poly_bid = poly_book.highest_bid
                if best_kalshi_ask is not None and best_poly_bid is not None and (best_poly_bid - best_kalshi_ask) > 0:
                    buy_liquidity = sum(size for price, size in kalshi_book.asks if price <= best_poly_bid)
                    sell_liquidity = sum(size for price, size in poly_book.bids if price >= best_kalshi_ask)
                    theoretical_trade_size = min(buy_liquidity, sell_liquidity)
                    if theoretical_trade_size > 0:
                        est_fees = calculate_kalshi_fee(theoretical_trade_size, best_kalshi_ask)
                        est_net_profit_per_share = ((best_poly_bid - best_kalshi_ask) * theoretical_trade_size - est_fees) / theoretical_trade_size if theoretical_trade_size > 0 else 0
                        if est_net_profit_per_share > PROFIT_THRESHOLD:
                            kalshi_avg_price, executed_kalshi = execute_trade_on_book(kalshi_book, 'ask', theoretical_trade_size)
                            poly_avg_price, executed_poly = execute_trade_on_book(poly_book, 'bid', theoretical_trade_size)
                            actual_trade_size = min(executed_kalshi, executed_poly)
                            if actual_trade_size > 0:
                                final_fees = calculate_kalshi_fee(actual_trade_size, kalshi_avg_price)
                                final_net_profit = (poly_avg_price - kalshi_avg_price) * actual_trade_size - final_fees
                                if final_net_profit > 0:
                                    TRADE_ID_COUNTER += 1
                                    CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(final_net_profit/actual_trade_size):.4f}", f"{actual_trade_size:.2f}", f"{final_net_profit:.2f}", f"{final_fees:.2f}", updated_market_name, 'Kalshi', 'BUY', f"{kalshi_avg_price:.4f}", updated_market_name, 'Polymarket', 'SELL', f"{poly_avg_price:.4f}"])
                                    logger.info(f"TRADE {TRADE_ID_COUNTER}: Same-outcome arb on {updated_market_name}, Size: {actual_trade_size:.2f}, Net Profit: ${final_net_profit:.2f}")

            # --- Opp 3: Cross-Outcome Arbitrage ---
            complementary_name = BIDIRECTIONAL_COMPLEMENTS.get(updated_market_name)
            # Ensure we only check each pair once by enforcing an order
            if complementary_name and updated_market_name < complementary_name:
                check_and_execute_cross_outcome_arb(timestamp, updated_market_name, complementary_name)


    logger.info(f"Normal run complete. Found {TRADE_ID_COUNTER} profitable trades.")

# --- DELAY ANALYSIS MODE: Functions ---

def find_and_execute_opportunities(timestamp: str, market_name: str, backtest_stats: Dict):
    """Finds and executes opportunities for a given market at a specific timestamp."""
    # --- Opp 1 & 2: Same-Outcome Arbitrage ---
    poly_book, kalshi_book = MARKET_MAPPING.get(market_name, {}).get("polymarket"), MARKET_MAPPING.get(market_name, {}).get("kalshi")
    if poly_book and kalshi_book and poly_book in ALL_ORDER_BOOKS and kalshi_book in ALL_ORDER_BOOKS:
        pb, kb = ALL_ORDER_BOOKS[poly_book], ALL_ORDER_BOOKS[kalshi_book]
        
        # Buy Poly, Sell Kalshi
        best_poly_ask = pb.lowest_ask
        best_kalshi_bid = kb.highest_bid
        if best_poly_ask is not None and best_kalshi_bid is not None and (best_kalshi_bid - best_poly_ask) > PROFIT_THRESHOLD:
            buy_liquidity = sum(s for p, s in pb.asks if p <= best_kalshi_bid)
            sell_liquidity = sum(s for p, s in kb.bids if p >= best_poly_ask)
            theoretical_trade_size = min(buy_liquidity, sell_liquidity)
            if theoretical_trade_size > 0:
                poly_avg_price, executed_poly = execute_trade_on_book(pb, 'ask', theoretical_trade_size)
                kalshi_avg_price, executed_kalshi = execute_trade_on_book(kb, 'bid', theoretical_trade_size)
                actual_trade_size = min(executed_poly, executed_kalshi)
                if actual_trade_size > 0:
                    fees = calculate_kalshi_fee(actual_trade_size, kalshi_avg_price)
                    net_profit = (kalshi_avg_price - poly_avg_price) * actual_trade_size - fees
                    if net_profit > 0:
                        backtest_stats['total_trades'] += 1; backtest_stats['same_outcome_trades'] += 1; backtest_stats['total_net_profit'] += net_profit
        
        # Buy Kalshi, Sell Poly
        best_kalshi_ask = kb.lowest_ask
        best_poly_bid = pb.highest_bid
        if best_kalshi_ask is not None and best_poly_bid is not None and (best_poly_bid - best_kalshi_ask) > PROFIT_THRESHOLD:
            buy_liquidity = sum(s for p, s in kb.asks if p <= best_poly_bid)
            sell_liquidity = sum(s for p, s in pb.bids if p >= best_kalshi_ask)
            theoretical_trade_size = min(buy_liquidity, sell_liquidity)
            if theoretical_trade_size > 0:
                kalshi_avg_price, executed_kalshi = execute_trade_on_book(kb, 'ask', theoretical_trade_size)
                poly_avg_price, executed_poly = execute_trade_on_book(pb, 'bid', theoretical_trade_size)
                actual_trade_size = min(executed_kalshi, executed_poly)
                if actual_trade_size > 0:
                    fees = calculate_kalshi_fee(actual_trade_size, kalshi_avg_price)
                    net_profit = (poly_avg_price - kalshi_avg_price) * actual_trade_size - fees
                    if net_profit > 0:
                        backtest_stats['total_trades'] += 1; backtest_stats['same_outcome_trades'] += 1; backtest_stats['total_net_profit'] += net_profit

    # --- Opp 3: Cross-Outcome Arbitrage ---
    complementary_name = BIDIRECTIONAL_COMPLEMENTS.get(market_name)
    if complementary_name and market_name < complementary_name:
        market_a_name, market_b_name = market_name, complementary_name
        
        best_ask_a, platform_a, book_a = float('inf'), None, None
        for p_name, p_id in MARKET_MAPPING.get(market_a_name, {}).items():
            b = ALL_ORDER_BOOKS.get(p_id)
            if b and b.lowest_ask is not None and b.lowest_ask < best_ask_a: best_ask_a, platform_a, book_a = b.lowest_ask, p_name.capitalize(), b

        best_ask_b, platform_b, book_b = float('inf'), None, None
        for p_name, p_id in MARKET_MAPPING.get(market_b_name, {}).items():
            b = ALL_ORDER_BOOKS.get(p_id)
            if b and b.lowest_ask is not None and b.lowest_ask < best_ask_b: best_ask_b, platform_b, book_b = b.lowest_ask, p_name.capitalize(), b

        if book_a and book_b and (best_ask_a + best_ask_b) < 1.0 - PROFIT_THRESHOLD:
            liquidity_a = sum(s for p, s in book_a.asks if p + best_ask_b < 1.0)
            liquidity_b = sum(s for p, s in book_b.asks if best_ask_a + p < 1.0)
            theoretical_trade_size = min(liquidity_a, liquidity_b)
            if theoretical_trade_size > 0:
                avg_price_a, executed_a = execute_trade_on_book(book_a, 'ask', theoretical_trade_size)
                avg_price_b, executed_b = execute_trade_on_book(book_b, 'ask', theoretical_trade_size)
                actual_trade_size = min(executed_a, executed_b)
                if actual_trade_size > 0:
                    fees_a = calculate_kalshi_fee(actual_trade_size, avg_price_a) if platform_a == 'Kalshi' else 0.0
                    fees_b = calculate_kalshi_fee(actual_trade_size, avg_price_b) if platform_b == 'Kalshi' else 0.0
                    net_profit = (actual_trade_size * 1.0) - (actual_trade_size * (avg_price_a + avg_price_b)) - (fees_a + fees_b)
                    if net_profit > 0:
                        backtest_stats['total_trades'] += 1; backtest_stats['cross_outcome_trades'] += 1; backtest_stats['total_net_profit'] += net_profit

def perform_single_delay_backtest(delay_ms: int, all_entries: List[Dict]) -> Dict:
    """Performs a full backtest for a single specified execution delay."""
    logger.info(f"--- Starting backtest for delay: {delay_ms} ms ---")
    initialize_order_books()
    
    backtest_stats = {
        'total_trades': 0, 'total_net_profit': 0.0,
        'same_outcome_trades': 0, 'cross_outcome_trades': 0
    }
    
    pending_trades = [] # Stores tuples of (execution_datetime, market_name_to_check)

    for entry in all_entries:
        current_ts_str = entry['ts']
        current_dt = datetime.fromisoformat(current_ts_str.replace('Z', '+00:00'))

        # 1. Execute any pending trades that have "matured"
        # We iterate through a copy, as we might modify the list
        for trade_dt, market_name in list(pending_trades):
            if current_dt >= trade_dt:
                find_and_execute_opportunities(current_ts_str, market_name, backtest_stats)
                pending_trades.remove((trade_dt, market_name))

        # 2. Update the order book state with the current entry
        process_log_entry(entry)

        # 3. Find new opportunities and schedule them for future execution
        execution_dt = current_dt + timedelta(milliseconds=delay_ms)
        market_name = entry.get('name')
        if market_name:
            # We add the opportunity to be checked at the future timestamp.
            # This simulates finding the arb now, but only being able to act on it later.
            pending_trades.append((execution_dt, market_name))

    logger.info(f"Finished backtest for delay: {delay_ms} ms. Found {backtest_stats['total_trades']} trades, for a total profit of ${backtest_stats['total_net_profit']:.2f}.")
    return backtest_stats

def run_delay_analysis():
    """Main function for the delay analysis mode."""
    logger.info("Running in DELAY ANALYSIS mode.")
    logger.info(f"Loading all log entries from {JSONL_FILE_PATH} into memory... (This may take a moment)")
    
    try:
        with open(JSONL_FILE_PATH, 'r') as f:
            all_entries = [json.loads(line) for line in f]
        logger.info(f"Successfully loaded {len(all_entries)} log entries.")
    except Exception as e:
        logger.error(f"Failed to load log file: {e}")
        return

    setup_csv_writer(is_delay_mode=True)
    
    for delay in DELAY_LEVELS_MS:
        stats = perform_single_delay_backtest(delay, all_entries)
        
        total_trades = stats['total_trades']
        total_profit = stats['total_net_profit']
        avg_profit = (total_profit / total_trades) if total_trades > 0 else 0.0
        
        CSV_WRITER.writerow([
            delay,
            total_trades,
            f"{total_profit:.2f}",
            f"{avg_profit:.4f}",
            stats['same_outcome_trades'],
            stats['cross_outcome_trades']
        ])
        CSV_FILE.flush() # Ensure data is written immediately

    logger.info(f"Delay analysis complete. Summary results saved to {DELAY_ANALYSIS_CSV_FILE}")

# --- Main Execution ---
def main():
    setup_logging()
    load_market_data()
    setup_complementary_pairs()
    
    try:
        if ANALYSIS_MODE == 'delay':
            run_delay_analysis()
        elif ANALYSIS_MODE == 'normal':
            run_normal_mode()
        else:
            logger.error(f"Invalid ANALYSIS_MODE: '{ANALYSIS_MODE}'. Choose 'normal' or 'delay'.")
    except Exception as e:
        logger.exception("An unexpected error occurred during replay.")
    finally:
        cleanup()

if __name__ == "__main__":
    main()
