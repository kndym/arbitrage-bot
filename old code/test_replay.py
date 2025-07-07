import json
import csv
import logging
import math
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
import os
import copy

# --- New Dependency ---
# pip install tqdm
from tqdm import tqdm


# --- SELF-CONTAINED ORDER BOOK and RECREATE LOGIC (ORIGINAL DATA STRUCTURE) ---
class OrderBook:
    """
    The original OrderBook implementation using standard Python dicts.
    Accessing .bids or .asks is an O(N log N) operation due to sorting.
    """
    def __init__(self, market_id: str):
        self.market_id = market_id
        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}

    @property
    def bids(self) -> List[Tuple[float, float]]:
        return sorted(self._bids.items(), key=lambda x: x[0], reverse=True)

    @property
    def asks(self) -> List[Tuple[float, float]]:
        return sorted(self._asks.items(), key=lambda x: x[0])

    @property
    def highest_bid(self) -> Tuple[float, float] | None:
        """Returns the (price, size) of the highest bid, or None if empty."""
        sorted_bids = self.bids
        return sorted_bids[0] if sorted_bids else None

    @property
    def lowest_ask(self) -> Tuple[float, float] | None:
        """Returns the (price, size) of the lowest ask, or None if empty."""
        sorted_asks = self.asks
        return sorted_asks[0] if sorted_asks else None

    def _update_book_level(self, side: str, price: float, size: float):
        book_side = self._bids if side == 'bid' else self._asks
        if size > 1e-9:
            book_side[price] = size
        elif price in book_side:
            del book_side[price]

# --- Constants ---
PLATFORM_POLY = 'Polymarket'
PLATFORM_KALSHI = 'Kalshi'
SIDE_BID = 'bid'
SIDE_ASK = 'ask'
SIDE_BUY = 'BUY' # Polymarket specific
SIDE_YES = 'yes' # Kalshi specific
SIDE_NO = 'no'   # Kalshi specific

def robust_update_polymarket_order_book(book: OrderBook, data: Dict[str, Any]):
    event_type = data.get("event_type")
    if event_type == "book":
        book._bids.clear(); book._asks.clear()
        for bid in data.get("changes", {}).get("bids", []): book._update_book_level(SIDE_BID, float(bid['price']), float(bid['size']))
        for ask in data.get("changes", {}).get("asks", []): book._update_book_level(SIDE_ASK, float(ask['price']), float(ask['size']))
    elif event_type == "delta":
        for change in data.get("changes", []):
            side = SIDE_BID if change['side'] == SIDE_BUY else SIDE_ASK
            book._update_book_level(side, float(change['price']), float(change['size']))

def robust_update_kalshi_order_book(book: OrderBook, data: Dict[str, Any]):
    try:
        if SIDE_YES in data and SIDE_NO in data:
            book._bids.clear(); book._asks.clear()
            for price_cents, size in data.get(SIDE_YES, []): book._update_book_level(SIDE_BID, float(price_cents) / 100.0, float(size))
            for price_cents, size in data.get(SIDE_NO, []):
                ask_price = round(1.0 - (float(price_cents) / 100.0), 2)
                book._update_book_level(SIDE_ASK, ask_price, float(size))
        elif "price" in data and "delta" in data:
            price_cents, delta, side = int(data["price"]), float(data["delta"]), data["side"]
            if side == SIDE_YES:
                price = price_cents / 100.0
                book._update_book_level(SIDE_BID, price, book._bids.get(price, 0) + delta)
            elif side == SIDE_NO:
                ask_price = round(1.0 - (price_cents / 100.0), 2)
                book._update_book_level(SIDE_ASK, ask_price, book._asks.get(ask_price, 0) + delta)
    except Exception as e:
        logging.error(f"CRITICAL ERROR processing Kalshi data: {data} -> {e}")

# --- Configuration ---
LOG_LEVEL = logging.INFO
JSONL_FILE_PATH = 'order_book_deltas_jul_5_v2.jsonl'
MARKETS_FILE = 'markets.json'
COMP_FILE = 'compliment.json'
PROFIT_THRESHOLD = 0.015
ANALYSIS_MODE = 'delay' # <-- SET 'delay' FOR THE NEW MODE, 'normal' for original mode
DEBUG_MODE = False # Recommended to be False for delay mode to avoid excessive output
TARGETED_DEBUG_CONFIG = {
    'enabled': False,
    'market_name': 'Houston vs Los Angeles (LAD)',
    'timestamp_contains': '2025-07-06T02:03:11.650'
}
EXECUTED_TRADES_CSV = 'executed_arbitrage_trades_flipped_kalshi_v3.csv'
DELAY_MODE_CSV = 'delay_analysis_summary222.csv' # <-- New output file for delay mode
MARKET_MAPPING: Dict[str, Dict[str, str]] = {}
TRADE_ID_COUNTER = 0

# --- Setup and Helper Functions ---
def setup_logging(): logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
def load_market_data():
    global MARKET_MAPPING
    with open(MARKETS_FILE) as f: MARKET_MAPPING = json.load(f)
def setup_csv_writer():
    global CSV_FILE, CSV_WRITER
    if os.path.exists(EXECUTED_TRADES_CSV): os.remove(EXECUTED_TRADES_CSV)
    CSV_FILE = open(EXECUTED_TRADES_CSV, 'w', newline='')
    header = ['trade_id','timestamp','arbitrage_type','net_profit_per_share','trade_size','total_net_profit','fees_paid','market_a','platform_a','side_a','avg_price_a','market_b','platform_b','side_b','avg_price_b']
    CSV_WRITER = csv.writer(CSV_FILE); CSV_WRITER.writerow(header)
def cleanup():
    if 'CSV_FILE' in globals() and CSV_FILE and not CSV_FILE.closed: CSV_FILE.close()
def calculate_kalshi_fee(trade_size: float, price: float) -> float:
    if trade_size <= 0 or price <= 0 or price >= 1: return 0.0
    return math.ceil(0.07 * trade_size * price * (1.0 - price) * 100) / 100.0

def execute_trade_on_book(book: OrderBook, side_to_hit: str, size_to_trade: float) -> tuple[float, float]:
    if size_to_trade <= 0 or not book: return 0.0, 0.0
    levels_to_hit = book.asks if side_to_hit == SIDE_ASK else book.bids
    if not levels_to_hit: return 0.0, 0.0
    size_executed, total_cost, remaining_size = 0.0, 0.0, size_to_trade
    # We iterate over a copy of the list because we are modifying the underlying dict
    for price, available_size in list(levels_to_hit):
        if remaining_size <= 1e-9: break
        size_at_this_level = min(remaining_size, available_size)
        book._update_book_level(SIDE_BID if side_to_hit == SIDE_BID else SIDE_ASK, price, available_size - size_at_this_level)
        size_executed += size_at_this_level; total_cost += size_at_this_level * price; remaining_size -= size_at_this_level
    avg_price = (total_cost / size_executed) if size_executed > 0 else 0.0
    return avg_price, size_executed

def _format_book_for_debug(book: OrderBook, name: str) -> str:
    if not book: return f"  {name}: [Book Not Found in Dict]\n"
    if not book._bids and not book._asks: return f"  {name} ({book.market_id}): [Book is Empty]\n"
    return f"  {name} ({book.market_id}):\n    Asks: {book.asks[:5]}\n    Bids: {book.bids[:5]}\n"

# --- CORE LOGIC ---
def process_log_entry(log_entry: Dict[str, Any], order_books: Dict[str, OrderBook]):
    canonical_name = log_entry.get("name")
    if not canonical_name: return
    if "pm_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("polymarket")
        if market_id and market_id in order_books: robust_update_polymarket_order_book(order_books[market_id], log_entry["pm_delta"])
    elif "ks_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("kalshi")
        if market_id and market_id in order_books: robust_update_kalshi_order_book(order_books[market_id], log_entry["ks_delta"])

def find_opportunities(current_order_books: Dict[str, OrderBook]) -> List[Dict]:
    """Optimized function to find arbitrage opportunities without nested loops."""
    opportunities = []
    for market_name, platforms in MARKET_MAPPING.items():
        poly_id, kalshi_id = platforms.get("polymarket"), platforms.get("kalshi")
        if not (poly_id and kalshi_id and poly_id in current_order_books and kalshi_id in current_order_books):
            continue
        
        pb, kb = current_order_books[poly_id], current_order_books[kalshi_id]
        
        # Opportunity: Buy Polymarket (at their lowest ask), Sell Kalshi (at their highest bid)
        poly_ask, kalshi_bid = pb.lowest_ask, kb.highest_bid
        if poly_ask and kalshi_bid:
            ask_price, ask_size = poly_ask
            bid_price, bid_size = kalshi_bid
            spread = bid_price - ask_price
            if spread > PROFIT_THRESHOLD:
                size = min(ask_size, bid_size)
                if size > 0: opportunities.append({'type': 'same_outcome', 'market_name': market_name, 'buy_id': poly_id, 'sell_id': kalshi_id, 'size': size, 'buy_platform': PLATFORM_POLY, 'sell_platform': PLATFORM_KALSHI, 'spread': spread})

        # Opportunity: Buy Kalshi (at their lowest ask), Sell Polymarket (at their highest bid)
        kalshi_ask, poly_bid = kb.lowest_ask, pb.highest_bid
        if kalshi_ask and poly_bid:
            ask_price, ask_size = kalshi_ask
            bid_price, bid_size = poly_bid
            spread = bid_price - ask_price
            if spread > PROFIT_THRESHOLD:
                size = min(ask_size, bid_size)
                if size > 0: opportunities.append({'type': 'same_outcome', 'market_name': market_name, 'buy_id': kalshi_id, 'sell_id': poly_id, 'size': size, 'buy_platform': PLATFORM_KALSHI, 'sell_platform': PLATFORM_POLY, 'spread': spread})
    
    if opportunities:
        opportunities.sort(key=lambda x: x['spread'], reverse=True)
        for opp in opportunities: del opp['spread'] # Clean up temporary key
    return opportunities


def _execute_and_log_opportunity(opportunity: Dict, order_books: Dict[str, OrderBook], timestamp: str):
    global TRADE_ID_COUNTER, DEBUG_MODE, CSV_WRITER
    buy_book, sell_book = order_books.get(opportunity['buy_id']), order_books.get(opportunity['sell_id'])
    if DEBUG_MODE:
        print("\n" + "="*80 + f"\nEXECUTION DEBUG: Attempting {opportunity['type']} arbitrage at {timestamp}")
        print(f"  Opportunity: Buy {opportunity['market_name']} on {opportunity['buy_platform']} & Sell on {opportunity['sell_platform']}")
        print("  --- ORDER BOOKS BEFORE ---")
        print(_format_book_for_debug(buy_book, f"BUY BOOK ({opportunity['buy_platform']})"))
        print(_format_book_for_debug(sell_book, f"SELL BOOK ({opportunity['sell_platform']})"))
    if not (buy_book and sell_book):
        if DEBUG_MODE: print("  CONCLUSION: FAILED - One or both order books are missing.")
        return
    avg_buy_price, executed_buy = execute_trade_on_book(buy_book, SIDE_ASK, opportunity['size'])
    avg_sell_price, executed_sell = execute_trade_on_book(sell_book, SIDE_BID, opportunity['size'])
    actual_size = min(executed_buy, executed_sell)
    if DEBUG_MODE:
        print("  --- EXECUTION ---" + f"\n    Attempted Size: {opportunity['size']:.2f}" + f"\n    Buy Leg Executed: {executed_buy:.2f} shares @ avg ${avg_buy_price:.4f}" + f"\n    Sell Leg Executed: {executed_sell:.2f} shares @ avg ${avg_sell_price:.4f}" + f"\n    Final Trade Size (min of legs): {actual_size:.2f}")
    if actual_size > 0:
        fees = 0.0
        if opportunity['buy_platform'] == PLATFORM_KALSHI:
            fees = calculate_kalshi_fee(actual_size, avg_buy_price)
        elif opportunity['sell_platform'] == PLATFORM_KALSHI:
            fees = calculate_kalshi_fee(actual_size, avg_sell_price)
        
        net_profit = (avg_sell_price - avg_buy_price) * actual_size - fees
        if DEBUG_MODE: print("  --- RESULT ---" + f"\n    Gross Profit: ${((avg_sell_price - avg_buy_price) * actual_size):.4f}" + f"\n    Fees: ${fees:.4f}" + f"\n    Net Profit: ${net_profit:.4f}")
        if net_profit > 0:
            if DEBUG_MODE: print("  CONCLUSION: PROFITABLE TRADE")
            TRADE_ID_COUNTER += 1
            CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(net_profit/actual_size):.4f}", f"{actual_size:.2f}", f"{net_profit:.2f}", f"{fees:.2f}", opportunity['market_name'], opportunity['buy_platform'], 'BUY', f"{avg_buy_price:.4f}", opportunity['market_name'], opportunity['sell_platform'], 'SELL', f"{avg_sell_price:.4f}"])
        elif DEBUG_MODE: print(f"  CONCLUSION: FAILED - Net profit (${net_profit:.4f}) is not positive.")
    elif DEBUG_MODE: print("  CONCLUSION: FAILED - Zero liquidity executed.")

def run_normal_mode():
    global TRADE_ID_COUNTER
    TRADE_ID_COUNTER = 0
    setup_logging(); load_market_data(); setup_csv_writer()
    logging.info("Running in NORMAL mode (truly instant execution).")
    
    order_books = {}
    for market in MARKET_MAPPING.values():
        if "polymarket" in market: order_books[market["polymarket"]] = OrderBook(market["polymarket"])
        if "kalshi" in market: order_books[market["kalshi"]] = OrderBook(market["kalshi"])

    try:
        with open(JSONL_FILE_PATH, 'r') as f:
            lines = f.readlines()
            total_lines = len(lines)
            logging.info(f"Loaded {total_lines} log entries. Starting replay...")
            for i, line in tqdm(enumerate(lines), total=total_lines, desc="Normal Mode Replay"):
                try: log_entry = json.loads(line)
                except json.JSONDecodeError: continue
                process_log_entry(log_entry, order_books)
                timestamp, c_name = log_entry.get('ts', ''), log_entry.get("name")
                is_targeted = TARGETED_DEBUG_CONFIG['enabled'] and TARGETED_DEBUG_CONFIG['market_name'] == c_name and TARGETED_DEBUG_CONFIG['timestamp_contains'] in timestamp
                if is_targeted:
                    print("\n" + "#"*80 + f"\n### TARGETED DEBUG: State AFTER processing entry at {timestamp} ###")
                    poly_id, kalshi_id = MARKET_MAPPING.get(c_name, {}).get("polymarket"), MARKET_MAPPING.get(c_name, {}).get("kalshi")
                    print(_format_book_for_debug(order_books.get(poly_id), "Polymarket")); print(_format_book_for_debug(order_books.get(kalshi_id), "Kalshi") + "#"*80)
                opportunities = find_opportunities(order_books)
                if is_targeted and not opportunities: print("--- No opportunities found at this targeted debug point. ---")
                for opp in opportunities:
                    if (is_targeted and opp.get('market_name') == c_name) or not TARGETED_DEBUG_CONFIG['enabled']:
                         _execute_and_log_opportunity(opp, order_books, timestamp)
    finally:
        cleanup() # Ensures CSV file is closed even if an error occurs
    logging.info(f"Normal run complete. Found {TRADE_ID_COUNTER} profitable trades.")


# --- NEW DELAY SIMULATION MODE ---
def run_delay_mode():
    setup_logging(); load_market_data()
    logging.info("Running in DELAY mode.")

    if os.path.exists(DELAY_MODE_CSV): os.remove(DELAY_MODE_CSV)
    
    delays_ms = range(0, 1001, 100)
    
    logging.info(f"Loading log file {JSONL_FILE_PATH} into memory...")
    with open(JSONL_FILE_PATH, 'r') as f:
        lines = f.readlines()
        total_lines = len(lines)
    logging.info(f"Loaded {total_lines} log entries. Starting delay simulation for {len(delays_ms)} delay values.")

    with open(DELAY_MODE_CSV, 'w', newline='') as summary_file:
        summary_writer = csv.writer(summary_file)
        summary_writer.writerow(['delay_ms', 'total_trades', 'total_net_profit', 'total_volume_traded', 'total_fees_paid'])

        for delay in delays_ms:
            logging.info(f"--- Simulating with {delay}ms delay ---")
            
            order_books = {}
            for market in MARKET_MAPPING.values():
                if "polymarket" in market: order_books[market["polymarket"]] = OrderBook(market["polymarket"])
                if "kalshi" in market: order_books[market["kalshi"]] = OrderBook(market["kalshi"])
            
            scheduled_trades = []
            total_profit, total_trades, total_volume, total_fees = 0.0, 0, 0.0, 0.0

            for line in tqdm(lines, desc=f"Delay {delay:>4}ms"):
                try:
                    log_entry = json.loads(line)
                    ts_str = log_entry.get('ts')
                    if not ts_str: continue
                    # More robust ISO format parsing for 'Z' timezone
                    current_ts = datetime.fromisoformat(ts_str.removesuffix('Z') + '+00:00')
                except (json.JSONDecodeError, AttributeError, ValueError):
                    continue
                
                # 1. Execute any scheduled trades that are now due
                trades_to_execute_now = [t for t in scheduled_trades if t['execution_ts'] <= current_ts]
                if trades_to_execute_now:
                    scheduled_trades = [t for t in scheduled_trades if t['execution_ts'] > current_ts]
                    trades_to_execute_now.sort(key=lambda x: x.get('detected_spread', 0), reverse=True)

                    for trade in trades_to_execute_now:
                        opp = trade['opportunity']
                        buy_book, sell_book = order_books.get(opp['buy_id']), order_books.get(opp['sell_id'])
                        if not (buy_book and sell_book): continue
                        
                        avg_buy_price, executed_buy = execute_trade_on_book(buy_book, SIDE_ASK, opp['size'])
                        avg_sell_price, executed_sell = execute_trade_on_book(sell_book, SIDE_BID, opp['size'])
                        actual_size = min(executed_buy, executed_sell)

                        if actual_size > 0:
                            fees = 0.0
                            if opp['buy_platform'] == PLATFORM_KALSHI:
                                fees = calculate_kalshi_fee(actual_size, avg_buy_price)
                            elif opp['sell_platform'] == PLATFORM_KALSHI:
                                fees = calculate_kalshi_fee(actual_size, avg_sell_price)
                            
                            net_profit = (avg_sell_price - avg_buy_price) * actual_size - fees
                            if net_profit > 0:
                                total_trades += 1
                                total_profit += net_profit
                                total_volume += actual_size
                                total_fees += fees
                
                # 2. Process current log entry to update the books
                process_log_entry(log_entry, order_books)
                
                # 3. Find and schedule new opportunities based on the new book state
                opportunities = find_opportunities(order_books)
                for opp in opportunities:
                    buy_book, sell_book = order_books[opp['buy_id']], order_books[opp['sell_id']]
                    # This check is safe because find_opportunities only returns valid opps
                    spread = sell_book.highest_bid[0] - buy_book.lowest_ask[0]
                    execution_ts = current_ts + timedelta(milliseconds=delay)
                    scheduled_trades.append({'opportunity': opp, 'execution_ts': execution_ts, 'detected_spread': spread})
            
            summary_writer.writerow([delay, total_trades, f"{total_profit:.2f}", f"{total_volume:.2f}", f"{total_fees:.2f}"])
            logging.info(f"Delay {delay}ms Results: {total_trades} trades, ${total_profit:.2f} profit, ${total_volume:.2f} volume.")

    logging.info(f"Delay analysis complete. Results saved to {DELAY_MODE_CSV}")

if __name__ == "__main__":
    if ANALYSIS_MODE == 'delay':
        run_delay_mode()
    else:
        run_normal_mode()