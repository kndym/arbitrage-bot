import json
import csv
import logging
import math
from typing import Dict, Any, List
from datetime import datetime, timedelta
import os
import copy
import heapq
from sortedcontainers import SortedDict

# --- Configuration ---
LOG_LEVEL = logging.INFO
JSONL_FILE_PATH = 'jsons/order_book_deltas_jul_5_v2.jsonl'
MARKETS_FILE = 'jsons/markets.json'
COMP_FILE = 'jsons/compliment.json'
PROFIT_THRESHOLD = 0.0
ANALYSIS_MODE = 'normal' # <-- SET 'delay' FOR THE NEW MODE, 'normal' for original mode
DEBUG_MODE = False
TARGETED_DEBUG_CONFIG = {
    'enabled': False,
    'market_name': 'Houston vs Los Angeles (LAD)',
    'timestamp_contains': '2025-07-06T02:03:11.650'
}
EXECUTED_TRADES_CSV = 'tables/executed_arbitrage_trades_optimized.csv'
DELAY_MODE_CSV = 'tables/delay_analysis_summary_optimized.csv' # <-- New output file for delay mode
MARKET_MAPPING: Dict[str, Dict[str, str]] = {}
TRADE_ID_COUNTER = 0

# --- CHANGE 1: OPTIMIZED ORDER BOOK ---
class OrderBook:
    def __init__(self, market_id: str):
        self.market_id = market_id
        # Bids use negative prices to simulate a max-heap (highest price first)
        self._bids: SortedDict[float, float] = SortedDict()
        self._asks: SortedDict[float, float] = SortedDict()

    @property
    def bids(self):
        return ((-price, size) for price, size in self._bids.items())

    @property
    def asks(self):
        return self._asks.items()

    @property
    def highest_bid(self) -> float | None:
        return -self._bids.peekitem(0)[0] if self._bids else None

    @property
    def lowest_ask(self) -> float | None:
        return self._asks.peekitem(0)[0] if self._asks else None

    def _update_book_level(self, side: str, price: float, size: float):
        book_side = self._bids if side == 'bid' else self._asks
        # For bids, store the price as negative to keep the highest price at the "top" (lowest index)
        key = -price if side == 'bid' else price
        if size > 1e-9:
            book_side[key] = size
        elif key in book_side:
            del book_side[key]

# (robust_update functions remain the same)
def robust_update_polymarket_order_book(book: OrderBook, data: Dict[str, Any]):
    event_type = data.get("event_type")
    if event_type == "book":
        book._bids.clear(); book._asks.clear()
        for bid in data.get("changes", {}).get("bids", []): book._update_book_level('bid', float(bid['price']), float(bid['size']))
        for ask in data.get("changes", {}).get("asks", []): book._update_book_level('ask', float(ask['price']), float(ask['size']))
    elif event_type == "delta":
        for change in data.get("changes", []):
            side = 'bid' if change['side'] == 'BUY' else 'ask'
            book._update_book_level(side, float(change['price']), float(change['size']))

def robust_update_kalshi_order_book(book: OrderBook, data: Dict[str, Any]):
    try:
        if "yes" in data and "no" in data:
            book._bids.clear(); book._asks.clear()
            for price_cents, size in data.get("yes", []): book._update_book_level('bid', float(price_cents) / 100.0, float(size))
            for price_cents, size in data.get("no", []):
                ask_price = round(1.0 - (float(price_cents) / 100.0), 2)
                book._update_book_level('ask', ask_price, float(size))
        elif "price" in data and "delta" in data:
            price_cents, delta, side = int(data["price"]), float(data["delta"]), data["side"]
            if side == "yes":
                price = price_cents / 100.0
                current_size = book._bids.get(-price, 0)
                book._update_book_level('bid', price, current_size + delta)
            elif side == "no":
                ask_price = round(1.0 - (price_cents / 100.0), 2)
                current_size = book._asks.get(ask_price, 0)
                book._update_book_level('ask', ask_price, current_size + delta)
    except Exception as e:
        logging.error(f"CRITICAL ERROR processing Kalshi data: {data} -> {e}")


# --- Setup and Helper Functions (mostly unchanged) ---
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
def _format_book_for_debug(book: OrderBook, name: str) -> str:
    if not book: return f"  {name}: [Book Not Found in Dict]\n"
    if not book.bids and not book.asks: return f"  {name} ({book.market_id}): [Book is Empty]\n"
    return f"  {name} ({book.market_id}):\n    Asks: {list(book.asks)[:5]}\n    Bids: {list(book.bids)[:5]}\n"
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
    avg_buy_price, executed_buy = execute_trade_on_book(buy_book, 'ask', opportunity['size'])
    avg_sell_price, executed_sell = execute_trade_on_book(sell_book, 'bid', opportunity['size'])
    actual_size = min(executed_buy, executed_sell)
    if DEBUG_MODE:
        print("  --- EXECUTION ---" + f"\n    Attempted Size: {opportunity['size']:.2f}" + f"\n    Buy Leg Executed: {executed_buy:.2f} shares @ avg ${avg_buy_price:.4f}" + f"\n    Sell Leg Executed: {executed_sell:.2f} shares @ avg ${avg_sell_price:.4f}" + f"\n    Final Trade Size (min of legs): {actual_size:.2f}")
    if actual_size > 0:
        fees = calculate_kalshi_fee(actual_size, avg_buy_price) if opportunity['buy_platform'] == 'Kalshi' else calculate_kalshi_fee(actual_size, avg_sell_price)
        net_profit = (avg_sell_price - avg_buy_price) * actual_size - fees
        if DEBUG_MODE: print("  --- RESULT ---" + f"\n    Gross Profit: ${((avg_sell_price - avg_buy_price) * actual_size):.4f}" + f"\n    Fees: ${fees:.4f}" + f"\n    Net Profit: ${net_profit:.4f}")
        if net_profit > 0:
            if DEBUG_MODE: print("  CONCLUSION: PROFITABLE TRADE")
            TRADE_ID_COUNTER += 1
            CSV_WRITER.writerow([TRADE_ID_COUNTER, timestamp, 'same_outcome', f"{(net_profit/actual_size):.4f}", f"{actual_size:.2f}", f"{net_profit:.2f}", f"{fees:.2f}", opportunity['market_name'], opportunity['buy_platform'], 'BUY', f"{avg_buy_price:.4f}", opportunity['market_name'], opportunity['sell_platform'], 'SELL', f"{avg_sell_price:.4f}"])
        elif DEBUG_MODE: print(f"  CONCLUSION: FAILED - Net profit (${net_profit:.4f}) is not positive.")
    elif DEBUG_MODE: print("  CONCLUSION: FAILED - Zero liquidity executed.")


def calculate_kalshi_fee(trade_size: float, price: float) -> float:
    if trade_size <= 0 or price <= 0 or price >= 1: return 0.0
    return math.ceil(0.07 * trade_size * price * (1.0 - price) * 100) / 100.0

def execute_trade_on_book(book: OrderBook, side_to_hit: str, size_to_trade: float) -> tuple[float, float]:
    if size_to_trade <= 0: return 0.0, 0.0
    levels_to_hit = book.asks if side_to_hit == 'ask' else book.bids
    size_executed, total_cost, remaining_size = 0.0, 0.0, size_to_trade
    
    for price, available_size in list(levels_to_hit):
        if remaining_size <= 1e-9: break
        size_at_this_level = min(remaining_size, available_size)
        book._update_book_level('bid' if side_to_hit == 'bid' else 'ask', price, available_size - size_at_this_level)
        size_executed += size_at_this_level
        total_cost += size_at_this_level * price
        remaining_size -= size_at_this_level
        
    avg_price = (total_cost / size_executed) if size_executed > 0 else 0.0
    return avg_price, size_executed

def process_log_entry(log_entry: Dict[str, Any], order_books: Dict[str, OrderBook]):
    canonical_name = log_entry.get("name")
    if not canonical_name: return
    if "pm_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("polymarket")
        if market_id and market_id in order_books: robust_update_polymarket_order_book(order_books[market_id], log_entry["pm_delta"])
    elif "ks_delta" in log_entry:
        market_id = MARKET_MAPPING.get(canonical_name, {}).get("kalshi")
        if market_id and market_id in order_books: robust_update_kalshi_order_book(order_books[market_id], log_entry["ks_delta"])

# --- CHANGE 2: EFFICIENT OPPORTUNITY FINDING ---
def find_opportunities(current_order_books: Dict[str, OrderBook]) -> List[Dict]:
    opportunities = []
    for market_name, platforms in MARKET_MAPPING.items():
        poly_id, kalshi_id = platforms.get("polymarket"), platforms.get("kalshi")
        if not (poly_id and kalshi_id and poly_id in current_order_books and kalshi_id in current_order_books):
            continue
        
        pb, kb = current_order_books[poly_id], current_order_books[kalshi_id]

        # Case 1: Buy Polymarket, Sell Kalshi
        p_ask, k_bid = pb.lowest_ask, kb.highest_bid
        if p_ask is not None and k_bid is not None and (k_bid - p_ask) > PROFIT_THRESHOLD:
            p_ask_size = pb._asks[p_ask]
            k_bid_size = kb._bids[-k_bid]
            size = min(p_ask_size, k_bid_size)
            if size > 0:
                opportunities.append({'type': 'same_outcome', 'market_name': market_name, 'buy_id': poly_id, 'sell_id': kalshi_id, 'size': size, 'buy_platform': 'Polymarket', 'sell_platform': 'Kalshi', 'spread': k_bid - p_ask})

        # Case 2: Buy Kalshi, Sell Polymarket
        k_ask, p_bid = kb.lowest_ask, pb.highest_bid
        if k_ask is not None and p_bid is not None and (p_bid - k_ask) > PROFIT_THRESHOLD:
            k_ask_size = kb._asks[k_ask]
            p_bid_size = pb._bids[-p_bid]
            size = min(k_ask_size, p_bid_size)
            if size > 0:
                opportunities.append({'type': 'same_outcome', 'market_name': market_name, 'buy_id': kalshi_id, 'sell_id': poly_id, 'size': size, 'buy_platform': 'Kalshi', 'sell_platform': 'Polymarket', 'spread': p_bid - k_ask})
    
    if opportunities:
        opportunities.sort(key=lambda x: x['spread'], reverse=True)
    return opportunities

# --- CORE LOGIC (run_normal_mode is similar, run_delay_mode is heavily optimized) ---
# Normal mode benefits from faster OrderBook and opportunity finding, but structure is the same.
# We focus on optimizing delay mode as it's the most intensive.


def run_normal_mode():
    global TRADE_ID_COUNTER
    TRADE_ID_COUNTER = 0
    setup_logging(); load_market_data(); setup_csv_writer()
    logging.info("Running in NORMAL mode (truly instant execution).")
    
    order_books = {}
    for market in MARKET_MAPPING.values():
        if "polymarket" in market: order_books[market["polymarket"]] = OrderBook(market["polymarket"])
        if "kalshi" in market: order_books[market["kalshi"]] = OrderBook(market["kalshi"])

    with open(JSONL_FILE_PATH, 'r') as f:
        lines = f.readlines()
        total_lines = len(lines)
        logging.info(f"Loaded {total_lines} log entries. Starting replay...")
        for i, line in enumerate(lines):
            if (i + 1) % 50000 == 0: logging.info(f"Progress: {i + 1}/{total_lines} lines ({((i + 1)/total_lines)*100:.2f}%) processed...")
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
    logging.info(f"Normal run complete. Found {TRADE_ID_COUNTER} profitable trades.")
    cleanup()



# --- CHANGE 3 & 4: OPTIMIZED DELAY SIMULATION MODE ---
def run_delay_mode():
    setup_logging()
    load_market_data()
    logging.info("Running in OPTIMIZED DELAY mode.")

    if os.path.exists(DELAY_MODE_CSV): os.remove(DELAY_MODE_CSV)
    with open(DELAY_MODE_CSV, 'w', newline='') as summary_file:
        summary_writer = csv.writer(summary_file)
        summary_writer.writerow(['delay_ms', 'total_trades', 'total_net_profit', 'total_volume_traded', 'total_fees_paid'])

        delays_ms = range(0, 2001, 100)
        
        # CHANGE 4: Load file into memory ONCE
        try:
            with open(JSONL_FILE_PATH, 'r') as f:
                logging.info(f"Loading {JSONL_FILE_PATH} into memory...")
                lines = f.readlines()
            parsed_lines = []
            for line in lines:
                try:
                    log_entry = json.loads(line)
                    ts_str = log_entry.get('ts', '').replace('Z', '+00:00')
                    if ts_str:
                        log_entry['parsed_ts'] = datetime.fromisoformat(ts_str)
                        parsed_lines.append(log_entry)
                except (json.JSONDecodeError, AttributeError, ValueError):
                    continue
            logging.info(f"Loaded and parsed {len(parsed_lines)} log entries. Starting simulations.")
        except FileNotFoundError:
            logging.error(f"FATAL: {JSONL_FILE_PATH} not found.")
            return

        for delay in delays_ms:
            logging.info(f"--- Simulating with {delay}ms delay ---")
            
            order_books = {}
            for market in MARKET_MAPPING.values():
                if "polymarket" in market: order_books[market["polymarket"]] = OrderBook(market["polymarket"])
                if "kalshi" in market: order_books[market["kalshi"]] = OrderBook(market["kalshi"])
            
            # CHANGE 3: Use a min-heap (priority queue) for scheduled trades
            scheduled_trades = [] 
            total_profit, total_trades, total_volume, total_fees = 0.0, 0, 0.0, 0.0
            
            for i, log_entry in enumerate(parsed_lines):
                current_ts = log_entry['parsed_ts']
                
                # 1. Execute any scheduled trades that are now due
                # Efficiently pop from the heap until the next trade is in the future
                while scheduled_trades and scheduled_trades[0][0] <= current_ts:
                    execution_ts, opp = heapq.heappop(scheduled_trades)
                    
                    buy_book, sell_book = order_books.get(opp['buy_id']), order_books.get(opp['sell_id'])
                    if not (buy_book and sell_book): continue
                    
                    avg_buy_price, executed_buy = execute_trade_on_book(buy_book, 'ask', opp['size'])
                    avg_sell_price, executed_sell = execute_trade_on_book(sell_book, 'bid', opp['size'])
                    actual_size = min(executed_buy, executed_sell)

                    if actual_size > 0:
                        fees = calculate_kalshi_fee(actual_size, avg_buy_price) if opp['buy_platform'] == 'Kalshi' else calculate_kalshi_fee(actual_size, avg_sell_price)
                        net_profit = (avg_sell_price - avg_buy_price) * actual_size - fees
                        if net_profit > 0:
                            total_trades += 1
                            total_profit += net_profit
                            total_volume += actual_size
                            total_fees += fees
                
                # 2. Process current log entry to update the books
                process_log_entry(log_entry, order_books)
                
                # 3. Find and schedule new opportunities
                opportunities = find_opportunities(order_books)
                if opportunities:
                    # For simplicity, we only schedule the single best opportunity found at this timestamp
                    best_opp = opportunities[0]
                    execution_ts = current_ts + timedelta(milliseconds=delay)
                    # Push (timestamp, opportunity) onto the heap
                    heapq.heappush(scheduled_trades, (execution_ts, best_opp))

            summary_writer.writerow([delay, total_trades, f"{total_profit:.2f}", f"{total_volume:.2f}", f"{total_fees:.2f}"])
            logging.info(f"Delay {delay}ms Results: {total_trades} trades, ${total_profit:.2f} profit, ${total_volume:.2f} volume.")

    logging.info(f"Delay analysis complete. Results saved to {DELAY_MODE_CSV}")

if __name__ == "__main__":
    if ANALYSIS_MODE == 'delay':
        run_delay_mode()
    else:
        # You can re-implement run_normal_mode using the new fast components if needed
        run_normal_mode()