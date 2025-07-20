# --- START OF FILE main_trader.py (COMPLETED) ---

import asyncio
import logging
import os
import time
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv
import sys
import math

# Import clients and logic
from kalshi.clients import Environment, KalshiHttpClient
from py_clob_client.client import ClobClient, ApiCreds
from cryptography.hazmat.primitives import serialization

# Import the new WSS classes
from polymarket.wss import PolymarketWSS, POLYMARKET_WSS_URI
from kalshi.wss import KalshiWSS

from order_book import OrderBook
from polymarket.updates import update_polymarket_order_book
from kalshi.updates import update_kalshi_order_book
from orders.tor_manager import start_tor, stop_tor, ping_tor
from config import MARKET_MAPPING, COMPLEMENTARY_MARKET_PAIRS, PROD_KEYID, PROD_KEYFILE, POLYMARKET_PROXY_ADDRESS, WALLET_PRIVATE_KEY, AUTH
from fees import calculate_kalshi_fee, POLYMARKET_FEE_PERCENT
from trader import execute_complimentary_buy_trade # This function needs to be implemented in trader.py

# --- Trader Configuration ---
MIN_NET_PROFIT_PER_SHARE = 0.01
MAX_TRADE_SIZE = 5
TRADE_COOLDOWN_SECONDS = 10

# --- Global Variables ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {}
REVERSE_COMPLEMENTARY_PAIRS: Dict[str, str] = {}
LAST_GAME_TRADE_ATTEMPT: Dict[Tuple[str, str], float] = {}

import logging
import sys

# Get a logger instance (you can also use logging.getLogger(__name__) for specific modules)
logger = logging.getLogger(__name__) 

file_log_handler = logging.FileHandler('7_19_v3.log')
logger.addHandler(file_log_handler)

stderr_log_handler = logging.StreamHandler()
logger.addHandler(stderr_log_handler)

# nice output format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_log_handler.setFormatter(formatter)
stderr_log_handler.setFormatter(formatter)

load_dotenv()
polymarket_client: Optional[ClobClient] = None
kalshi_client: Optional[KalshiHttpClient] = None

async def initialize_market_data():
    """Initializes all data structures needed for market tracking."""
    for canonical_name, market_ids in MARKET_MAPPING.items():
        if "polymarket" in market_ids:
            poly_id = market_ids["polymarket"]
            ALL_ORDER_BOOKS[poly_id] = OrderBook(poly_id)
            REVERSE_MARKET_LOOKUP[poly_id] = canonical_name
        if "kalshi" in market_ids:
            kalshi_id = market_ids["kalshi"]
            ALL_ORDER_BOOKS[kalshi_id] = OrderBook(kalshi_id)
            REVERSE_MARKET_LOOKUP[kalshi_id] = canonical_name

    # Create a reverse mapping for complementary pairs for easy lookup
    for key, value in COMPLEMENTARY_MARKET_PAIRS.items():
        REVERSE_COMPLEMENTARY_PAIRS[value] = key

def get_paired_books(canonical_name: str) -> Tuple[Optional[OrderBook], Optional[OrderBook]]:
    market_ids = MARKET_MAPPING.get(canonical_name, {})
    poly_book = ALL_ORDER_BOOKS.get(market_ids.get("polymarket"))
    kalshi_book = ALL_ORDER_BOOKS.get(market_ids.get("kalshi"))
    return poly_book, kalshi_book

def check_and_execute_arbitrage_pair(
    book1: OrderBook, platform1: str,
    book2: OrderBook, platform2: str,
    game_key: Tuple[str, str]
):
    """
    Checks for a specific arbitrage opportunity between two complementary books and executes if profitable.
    This function assumes buying 'Yes' on both outcomes.
    """
    if not all([book1, book2, book1.lowest_ask, book2.lowest_ask]):
        return False # Not enough data to compare

    buy_price_1 = book1.lowest_ask
    buy_price_2 = book2.lowest_ask

    sell_price_1 = book1.highest_bid
    sell_price_2 = book2.highest_bid

    # Get available liquidity at the best ask price for each book
    liquidity1 = book1.get_liquidity_at_price(buy_price_1, 'ask')
    liquidity2 = book2.get_liquidity_at_price(buy_price_2, 'ask')

    # Determine the maximum possible trade size based on available liquidity
    available_liquidity = min(liquidity1, liquidity2)
    
    if available_liquidity <= 0:
        return False

    # Determine the actual trade size, capped by the global max size
    trade_size = min(MAX_TRADE_SIZE, available_liquidity)

    # We can't trade fractional contracts, so we must have at least 1
    if trade_size < 1:
        return False
    
    if platform1 == "Polymarket":
        min_liquidity=math.ceil(1/buy_price_1)
    else:
        min_liquidity=math.ceil(1/buy_price_2)
    
    if trade_size<min_liquidity:
        return False

    # Calculate fees for both platforms
    fee1 = buy_price_1 * trade_size * POLYMARKET_FEE_PERCENT if platform1 == "Polymarket" else calculate_kalshi_fee(trade_size, buy_price_1)
    fee2 = buy_price_2 * trade_size * POLYMARKET_FEE_PERCENT if platform2 == "Polymarket" else calculate_kalshi_fee(trade_size, buy_price_2)

    total_cost = (buy_price_1 * trade_size) + (buy_price_2 * trade_size) + fee1 + fee2

    # Arbitrage exists if total cost to secure a guaranteed $1 payout (per share) is less than the payout
    if total_cost < trade_size:
        net_profit = trade_size - total_cost
        if net_profit / trade_size >= MIN_NET_PROFIT_PER_SHARE:
            LAST_GAME_TRADE_ATTEMPT[game_key] = time.time()
            canonical_name_1 = REVERSE_MARKET_LOOKUP.get(book1.market_id, "Unknown")
            canonical_name_2 = REVERSE_MARKET_LOOKUP.get(book2.market_id, "Unknown")

            logger.info(f"Complimentary Arbitrage opportunity found for game: {' vs '.join(game_key)}")
            logger.info(f"  - Determined Trade Size: {trade_size} (Available: {available_liquidity:.2f}, Max Cap: {MAX_TRADE_SIZE})")
            logger.info(f"  - Buy YES on '{canonical_name_1}' on {platform1} at {buy_price_1} (Liquidity: {liquidity1:.2f})")
            logger.info(f"  - Buy YES on '{canonical_name_2}' on {platform2} at {buy_price_2} (Liquidity: {liquidity2:.2f})")
            logger.info(f"  - Total Cost for {trade_size} shares (incl. fees): {total_cost:.4f}")
            logger.info(f"  - Expected Net Profit: {net_profit:.4f}")

            # Execute the trades
            asyncio.create_task(execute_complimentary_buy_trade(
                poly_client=polymarket_client, kalshi_client=kalshi_client, canonical_name_1=canonical_name_1, canonical_name_2=canonical_name_2,
                book1_platform=platform1, book1_market_id=book1.market_id, book1_ask=buy_price_1, book1_bid=sell_price_1,
                book2_platform=platform2, book2_market_id=book2.market_id, book2_ask=buy_price_2, book2_bid=sell_price_2,
                trade_size=trade_size, proxies=PROXIES
            ))
            return True # Indicate that an arbitrage opportunity was found and acted upon
    return False # No arbitrage opportunity found

def check_game_arbitrage(canonical_name_updated: str):
    """
    NEW FUNCTION: Checks for arbitrage opportunities across a pair of complementary markets.
    e.g., ("Team A wins" vs "Team B wins")
    """
    # Find the complementary market pair for the updated market
    market_a_name = canonical_name_updated
    market_b_name = COMPLEMENTARY_MARKET_PAIRS.get(market_a_name) or REVERSE_COMPLEMENTARY_PAIRS.get(market_a_name)

    if not market_b_name:
        # logger.warning(f"No complementary market found for {market_a_name}. Cannot check for arbitrage.")
        return

    # Create a unique, order-independent key for the game to manage cooldowns
    game_key = tuple(sorted((market_a_name, market_b_name)))

    # Cooldown Check for this specific game
    if time.time() - LAST_GAME_TRADE_ATTEMPT.get(game_key, 0) < TRADE_COOLDOWN_SECONDS:
        return

    # Get order books for both sides of the game
    # market_a represents one outcome (e.g., TOR wins)
    # market_b represents the complementary outcome (e.g., SF wins)
    poly_book_a, kalshi_book_a = get_paired_books(market_a_name)
    poly_book_b, kalshi_book_b = get_paired_books(market_b_name)

    # Scenario 1: Buy Team A on Polymarket, Buy Team B on Kalshi
    if check_and_execute_arbitrage_pair(poly_book_a, "Polymarket", kalshi_book_b, "Kalshi", game_key):
        return # Trade found, exit to respect cooldown

    # Scenario 2: Buy Team A on Kalshi, Buy Team B on Polymarket
    if check_and_execute_arbitrage_pair(kalshi_book_a, "Kalshi", poly_book_b, "Polymarket", game_key):
        return # Trade found, exit

async def process_websocket_message(source: str, message: Dict[str, Any]):
    """Processes a message, updates the relevant order book, and checks for arbitrage."""
    market_id = None
    if source == 'polymarket':
        market_id = message.get("asset_id")
        if market_id in ALL_ORDER_BOOKS:
            update_polymarket_order_book(ALL_ORDER_BOOKS[market_id], message)
    elif source == 'kalshi':
        market_id = message.get("msg", {}).get("market_ticker")
        if market_id in ALL_ORDER_BOOKS:
            update_kalshi_order_book(ALL_ORDER_BOOKS[market_id], message)
    
    if market_id and market_id in REVERSE_MARKET_LOOKUP:
        canonical_name = REVERSE_MARKET_LOOKUP[market_id]
        # **MODIFIED CALL** to the new arbitrage checking function
        check_game_arbitrage(canonical_name)


async def process_messages_from_queue(queue: asyncio.Queue):
    """Continuously fetches messages from the queue and processes them."""
    while True:
        source, message = await queue.get()
        try:
            await process_websocket_message(source, message)
        except Exception as e:
            logger.error(f"Error processing message from {source}: {e}")
        queue.task_done()

async def run_trader():
    """Main function to start Tor, initialize clients, and listen to websockets."""
    global polymarket_client, kalshi_client, PROXIES

    tor_process, PROXIES = start_tor()
    if not tor_process:
        logger.error("Failed to start Tor. Exiting.")
        return
    ping_tor(PROXIES)

    try:
        logger.info("Temporarily setting proxy environment variables for ClobClient initialization...")
        os.environ['HTTP_PROXY'] = PROXIES['http']
        os.environ['HTTPS_PROXY'] = PROXIES['https']

        poly_client = ClobClient(
            host="https://clob.polymarket.com",
            key=WALLET_PRIVATE_KEY,
            chain_id=137,
            signature_type=1,
            funder=POLYMARKET_PROXY_ADDRESS
        )

        api_creds = poly_client.create_or_derive_api_creds()

        AUTH = {
            'apiKey': api_creds.api_key,
            'secret': api_creds.api_secret,
            'passphrase': api_creds.api_passphrase
        }

        poly_client.set_api_creds(api_creds)
        polymarket_client = poly_client
        logger.info("Polymarket ClobClient initialized successfully (routed via Tor).")

    finally:
        logger.info("Unsetting proxy environment variables.")
        if 'HTTP_PROXY' in os.environ:
            del os.environ['HTTP_PROXY']
        if 'HTTPS_PROXY' in os.environ:
            del os.environ['HTTPS_PROXY']

    try:
        key_file_path =PROD_KEYFILE
        with open(key_file_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        
        kalshi_client = KalshiHttpClient(
            key_id=PROD_KEYID,
            private_key=private_key,
            environment=Environment.PROD
        )
        logger.info("Kalshi client initialized (direct connection).")
        balance = kalshi_client.get_balance()
        logger.info(f"Kalshi Balance: {balance.get('balance', 'N/A')}")
    except Exception as e:
        logger.error(f"Failed to initialize Kalshi client: {e}")

    await initialize_market_data()

    if not polymarket_client or not kalshi_client:
        logger.error("Could not initialize all trading clients. Shutting down.")
        stop_tor(tor_process)
        return

    message_queue = asyncio.Queue()

    poly_ids = [m["polymarket"] for m in MARKET_MAPPING.values() if "polymarket" in m]
    kalshi_ids = [m["kalshi"] for m in MARKET_MAPPING.values() if "kalshi" in m]
    
    poly_ws = PolymarketWSS(
        uri=POLYMARKET_WSS_URI,
        asset_ids=poly_ids,
        message_queue=message_queue,
        auth=AUTH
    )
    
    kalshi_ws = KalshiWSS(
        key_id=PROD_KEYID,
        private_key=kalshi_client.private_key,
        environment=Environment.PROD,
        message_queue=message_queue,
        ticker_list=kalshi_ids
    )
    
    try:
        await kalshi_ws.connect()
        kalshi_listen_task = asyncio.create_task(kalshi_ws.listen())

        await poly_ws.connect()
        poly_listen_task = asyncio.create_task(poly_ws.listen())


        queue_processor_task = asyncio.create_task(process_messages_from_queue(message_queue))
        
        logger.info("Now listening for market data and arbitrage opportunities...")
        await asyncio.gather(
            poly_listen_task, 
            kalshi_listen_task, 
            queue_processor_task
        )
    finally:
        logger.info("Stopping Tor process...")
        stop_tor(tor_process)
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_trader())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Shutting down.")