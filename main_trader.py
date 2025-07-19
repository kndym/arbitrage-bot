# --- START OF FILE main_trader.py (COMPLETED) ---

import asyncio
import logging
import os
import time
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv

# Import clients and logic
from kalshi.clients import Environment, KalshiHttpClient
from py_clob_client.client import ClobClient, ApiCreds
from cryptography.hazmat.primitives import serialization

# Import the new WSS classes
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI
from kalshi.wss import KalshiWSS

from order_book import OrderBook
from polymarket.updates import update_polymarket_order_book
from kalshi.updates import update_kalshi_order_book
from orders.tor_manager import start_tor, stop_tor
from config import MARKET_MAPPING
from fees import calculate_kalshi_fee, POLYMARKET_FEE_PERCENT
from trader import execute_complimentary_buy_trade # This function needs to be implemented in trader.py

# --- Trader Configuration ---
MIN_NET_PROFIT_PER_SHARE = 0.01
MAX_TRADE_SIZE = 5
TRADE_COOLDOWN_SECONDS = 60

# --- Global Variables ---
ALL_ORDER_BOOKS: Dict[str, OrderBook] = {}
REVERSE_MARKET_LOOKUP: Dict[str, str] = {}
LAST_TRADE_ATTEMPT: Dict[str, float] = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        LAST_TRADE_ATTEMPT[canonical_name] = 0

def get_paired_books(canonical_name: str) -> Tuple[Optional[OrderBook], Optional[OrderBook]]:
    market_ids = MARKET_MAPPING.get(canonical_name, {})
    poly_book = ALL_ORDER_BOOKS.get(market_ids.get("polymarket"))
    kalshi_book = ALL_ORDER_BOOKS.get(market_ids.get("kalshi"))
    return poly_book, kalshi_book

def check_complimentary_arbitrage(book1: OrderBook, platform1: str, book2: OrderBook, platform2: str, canonical_name: str):
    """
    Checks for arbitrage opportunities by buying complimentary outcomes across two platforms.
    For example, buying 'Yes' on Platform 1 and 'No' on Platform 2 for the same event.
    """
    if not all([book1.lowest_ask, book2.lowest_ask]):
        return False # Not enough data to compare

    buy_price_1 = book1.lowest_ask
    buy_price_2 = book2.lowest_ask
    trade_size = MAX_TRADE_SIZE

    # Calculate fees for both platforms
    fee1 = buy_price_1 * trade_size * POLYMARKET_FEE_PERCENT if platform1 == "Polymarket" else calculate_kalshi_fee(trade_size, buy_price_1)
    fee2 = buy_price_2 * trade_size * POLYMARKET_FEE_PERCENT if platform2 == "Polymarket" else calculate_kalshi_fee(trade_size, buy_price_2)

    total_cost = (buy_price_1 * trade_size) + (buy_price_2 * trade_size) + fee1 + fee2

    # An arbitrage opportunity exists if the total cost to buy both complimentary outcomes is less than the settlement value ($1 * trade_size)
    if total_cost < trade_size:
        net_profit = trade_size - total_cost
        if net_profit / trade_size >= MIN_NET_PROFIT_PER_SHARE:
            LAST_TRADE_ATTEMPT[canonical_name] = time.time()
            logger.info(f"Complimentary Arbitrage opportunity found for {canonical_name}!")
            logger.info(f"  - Buy {trade_size} on {platform1} (Market ID: {book1.market_id}) at {buy_price_1}")
            logger.info(f"  - Buy {trade_size} on {platform2} (Market ID: {book2.market_id}) at {buy_price_2}")
            logger.info(f"  - Total Cost (including fees): {total_cost:.4f}")
            logger.info(f"  - Expected Profit: {net_profit:.4f}")

            # Execute the trades - this function needs to be implemented in trader.py
            asyncio.create_task(execute_complimentary_buy_trade(
                poly_client=polymarket_client, kalshi_client=kalshi_client,
                canonical_name=canonical_name,
                book1_platform=platform1, book1_market_id=book1.market_id, book1_price=buy_price_1,
                book2_platform=platform2, book2_market_id=book2.market_id, book2_price=buy_price_2,
                trade_size=trade_size, proxies=PROXIES
            ))
            return True # Indicate that an arbitrage opportunity was found and acted upon
    return False # No arbitrage opportunity found

def check_for_arbitrage_and_trade(canonical_name: str):
    """Compares prices and triggers trades if profitable opportunities exist for complimentary markets."""
    poly_book, kalshi_book = get_paired_books(canonical_name)
    if not all([poly_book, kalshi_book]):
        return # Not enough data to compare

    # Cooldown Check
    if time.time() - LAST_TRADE_ATTEMPT.get(canonical_name, 0) < TRADE_COOLDOWN_SECONDS:
        return

    # Scenario 1: Buy on Polymarket and Kalshi assuming poly_book is 'Yes' and kalshi_book is 'No'
    # The actual 'yes'/'no' designation depends on your MARKET_MAPPING and how the order books are populated.
    if check_complimentary_arbitrage(poly_book, "Polymarket", kalshi_book, "Kalshi", canonical_name):
        return

    # Scenario 2: Buy on Kalshi and Polymarket assuming kalshi_book is 'Yes' and poly_book is 'No'
    if check_complimentary_arbitrage(kalshi_book, "Kalshi", poly_book, "Polymarket", canonical_name):
        return


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
        check_for_arbitrage_and_trade(canonical_name)

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

    try:
        logger.info("Temporarily setting proxy environment variables for ClobClient initialization...")
        os.environ['HTTP_PROXY'] = PROXIES['http']
        os.environ['HTTPS_PROXY'] = PROXIES['https']

        poly_client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.getenv("WALLET_PRIVATE_KEY"),
            chain_id=137,
            signature_type=1,
            funder=os.getenv("POLYMARKET_PROXY_ADDRESS")
        )

        api_creds = poly_client.create_or_derive_api_creds()

        creds = ApiCreds(
            api_key= api_creds.api_key,
            api_secret= api_creds.api_secret,
            api_passphrase=api_creds.api_passphrase
        )

        poly_client.set_api_creds(creds)
        polymarket_client = poly_client
        logger.info("Polymarket ClobClient initialized successfully (routed via Tor).")

    finally:
        logger.info("Unsetting proxy environment variables.")
        if 'HTTP_PROXY' in os.environ:
            del os.environ['HTTP_PROXY']
        if 'HTTPS_PROXY' in os.environ:
            del os.environ['HTTPS_PROXY']

    try:
        key_file_path = os.getenv('PROD_KEYFILE')
        with open(key_file_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        
        kalshi_client = KalshiHttpClient(
            key_id=os.getenv('PROD_KEYID'),
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
        uri=POLYMARKET_MARKET_WSS_URI,
        asset_ids=poly_ids,
        message_queue=message_queue
    )
    
    kalshi_ws = KalshiWSS(
        key_id=os.getenv('PROD_KEYID'),
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