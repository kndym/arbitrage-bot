# kalshi/updates.py
from order_book import OrderBook
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def update_kalshi_order_book(order_book: OrderBook, data: Dict[str, Any]):
    """
    Updates a Kalshi OrderBook instance based on a WSS message from the log file.
    This version implements the requested FLIPPED logic for Kalshi's 'yes'/'no' sides.
    
    New Interpretation (Flipped):
    - Kalshi 'yes' book prices are now interpreted as BIDS for "Yes" contracts.
    - Kalshi 'no' book prices (inverted 1-price) are now interpreted as ASKS for "Yes" contracts.
    """
    
    # --- Full Snapshot (from "yes" and "no" keys) ---
    if "yes" in data and "no" in data:
        order_book._bids.clear()
        order_book._asks.clear()

        # The 'yes' book (e.g., a user wants to SELL Yes at this price)
        # We now interpret this as a BUYER'S desire to buy Yes.
        # This means prices in Kalshi's 'yes' list become BIDS in our standard order book.
        for price_cents, size in data.get("yes", []):
            try:
                price = round(price_cents / 100.0, 4)
                order_book._update_book_level('bid', price, float(size)) # CHANGED: 'ask' -> 'bid'
            except (TypeError, ValueError) as e:
                logger.warning(f"Error parsing Kalshi snapshot 'yes' (bid) data: {[price_cents, size]} - {e}")

        # The 'no' book (e.g., a user wants to SELL No at this price)
        # This is equivalent to an offer to BUY No or SELL Yes.
        # We now interpret (1 - P_no) as an ASKER'S desire to sell Yes.
        # This means (1 - P_no) prices become ASKS in our standard order book.
        for price_cents, size in data.get("no", []):
            try:
                no_price = price_cents / 100.0
                yes_ask_price = round(1.0 - no_price, 4)
                order_book._update_book_level('ask', yes_ask_price, float(size)) # CHANGED: 'bid' -> 'ask'
            except (TypeError, ValueError) as e:
                logger.warning(f"Error parsing Kalshi snapshot 'no' (ask) data: {[price_cents, size]} - {e}")
    
    # --- Delta Update ---
    elif "price" in data and "delta" in data:
        try:
            price_cents = data["price"]
            delta = float(data["delta"])
            side = data["side"]

            if side == "yes":
                # A delta on the 'yes' book now affects the BIDS for "Yes" contracts.
                price = round(price_cents / 100.0, 4)
                current_size = order_book.get_liquidity_at_price(price, 'bid') # CHANGED: 'ask' -> 'bid'
                order_book._update_book_level('bid', price, current_size + delta) # CHANGED: 'ask' -> 'bid'
            
            elif side == "no":
                # A delta on the 'no' book now affects the ASKS for "Yes" contracts.
                no_price = price_cents / 100.0
                price = round(1.0 - no_price, 4)
                current_size = order_book.get_liquidity_at_price(price, 'ask') # CHANGED: 'bid' -> 'ask'
                order_book._update_book_level('ask', price, current_size + delta) # CHANGED: 'bid' -> 'ask'
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Error parsing Kalshi delta update: {data} - {e}")