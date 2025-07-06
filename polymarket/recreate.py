from order_book import OrderBook
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def update_polymarket_order_book(order_book: OrderBook, data: Dict[str, Any]):
    """
    Updates a Polymarket OrderBook instance based on a WSS message from the log file.
    """
    event_type = data.get("event_type")

    # Full snapshot
    if event_type == "book":
        order_book._bids.clear()
        order_book._asks.clear()
        
        changes = data.get("changes", {})
        for bid in changes.get("bids", []):
            try:
                price = float(bid["price"])
                size = float(bid["size"])
                order_book._update_book_level('bid', price, size)
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Error parsing Polymarket bid data: {bid} - {e}")

        for ask in changes.get("asks", []):
            try:
                price = float(ask["price"])
                size = float(ask["size"])
                order_book._update_book_level('ask', price, size)
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Error parsing Polymarket ask data: {ask} - {e}")
    
    # Delta update
    elif event_type == "delta":
        for change in data.get("changes", []):
            try:
                price = float(change["price"])
                size = float(change["size"])
                side = change["side"]

                if side == "BUY":  # BUY side updates the bids
                    order_book._update_book_level('bid', price, size)
                elif side == "SELL":  # SELL side updates the asks
                    order_book._update_book_level('ask', price, size)
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Error parsing Polymarket delta change: {change} - {e}")
    else:
        logger.warning(f"Unhandled Polymarket event type in log: {event_type}")