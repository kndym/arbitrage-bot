# Assuming order_book.py is in the same directory or accessible via PYTHONPATH
from order_book import OrderBook
from typing import Dict, Any

def update_polymarket_order_book(order_book: OrderBook, data: Dict[str, Any]):
    """
    Updates a Polymarket OrderBook instance based on a WSS message.

    Args:
        order_book (OrderBook): The OrderBook instance to update.
        data (Dict[str, Any]): The raw dictionary data received from Polymarket WSS.
    """
    event_type = data.get("event_type")
    
    # Polymarket uses asset_id + market for unique market identification
    # We'll use the 'market' hash as the primary identifier for simplicity here,
    # or you could combine them.
    market_identifier = data.get("asset_id") 
    if market_identifier is None:
        print(f"Warning: Polymarket update missing 'market' identifier. Data: {data}")
        return

    # For consistency, ensure the market_id matches. 
    # In a real system, you'd likely have a map of market_id to OrderBook instances.
    if order_book.market_id != market_identifier:
        print(f"Warning: Attempted to update order book for {order_book.market_id} "
              f"with data for {market_identifier}. Skipping.")
        return

    order_book.last_updated_timestamp = int(data.get("timestamp", 0))

    if event_type == "book":
        # This is a full snapshot
        order_book._bids = {} # Clear existing book
        order_book._asks = {} # Clear existing book

        for bid in data.get("bids", []):
            try:
                price = float(bid["price"])
                size = float(bid["size"])
                order_book._update_book_level('bid', price, size)
            except (ValueError, KeyError) as e:
                print(f"Error parsing Polymarket bid data: {bid} - {e}")
                continue

        for ask in data.get("asks", []):
            try:
                price = float(ask["price"])
                size = float(ask["size"])
                order_book._update_book_level('ask', price, size)
            except (ValueError, KeyError) as e:
                print(f"Error parsing Polymarket ask data: {ask} - {e}")
                continue
        # print(f"Polymarket: Snapshot updated for {order_book.market_id}")

    elif event_type == "price_change":
        # This is a delta update with absolute sizes
        for change in data.get("changes", []):
            try:
                price = float(change["price"])
                size = float(change["size"])
                side = change["side"]

                if side == "BUY": # Polymarket uses BUY for bids
                    order_book._update_book_level('bid', price, size)
                elif side == "SELL": # Polymarket uses SELL for asks
                    order_book._update_book_level('ask', price, size)
                else:
                    print(f"Warning: Unknown Polymarket side '{side}' in change: {change}")
            except (ValueError, KeyError) as e:
                print(f"Error parsing Polymarket price_change data: {change} - {e}")
                continue
        # print(f"Polymarket: Price change updated for {order_book.market_id}")
    else:
        pass
        #print(f"Warning: Unhandled Polymarket event type: {event_type}.")