# Assuming order_book.py is in the same directory or accessible via PYTHONPATH
from order_book import OrderBook
from typing import Dict, Any

def update_kalshi_order_book(order_book: OrderBook, data: Dict[str, Any]):
    """
    Updates a Kalshi OrderBook instance based on a WSS message.

    Args:
        order_book (OrderBook): The OrderBook instance to update.
        data (Dict[str, Any]): The raw dictionary data received from Kalshi WSS.
    """
    msg_type = data.get("type")
    msg_content = data.get("msg", {})

    market_identifier = msg_content.get("market_ticker")
    if market_identifier is None:
        print(f"Warning: Kalshi update missing 'market_ticker'. Data: {data}")
        return

    # For consistency, ensure the market_id matches. 
    if order_book.market_id != market_identifier:
        print(f"Warning: Attempted to update order book for {order_book.market_id} "
              f"with data for {market_identifier}. Skipping.")
        return

    order_book.last_updated_timestamp = data.get("seq") # Kalshi uses 'seq' for ordering, could use current time as well

    if msg_type == "orderbook_snapshot":
        # This is a full snapshot
        order_book._bids = {} # Clear existing book
        order_book._asks = {} # Clear existing book

        # "yes" side in Kalshi is equivalent to bids (betting 'Yes' means buying at that price)
        for level in msg_content.get("yes", []):
            try:
                price_cents = level[0]
                size = level[1]
                price_dollars = price_cents / 100.0 # Convert cents to dollars
                order_book._update_book_level('bid', price_dollars, float(size))
            except (IndexError, TypeError, ValueError) as e:
                print(f"Error parsing Kalshi 'yes' (bid) data: {level} - {e}")
                continue

        # "no" side in Kalshi is equivalent to asks (betting 'No' means selling at that price)
        for level in msg_content.get("no", []):
            try:
                price_cents = level[0]
                size = level[1]
                price_dollars = price_cents / 100.0 # Convert cents to dollars
                order_book._update_book_level('ask', price_dollars, float(size))
            except (IndexError, TypeError, ValueError) as e:
                print(f"Error parsing Kalshi 'no' (ask) data: {level} - {e}")
                continue
        # print(f"Kalshi: Snapshot updated for {order_book.market_id}")

    elif msg_type == "orderbook_delta":
        # This is a delta update
        try:
            price_cents = msg_content["price"]
            delta_size = msg_content["delta"]
            side = msg_content["side"]
            price_dollars = price_cents / 100.0 # Convert cents to dollars

            current_book = order_book._bids if side == "yes" else order_book._asks
            current_size = current_book.get(price_dollars, 0.0)
            new_size = current_size + float(delta_size)

            if side == "yes": # Kalshi 'yes' is bids
                order_book._update_book_level('bid', price_dollars, new_size)
            elif side == "no": # Kalshi 'no' is asks
                order_book._update_book_level('ask', price_dollars, new_size)
            else:
                print(f"Warning: Unknown Kalshi side '{side}' in delta: {msg_content}")
        except (KeyError, TypeError, ValueError) as e:
            print(f"Error parsing Kalshi orderbook_delta data: {msg_content} - {e}")
        # print(f"Kalshi: Delta updated for {order_book.market_id}")
    else:
        print(f"Warning: Unhandled Kalshi message type: {msg_type}. Data: {data}")