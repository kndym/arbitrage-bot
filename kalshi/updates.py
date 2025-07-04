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

        # "yes" side in Kalshi represents asks for "Yes" shares directly
        for level in msg_content.get("yes", []):
            try:
                price_cents = level[0]
                size = level[1]
                price_dollars = price_cents / 100.0 # Price to SELL a YES share
                order_book._update_book_level('bid', price_dollars, float(size))
            except (IndexError, TypeError, ValueError) as e:
                print(f"Error parsing Kalshi 'yes' (ask) data: {level} - {e}")
                continue

        # "no" side in Kalshi represents offers to SELL "No" shares.
        # Selling a "No" share at P_no is equivalent to BUYING a "Yes" share at (1 - P_no).
        # So, these are BIDS for "Yes" shares.
        for level in msg_content.get("no", []):
            try:
                price_cents = level[0]
                size = level[1]
                kalshi_no_price_dollars = price_cents / 100.0
                
                # The effective price for the "Yes" share bid is 1 - Kalshi_No_Price
                yes_share_bid_price = 1.0 - kalshi_no_price_dollars
                
                # We need to handle floating point precision for 1.0 - X scenarios
                # Ensure the price is non-negative and not excessively small
                yes_share_bid_price = max(0.0, round(yes_share_bid_price, 4)) # Round to 4 decimal places for consistency

                order_book._update_book_level('ask', yes_share_bid_price, float(size))
            except (IndexError, TypeError, ValueError) as e:
                print(f"Error parsing Kalshi 'no' (bid for Yes) data: {level} - {e}")
                continue
        # print(f"Kalshi: Snapshot updated for {order_book.market_id}")

    elif msg_type == "orderbook_delta":
        # This is a delta update
        try:
            price_cents = msg_content["price"]
            delta_size = msg_content["delta"]
            side = msg_content["side"]
            
            kalshi_raw_price_dollars = price_cents / 100.0

            if side == "yes": # Kalshi 'yes' is asks for Yes shares directly
                target_price = kalshi_raw_price_dollars
                update_side = 'bid'
                current_book = order_book._asks
            elif side == "no": # Kalshi 'no' is bids for Yes shares (derived from 1 - No_Price)
                target_price = 1.0 - kalshi_raw_price_dollars
                target_price = max(0.0, round(target_price, 4)) # Round for consistency
                update_side = 'ask'
                current_book = order_book._bids
            else:
                print(f"Warning: Unknown Kalshi side '{side}' in delta: {msg_content}")
                return # Skip this update

            current_size = current_book.get(target_price, 0.0)
            new_size = current_size + float(delta_size)

            order_book._update_book_level(update_side, target_price, new_size)

        except (KeyError, TypeError, ValueError) as e:
            print(f"Error parsing Kalshi orderbook_delta data: {msg_content} - {e}")
    else:
        print(f"Warning: Unhandled Kalshi message type: {msg_type}. Data: {data}")