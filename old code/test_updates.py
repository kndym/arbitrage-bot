from order_book import OrderBook
from polymarket.updates import update_polymarket_order_book
from kalshi.updates import update_kalshi_order_book
import json # For pretty printing example data

# --- Polymarket Example ---
print("--- Polymarket Example ---")
polymarket_market_id = "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"
pm_book = OrderBook(polymarket_market_id)

# Polymarket Initial Snapshot
pm_snapshot_data = {
  "event_type": "book",
  "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
  "market": polymarket_market_id,
  "bids": [
    { "price": ".48", "size": "30" },
    { "price": ".47", "size": "10" },
    { "price": ".49", "size": "20" }, # Out of order, class handles sorting
    { "price": ".50", "size": "15" }
  ],
  "asks": [
    { "price": ".52", "size": "25" },
    { "price": ".53", "size": "60" },
    { "price": ".54", "size": "10" }
  ],
  "timestamp": "123456789000",
  "hash": "0x0...."
}

print("\n--- Applying Polymarket Snapshot ---")
update_polymarket_order_book(pm_book, pm_snapshot_data)
print(pm_book)

# Polymarket Price Change (Delta - absolute new size)
pm_price_change_data = {
  "asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
  "changes": [
    {
      "price": "0.4", # New Ask
      "side": "SELL",
      "size": "33"
    },
    {
      "price": "0.5", # Modify Ask
      "side": "SELL",
      "size": "34"
    },
    {
      "price": "0.48", # Modify Bid
      "side": "BUY",
      "size": "50"
    },
    {
      "price": "0.50", # Remove Bid (size 0)
      "side": "BUY",
      "size": "0"
    }
  ],
  "event_type": "price_change",
  "market": polymarket_market_id, # Ensure market ID matches the order book
  "timestamp": "1729084877448",
  "hash": "3cd4d61e042c81560c9037ece0c61f3b1a8fbbdd"
}

print("\n--- Applying Polymarket Price Change ---")
update_polymarket_order_book(pm_book, pm_price_change_data)
print(pm_book)

# --- Kalshi Example ---
print("\n\n--- Kalshi Example ---")
kalshi_market_id = "TESTUSD-20231225" # Example Kalshi ticker
kalshi_book = OrderBook(kalshi_market_id)

# Kalshi Snapshot
kalshi_snapshot_data = {
	"sid": 123,
	"type": "orderbook_snapshot",
	"seq": 1,
	"msg": {
        "market_ticker": kalshi_market_id,
		"yes": [ # Bids
			[40, 100], # Price 0.40, Size 100
			[41, 50],
            [42, 200]
		],
		"no": [ # Asks
			[45, 150], # Price 0.45, Size 150
			[46, 75],
            [47, 120]
		]
	}
}

print("\n--- Applying Kalshi Snapshot ---")
update_kalshi_order_book(kalshi_book, kalshi_snapshot_data)
print(kalshi_book)

# Kalshi Delta
kalshi_delta_data = {
	"type": "orderbook_delta",
	"sid": 123,
	"seq": 2,
	"msg": {
		"market_ticker": kalshi_market_id,
		"price": 40, # Price 0.40
		"delta": -50, # Decrease size by 50
		"side": "yes" # Bid side
	}
}

print("\n--- Applying Kalshi Delta (Decrease Bid at 0.40) ---")
update_kalshi_order_book(kalshi_book, kalshi_delta_data)
print(kalshi_book)

kalshi_delta_data_2 = {
	"type": "orderbook_delta",
	"sid": 123,
	"seq": 3,
	"msg": {
		"market_ticker": kalshi_market_id,
		"price": 43, # Price 0.43
		"delta": 25, # Add new bid level
		"side": "yes"
	}
}
print("\n--- Applying Kalshi Delta (Add new Bid at 0.43) ---")
update_kalshi_order_book(kalshi_book, kalshi_delta_data_2)
print(kalshi_book)

kalshi_delta_data_3 = {
	"type": "orderbook_delta",
	"sid": 123,
	"seq": 4,
	"msg": {
		"market_ticker": kalshi_market_id,
		"price": 46, # Price 0.46
		"delta": -75, # Decrease Ask by 75 (removes it)
		"side": "no"
	}
}
print("\n--- Applying Kalshi Delta (Remove Ask at 0.46) ---")
update_kalshi_order_book(kalshi_book, kalshi_delta_data_3)
print(kalshi_book)