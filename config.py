import json

MARKETS_FILE='jsons\markets_07_13.json'
COMP_FILE='jsons\compliment_07_13.json'

RUN_DURATION_MINUTES = 5
OUTPUT_FILE_NAME = "order_book_snapshot.txt"
JSON_OUTPUT_FILE_NAME = "order_book_updates.json" 
PRINT_INTERVAL_SECONDS = 10 # New constant for print interval

with open(MARKETS_FILE) as json_file:
    MARKET_MAPPING = json.load(json_file)

with open(COMP_FILE) as json_file:
    COMPLEMENTARY_MARKET_PAIRS = json.load(json_file)

poly_asset_ids_to_subscribe = [
    ids["polymarket"] for ids in MARKET_MAPPING.values() if "polymarket" in ids
]
kalshi_tickers_to_subscribe = [
    ids["kalshi"] for ids in MARKET_MAPPING.values() if "kalshi" in ids
]



