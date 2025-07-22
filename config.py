# --- START OF FILE config.py ---

import json
import os
from dotenv import load_dotenv

load_dotenv()

# Updated to use the newer JSON files provided in the prompt
MARKETS_FILE='jsons/markets_07_21.json'
COMP_FILE='jsons/compliment_07_21.json'

RUN_DURATION_MINUTES = 5
OUTPUT_FILE_NAME = "order_book_snapshot_7_19.txt"
JSON_OUTPUT_FILE_NAME = "order_book_updates_7_19.json" 
PRINT_INTERVAL_SECONDS = 10 # New constant for print interval

with open(MARKETS_FILE) as json_file:
    MARKET_MAPPING = json.load(json_file)

# This was missing from the original logic but is crucial for the new strategy
with open(COMP_FILE) as json_file:
    COMPLEMENTARY_MARKET_PAIRS = json.load(json_file)

poly_asset_ids_to_subscribe = [
    ids["polymarket"] for ids in MARKET_MAPPING.values() if "polymarket" in ids
]
kalshi_tickers_to_subscribe = [
    ids["kalshi"] for ids in MARKET_MAPPING.values() if "kalshi" in ids
]

CLOB_API_KEY=os.getenv("CLOB_API_KEY")
CLOB_SECRET=os.getenv("CLOB_SECRET")
CLOB_PASSPHRASE=os.getenv("CLOB_PASSPHRASE")


WALLET_PRIVATE_KEY=os.getenv("WALLET_PRIVATE_KEY")
WALLET_PUBLIC_KEY=os.getenv("WALLET_PUBLIC_KEY")

DEMO_KEYID=os.getenv("DEMO_KEYID")
DEMO_KEYFILE=os.getenv("DEMO_KEYFILE")
PROD_KEYID=os.getenv("PROD_KEYID")
PROD_KEYFILE=os.getenv("PROD_KEYFILE")

POLYMARKET_PROXY_ADDRESS=os.getenv("POLYMARKET_PROXY_ADDRESS")

AUTH={'apiKey': CLOB_API_KEY,
      'secret': CLOB_SECRET,
      'passphrase': CLOB_PASSPHRASE}