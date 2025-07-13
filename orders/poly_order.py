# polymarket_api.py
import os
import requests
import json
from datetime import datetime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
from py_clob_client.order_builder.constants import BUY
from dotenv import load_dotenv

def verify_tor_connection():
    """
    Verifies that the current connection is routed through Tor by checking the public IP.
    """
    # This function expects the proxy environment variables to be set
    proxies = {
        'http': os.environ.get('HTTP_PROXY'),
        'https': os.environ.get('HTTPS_PROXY')
    }
    if not proxies['http']:
        print("[ERROR] Proxy environment variables not set for Tor verification.")
        return

    try:
        response = requests.get("http://ip-api.com/json/", proxies=proxies)
        response.raise_for_status()
        result = response.json()
        print(f'[SUCCESS] TOR IP [{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}]: {result.get("query")} {result.get("country")}')
        return True
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f'[ERROR] Could not verify Tor connection: {e}')
        return False

def buy_polymarket_contract():
    """
    Connects to Polymarket and places a buy order.
    IMPORTANT: This function requires HTTP_PROXY and HTTPS_PROXY environment
    variables to be set to route traffic through the Tor proxy.
    """
    load_dotenv()
    print("\n--- Executing Polymarket Trade (via Tor) ---")

    if not verify_tor_connection():
        print("[ERROR] Halting Polymarket trade due to Tor connection failure.")
        return

    try:
        host = "https://clob.polymarket.com"
        key = os.getenv("WALLET_PRIVATE_KEY")
        polymarket_proxy_address = os.getenv("POLYMARKET_PROXY_ADDRESS")
        chain_id = 137

        client = ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=polymarket_proxy_address)
        creds = ApiCreds(
            api_key=os.getenv("CLOB_API_KEY"),
            api_secret=os.getenv("CLOB_SECRET"),
            api_passphrase=os.getenv("CLOB_PASS_PHRASE"),
        )
        client.set_api_creds(creds)

        order_args = OrderArgs(
            side=BUY,
            token_id="17793181274537832236554079944355295589343285861808056506317793204247664921006",
            size=1.0,
            price=0.47
        )

        print("[INFO] Creating signed order for Polymarket...")
        signed_order = client.create_order(order_args)
        
        print("[INFO] Posting Fill-Or-Kill (FOK) order...")
        resp = client.post_order(signed_order, OrderType.FOK)
        print("[SUCCESS] Polymarket Response:", resp)
        
    except Exception as e:
        print(f"[ERROR] An error occurred during the Polymarket transaction: {e}")