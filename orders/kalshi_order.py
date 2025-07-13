# kalshi_api.py
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization


from kalshi.clients import Environment, KalshiHttpClient


def buy_kalshi_contract():
    """
    Connects to Kalshi and places a buy order using a direct internet connection.
    """
    load_dotenv()
    print("\n--- Executing Kalshi Trade (Direct Connection) ---")

    try:
        env = Environment.PROD
        key_id = os.getenv('PROD_KEYID')
        key_file_path = os.getenv('PROD_KEYFILE')

        if not key_id or not key_file_path:
            raise ValueError("PROD_KEYID or PROD_KEYFILE clients.Environment variables not set.")

        with open(key_file_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )

        client = KalshiHttpClient(
            key_id=key_id,
            private_key=private_key,
            Environment=env
        )

        path = "/trade-api/v2/portfolio/orders"
        body = {
            "action": "buy",
            "client_order_id": "test_v1",
            "count": 1,
            "side": 'yes',
            "ticker": 'KXMLBGAME-25JUL12ARILAA-ARI',
            "type": 'market'
        }

        balance = client.get_balance()
        print("[INFO] Kalshi Balance:", balance)

        print("[INFO] Posting order to Kalshi...")
        response = client.post(path=path, body=body)
        print("[SUCCESS] Kalshi Response:", response)

    except FileNotFoundError:
        print(f"[ERROR] Private key file not found at {key_file_path}")
    except Exception as e:
        print(f"[ERROR] An error occurred during the Kalshi transaction: {str(e)}")