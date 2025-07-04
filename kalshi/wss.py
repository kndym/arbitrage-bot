import asyncio
import websockets
import json
import logging
from kalshi.clients import KalshiWebSocketClient, Environment
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from dotenv import load_dotenv
import os
from cryptography.hazmat.primitives import serialization

# Load environment variables
load_dotenv()
env = Environment.PROD # toggle environment here
KEYID = os.getenv('DEMO_KEYID') if env == Environment.DEMO else os.getenv('PROD_KEYID')
KEYFILE = os.getenv('DEMO_KEYFILE') if env == Environment.DEMO else os.getenv('PROD_KEYFILE')




try:
    with open(KEYFILE, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None  # Provide the password if your key is encrypted
        )
except FileNotFoundError:
    raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
except Exception as e:
    raise Exception(f"Error loading private key: {str(e)}")

try:
    with open(KEYFILE, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None  # Provide the password if your key is encrypted
        )
except FileNotFoundError:
    raise FileNotFoundError(f"Private key file not found at {KEYFILE}")
except Exception as e:
    raise Exception(f"Error loading private key: {str(e)}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KalshiWSS(KalshiWebSocketClient):
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment.DEMO,
        message_queue: asyncio.Queue,
        ticker_list: list
    ):
        super().__init__(key_id, private_key, environment, message_queue, ticker_list)
        self.message_queue=message_queue
        self.ticker_list = ticker_list

