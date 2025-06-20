import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
import asyncio
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature
from clients import KalshiHttpClient, KalshiWebSocketClient, Environment
import pprint
import requests
import datetime

# Load environment variables
load_dotenv()
env = Environment.PROD # toggle environment here
KEYID = os.getenv('DEMO_KEYID') if env == Environment.DEMO else os.getenv('PROD_KEYID')
KEYFILE = os.getenv('DEMO_KEYFILE') if env == Environment.DEMO else os.getenv('PROD_KEYFILE')
BASE_URL = 'https://api.elections.kalshi.com'

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

def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    # Before signing, we need to hash our message.
    # The hash is what we actually sign.
    # Convert the text to bytes
    message = text.encode('utf-8')
    try:
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    except InvalidSignature as e:
        raise ValueError("RSA sign PSS failed") from e


# Initialize the HTTP client
client = KalshiHttpClient(
    key_id=KEYID,
    private_key=private_key,
    environment=env
)




def get_headers():
    # Get the current time
    current_time = datetime.datetime.now()

    # Convert the time to a timestamp (seconds since the epoch)
    timestamp = current_time.timestamp()
    # Convert the timestamp to milliseconds
    current_time_milliseconds = int(timestamp * 1000)
    timestampt_str = str(current_time_milliseconds)

    method = "GET"
    path='/trade-api/v2/portfolio/balance'

    msg_string = timestampt_str + method + path

    sig = sign_pss_text(private_key, msg_string)

    headers = {
            'KALSHI-ACCESS-KEY': KEYID,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': timestampt_str
        }
    return headers

def find_markets(title_string):
    cursor=""
    for x in range(100):
        headers = {"accept": "application/json",
                    }

        path_2=f"/trade-api/v2/events?limit=200&status=open&with_nested_markets=true&cursor={cursor}"

        r = requests.get(BASE_URL + path_2, headers=headers)
        response=r.json()
        #pprint.pp(response)
        cursor=response["cursor"]
        #pprint.pp(response["events"][0])
        for event in response["events"]:
            if title_string in event["title"]:
                pprint.pp(event["title"])
                for market in event["markets"]:
                    pprint.pp(market)
        if cursor=="":
            break

