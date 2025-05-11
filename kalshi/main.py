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

# Load environment variables
load_dotenv()
env = Environment.DEMO # toggle environment here
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

# Get account balance
balance = client.get_balance()
print("Balance:", balance)

if False:
    # Initialize the WebSocket client
    ws_client = KalshiWebSocketClient(
        key_id=KEYID,
        private_key=private_key,
        environment=env
    )
    
    # Connect via WebSocket
    asyncio.run(ws_client.connect())


import requests
import datetime

if True:
    # Get the current time
    current_time = datetime.datetime.now()

    # Convert the time to a timestamp (seconds since the epoch)
    timestamp = current_time.timestamp()

    # Convert the timestamp to milliseconds
    current_time_milliseconds = int(timestamp * 1000)
    timestampt_str = str(current_time_milliseconds)

    method = "GET"
    base_url = 'https://api.elections.kalshi.com'
    path='/trade-api/v2/portfolio/balance'


    msg_string = timestampt_str + method + path

    sig = sign_pss_text(private_key, msg_string)

    headers = {
            'KALSHI-ACCESS-KEY': KEYID,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': timestampt_str
        }
    if False:
        response = requests.get(base_url + path, headers=headers)
        print("Status Code:", response.status_code)
        print("Response Body:", response.text)
    if False:
        headers = {"accept": "application/json"}

        path_2="/trade-api/v2/events"

        response = requests.get(base_url + path_2, headers=headers)

        pprint.pp(response.text)
    if True:
        cursor=""
        for x in range(100):
            headers = {"accept": "application/json",
                        }

            path_2=f"/trade-api/v2/events?limit=200&status=open&with_nested_markets=true&cursor={cursor}"

            r = requests.get(base_url + path_2, headers=headers)
            response=r.json()
            #pprint.pp(response)
            cursor=response["cursor"]
            #pprint.pp(response["events"][0])
            for event in response["events"]:
                if "Minnesota vs Golden State"in event["title"]:
                    pprint.pp(event["title"])
                    for market in event["markets"]:
                        pprint.pp(market)
            if cursor=="":
                break
        