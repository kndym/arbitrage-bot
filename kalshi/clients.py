# --- START OF FILE clients.py (CORRECTED) ---

import requests
import base64
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
import json
import pprint as pp

from requests.exceptions import HTTPError

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

import websockets
import asyncio # Added for WebSocket client's message_queue and async operations

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger('KalshiClient') # A dedicated logger for Kalshi client classes


class Environment(Enum):
    DEMO = "demo"
    PROD = "prod"

# The 'env' variable here can be removed or set to a default, as it's often passed via constructor.
# env = Environment.PROD

class KalshiBaseClient:
    """Base client class for interacting with the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """Initializes the client with the provided API key and private key.

        Args:
            key_id (str): Your Kalshi API key ID.
            private_key (rsa.RSAPrivateKey): Your RSA private key.
            environment (Environment): The API environment to use (DEMO or PROD).
        """
        self.key_id = key_id
        self.private_key = private_key
        self.environment = environment
        self.last_api_call = datetime.now()
        self.logger = logger.getChild('BaseClient') # Child logger for more granular control

        if self.environment == Environment.DEMO:
            self.HTTP_BASE_URL = "https://demo-api.kalshi.co"
            self.WS_BASE_URL = "wss://demo-api.kalshi.co"
            self.logger.info("Kalshi client initialized for DEMO environment.")
        elif self.environment == Environment.PROD:
            self.HTTP_BASE_URL = "https://api.elections.kalshi.com"
            self.WS_BASE_URL = "wss://api.elections.kalshi.com"
            self.logger.info("Kalshi client initialized for PROD environment.")
        else:
            self.logger.error(f"Invalid environment specified: {environment}")
            raise ValueError("Invalid environment")

    def request_headers(self, method: str, path: str) -> Dict[str, Any]:
        """Generates the required authentication headers for API requests."""
        current_time_milliseconds = int(time.time() * 1000)
        timestamp_str = str(current_time_milliseconds)

        # Remove query params from path
        path_parts = path.split('?')
        clean_path = path_parts[0]

        msg_string = timestamp_str + method + clean_path
        self.logger.debug(f"Signing message string: '{msg_string}' for method {method} and path {clean_path}")
        signature = self.sign_pss_text(msg_string)

        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }
        self.logger.debug(f"Generated headers for {method} {path}: {headers.keys()}")
        return headers

    def sign_pss_text(self, text: str) -> str:
        """Signs the text using RSA-PSS and returns the base64 encoded signature."""
        message = text.encode('utf-8')
        try:
            signature = self.private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH
                ),
                hashes.SHA256()
            )
            b64_signature = base64.b64encode(signature).decode('utf-8')
            self.logger.debug("Successfully signed text.")
            return b64_signature
        except InvalidSignature as e:
            self.logger.error(f"RSA sign PSS failed for text: '{text}' - {e}", exc_info=True)
            raise ValueError("RSA sign PSS failed") from e
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during signing: {e}", exc_info=True)
            raise

class KalshiHttpClient(KalshiBaseClient):
    """Client for handling HTTP connections to the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        super().__init__(key_id, private_key, environment)
        self.host = self.HTTP_BASE_URL
        self.exchange_url = "/trade-api/v2/exchange"
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"
        self.logger = logger.getChild('HttpClient') # Child logger

    def rate_limit(self) -> None:
        """Built-in rate limiter to prevent exceeding API rate limits."""
        THRESHOLD_IN_MILLISECONDS = 100
        now = datetime.now()
        threshold_in_microseconds = 1000 * THRESHOLD_IN_MILLISECONDS
        threshold_in_seconds = THRESHOLD_IN_MILLISECONDS / 1000
        time_since_last_call = now - self.last_api_call
        if time_since_last_call < timedelta(microseconds=threshold_in_microseconds):
            sleep_time = threshold_in_seconds - time_since_last_call.total_seconds()
            self.logger.debug(f"Rate limit hit. Sleeping for {sleep_time:.4f} seconds.")
            time.sleep(sleep_time)
        self.last_api_call = datetime.now()
        self.logger.debug("Rate limit checked. API call proceeding.")

    def raise_if_bad_response(self, response: requests.Response) -> dict:
        """Raises an HTTPError if the response status code indicates an error."""
        if not (200 <= response.status_code <= 299):
            self.logger.error(f"HTTP request failed with status code {response.status_code} for {response.request.method} {response.url}")
            try:
                error_details = response.json()
                self.logger.error(f"Error response body: {json.dumps(error_details)}")
                return json.dumps(error_details)
            except json.JSONDecodeError:
                self.logger.error(f"Error response body (non-JSON): {response.text[:200]}...") # Log partial text
                return {}

    def post(self, path: str, body: dict) -> Any:
        """Performs an authenticated POST request to the Kalshi API."""
        full_url = self.host + path
        headers = self.request_headers("POST", path)
        self.logger.info(f"Making POST request to {full_url} with body: {json.dumps(body)}")
        self.rate_limit()
        try:
            response = requests.post(
                full_url,
                json=body,
                headers=headers
            )
            self.raise_if_bad_response(response)
            json_response = response.json()
            self.logger.debug(f"POST request to {full_url} successful. Response: {json.dumps(json_response)}")
            return json_response
        except HTTPError as e:
            self.logger.error(f"HTTPError during POST to {full_url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error during POST to {full_url}: {e}", exc_info=True)
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response for POST to {full_url}: {e}. Response text: {response.text[:200]}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during POST to {full_url}: {e}", exc_info=True)
            raise

    def get(self, path: str, params: Dict[str, Any] = {}) -> Any:
        """Performs an authenticated GET request to the Kalshi API."""
        full_url = self.host + path
        headers = self.request_headers("GET", path)
        self.logger.info(f"Making GET request to {full_url} with params: {params}")
        self.rate_limit()
        try:
            response = requests.get(
                full_url,
                headers=headers,
                params=params
            )
            self.raise_if_bad_response(response)
            json_response = response.json()
            self.logger.debug(f"GET request to {full_url} successful. Response: {json.dumps(json_response)}")
            return json_response
        except HTTPError as e:
            self.logger.error(f"HTTPError during GET to {full_url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error during GET to {full_url}: {e}", exc_info=True)
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response for GET to {full_url}: {e}. Response text: {response.text[:200]}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during GET to {full_url}: {e}", exc_info=True)
            raise

    def delete(self, path: str, params: Dict[str, Any] = {}) -> Any:
        """Performs an authenticated DELETE request to the Kalshi API."""
        full_url = self.host + path
        headers = self.request_headers("DELETE", path)
        self.logger.info(f"Making DELETE request to {full_url} with params: {params}")
        self.rate_limit()
        try:
            response = requests.delete(
                full_url,
                headers=headers,
                params=params
            )
            self.raise_if_bad_response(response)
            json_response = response.json()
            self.logger.debug(f"DELETE request to {full_url} successful. Response: {json.dumps(json_response)}")
            return json_response
        except HTTPError as e:
            self.logger.error(f"HTTPError during DELETE to {full_url}: {e}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error during DELETE to {full_url}: {e}", exc_info=True)
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response for DELETE to {full_url}: {e}. Response text: {response.text[:200]}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during DELETE to {full_url}: {e}", exc_info=True)
            raise

    def get_balance(self) -> Dict[str, Any]:
        """Retrieves the account balance."""
        self.logger.info("Retrieving account balance.")
        return self.get(self.portfolio_url + '/balance')

    def get_exchange_status(self) -> Dict[str, Any]:
        """Retrieves the exchange status."""
        self.logger.info("Retrieving exchange status.")
        return self.get(self.exchange_url + "/status")

    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        max_ts: Optional[int] = None,
        min_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Retrieves trades based on provided filters."""
        params = {
            'ticker': ticker,
            'limit': limit,
            'cursor': cursor,
            'max_ts': max_ts,
            'min_ts': min_ts,
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        self.logger.info(f"Retrieving trades with parameters: {params}")
        return self.get(self.markets_url + '/trades', params=params)

class KalshiWebSocketClient(KalshiBaseClient):
    """Client for handling WebSocket connections to the Kalshi API."""
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment, # EDITED: Corrected type hint from Environment.DEMO to Environment
        message_queue: Optional[asyncio.Queue], # Make message_queue explicit for constructor
        ticker_list: list
    ):
        super().__init__(key_id, private_key, environment)
        self.ws = None
        self.url_suffix = "/trade-api/ws/v2"
        self.message_id = 1  # Add counter for message IDs
        self.message_queue = message_queue # Assign the queue
        self.logger = logger.getChild('WebSocketClient') # Child logger
        self.ticker_list = ticker_list 
        self.server_id=0


    async def connect(self): # EDITED: Removed tickers_to_subscribe argument here
        """Establishes a WebSocket connection using authentication."""
        host = self.WS_BASE_URL + self.url_suffix
        auth_headers = self.request_headers("GET", self.url_suffix)
        self.logger.info(f"Attempting to connect to Kalshi WebSocket: {host}")

        try:
            self.ws = await websockets.connect(host, additional_headers=auth_headers, proxy=None)
            self.logger.info(f"Successfully connected to Kalshi WebSocket: {host}")
            await self.subscribe_to_tickers() # This method uses self.ticker_list
        except websockets.ConnectionClosed as e:
            await self.on_close(e.code, e.reason)
            self.logger.warning(f"WebSocket connection closed unexpectedly. Code: {e.code}, Reason: {e.reason}")
            self.ws = None # Set to None on failure
        except ConnectionRefusedError:
            self.logger.error(f"Connection refused for {host}. Is the server running and accessible?")
            self.ws = None # Set to None on failure
        except Exception as e:
            await self.on_error(e)
            self.logger.critical(f"Unhandled exception during WebSocket connection: {e}", exc_info=True)
            self.ws = None # Set to None on failure


    async def subscribe_to_tickers(self):
        """Subscribe to ticker updates for specified markets."""
        if not self.ticker_list:
            self.logger.warning("No tickers provided for subscription. Skipping subscription.")
            return
        subscription_message = {
            "id": self.message_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"], # Ensure this is correct for your desired data
            }
        }
        if len(self.ticker_list)!=1:
            subscription_message["params"]["market_tickers"]=self.ticker_list
        else:
            subscription_message["params"]["market_ticker"]=self.ticker_list[0]

        self.logger.info(f"Sending subscription message: {json.dumps(subscription_message)}")
        if self.ws:
            try:
                await self.ws.send(json.dumps(subscription_message))
                self.message_id += 1
                self.logger.info(f"Subscription request sent for tickers: {self.ticker_list}")
            except Exception as e:
                self.logger.error(f"Error sending subscription message: {e}", exc_info=True)
        else:
            self.logger.warning("WebSocket not connected or closed. Cannot send subscription message.")
    
    async def unsubscribe(self, ticker):
        unsubscription_message = {
            "id": self.message_id,
            "cmd": "update_subscription",
            "params": {
                "sids": [456], # NOTE: sids are specific to an active subscription. You might need to track these.
                "market_tickers": [ticker],
                "action": "delete_markets"
            }
        }

        self.logger.info(f"Sending unsubscription message: {json.dumps(unsubscription_message)}")
        if self.ws:
            try:
                await self.ws.send(json.dumps(unsubscription_message))
                self.message_id += 1
                self.logger.info(f"UNubscription request sent for tickers: {ticker}")
            except Exception as e:
                self.logger.error(f"Error sending unsubscription message: {e}", exc_info=True)
        else:
            self.logger.warning("WebSocket not connected or closed. Cannot send unsubscription message.")


    async def listen(self):
        """Handle incoming messages."""
        self.logger.info("Starting WebSocket message handler.")
        # EDITED: Add null check for self.ws before async for loop (critical fix)
        if not self.ws:
            self.logger.error("Kalshi WebSocket (self.ws) is None. Cannot start listening.")
            return
        
        try:
            async for message in self.ws:
                try:
                    data=json.loads(message)
                    await self.on_message(data)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Failed to decode JSON from WebSocket message: {e}. Message: {message[:200]}...")
                except Exception as e:
                    self.logger.error(f"Error processing received message: {e}. Message: {message[:200]}...", exc_info=True)
        except websockets.ConnectionClosedOK:
            self.logger.info("WebSocket connection closed gracefully during handler loop.")
        except websockets.ConnectionClosedError as e:
            self.logger.error(f"WebSocket connection closed with error during handler loop: Code={e.code}, Reason={e.reason}")
            self.ws = None # Set to None on error closure
        except Exception as e:
            self.logger.critical(f"Unhandled exception in WebSocket handler: {e}", exc_info=True)
            self.ws = None # Set to None on error

    async def on_message(self, data):
        """Callback for handling incoming messages."""
        self.logger.debug("Received message from Kalshi WebSocket.")
        if self.message_queue:
            try:
                event_type=data.get("type", "unknown")
                if event_type in ["orderbook_snapshot", "orderbook_delta"]:
                    await self.message_queue.put(('kalshi', data))
                    self.logger.debug(f"Put '{event_type}' event into queue from Kalshi.")
                elif event_type == "market_lifecycle_v2":
                    await self.message_queue.put(('update', data)) # Consider a more specific key if 'update' is general
                    self.logger.debug(f"Put '{event_type}' event into queue from Kalshi.")
                    pp.pprint(data)
                else:
                    if event_type in ["subscribed", "error"]:
                        pp.pprint(data)
                    self.logger.info(f"Unkown Kalshi event called {event_type}")
            except Exception as e:
                self.logger.error(f"Error putting message into queue: {e}. Message data: {json.dumps(data)}", exc_info=True)
        else:
            self.logger.warning("No message queue set for Kalshi WebSocket. Message will not be processed by consumer.")

    async def on_error(self, error):
        """Callback for handling errors."""
        self.logger.error(f"WebSocket error: {error}", exc_info=True)

    async def on_close(self, close_status_code, close_msg):
        """Callback when WebSocket connection is closed."""
        self.logger.info(f"WebSocket connection closed with code: {close_status_code} and message: '{close_msg}'")
    
    async def close(self):
        """Closes the Kalshi WebSocket connection."""
        if self.ws:
            await self.ws.close()
            logging.info("Kalshi WebSocket connection closed.")
            self.ws = None # Ensure it's cleared
        else:
            logging.info("Kalshi WebSocket not connected.")

    async def disconnect(self):
        """Closes the Kalshi WebSocket connection gracefully. (Alias for close)"""
        await self.close()

# --- EDITED: REMOVED THE EXAMPLE `main()` FUNCTION FROM HERE ---
# This function should not be in the client library file.
# It caused conflicts and unnecessary dummy key generation.