import asyncio
import websockets
import json
import logging
import ssl
import certifi  # Useful for cross-platform certificate handling
import pprint

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Polymarket CLOB WebSocket URI
# We'll connect to the /market endpoint for market data subscriptions
POLYMARKET_MARKET_WSS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Example Asset ID (Token ID) from the documentation.
# You can add more asset IDs here in a list: ["ID1", "ID2", "ID3"]
MARKET_ASSET_IDS = ["65818619657568813474341868652308942079804919287380422192892211131408793125422"]


class PolymarketWSS:
    def __init__(self, uri, asset_ids, message_queue):
        self.uri = uri
        self.asset_ids = asset_ids
        self.message_queue = message_queue
        self.websocket = None
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    async def connect(self):
        """Connects to the Polymarket WebSocket."""
        try:
            logging.info(f"Connecting to Polymarket WebSocket: {self.uri}")
            self.websocket = await websockets.connect(self.uri, ssl=self.ssl_context)
            logging.info(f"Connected to Polymarket WebSocket: {self.uri}")

            # Subscribe to the specified market data after connecting
            await self._subscribe_to_market_data()

        except ConnectionRefusedError:
            logging.error(f"Connection refused for {self.uri}. Make sure the URI is correct and accessible.")
            self.websocket = None  # Ensure websocket is None on failure
            # Implement a reconnection strategy here
        except Exception as e:
            logging.error(f"Error connecting to Polymarket WebSocket: {e}")
            self.websocket = None  # Ensure websocket is None on failure
            # Implement a reconnection strategy here

    async def _subscribe_to_market_data(self):
        """Sends the subscription message for market data."""
        if self.websocket and not self.websocket.closed:
            # According to the documentation, the subscription message requires:
            # - type: "MARKET" (for the market channel)
            # - assets_ids: A list of asset IDs (token IDs)
            # - No authentication is needed for the MARKET channel.
            subscribe_message = {
                "type": "MARKET",  # CORRECTED: Changed from "Market" to "MARKET" to match docs
                "assets_ids": self.asset_ids
            }
            try:
                await self.websocket.send(json.dumps(subscribe_message))
                logging.info(f"Sent subscription request for MARKET channel with asset IDs: {self.asset_ids}")
            except Exception as e:
                logging.error(f"Error sending subscription message to Polymarket: {e}")
        else:
            logging.warning("Polymarket WebSocket not connected or closed. Cannot send subscription message.")

    async def listen(self):
        """Listens for messages from the Polymarket WebSocket."""
        if not self.websocket:
            logging.warning("Polymarket WebSocket not connected. Attempting to reconnect...")
            await self.connect()
            if not self.websocket:
                logging.error("Failed to reconnect to Polymarket WebSocket. Listener stopping.")
                return

        try:
            async for message in self.websocket:
                # Process the message (e.g., parse JSON)
                try:
                    all_events = json.loads(message)

                    # The Polymarket Market channel can send a list of event objects in a single message.
                    if not isinstance(all_events, list):
                        all_events = [all_events]  # Ensure we always iterate over a list

                    for data in all_events:
                        event_type = data.get("event_type")

                        # The MARKET channel provides "book", "price_change", and "tick_size_change" events.
                        if event_type in ["book", "price_change", "tick_size_change"]:
                            # Note on 'price_change' events: The `data` object contains a 'changes'
                            # key, which is a list of individual price level updates.
                            # We put the entire event object into the queue to preserve context
                            # like timestamp and hash. The consumer can then iterate `data['changes']`.
                            await self.message_queue.put(('polymarket', data))
                            logging.debug(f"Put {event_type} event into queue from Polymarket")
                        else:
                            # Log other messages (e.g., initial confirmations, errors)
                            logging.info(f"Received non-market-data event from Polymarket:\n{pprint.pformat(data)}")

                except json.JSONDecodeError:
                    logging.warning(f"Failed to decode JSON from Polymarket: {message}")
                except Exception as e:
                    logging.error(f"Error processing message from Polymarket: {e}")
        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Polymarket WebSocket connection closed gracefully.")
        except websockets.exceptions.ConnectionClosedError as e:
            logging.error(f"Polymarket WebSocket connection closed with error: {e}")
            # Implement a reconnection strategy here
        except Exception as e:
            logging.error(f"Error in Polymarket WebSocket listen: {e}")
            # Implement a reconnection strategy here

    async def send(self, message):
        """
        Sends a message to the Polymarket WebSocket.
        Note: The MARKET channel is for receiving data. Sending messages other
        than the initial subscription has no documented effect.
        """
        if self.websocket and not self.websocket.closed:
            try:
                await self.websocket.send(json.dumps(message))
                logging.debug(f"Sent to Polymarket: {message}")
            except Exception as e:
                logging.error(f"Error sending message to Polymarket WebSocket: {e}")
        else:
            logging.warning("Polymarket WebSocket not connected or closed. Cannot send message.")

    async def close(self):
        """Closes the Polymarket WebSocket connection."""
        if self.websocket:
            await self.websocket.close()
            logging.info("Polymarket WebSocket connection closed.")


# Example usage (for testing the class individually)
async def main():
    message_queue = asyncio.Queue()
    # Replace with actual asset IDs you want to subscribe to
    polymarket_wss = PolymarketWSS(POLYMARKET_MARKET_WSS_URI, MARKET_ASSET_IDS, message_queue)
    await polymarket_wss.connect()

    if polymarket_wss.websocket:
        # Start listening in the background only if connection was successful
        asyncio.create_task(polymarket_wss.listen())

        print("Polymarket listener started. Waiting for messages... (Press Ctrl+C to stop)")
        while True:
            # Get messages from the queue (this simulates the main processor)
            source, message = await message_queue.get()
            print(f"\n--- Main received from {source} ---")
            pprint.pprint(message)
    else:
        print("Could not start listener, connection to WebSocket failed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Shutting down.")