import asyncio
import websockets
import json
import logging
import pprint as pp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Polymarket CLOB WebSocket URI
# We'll connect to the /market endpoint for market data subscriptions
POLYMARKET_MARKET_WSS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"  # Replace with the actual WSS URL if different



class PolymarketWSS:
    def __init__(self, uri, asset_ids, message_queue):
        self.uri = uri
        self.asset_ids = asset_ids # This list will now be dynamically managed
        self.message_queue = message_queue
        self.websocket = None

    async def connect(self):
        """Connects to the Polymarket WebSocket and subscribes to current asset_ids."""
        try:
            logging.info(f"Connecting to Polymarket WebSocket: {self.uri}")
            self.websocket = await websockets.connect(self.uri)
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
        """Sends the subscription message for market data using current asset_ids."""
        if self.websocket:
            if not self.asset_ids:
                logging.info("No Polymarket assets to subscribe to. Skipping subscription.")
                return

            subscribe_message = {
                "type": "MARKET",
                "assets_ids": self.asset_ids
            }
            try:
                await self.websocket.send(json.dumps(subscribe_message))
                logging.info(f"Sent subscription request for MARKET channel with asset IDs: {self.asset_ids}")
            except Exception as e:
                logging.error(f"Error sending subscription message to Polymarket: {e}")
        else:
            logging.warning("Polymarket WebSocket not connected or closed. Cannot send subscription message.")

    async def _subscribe_to_user_data(self):
        """Sends the subscription message for market data using current asset_ids."""
        if self.websocket:
            if not self.asset_ids:
                logging.info("No Polymarket assets to subscribe to. Skipping subscription.")
                return

            subscribe_message = {
                "type": "MARKET",
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

                        if event_type in ["book", "price_change", "tick_size_change", "last_trade_price"]:
                            await self.message_queue.put(('polymarket', data))
                            logging.debug(f"Put {event_type} event into queue from Polymarket")
                        else:
                            logging.info(f"Received non-market-data event from Polymarket: {data}")

                except json.JSONDecodeError:
                    logging.warning(f"Failed to decode JSON from Polymarket: {message}")
                except Exception as e:
                    logging.error(f"Error processing message from Polymarket: {e}")
        except websockets.exceptions.ConnectionClosedOK:
            logging.info("Polymarket WebSocket connection closed gracefully.")
        except websockets.exceptions.ConnectionClosedError as e:
            logging.error(f"Polymarket WebSocket connection closed with error: {e}")
            # Implement a reconnection strategy here if desired (e.g., attempt connect after a delay)
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

    async def disconnect(self):
        """Closes the Polymarket WebSocket connection gracefully."""
        if self.websocket:
            logging.info("Closing Polymarket WebSocket connection...")
            await self.websocket.close()
            self.websocket = None # Clear the websocket instance
            logging.info("Polymarket WebSocket connection closed.")
        else:
            logging.info("Polymarket WebSocket not connected.")

    # NEW METHOD: Encapsulates the unsubscription logic
    async def unsubscribe(self, asset_id: str):
        """
        Removes an asset from the subscribed list and re-establishes
        the connection with the updated subscription set.
        """
        if asset_id in self.asset_ids:
            self.asset_ids.remove(asset_id)
            logging.info(f"Removed asset ID {asset_id} from Polymarket subscription list. Reconnecting WSS.")
            
            # Disconnect the current WebSocket
            await self.disconnect()
            
            # Reconnect, which will trigger a new subscription with the updated asset_ids list
            await self.connect()
            logging.info(f"Polymarket WSS reconnected with updated subscriptions.")
        else:
            logging.debug(f"Polymarket asset ID {asset_id} not in active subscriptions, no action needed for unsubscribe.")