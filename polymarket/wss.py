import asyncio
import websockets
import json
import logging
import pprint as pp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Polymarket CLOB WebSocket base URI
POLYMARKET_WSS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/"

class PolymarketWSS:
    def __init__(self, uri, asset_ids, message_queue, auth):
        self.base_uri = uri
        self.market_uri = uri + "market"
        self.user_uri = uri + "user"
        self.asset_ids = asset_ids  # This list will now be dynamically managed
        self.message_queue = message_queue
        self.auth = auth
        self.market =None
        self.user = None

    async def connect(self):
        """Connects to both the market and user Polymarket WebSockets concurrently."""
        try:
            # Establish both connections concurrently
            results = await asyncio.gather(
                self._connect_to_endpoint(self.market_uri),
                self._connect_to_endpoint(self.user_uri),
                return_exceptions=True  # Prevent one failure from stopping the other
            )

            market_conn, user_conn = results



            if market_conn:
                self.market = market_conn
                logging.info(f"Connected to Polymarket WebSocket: {self.market_uri}")
                await self._subscribe_to_market_data()
            else:
                logging.error(f"Failed to connect to {self.market_uri}: {market_conn}")

            if user_conn:
                self.user = user_conn
                logging.info(f"Connected to Polymarket WebSocket: {self.user_uri}")
                await self._subscribe_to_user_data()
            else:
                logging.error(f"Failed to connect to {self.user_uri}: {user_conn}")

        except Exception as e:
            logging.error(f"Error during concurrent connection process: {e}")

    async def _connect_to_endpoint(self, uri):
        """A helper function to connect to a single WebSocket endpoint."""
        try:
            return await websockets.connect(uri)
        except Exception as e:
            logging.error(f"Error connecting to {uri}: {e}")
            return None # Return exception to be handled by the gather call

    async def _subscribe_to_market_data(self):
        """Sends the subscription message for market data."""
        if self.market:
            if not self.asset_ids:
                logging.info("No Polymarket assets to subscribe to. Skipping market subscription.")
                return

            subscribe_message = {
                "type": "market",
                "assets_ids": self.asset_ids
            }
            try:
                await self.market.send(json.dumps(subscribe_message))
                logging.info(f"Sent subscription request for MARKET channel with asset IDs: {self.asset_ids}")
            except Exception as e:
                logging.error(f"Error sending subscription message to Market WebSocket: {e}")
        else:
            logging.warning("Market WebSocket not connected. Cannot send subscription message.")

    async def _subscribe_to_user_data(self):
        """Sends the subscription message for user data."""
        if self.user:
            if not self.asset_ids:
                logging.info("No Polymarket assets to track. Skipping user subscription.")
                return

            subscribe_message = {
                "type": "USER",
                "markets": self.asset_ids,
                "auth": self.auth
            }
            try:
                await self.user.send(json.dumps(subscribe_message))
                logging.info("PING")
                await self.user.send("PING")
                logging.info(f"Sent subscription request for USER channel with asset IDs: {self.asset_ids}")
            except Exception as e:
                logging.error(f"Error sending subscription message to User WebSocket: {e}")
        else:
            logging.warning("User WebSocket not connected. Cannot send subscription message.")

    async def _listen_loop(self, websocket, name: str):
        """Generic listening loop for a single websocket connection."""
        while True:
            if not websocket:
                logging.warning(f"Polymarket {name} WebSocket not connected. Attempting to reconnect...")
                # Specific reconnection logic can be placed here if needed
                # For simplicity, we break the loop and rely on the outer management to reconnect.
                await asyncio.sleep(5) # Cooldown before next check
                # A more robust implementation would try to reconnect here.
                # For example: await self._reconnect_endpoint(name)
                logging.error(f"Connection to {name} lost. Listener for this endpoint is stopping.")
                break

            try:
                async for message in websocket:
                    try:
                        all_events = json.loads(message)
                        if not isinstance(all_events, list):
                            all_events = [all_events]

                        for data in all_events:
                            event_type = data.get("event_type")
                            if name=="User":
                                logging.info(f"Received event from '{name}' channel: {event_type}")
                                logging.info(f"User-related event received: {event_type}")
                                pp.pprint(data)
                                #await self.message_queue.put(('polymarket_user', data))
                            else:
                                if event_type in ["book", "price_change", "tick_size_change", "last_trade_price"]:
                                    await self.message_queue.put(('polymarket', data))
                                    logging.debug(f"Put {event_type} event into queue from Polymarket {name}")
                                else:
                                    logging.info(f"Received non-standard event from Polymarket {name}: {data}")

                    except json.JSONDecodeError:
                        logging.warning(f"Failed to decode JSON from Polymarket {name}: {message}")
                    except Exception as e:
                        logging.error(f"Error processing message from Polymarket {name}: {e}")

            except websockets.exceptions.ConnectionClosed as e:
                logging.error(f"Polymarket {name} WebSocket connection closed: {e}")
                await asyncio.sleep(5)
                if name=="User":
                    self.user = await self._connect_to_endpoint(self.user_uri)
                    await self._subscribe_to_user_data()
            except Exception as e:
                logging.error(f"An unexpected error occurred in {name} listener: {e}")


    async def listen(self):
        """Listens for messages from both market and user websockets concurrently."""
        if not self.market or not self.user:
            logging.error("Websockets not connected. Call connect() before listening.")
            return

        logging.info("Starting to listen on both MARKET and USER channels...")
        # Run both listening loops concurrently. If one fails, the other continues.
        await asyncio.gather(
            self._listen_loop(self.market, "Market"),
            self._listen_loop(self.user, "User")
        )

    async def send_to_user(self, message):
        """Sends a message to the User WebSocket (e.g., for placing orders)."""
        if self.user and not self.user.closed:
            try:
                await self.user.send(json.dumps(message))
                logging.debug(f"Sent to Polymarket User channel: {message}")
            except Exception as e:
                logging.error(f"Error sending message to User WebSocket: {e}")
        else:
            logging.warning("User WebSocket not connected or closed. Cannot send message.")

    async def disconnect(self):
        """Closes both WebSocket connections gracefully."""
        logging.info("Closing Polymarket WebSocket connections...")
        tasks = []
        if self.market:
            tasks.append(self.market.close())
            self.market = None
        if self.user:
            tasks.append(self.user.close())
            self.user = None

        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info("Polymarket WebSocket connections closed.")

    async def unsubscribe(self, asset_id: str):
        """
        Removes an asset from the subscribed list and re-establishes
        the connections with the updated subscription set.
        """
        if asset_id in self.asset_ids:
            self.asset_ids.remove(asset_id)
            logging.info(f"Removed asset ID {asset_id}. Re-establishing WebSocket connections with updated subscriptions.")
            
            # Disconnect the current WebSockets
            await self.disconnect()
            
            # Reconnect, which will trigger new subscriptions with the updated asset_ids list
            await self.connect()
            logging.info("Polymarket WSS reconnected with updated subscriptions.")
        else:
            logging.debug(f"Asset ID {asset_id} not in active subscriptions, no action needed for unsubscribe.")