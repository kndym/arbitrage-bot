import asyncio
import json
import websockets
import ssl
import certifi # Useful for cross-platform certificate handling
import pprint 

# Polymarket CLOB WebSocket URI
CLOB_WSS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws"

WSS_USER=CLOB_WSS_URI+"/user"

WSS_MARKET=CLOB_WSS_URI+"/market"


# Example Asset ID (Token ID) - replace with the ID of the market you want
# This is the example YES token ID from the documentation
EXAMPLE_ASSET_ID = "28944649550139856038380412491094359204452944318459138302272032692722910909490"

async def subscribe_to_market_data():
    """Connects to Polymarket WSS and subscribes to market data."""

    # Create an SSL context that trusts certificates from certifi package
    # This helps prevent SSL errors on some systems
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Message to subscribe to the 'Market' channel for specific asset(s)
    subscribe_message = {
        "type": "Market",
        "assets_ids": [EXAMPLE_ASSET_ID]
        # You can add more asset IDs here: ["ID1", "ID2", "ID3"]
        # "auth": { ... } # Auth is NOT required for the Market channel
    }

    print(f"Connecting to {WSS_MARKET}...")
    try:
        async with websockets.connect(WSS_MARKET, ssl=ssl_context) as websocket:
            print("Connection established.")

            # Send the subscription message
            await websocket.send(json.dumps(subscribe_message))
            print(f"Subscribed to Market channel for asset ID: {EXAMPLE_ASSET_ID}")

            # Listen for incoming messages
            a, b = True, True
            while True:
                try:
                    message = await websocket.recv()
                    all_events = json.loads(message)


                    for data in all_events:
                        event_type = data.get("event_type")

                        if event_type == "book":

                            if a:
                                pprint.pp(data)
                                a=not a

                            # Print aggregated order book levels (bids and asks)
                            asset_id = data.get("asset_id", "N/A")
                            timestamp = data.get("timestamp", "N/A")
                            bids = data.get("buys", []) # 'buys' corresponds to bids in the book
                            asks = data.get("sells", []) # 'sells' corresponds to asks in the book

                            print(f"\n--- Book Update for Asset {asset_id} (TS: {timestamp}) ---")
                            print("Bids:")
                            for bid in bids:
                                print(f"  Price: {bid['price']}, Size: {bid['size']}")
                            print("Asks:")
                            for ask in asks:
                                print(f"  Price: {ask['price']}, Size: {ask['size']}")
                            print("-------------------------------------------------")

                        elif event_type == "price_change":
                            
                            if b:
                                pprint.pp(data)
                                b=not b


                            # Print individual price level changes
                            asset_id = data.get("asset_id", "N/A")
                            timestamp = data.get("timestamp", "N/A")
                            changes = data.get("changes", [])

                            print(f"\n--- Price Change for Asset {asset_id} (TS: {timestamp}) ---")
                            for change in changes:
                                print(f"  Side: {change['side']}, Price: {change['price']}, New Size: {change['size']}")
                            print("-------------------------------------------------")

                        elif event_type == "tick_size_change":
                            asset_id = data.get("asset_id", "N/A")
                            timestamp = data.get("timestamp", "N/A")
                            old_tick = data.get("old_tick_size", "N/A")
                            new_tick = data.get("new_tick_size", "N/A")
                            print(f"\n--- Tick Size Change for Asset {asset_id} (TS: {timestamp}) ---")
                            print(f"  Old Tick Size: {old_tick}, New Tick Size: {new_tick}")
                            print("-------------------------------------------------")

                        else:
                            # Print other messages (e.g., initial confirmation, errors)
                            print(f"Received message: {data}")

                except websockets.exceptions.ConnectionClosedOK:
                    print("Connection closed gracefully.")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    print(f"Connection closed with error: {e}")
                    break
                except json.JSONDecodeError:
                    print(f"Received non-JSON message: {message}")
                except Exception as e:
                    print(f"An error occurred while processing message: {e}")
                    # Optionally, break the loop or reconnect logic here

    except ConnectionRefusedError:
        print("Connection refused. Make sure the WSS URI is correct and accessible.")
    except Exception as e:
        print(f"Could not connect to WebSocket: {e}")

# Run the asynchronous client
if __name__ == "__main__":
    # To subscribe to multiple assets, replace EXAMPLE_ASSET_ID in the
    # subscribe_message list with the IDs you need.
    # Example: assets_ids": ["ID1", "ID2", "ID3"]
    asyncio.run(subscribe_to_market_data())