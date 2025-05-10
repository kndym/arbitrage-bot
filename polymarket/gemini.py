import asyncio
import websockets
import json
import os  # Using os for potentially storing API keys (optional for 'market' channel)

# Configuration
WSS_URL = "wss://clob.polymarket.com/ws"  # Replace with the actual WSS URL if different
# For the 'market' channel, authentication is not needed.
# If you were using the 'user' channel, you would define these:
# POLYMARKET_API_KEY = os.environ.get("POLYMARKET_API_KEY")
# POLYMARKET_SECRET = os.environ.get("POLYMARKET_SECRET")
# POLYMARKET_PASSPHRASE = os.environ.get("POLYMARKET_PASSPHRASE")

async def receive_market_data():
    """
    Connects to the Polymarket CLOP WSS API and receives market data.
    """
    uri = WSS_URL
    print(f"Connecting to {uri}")

    try:
        async with websockets.connect(uri) as websocket:
            print("Connection established.")

            # Subscribe to the 'market' channel
            subscription_message = {
                "type": "Market",
                "assets_ids": ["65818619657568813474341868652308942079804919287380422192892211131408793125422"], # Example asset ID
                # "markets": ["0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"] # Example market ID (condition ID)
                # You can include either asset_ids or markets, or both, depending on what you want to track.
                # The example uses asset_ids.
            }
            await websocket.send(json.dumps(subscription_message))
            print(f"Sent subscription message: {json.dumps(subscription_message)}")

            # Continuously receive and process messages
            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)

                    # Process the received data
                    event_type = data.get("event_type")

                    if event_type == "book":
                        print("\n--- Book Update ---")
                        print(f"Asset ID: {data.get('asset_id')}")
                        print(f"Market: {data.get('market')}")
                        print(f"Timestamp: {data.get('timestamp')}")
                        print("Buys:", data.get('buys'))
                        print("Sells:", data.get('sells'))
                        # You would typically store and manage the order book data here
                    elif event_type == "price_change":
                        print("\n--- Price Change ---")
                        print(f"Asset ID: {data.get('asset_id')}")
                        print(f"Market: {data.get('market')}")
                        print(f"Timestamp: {data.get('timestamp')}")
                        print("Changes:", data.get('changes'))
                        # Update your internal representation of price levels
                    elif event_type == "tick_size_change":
                        print("\n--- Tick Size Change ---")
                        print(f"Asset ID: {data.get('asset_id')}")
                        print(f"Market: {data.get('market')}")
                        print(f"Timestamp: {data.get('timestamp')}")
                        print(f"Old Tick Size: {data.get('old_tick_size')}")
                        print(f"New Tick Size: {data.get('new_tick_size')}")
                        # Adjust your handling of prices if needed
                    else:
                        print("\n--- Unhandled Message ---")
                        print(json.dumps(data, indent=2))

                except websockets.exceptions.ConnectionClosedOK:
                    print("Connection closed gracefully.")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    print(f"Connection closed with error: {e}")
                    break
                except json.JSONDecodeError:
                    print(f"Failed to decode JSON message: {message}")
                except Exception as e:
                    print(f"An error occurred while processing message: {e}")

    except websockets.exceptions.InvalidURI:
        print(f"Invalid WebSocket URI: {uri}")
    except ConnectionRefusedError:
        print(f"Connection refused. Is the server running at {uri}?")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

async def main():
    """
    Main function to run the WebSocket client.
    """
    await receive_market_data()

if __name__ == "__main__":
    # To run this script, use: python your_script_name.py
    asyncio.run(main())