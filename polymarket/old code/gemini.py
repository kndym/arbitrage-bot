import asyncio
import websockets
import json
import os  # Using os for potentially storing API keys (optional for 'market' channel)
import pprint as pp

# Configuration
WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"  # Replace with the actual WSS URL if different
# For the 'market' channel, authentication is not needed.
# If you were using the 'user' channel, you would define these:


async def receive_market_data():
    """
    Connects to the Polymarket CLOP WSS API and receives market data.
    """
    uri = WSS_URL
    api_key=os.getenv("CLOB_API_KEY")
    api_secret=os.getenv("CLOB_SECRET")
    api_passphrase=os.getenv("CLOB_PASS_PHRASE")
    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}



    print(f"Connecting to {uri}")

    try:
        async with websockets.connect(uri) as websocket:
            print("Connection established.")

            # Subscribe to the 'market' channel
            subscription_message = {
                "type": "MARKET",
                "assets_ids": ["45374581549195993272455335447780192076746148907066452139786558534049308360520"]
                # Example asset ID
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
                    all_data = json.loads(message)

                    # Process the received data
                    for data in all_data:
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
                        elif event_type == "last_trade_price":
                            print("\n--- Last Trade Price ---")
                            print(f"Asset ID: {data.get('asset_id')}")
                            print(f"Market: {data.get('market')}")
                            print(f"Timestamp: {data.get('timestamp')}")
                            print(f"Side: {data.get('side')}")
                            print(f"Price: {data.get('price')}")
                            print(f"Size: {data.get('size')}")
                            print(f"Fee Rate (bps): {data.get('fee_rate_bps')}")
                            # You can also log or update the last trade details in your internal state here

                        else:
                            print("\n--- Unhandled Message ---")
                            pp.pprint(data)

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