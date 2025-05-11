import asyncio
import json
import websockets
import ssl
import certifi # Useful for cross-platform certificate handling
import pprint 

from main import sign_pss_text

WSS_URI = "wss://api.elections.kalshi.com/trade-api/ws/v2"




MARKET_TICKER = "KXNBAGAME-25MAY10MINGSW-GSW"

async def subscribe_to_market_data():
    """Connects to Polymarket WSS and subscribes to market data."""

    # Create an SSL context that trusts certificates from certifi package
    # This helps prevent SSL errors on some systems
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    # Message to subscribe to the 'Market' channel for specific asset(s)
    subscribe_message = {
	"id": 1, 
	"cmd": "subscribe", 
	"params": {
		"channels": ["orderbook_delta"], 
		"market_tickers": [MARKET_TICKER] 
    }
}

    print(f"Connecting to {WSS_URI}...")
    try:
        async with websockets.connect(WSS_URI, ssl=ssl_context) as websocket:
            print("Connection established.")

            # Send the subscription message
            await websocket.send(json.dumps(subscribe_message))
            print(f"Subscribed to Market channel for asset ID: {MARKET_TICKER}")

            first_message=True
            while True:
                try:
                    message = await websocket.recv()
                    book_update = json.loads(message)
                    if first_message:
                        pprint.pp(book_update)
                    else:
                        pprint.pp(book_update)

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