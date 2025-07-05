import asyncio
import pprint
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI


# Example Asset ID (Token ID) from the documentation.
# You can add more asset IDs here in a list: ["ID1", "ID2", "ID3"]
MARKET_ASSET_IDS = ["5986371862208839175485490179523653880632219954307111830409221264009788091256"]

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
        print('loooooooool')
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Shutting down.")