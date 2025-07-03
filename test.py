import asyncio
import logging
import pprint
from polymarket.wss import PolymarketWSS, POLYMARKET_MARKET_WSS_URI, MARKET_ASSET_IDS
from kalshi.wss import KalshiWSS,  env, KEYID, private_key, TICKERS


async def main():
    message_queue = asyncio.Queue()
    kalshi_wss = KalshiWSS(key_id=KEYID,
        private_key=private_key,
        environment=env,
        message_queue = message_queue,
        ticker_list=TICKERS)
    polymarket_wss = PolymarketWSS(POLYMARKET_MARKET_WSS_URI, 
                                   MARKET_ASSET_IDS, 
                                   message_queue)
    await kalshi_wss.connect()
    # Replace with actual asset IDs you want to subscribe to
    await polymarket_wss.connect()

    if polymarket_wss.websocket and kalshi_wss.ws:
        # Start listening in the background only if connection was successful
        asyncio.create_task(kalshi_wss.listen())
        asyncio.create_task(polymarket_wss.listen())
        #asyncio.create_task(kalshi_wss.listen())

        print("Polymarket listener started. Waiting for messages... (Press Ctrl+C to stop)")
        while True:
            # Get messages from the queue (this simulates the main processor)
            source, message = await message_queue.get()
            print(f"\n--- Main received from {source} ---")
            #pprint.pprint(message)
    else:
        print("Could not start listener, connection to WebSocket failed.")


if __name__ == "__main__":
    asyncio.run(main())

