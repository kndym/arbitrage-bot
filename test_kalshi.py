import asyncio
from kalshi.wss import KalshiWSS,  env, KEYID, private_key
import pprint as pp

TICKERS= ['KXMLBGAME-25JUL03SFARI-ARI']

async def main():
    message_queue = asyncio.Queue()
    kalshi_wss = KalshiWSS(key_id=KEYID,
        private_key=private_key,
        environment=env,
        message_queue = message_queue,
        ticker_list=TICKERS)
    await kalshi_wss.connect()
    if kalshi_wss.ws:
        # Start listening in the background only if connection was successful
        asyncio.create_task(kalshi_wss.listen())
        a=0
        print("Polymarket listener started. Waiting for messages... (Press Ctrl+C to stop)")
        while True:
            # Get messages from the queue (this simulates the main processor)
            source, message = await message_queue.get()
            print(f"\n--- Main received from {source} ---")
            if a<5:
                pp.pprint(message)
                a+=1
    else:
        print("Could not start listener, connection to WebSocket failed.")


if __name__ == "__main__":
    try:
        print('loooooooool')
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Shutting down.")


if __name__ == "__main__":
    asyncio.run(main())