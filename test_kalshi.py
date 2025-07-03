import asyncio
from kalshi.wss import KalshiWSS,  env, KEYID, private_key, TICKERS

#Example usage (for testing the class individually)
async def main():
    message_queue = asyncio.Queue()
    kalshi_wss = KalshiWSS(key_id=KEYID,
        private_key=private_key,
        environment=env,
        message_queue = message_queue,
        ticker_list=TICKERS)
    await kalshi_wss.connect()
    await kalshi_wss.listen()



if __name__ == "__main__":
    asyncio.run(main())