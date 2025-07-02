import asyncio
import websockets
import json
import logging
from clients import KalshiWebSocketClient, Environment
from cryptography.hazmat.primitives.asymmetric import padding, rsa


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KalshiWSS(KalshiWebSocketClient):
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment.DEMO,
        message_queue: asyncio.Queue
    ):
        super().__init__(key_id, private_key, environment)
        self.message_queue=message_queue

#Example usage (for testing the class individually)
async def main():
    message_queue = asyncio.Queue()
    kalshi_wss = KalshiWSS(message_queue) # Replace with actual URI
    await kalshi_wss.connect()
    await kalshi_wss.listen()

if __name__ == "__main__":
    asyncio.run(main())