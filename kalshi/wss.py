import asyncio
import json
import websockets
import ssl
import certifi # Useful for cross-platform certificate handling
import pprint 

from clients import KalshiWebSocketClient

from main import KEYID, private_key, env



MARKET_TICKER = "KXNBAGAME-25MAY16BOSNYK-BOS"


async def websocket_connect():
    # Initialize the WebSocket client
    ws_client = KalshiWebSocketClient(
        key_id=KEYID,
        private_key=private_key,
        environment=env
    )
    
    
    # Connect via WebSocket
    await ws_client.connect(MARKET_TICKER)



asyncio.run(websocket_connect())




