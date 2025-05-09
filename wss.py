from dotenv import load_dotenv
import os
import asyncio
import websockets

from py_clob_client.client import ClobClient
from py_clob_client.constants import AMOY

load_dotenv()


def main():
    host = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    key = os.getenv("PK")
    chain_id = 137
    client = ClobClient(host, key=key, chain_id=chain_id)

    print(client.create_api_key())


main()