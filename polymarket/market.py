import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON
import pprint

load_dotenv()


def main():
    host = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    key = os.getenv("PK")
    creds = ApiCreds(
        api_key=os.getenv("CLOB_API_KEY"),
        api_secret=os.getenv("CLOB_SECRET"),
        api_passphrase=os.getenv("CLOB_PASS_PHRASE"),
    )
    chain_id = POLYGON
    client = ClobClient(host, key=key, chain_id=chain_id, creds=creds)

    
    resp = client.get_sampling_markets()
    print('hi')
    next_cursor=""
    while next_cursor!="LTE=":
        resp = client.get_sampling_markets(next_cursor=next_cursor)
        for data in resp["data"]:
            if False:
                if data["active"] and not data["closed"] and data["accepting_orders"]:
                    if any(x in data["tags"] for x in ['NBA Playoffs']):
                        print(data['question'], data["tags"])
                        pprint.pp(data)
                        break
            else:
                if "braves" in data["market_slug"]:
                    print(data['question'], data["tags"])
                    pprint.pp(data)

        print('     NEXT PAGE')
        next_cursor=resp['next_cursor']



    print('hi1')

    print("Done!")


main()