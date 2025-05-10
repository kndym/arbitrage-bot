import os
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON


host = "https://clob.polymarket.com"
key = os.getenv("PK")
creds = ApiCreds(
    api_key=os.getenv("CLOB_API_KEY"),
    api_secret=os.getenv("CLOB_SECRET"),
    api_passphrase=os.getenv("CLOB_PASS_PHRASE"),
)
chain_id = POLYGON
client = ClobClient(host, key=key, chain_id=chain_id, creds=creds)

print(
    client.get_order_book(
        "0xf5bf6a569350f676cc8a7c145d0ff99c7b7b140375d6989c2448377bec019dff"
    )
)


## Create and sign a market BUY order for $100
order_args = MarketOrderArgs(
    token_id="60777290337556307846082122611643867373415691927705756558171303096586770149710",
    amount=1.0, # $$$
    side=BUY,
)



signed_order = client.create_market_order(order_args)

## FOK Order
resp = client.post_order(signed_order, OrderType.FOK)
print(resp)
