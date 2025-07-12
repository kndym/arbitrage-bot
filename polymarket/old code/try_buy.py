import os
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from py_clob_client.clob_types import ApiCreds
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON

load_dotenv()

host = "https://clob.polymarket.com"
key = os.getenv("WALLET_PRIVATE_KEY")
#key = os.getenv("CLOB_API_KEY")
POLYMARKET_PROXY_ADDRESS=os.getenv("POLYMARKET_PROXY_ADDRESS")
creds = ApiCreds(
    api_key=os.getenv("CLOB_API_KEY"),
    api_secret=os.getenv("CLOB_SECRET"),
    api_passphrase=os.getenv("CLOB_PASS_PHRASE"),
)
chain_id = 137
#client = ClobClient(host, key=key, chain_id=chain_id, creds=creds)

client = ClobClient(host, key=key, chain_id=chain_id, signature_type=1, funder=POLYMARKET_PROXY_ADDRESS)

client.set_api_creds(client.create_or_derive_api_creds()) 


order_args = OrderArgs(
    side=BUY,
    token_id="68981407591275819355155557897174725090207140821614494691529013059207415432892",
    size=1.0, # $$$
    price=0.54
)




signed_order = client.create_order(order_args)

## FOK Order
resp = client.post_order(signed_order, OrderType.FOK)
print(resp)
