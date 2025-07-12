# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 22:15:33 2021

@author: Yicong
"""
import io
import os
import stem.process
import re
import urllib.request
import requests
import json
from datetime import datetime

root="C:\\Users\\Kevin\\Github\\Tor"


# --- Tor Configuration ---
SOCKS_PORT = 9050
CONTROL_PORT = 9051
TOR_PATH = os.path.normpath(root+"\\tor\\tor.exe")
GEOIPFILE_PATH = os.path.normpath(root+"\\data\\tor\\geoip")
print(GEOIPFILE_PATH)
try:
    urllib.request.urlretrieve('https://raw.githubusercontent.com/torproject/tor/main/src/config/geoip', GEOIPFILE_PATH)
except:
    print ('[INFO] Unable to update geoip file. Using local copy.')
    
tor_process = stem.process.launch_tor_with_config(
  config = {
    'SocksPort' : str(SOCKS_PORT),
    'ControlPort' : str(CONTROL_PORT),
    'EntryNodes' : '{US}',
    'ExitNodes' : '{BR}',
    'StrictNodes' : '1',
    'CookieAuthentication' : '1',
    'MaxCircuitDirtiness' : '60000',
    'GeoIPFile' : GEOIPFILE_PATH,
    
  },
  init_msg_handler = lambda line: print(line) if re.search('Bootstrapped', line) else False,
  tor_cmd = TOR_PATH
)

PROXIES = {
    'http': 'socks5://127.0.0.1:9050',
    'https': 'socks5://127.0.0.1:9050'
}

def verify():
    # Verify Tor connection (this still works as before)
    response = requests.get("http://ip-api.com/json/", proxies=PROXIES)
    result = json.loads(response.content)
    print('TOR IP [%s]: %s %s'%(datetime.now().strftime("%d-%m-%Y %H:%M:%S"), result["query"], result["country"]))

# --- Set environment variables for proxies ---
# This is the key to making py-clob-client use the Tor proxy
os.environ['HTTP_PROXY'] = PROXIES['http']
os.environ['HTTPS_PROXY'] = PROXIES['https']


import os
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
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
    size=5.0, # $$$
    price=0.55
)

print('hi')
signed_order = client.create_order(order_args)
print('hi')
## FOK Order
resp = client.post_order(signed_order, OrderType.FOK)
print('hi')
print(resp)

# Optional: Clean up environment variables if you want subsequent requests to not use the proxy
del os.environ['HTTP_PROXY']
del os.environ['HTTPS_PROXY']