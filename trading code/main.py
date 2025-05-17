import json

markets_path="markets.json"

with open(markets_path, 'r') as file:
    data=json.load(file)

