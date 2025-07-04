import requests


#r=requests.get("https://gamma-api.polymarket.com/events?end_date_max=2025-05-18T00:00:00Z&end_date_min=2025-05-07T00:00:00Z&closed=false&offset=3")


for x in range(3):
    r=requests.get(f"https://gamma-api.polymarket.com/events?tag_id=100639&related_tags=true&closed=false&limit=1000&offset={500*x}")
    response=r.json()

    for event in response:
        if any( x in event["slug"] for x in ["mlb"]):
            print(event["slug"])
            print(event["markets"][0]["clobTokenIds"])
 
