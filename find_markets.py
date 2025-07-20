import json
import requests
import datetime

# --- Configuration ---
# A mapping of full team names to their common tickers. This helps in matching
# data between Polymarket and Kalshi, which use different naming conventions.
TEAM_MAP = {
    'Arizona Diamondbacks': 'ARI',
    'Atlanta Braves': 'ATL',
    'Baltimore Orioles': 'BAL',
    'Boston Red Sox': 'BOS',
    'Chicago Cubs': 'CHC',
    'Chicago White Sox': 'CWS',
    'Cincinnati Reds': 'CIN',
    'Cleveland Guardians': 'CLE',
    'Colorado Rockies': 'COL',
    'Detroit Tigers': 'DET',
    'Houston Astros': 'HOU',
    'Kansas City Royals': 'KC',
    'Los Angeles Angels': 'LAA',
    'Los Angeles Dodgers': 'LAD',
    'Miami Marlins': 'MIA',
    'Milwaukee Brewers': 'MIL',
    'Minnesota Twins': 'MIN',
    'New York Mets': 'NYM',
    'New York Yankees': 'NYY',
    'Oakland Athletics': 'ATH',
    "A's": 'ATH',
    'Oakland A\'s': 'ATH',
    'Philadelphia Phillies': 'PHI',
    'Pittsburgh Pirates': 'PIT',
    'San Diego Padres': 'SD',
    'San Francisco Giants': 'SF',
    'Seattle Mariners': 'SEA',
    'St. Louis Cardinals': 'STL',
    'Tampa Bay Rays': 'TB',
    'Texas Rangers': 'TEX',
    'Toronto Blue Jays': 'TOR',
    'Washington Nationals': 'WSH'
}

# --- Polymarket Data Fetching ---
def fetch_polymarket_data(target_date):
    """
    Fetches and processes MLB game data from the Polymarket API for a specific date.

    Args:
        target_date (datetime.date): The date for which to fetch the markets.

    Returns:
        dict: A dictionary of games, with a frozenset of team tickers as the key.
    """
    print("Fetching Polymarket data...")
    polymarket_games = {}
    # Polymarket's API is paginated, so we loop to get all events.
    for i in range(5): # Check first 5 pages, should be sufficient for daily games
        try:
            url = f"https://gamma-api.polymarket.com/events?tag_id=100639&related_tags=true&closed=false&limit=1000&offset={500*i}"
            response = requests.get(url)
            response.raise_for_status()
            events = response.json()

            for event in events:
                # We are interested in MLB games only.
                if "mlb" in event.get("slug", "") and event.get("markets"):
                    market = event["markets"][0]
                    # The event date is in the slug.
                    try:
                        game_date_str = event["slug"][-10:]
                        game_date = datetime.datetime.strptime(game_date_str, '%Y-%m-%d').date()

                        if game_date == target_date:
                            # Extract team tickers from the slug.
                            slug_parts = event["slug"].split('-')
                            team1_ticker = slug_parts[1].upper()
                            team2_ticker = slug_parts[2].upper()
                            token_list=json.loads(event['markets'][0]['clobTokenIds'])
                            game_markets={
                                team1_ticker: token_list[0],
                                team2_ticker: token_list[1],
                                "condition_id": market["conditionId"],
                                "title": event["title"]
                            }
                            
                            # Create a unique, order-independent key for the game.
                            game_key = frozenset([team1_ticker, team2_ticker])

                            polymarket_games[game_key] = game_markets
                    except (ValueError, IndexError):
                        continue # Skip if the slug format is not as expected.
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Polymarket data: {e}")
            break
            
    print(f"Found {len(polymarket_games)} MLB games on Polymarket for {target_date}.")
    return polymarket_games

# --- Kalshi Data Fetching ---
def fetch_kalshi_data(target_date):
    """
    Fetches and processes MLB game data from the Kalshi API for a specific date.

    Args:
        target_date (datetime.date): The date for which to fetch the markets.

    Returns:
        dict: A dictionary of games, with a frozenset of team tickers as the key.
    """
    print("Fetching Kalshi data...")
    kalshi_games = {}
    cursor = ""
    kalshi_date_str = target_date.strftime("%y%b%d").upper() # Format: 25JUL06

    # Kalshi's API is paginated; loop until the cursor indicates no more results.
    for _ in range(10): # Limit loops to prevent infinite loops on error
        try:
            url = f"https://api.elections.kalshi.com/trade-api/v2/events?series_ticker=KXMLBGAME&status=open&with_nested_markets=true&cursor={cursor}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for event in data.get("events", []):
                # The event ticker contains the date and team tickers.
                if kalshi_date_str in event["event_ticker"]:
                    try:
                        # Create the inner dictionary mapping team ticker to full market ticker
                        # and simultaneously collect all team tickers for the game_key.
                        game_markets = {
                            market['ticker'].split('-')[-1]: market['ticker']
                            for market in event.get('markets', [])
                        }
                        
                        # Create the game_key from the keys of the newly created dictionary
                        game_key = frozenset(game_markets.keys())
                        
                        # Store the game_markets in the main kalshi_games dictionary
                        kalshi_games[game_key] = game_markets
                        
                    except IndexError:
                        continue # Skip malformed tickers

            cursor = data.get("cursor", "")
            if not cursor:
                break # Exit loop if there are no more pages.
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Kalshi data: {e}")
            break

    print(f"Found {len(kalshi_games)} MLB games on Kalshi for {target_date}.")
    return kalshi_games

# --- Main Logic ---
def create_market_files():
    """
    Main function to orchestrate data fetching, processing, and file creation.
    """
    # Set the target date for which to generate the files.
    # Using a fixed date for this example based on the prompt's data.
    target_date = datetime.date(2025, 7, 19)
    
    # Fetch data from both platforms.
    polymarket_data = fetch_polymarket_data(target_date)
    kalshi_data = fetch_kalshi_data(target_date)

    if not polymarket_data or not kalshi_data:
        print("Could not fetch data from one or both sources. Exiting.")
        return

    markets_json = {}
    compliment_json = {}

    # Invert the TEAM_MAP to easily look up full names from tickers.
    inverted_team_map = {v: k for k, v in TEAM_MAP.items()}


    # Correlate data from Polymarket and Kalshi.
    for game_key, poly_market in polymarket_data.items():
        if game_key in kalshi_data:
            kalshi_market = kalshi_data[game_key]
            
            # Identify the two teams involved in the matchup.
            teams = list(game_key)
            team1_ticker = teams[0]
            team2_ticker = teams[1]

            # Get full team names from Polymarket's title for better readability.
            try:
                team1_full_name, team2_full_name = poly_market['title'].split(' vs. ')
            except ValueError:
                print(f"Could not parse title: {poly_market['title']}")
                continue

            # Create the unique keys for the JSON output files.
            market1_key = f"{team1_full_name} vs {team2_full_name} ({team1_ticker})"
            market2_key = f"{team1_full_name} vs {team2_full_name} ({team2_ticker})"

            # Populate the markets dictionary.
            markets_json[market1_key] = {
                "polymarket": poly_market.get(team1_ticker, "N/A"),
                "kalshi": kalshi_market.get(team1_ticker, "N/A")
            }
            markets_json[market2_key] = {
                "polymarket": poly_market.get(team2_ticker, "N/A"),
                "kalshi": kalshi_market.get(team2_ticker, "N/A")
            }

            # Populate the compliment dictionary, linking the two opposing markets.
            compliment_json[market1_key] = market2_key

    # --- File Generation ---
    date_str = target_date.strftime('%m_%d')
    markets_filename = f"jsons/markets_{date_str}.json"
    compliment_filename = f"jsons/compliment_{date_str}.json"

    # Write the markets JSON file.
    with open(markets_filename, 'w') as f:
        json.dump(markets_json, f, indent=4)
    print(f"Successfully created {markets_filename}")

    # Write the compliment JSON file.
    with open(compliment_filename, 'w') as f:
        json.dump(compliment_json, f, indent=4)
    print(f"Successfully created {compliment_filename}")

if __name__ == '__main__':
    create_market_files()