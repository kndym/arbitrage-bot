# tor_manager.py
import os
import stem.process
import re
import urllib.request
import requests 
import datetime

def start_tor(depth=0):
    """
    Starts the Tor process with a specific configuration and returns the process
    and proxy details.
    """
    SOCKS_PORT = 9050
    CONTROL_PORT = 9051
    
    # Assuming the script is run from the root directory of your project
    root = "C:\\Users\\Kevin\\Github\\Tor"
    TOR_PATH = os.path.normpath(os.path.join(root, "tor", "tor.exe"))
    GEOIPFILE_PATH = os.path.normpath(os.path.join(root, "data", "tor", "geoip"))

    print("[INFO] Checking for GeoIP file updates...")
    try:
        urllib.request.urlretrieve('https://raw.githubusercontent.com/torproject/tor/main/src/config/geoip', GEOIPFILE_PATH)
        print("[INFO] GeoIP file updated successfully.")
    except Exception as e:
        print(f'[WARNING] Unable to update geoip file: {e}. Using local copy.')

    print("[INFO] Starting Tor process...")
    try:
        tor_process = stem.process.launch_tor_with_config(
             config={
                'SocksPort': str(SOCKS_PORT),
                'ControlPort': str(CONTROL_PORT),
                'ExcludeExitNodes ': '{US},{GB},{FR},{CA},{SG},{PL},{TH},{BE},{TW}',
                'GeoIPFile': GEOIPFILE_PATH,
                'NewCircuitPeriod': '300',
                'MaxCircuitDirtiness': '300',
                'StrictNodes': '1'
            },
            init_msg_handler=lambda line: print(f"[TOR] {line}") if re.search('Bootstrapped', line) else False,
            tor_cmd=TOR_PATH
        )
        print("[SUCCESS] Tor process started and bootstrapped.")
    except OSError:
        if depth>10:
            print("FAILED TO CONNECT TO TOR")
            return None, None
        else:
            return start_tor()
    except Exception as e:
        print(f"[ERROR] Failed to start Tor process: {e}")
        return None, None

    PROXIES = {
        'http': f'socks5://127.0.0.1:{SOCKS_PORT}',
        'https': f'socks5://127.0.0.1:{SOCKS_PORT}'
    }
    
    return tor_process, PROXIES

def stop_tor(tor_process):
    """
    Stops the given Tor process.
    """
    if tor_process:
        print("[INFO] Stopping Tor process...")
        tor_process.kill()
        print("[SUCCESS] Tor process stopped.")

def ping_tor(proxies):
    """
    Tests the Tor connection by making a request through the Tor SOCKS proxy
    and checking the returned IP address and ping time.
    """
    print("[INFO] Testing Tor connection and getting ping time...")
    try:
        # Using a service that returns the public IP address
        # ip-api.com is a good option for this purpose.
        start_time = datetime.datetime.now() # Record start time for manual timing if needed
        response = requests.get("http://ip-api.com/json/", proxies=proxies, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        end_time = datetime.datetime.now() # Record end time

        data = response.json()
        tor_ip = data.get("query")
        tor_country = data.get("country")

        # Get the elapsed time using response.elapsed
        ping_time_elapsed = response.elapsed.total_seconds() * 1000  # Convert to milliseconds [3]

        print(f"[INFO] Request made through Tor. IP: {tor_ip}, Country: {tor_country}, Ping: {ping_time_elapsed:.2f} ms")

        return True, tor_ip, tor_country, ping_time_elapsed
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Tor connection test failed: {e}")
        return False, None, None, None