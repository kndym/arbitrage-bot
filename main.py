# main.py
import os
import time

# Import the functions from your new modules
from orders.tor_manager import start_tor, stop_tor
from orders.poly_order import buy_polymarket_contract
from orders.kalshi_order import buy_kalshi_contract

def main():
    """
    Main execution function.
    1. Starts Tor.
    2. Runs Polymarket trade through Tor.
    3. Runs Kalshi trade directly.
    4. Shuts down Tor.
    """
    tor_process, proxies = start_tor()

    # If Tor fails to start, exit gracefully.
    if not tor_process:
        print("[FATAL] Exiting due to Tor startup failure.")
        return

    # --- POLYMARKET (via Tor) ---
    # Set environment variables to force traffic through the Tor proxy
    os.environ['HTTP_PROXY'] = proxies['http']
    os.environ['HTTPS_PROXY'] = proxies['https']
    
    buy_polymarket_contract()

    # Clean up environment variables so subsequent requests are not proxied
    del os.environ['HTTP_PROXY']
    del os.environ['HTTPS_PROXY']
    
    print("\n" + "="*50 + "\n")
    time.sleep(2) # Brief pause for clarity in logs

    # --- KALSHI (Direct Connection) ---
    # With proxy variables unset, this function will use the default network
    #buy_kalshi_contract()

    # --- Shutdown ---
    stop_tor(tor_process)

if __name__ == "__main__":
    main()