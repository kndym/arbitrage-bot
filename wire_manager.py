import subprocess
import time
import os
import re

# --- Configuration ---
# Directory where your WireGuard .conf files are stored
# IMPORTANT: Replace with the actual path to your WireGuard configs
# Example for Linux/macOS: "/home/user/vpn_configs"
# Example for Windows: "C:\\Users\\YourUser\\vpn_configs"
WIREGUARD_CONFIG_DIR = "C:\\Users\\Kevin\\Documents\\ProtonVPN_Wireguard_Configs" # Adjust this path

# Name of the WireGuard configuration file (without .conf extension)
# This should match the filename you downloaded (e.g., "ProtonVPN_DE_Free_#5")
# You will need to download these from your Proton VPN account.
WIREGUARD_CONFIG_NAME = "ProtonVPN_DE_Free_#5" # Example: A free server in Germany

# Name for the WireGuard interface (can be anything, e.g., 'pvpn-de-5')
# This is how wg-quick identifies the connection.
INTERFACE_NAME = "pvpn-de-5"

# --- Functions ---

def connect_wireguard(config_path, interface_name):
    """
    Connects to WireGuard using the specified configuration file.
    """
    print(f"[INFO] Attempting to connect to WireGuard interface '{interface_name}' using '{config_path}'...")
    try:
        # On Linux/macOS, wg-quick usually requires sudo.
        # On Windows, you might need to run the script as Administrator or
        # configure WireGuard service.
        # Check `wg-quick help` for platform-specific details.
        
        # We use a Popen process to allow for background execution and potential later termination
        # For simplicity, we are capturing output, but you might want to redirect to /dev/null
        # for a truly silent background process.
        
        command = ['wg-quick', 'up', config_path]
        
        # If running on Windows with elevated privileges, you might not need `sudo`
        # but the `wg-quick` command itself needs to be available in PATH.
        # For cross-platform, you might add 'sudo' if on Linux/macOS.
        if os.name == 'posix': # Linux or macOS
            command.insert(0, 'sudo')
            
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate(timeout=30) # Wait for 30 seconds for connection
        
        if process.returncode == 0:
            print(f"[SUCCESS] WireGuard interface '{interface_name}' brought up.")
            print("wg-quick output:\n", stdout)
            return True
        else:
            print(f"[ERROR] Failed to bring up WireGuard interface '{interface_name}'.")
            print("wg-quick stdout:\n", stdout)
            print("wg-quick stderr:\n", stderr)
            return False
    except subprocess.TimeoutExpired:
        print(f"[ERROR] WireGuard connection timed out for '{interface_name}'.")
        process.kill()
        return False
    except FileNotFoundError:
        print("[ERROR] 'wg-quick' command not found. Is WireGuard installed and in your PATH?")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during connection: {e}")
        return False

def disconnect_wireguard(interface_name):
    """
    Disconnects the specified WireGuard interface.
    """
    print(f"[INFO] Attempting to bring down WireGuard interface '{interface_name}'...")
    try:
        command = ['wg-quick', 'down', interface_name]
        if os.name == 'posix': # Linux or macOS
            command.insert(0, 'sudo')
            
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        print(f"[SUCCESS] WireGuard interface '{interface_name}' brought down.")
        print("wg-quick output:\n", result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to bring down WireGuard interface '{interface_name}': {e}")
        print("wg-quick stdout:\n", e.stdout)
        print("wg-quick stderr:\n", e.stderr)
        return False
    except FileNotFoundError:
        print("[ERROR] 'wg-quick' command not found. Is WireGuard installed and in your PATH?")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during disconnection: {e}")
        return False

def check_external_ip():
    """
    Checks the current external IP address to verify VPN connection.
    """
    print("[INFO] Checking external IP address...")
    try:
        response = requests.get("http://icanhazip.com/", timeout=5)
        response.raise_for_status()
        ip_address = response.text.strip()
        print(f"Your current external IP: {ip_address}")
        return ip_address
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Could not check external IP: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure requests library is installed for IP check
    try:
        import requests
    except ImportError:
        print("The 'requests' library is not installed. Please install it: pip install requests")
        exit()

    config_filepath = os.path.join(WIREGUARD_CONFIG_DIR, f"{WIREGUARD_CONFIG_NAME}.conf")

    if not os.path.exists(config_filepath):
        print(f"[CRITICAL ERROR] WireGuard config file not found: {config_filepath}")
        print("Please ensure you have downloaded your Proton VPN WireGuard config and set WIREGUARD_CONFIG_DIR and WIREGUARD_CONFIG_NAME correctly.")
        exit()

    print("\n--- Initial IP Check ---")
    initial_ip = check_external_ip()

    print(f"\n--- Attempting to connect WireGuard: {WIREGUARD_CONFIG_NAME} ---")
    if connect_wireguard(config_filepath, INTERFACE_NAME):
        print("\n--- IP Check after VPN Connection ---")
        vpn_ip = check_external_ip()

        if vpn_ip and vpn_ip != initial_ip:
            print("[VERIFICATION] IP address has changed, VPN appears to be working.")
        else:
            print("[WARNING] IP address did NOT change or could not be verified. VPN might not be working as expected.")
            print("This could be due to DNS leaks, routing issues, or the IP hasn't propagated yet.")

        print("\n[INFO] VPN is active. You can now perform your network operations.")
        print("Pausing for 20 seconds to simulate work...")
        time.sleep(20) # Simulate your program doing work while VPN is active

        print("\n--- Disconnecting WireGuard ---")
        disconnect_wireguard(INTERFACE_NAME)
        
        print("\n--- Final IP Check ---")
        check_external_ip()
    else:
        print("\n[FATAL] Could not establish WireGuard connection. Exiting.")