import subprocess
import time
import os
import re
import requests
import sys
# --- Configuration ---
# Directory where your WireGuard .conf files are stored
# IMPORTANT: Replace with the actual path to your WireGuard configs
# Example for Linux/macOS: "/home/user/vpn_configs"
# Example for Windows: "C:\\Users\\YourUser\\vpn_configs"
WIREGUARD_CONFIG_DIR = "C:\\Users\\Kevin\\Github\\arbitragebot" # Adjust this path

# Name of the WireGuard configuration file (without .conf extension)
# This should match the filename you downloaded (e.g., "ProtonVPN_DE_Free_#5")
# You will need to download these from your Proton VPN account.
WIREGUARD_CONFIG_NAME = "wg-CA-85" # Example: A free server in Germany




INTERFACE_NAME_WINDOWS = "wg-CA-85" # Must match exactly!

# Path to the wg.exe executable. Usually it's added to PATH, but if not:
# WIREGUARD_EXE_PATH = "C:\\Program Files\\WireGuard\\wg.exe"
# If wg.exe is in your PATH, you can just use "wg"
WIREGUARD_EXE_PATH = "C:\Program Files\WireGuard\wg.exe" # Assuming 'wg.exe' is in your system's PATH

# --- Functions ---

def connect_wireguard_windows(config_path, interface_name):
    """
    Connects to WireGuard on Windows by applying the config using wg.exe setconf.
    Requires an existing WireGuard interface with the given name.
    """
    print(f"[INFO] Attempting to connect to WireGuard interface '{interface_name}' using '{config_path}'...")
    try:
        # On Windows, this operation often requires Administrator privileges.
        # Ensure your Python script is run as Administrator.
        
        # Command to apply the configuration file
        command = [WIREGUARD_EXE_PATH, 'setconf', interface_name, config_path]
        
        # Using subprocess.run for simplicity, capturing output
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        print(f"[SUCCESS] WireGuard interface '{interface_name}' configured and active.")
        print("wg.exe output:\n", result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to setconf WireGuard interface '{interface_name}':")
        print(f"  Return Code: {e.returncode}")
        print(f"  Stdout: {e.stdout}")
        print(f"  Stderr: {e.stderr}")
        print("This often means the interface name is wrong, the config file has issues, or you need Administrator privileges.")
        return False
    except FileNotFoundError:
        print(f"[ERROR] '{WIREGUARD_EXE_PATH}' command not found. Is WireGuard installed and in your PATH?")
        print("If not in PATH, set WIREGUARD_EXE_PATH to the full path, e.g., 'C:\\Program Files\\WireGuard\\wg.exe'")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during connection: {e}")
        return False

def disconnect_wireguard_windows(interface_name):
    """
    Disconnects the specified WireGuard interface on Windows by setting an empty config.
    """
    print(f"[INFO] Attempting to disconnect WireGuard interface '{interface_name}'...")
    try:
        # To disconnect, we apply an empty configuration to the interface.
        # This effectively clears its keys and routes, stopping the tunnel.
        command = [WIREGUARD_EXE_PATH, 'setconf', interface_name, '/dev/null'] # Linux/macOS equivalent for empty file
        if sys.platform == "win32":
            # On Windows, use "NUL" as the empty file equivalent
            command = [WIREGUARD_EXE_PATH, 'setconf', interface_name, 'NUL']
            
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        print(f"[SUCCESS] WireGuard interface '{interface_name}' disconnected.")
        print("wg.exe output:\n", result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to disconnect WireGuard interface '{interface_name}': {e}")
        print("wg.exe stdout:\n", e.stdout)
        print("wg.exe stderr:\n", e.stderr)
        return False
    except FileNotFoundError:
        print(f"[ERROR] '{WIREGUARD_EXE_PATH}' command not found. Is WireGuard installed and in your PATH?")
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

def set_proxy_environment_variables(proxy_type, ip, port):
    """
    Sets HTTP_PROXY, HTTPS_PROXY, and SOCKS_PROXY environment variables.
    This is for *additional* proxying, not for the WireGuard tunnel itself.
    """
    proxy_url = f"{proxy_type}://{ip}:{port}"
    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    os.environ['ALL_PROXY'] = proxy_url # Some tools use ALL_PROXY for all protocols
    os.environ['SOCKS_PROXY'] = proxy_url # For SOCKS specific
    print(f"[INFO] Proxy environment variables set to: {proxy_url}")
    print(f"    HTTP_PROXY: {os.environ.get('HTTP_PROXY')}")
    print(f"    HTTPS_PROXY: {os.environ.get('HTTPS_PROXY')}")
    print(f"    SOCKS_PROXY: {os.environ.get('SOCKS_PROXY')}")

def clear_proxy_environment_variables():
    """
    Clears HTTP_PROXY, HTTPS_PROXY, and SOCKS_PROXY environment variables.
    """
    for var in ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'SOCKS_PROXY']:
        if var in os.environ:
            del os.environ[var]
    print("[INFO] Proxy environment variables cleared.")

# --- Main Execution ---
if __name__ == "__main__":
    try:
        import requests
    except ImportError:
        print("The 'requests' library is not installed. Please install it: pip install requests")
        sys.exit(1)

    config_filepath = os.path.join(WIREGUARD_CONFIG_DIR, f"{WIREGUARD_CONFIG_NAME}.conf")

    if not os.path.exists(config_filepath):
        print(f"[CRITICAL ERROR] WireGuard config file not found: {config_filepath}")
        print("Please ensure you have downloaded your Proton VPN WireGuard config and set WIREGUARD_CONFIG_DIR and WIREGUARD_CONFIG_NAME correctly.")
        sys.exit(1)

    print("\n--- Initial IP Check (without VPN) ---")
    initial_ip = check_external_ip()

    print(f"\n--- Attempting to connect WireGuard: {WIREGUARD_CONFIG_NAME} ---")
    # Call the Windows-specific connection function
    if connect_wireguard_windows(config_filepath, INTERFACE_NAME_WINDOWS):
        print("\n--- IP Check after VPN Connection ---")
        vpn_ip = check_external_ip()

        if vpn_ip and vpn_ip != initial_ip:
            print("[VERIFICATION] IP address has changed, VPN appears to be working.")
            print(f"New IP: {vpn_ip}")
        else:
            print("[WARNING] IP address did NOT change or could not be verified. VPN might not be working as expected.")
            print("This could be due to DNS leaks, routing issues, or the IP hasn't propagated yet.")

        # Illustrative proxy environment variable usage remains the same
        # As explained previously, this is for a *secondary* proxy layer.
        # Example: set_proxy_environment_variables("socks5", "127.0.0.1", "9050")
        
        print("\n[INFO] VPN is active. You can now perform your network operations.")
        print("Pausing for 20 seconds to simulate work...")
        time.sleep(20) # Simulate your program doing work while VPN is active

        # clear_proxy_environment_variables() # Uncomment if you set them
        
        print("\n--- Disconnecting WireGuard ---")
        # Call the Windows-specific disconnection function
        disconnect_wireguard_windows(INTERFACE_NAME_WINDOWS)
        
        print("\n--- Final IP Check (after VPN disconnected) ---")
        check_external_ip()
    else:
        print("\n[FATAL] Could not establish WireGuard connection. Exiting.")
        sys.exit(1)