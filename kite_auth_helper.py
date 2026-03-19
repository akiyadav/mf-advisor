"""
kite_auth_helper.py
═══════════════════
Run this ONCE per month to refresh your Zerodha Kite access token.
Takes 30 seconds. Updates KITE_ACCESS_TOKEN in your .env file automatically.

Usage:
  python kite_auth_helper.py

Requirements:
  pip install kiteconnect python-dotenv
"""

import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

try:
    from kiteconnect import KiteConnect
    from dotenv import load_dotenv, set_key
    load_dotenv()
except ImportError:
    print("Run: pip install kiteconnect python-dotenv")
    exit(1)

API_KEY = os.environ.get("KITE_API_KEY")
API_SECRET = os.environ.get("KITE_API_SECRET")
ENV_FILE = ".env"

if not API_KEY or not API_SECRET:
    print("Set KITE_API_KEY and KITE_API_SECRET in your .env file first")
    exit(1)

kite = KiteConnect(api_key=API_KEY)
login_url = kite.login_url()

request_token_holder = {}

class TokenCatcher(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if "request_token" in params:
            request_token_holder["token"] = params["request_token"][0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Token captured. You can close this tab.")

    def log_message(self, *args):
        pass  # suppress logs

print("\n Zerodha Kite Token Refresher")
print("─" * 40)
print(f"Opening Zerodha login in browser...")
webbrowser.open(login_url)
print("After login, browser redirects to localhost:5000")
print("Waiting for token...\n")

server = HTTPServer(("localhost", 5000), TokenCatcher)
server.handle_request()

request_token = request_token_holder.get("token")
if not request_token:
    print("[ERROR] No request token captured")
    exit(1)

data = kite.generate_session(request_token, api_secret=API_SECRET)
access_token = data["access_token"]

# Save to .env
set_key(ENV_FILE, "KITE_ACCESS_TOKEN", access_token)
print(f"[OK] Access token saved to {ENV_FILE}")
print(f"     Token: {access_token[:12]}...{access_token[-6:]}")
print("\nYou're done. The engine will use this token automatically this month.\n")
