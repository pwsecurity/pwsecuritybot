import logging
import requests
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants from h.py
PROXYCHECKER_API_TOKENS = [
    "TdAROzZAkgybV9z55FjSC6F0ySqOGTWJVe9nkTW4nQ13xIPbWHrTDIDK6d6vXUTY",
    "Y3H2R2t1kEt5Fy3Ql9Lj5vmaEC64jab8285VUDuApceemvhwB7hDO8Gfbv3t21R2"
]
PROXYCHECKER_API_URL = "https://proxychecker.org/api"

def check_socks5_proxy_via_api(proxy_string):
    """
    Check SOCKS5 proxy using ProxyChecker.org API.
    """
    print(f"\nTesting proxy: {proxy_string}")
    
    # Try both API tokens
    for token_index, api_token in enumerate(PROXYCHECKER_API_TOKENS, 1):
        try:
            headers = {
                'Authorization': f'Bearer {api_token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            data = {
                'proxy': proxy_string,
                'check_ssl': False,
                'check_anonymity': True,
                'check_speed': False,
                'check_location': False
            }
            
            print(f"Sending request to {PROXYCHECKER_API_URL}/proxy/check with Token {token_index}...")
            response = requests.post(
                f'{PROXYCHECKER_API_URL}/proxy/check',
                headers=headers,
                json=data,
                timeout=15
            )
            
            print(f"Status Code: {response.status_code}")
            try:
                result = response.json()
                print(f"FULL API RESPONSE: {json.dumps(result, indent=2)}")
            except:
                print(f"Raw Response: {response.text}")
                
            if response.status_code == 429:
                print("Rate limited, trying next token...")
                continue
            
            # Simulate the parsing logic
            result = response.json()
            if result.get('success'):
                data = result.get('data', {})
                is_working = data.get('working', False)
                print(f"PARSED STATUS: {'✅ Online' if is_working else '❌ Offline'}")
                
            return result
            
        except Exception as e:
            print(f"Error: {e}")

# Test with one of the proxies from ip.txt
test_proxy = "31.57.243.5:9094:bws111:bws222"
check_socks5_proxy_via_api(test_proxy)
