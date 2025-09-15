#!/usr/bin/env python3
"""
Debug script to test Snowflake connection with proxy settings
"""

import os
import json
import sys
from pathlib import Path

def test_snowflake_connection():
    """Test Snowflake connection with various proxy configurations"""
    
    print("=== Snowflake Proxy Connection Test ===\n")
    
    # Show current proxy settings
    print("Current Environment Variables:")
    for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'no_proxy']:
        value = os.environ.get(var, 'not set')
        if value != 'not set' and '@' in value:
            # Hide password in proxy URL
            import urllib.parse
            parsed = urllib.parse.urlparse(value)
            if parsed.password:
                value = value.replace(f":{parsed.password}@", ":***@")
        print(f"  {var}: {value}")
    
    print("\nSnowflake-specific settings:")
    print(f"  SNOWFLAKE_INSECURE_MODE: {os.environ.get('SNOWFLAKE_INSECURE_MODE', 'not set')}")
    
    # Check for proxy config file
    proxy_file = Path.home() / '.snowflake_etl' / '.proxy_config'
    if not proxy_file.exists():
        proxy_file = Path('.etl_state') / '.proxy_config'
    
    if proxy_file.exists():
        print(f"\nFound proxy config file: {proxy_file}")
        proxy_url = proxy_file.read_text().strip()
        # Hide password
        if '@' in proxy_url:
            import urllib.parse
            parsed = urllib.parse.urlparse(proxy_url)
            if parsed.password:
                proxy_url = proxy_url.replace(f":{parsed.password}@", ":***@")
        print(f"  Proxy URL: {proxy_url}")
    
    # Try to load Snowflake config
    config_file = None
    for path in ['config/config.json', 'config.json', '../config/config.json']:
        if Path(path).exists():
            config_file = path
            break
    
    if not config_file:
        print("\nNo config file found. Please specify path to config.json")
        return
    
    print(f"\nUsing config file: {config_file}")
    
    try:
        with open(config_file) as f:
            config = json.load(f)
        
        sf_config = config.get('snowflake', {})
        print(f"Snowflake account: {sf_config.get('account', 'not set')}")
        
        # Try different connection modes
        print("\n=== Testing Connection Modes ===\n")
        
        import snowflake.connector
        
        # Test 1: Standard connection with proxy from environment
        print("1. Testing with environment proxy settings...")
        try:
            conn_params = sf_config.copy()
            conn_params['ocsp_fail_open'] = True
            conn_params['validate_default_parameters'] = False
            
            conn = snowflake.connector.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"   SUCCESS: Connected! Snowflake version: {version}")
            conn.close()
        except Exception as e:
            print(f"   FAILED: {str(e)[:200]}")
        
        # Test 2: With insecure mode
        print("\n2. Testing with insecure mode...")
        try:
            conn_params = sf_config.copy()
            conn_params['insecure_mode'] = True
            conn_params['ocsp_fail_open'] = True
            conn_params['validate_default_parameters'] = False
            conn_params['protocol'] = 'http'
            
            conn = snowflake.connector.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            print(f"   SUCCESS: Connected! Snowflake version: {version}")
            conn.close()
        except Exception as e:
            print(f"   FAILED: {str(e)[:200]}")
        
        # Test 3: With explicit proxy settings
        if proxy_file.exists():
            print("\n3. Testing with explicit proxy configuration...")
            proxy_url = proxy_file.read_text().strip()
            
            import urllib.parse
            parsed = urllib.parse.urlparse(proxy_url)
            
            try:
                conn_params = sf_config.copy()
                if parsed.hostname:
                    conn_params['proxy_host'] = parsed.hostname
                    if parsed.port:
                        conn_params['proxy_port'] = parsed.port
                    if parsed.username:
                        conn_params['proxy_user'] = parsed.username
                    if parsed.password:
                        conn_params['proxy_password'] = parsed.password
                
                conn_params['insecure_mode'] = True
                conn_params['ocsp_fail_open'] = True
                conn_params['validate_default_parameters'] = False
                conn_params['protocol'] = 'http'
                conn_params['disable_request_pooling'] = True
                
                conn = snowflake.connector.connect(**conn_params)
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                print(f"   SUCCESS: Connected! Snowflake version: {version}")
                conn.close()
            except Exception as e:
                print(f"   FAILED: {str(e)[:200]}")
        
        print("\n=== Troubleshooting Tips ===")
        print("1. If all tests fail, try setting:")
        print("   export SNOWFLAKE_INSECURE_MODE=1")
        print("2. Ensure your proxy URL is in format: http://user:pass@proxy:port")
        print("3. Some proxies require authentication in the URL")
        print("4. Check if your Snowflake account requires specific network policies")
        
    except Exception as e:
        print(f"\nError: {e}")

if __name__ == "__main__":
    test_snowflake_connection()