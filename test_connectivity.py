#!/usr/bin/env python3
"""
Quick connectivity test for Snowflake
"""
import json
import sys
import time
from pathlib import Path

def test_snowflake_connection(config_file):
    """Test if we can connect to Snowflake"""
    
    # Read config
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error reading config: {e}")
        return False
    
    # Try to import snowflake connector
    try:
        import snowflake.connector
    except ImportError as e:
        print(f"Cannot import snowflake.connector: {e}")
        return False
    
    # Try to connect (with short timeout)
    try:
        print("Testing Snowflake connection...")
        conn = snowflake.connector.connect(
            account=config['snowflake']['account'],
            user=config['snowflake']['user'],
            password=config['snowflake']['password'],
            warehouse=config['snowflake'].get('warehouse'),
            database=config['snowflake'].get('database'),
            schema=config['snowflake'].get('schema'),
            role=config['snowflake'].get('role'),
            login_timeout=10,  # 10 second timeout
            network_timeout=10
        )
        
        # Try a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✓ Connected to Snowflake version: {version}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Cannot connect to Snowflake: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_connectivity.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    if test_snowflake_connection(config_file):
        sys.exit(0)
    else:
        sys.exit(1)