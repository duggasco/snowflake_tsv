#!/usr/bin/env python3
"""
Diagnostic script to check Snowflake table existence and get column info
Usage: python3 check_snowflake_table.py config.json TABLE_NAME
"""

import sys
import json
import snowflake.connector

def check_table(config_file, table_name):
    print(f"Loading config from: {config_file}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    sf_config = config.get('snowflake', {})
    
    print(f"Connecting to Snowflake...")
    print(f"  Account: {sf_config.get('account')}")
    print(f"  Database: {sf_config.get('database')}")
    print(f"  Schema: {sf_config.get('schema')}")
    print(f"  Warehouse: {sf_config.get('warehouse')}")
    
    try:
        conn = snowflake.connector.connect(**sf_config)
        cursor = conn.cursor()
        print("✓ Connected successfully")
        
        # Get current database/schema
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        current_db, current_schema = cursor.fetchone()
        print(f"Current context: {current_db}.{current_schema}")
        
        # Search for table in current schema
        print(f"\nSearching for table '{table_name}'...")
        
        # Method 1: Direct query with full qualification
        query1 = f"""
        SELECT COUNT(*) as col_count
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER('{table_name}')
          AND UPPER(table_schema) = UPPER('{current_schema}')
          AND UPPER(table_catalog) = UPPER('{current_db}')
        """
        cursor.execute(query1)
        count = cursor.fetchone()[0]
        print(f"  Columns in {current_db}.{current_schema}.{table_name}: {count}")
        
        if count > 0:
            # Get column names
            query2 = f"""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE UPPER(table_name) = UPPER('{table_name}')
              AND UPPER(table_schema) = UPPER('{current_schema}')
              AND UPPER(table_catalog) = UPPER('{current_db}')
            ORDER BY ordinal_position
            LIMIT 10
            """
            cursor.execute(query2)
            print("\n  First 10 columns:")
            for col_name, data_type, pos in cursor:
                print(f"    {pos}. {col_name} ({data_type})")
        
        # Method 2: Search all schemas
        query3 = f"""
        SELECT table_catalog, table_schema, COUNT(*) as col_count
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER('{table_name}')
        GROUP BY table_catalog, table_schema
        """
        cursor.execute(query3)
        results = cursor.fetchall()
        
        if results:
            print(f"\nTable '{table_name}' found in:")
            for db, schema, col_count in results:
                print(f"  - {db}.{schema} ({col_count} columns)")
        else:
            print(f"\nTable '{table_name}' not found in any accessible schema")
            
            # List available tables
            query4 = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{current_schema}'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            LIMIT 20
            """
            cursor.execute(query4)
            tables = [row[0] for row in cursor]
            
            if tables:
                print(f"\nAvailable tables in {current_schema}:")
                for t in tables:
                    print(f"  - {t}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 check_snowflake_table.py config.json TABLE_NAME")
        sys.exit(1)
    
    sys.exit(check_table(sys.argv[1], sys.argv[2]))