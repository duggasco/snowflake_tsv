#!/usr/bin/env python3
"""
Diagnostic script for Snowflake COPY errors
Analyzes incident 5452401 and provides troubleshooting guidance
"""

import snowflake.connector
import json
import sys
from datetime import datetime, timedelta

def load_config(config_path):
    """Load configuration from JSON file"""
    with open(config_path, 'r') as f:
        return json.load(f)

def analyze_copy_error(cursor):
    """Analyze recent COPY errors and provide recommendations"""
    
    print("\n" + "="*60)
    print("SNOWFLAKE COPY ERROR DIAGNOSTIC")
    print("="*60)
    
    # 1. Check recent failed queries
    print("\n1. Checking recent failed COPY operations...")
    
    query = """
    SELECT 
        QUERY_ID,
        QUERY_TEXT,
        ERROR_CODE,
        ERROR_MESSAGE,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME / 1000 as ELAPSED_SECONDS,
        DATABASE_NAME,
        SCHEMA_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        BYTES_SCANNED / (1024*1024*1024) as GB_SCANNED
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
        END_TIME_RANGE_END => CURRENT_TIMESTAMP()
    ))
    WHERE ERROR_CODE IN ('000603', '300005')
       OR ERROR_MESSAGE LIKE '%incident%'
    ORDER BY START_TIME DESC
    LIMIT 5
    """
    
    cursor.execute(query)
    errors = cursor.fetchall()
    
    if errors:
        print(f"Found {len(errors)} recent errors:")
        for error in errors:
            print(f"\n  Query ID: {error[0]}")
            print(f"  Error Code: {error[2]}")
            print(f"  Error Message: {error[3][:200]}...")
            print(f"  Time: {error[4]} ({error[6]:.1f} seconds)")
            print(f"  Warehouse: {error[9]} (Size: {error[10]})")
            print(f"  Data Scanned: {error[11]:.2f} GB")
    else:
        print("No recent errors found")
    
    # 2. Check stage files
    print("\n2. Checking stage files that might be problematic...")
    
    stage_query = """
    LIST @~/tsv_stage/ PATTERN='.*factMarkitExBlkBenchmark.*'
    """
    
    try:
        cursor.execute(stage_query)
        files = cursor.fetchall()
        
        if files:
            print(f"Found {len(files)} related files in stage:")
            for file in files[:5]:
                name = file[0]
                size_mb = file[1] / (1024*1024)
                last_modified = file[3]
                print(f"  - {name}: {size_mb:.1f} MB, Modified: {last_modified}")
        else:
            print("No related files found in stage")
    except Exception as e:
        print(f"Could not list stage files: {e}")
    
    # 3. Check table structure for issues
    print("\n3. Checking table structure...")
    
    table_query = """
    SELECT COUNT(*) as COLUMN_COUNT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK'
    """
    
    cursor.execute(table_query)
    result = cursor.fetchone()
    if result:
        print(f"Table has {result[0]} columns")
    
    # 4. Check for data type mismatches
    print("\n4. Common causes of error 000603/300005:")
    print("""
    a) Data type mismatches:
       - Date/timestamp format issues
       - Numeric precision overflow
       - String truncation
       
    b) File format issues:
       - Incorrect delimiter
       - Unescaped quotes
       - Encoding problems (non-UTF8)
       
    c) Resource constraints:
       - Warehouse too small
       - Memory limitations
       - Query timeout
       
    d) Data quality issues:
       - NULL values in NOT NULL columns
       - Duplicate primary keys
       - Foreign key violations
    """)
    
    # 5. Provide recommendations
    print("\n" + "="*60)
    print("RECOMMENDATIONS")
    print("="*60)
    
    print("""
    1. IMMEDIATE ACTIONS:
       - Contact Snowflake Support with incident number: 5452401
       - Try loading a smaller sample file to isolate the issue
       - Check if the file was partially loaded
    
    2. VALIDATION STEPS:
       # Check what was loaded (if anything)
       SELECT COUNT(*) FROM TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK 
       WHERE RECORDDATE >= '2023-08-01';
       
       # Validate file format
       SELECT $1, $2, $3, $4, $5 
       FROM @~/tsv_stage/TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK/
       (FILE_FORMAT => (TYPE = 'CSV' FIELD_DELIMITER = '\t'))
       LIMIT 10;
    
    3. ALTERNATIVE APPROACHES:
       a) Try with smaller chunks:
          - Split the file into smaller parts (100MB each)
          - Load incrementally
       
       b) Adjust COPY parameters:
          - Remove PURGE = TRUE temporarily
          - Try ON_ERROR = 'SKIP_FILE' to see specific errors
          - Increase SIZE_LIMIT if file is very large
       
       c) Use different warehouse:
          - ALTER SESSION SET USE_WAREHOUSE = 'LARGER_WAREHOUSE';
          - Try with WAREHOUSE_SIZE = 'LARGE' or 'X-LARGE'
    
    4. MODIFIED COPY COMMAND:
       COPY INTO TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK
       FROM @~/tsv_stage/TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK/
       FILE_FORMAT = (
           TYPE = 'CSV'
           FIELD_DELIMITER = '\\t'
           SKIP_HEADER = 0
           FIELD_OPTIONALLY_ENCLOSED_BY = '"'
           ESCAPE_UNENCLOSED_FIELD = NONE
           ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
           REPLACE_INVALID_CHARACTERS = TRUE
           DATE_FORMAT = 'AUTO'
           TIMESTAMP_FORMAT = 'AUTO'
       )
       ON_ERROR = 'SKIP_FILE'
       SIZE_LIMIT = 5368709120  -- 5GB limit
       RETURN_FAILED_ONLY = TRUE;
    """)
    
    # 6. Check for partial load
    print("\n5. Checking for partial data load...")
    
    check_query = """
    SELECT 
        COUNT(*) as ROW_COUNT,
        MIN(RECORDDATE) as MIN_DATE,
        MAX(RECORDDATE) as MAX_DATE
    FROM TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK
    WHERE RECORDDATE >= '2023-08-01'
      AND RECORDDATE <= '2023-08-31'
    """
    
    try:
        cursor.execute(check_query)
        result = cursor.fetchone()
        if result and result[0] > 0:
            print(f"Found {result[0]:,} rows already loaded")
            print(f"Date range: {result[1]} to {result[2]}")
            print("\nWARNING: Partial data may have been loaded!")
            print("Consider cleaning up before retry:")
            print(f"DELETE FROM TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK")
            print(f"WHERE RECORDDATE >= '2023-08-01' AND RECORDDATE <= '2023-08-31';")
    except Exception as e:
        print(f"Could not check for partial load: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python diagnose_copy_error.py <config.json>")
        sys.exit(1)
    
    config = load_config(sys.argv[1])
    
    try:
        conn = snowflake.connector.connect(**config['snowflake'])
        cursor = conn.cursor()
        
        analyze_copy_error(cursor)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()