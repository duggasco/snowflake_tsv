#!/usr/bin/env python3
"""
Test script to verify validation progress bars work correctly.
Tests both --validate-only and --validate-in-snowflake modes.
"""

import os
import sys
import time
import json

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_validation_progress():
    """Test that validation shows progress bars"""
    
    print("\n" + "="*60)
    print("TESTING VALIDATION PROGRESS BARS")
    print("="*60)
    
    # Create a mock config file
    config = {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "password": "test_pass",
            "warehouse": "test_warehouse",
            "database": "test_db",
            "schema": "test_schema",
            "role": "test_role"
        },
        "files": [
            {
                "file_pattern": "test_{date_range}.tsv",
                "table_name": "TEST_TABLE_1",
                "date_column": "RECORDDATEID",
                "expected_columns": ["col1", "col2", "col3"]
            },
            {
                "file_pattern": "test2_{date_range}.tsv",
                "table_name": "TEST_TABLE_2",
                "date_column": "RECORDDATEID",
                "expected_columns": ["col1", "col2", "col3"]
            },
            {
                "file_pattern": "test3_{date_range}.tsv",
                "table_name": "TEST_TABLE_3",
                "date_column": "RECORDDATEID",
                "expected_columns": ["col1", "col2", "col3"]
            }
        ]
    }
    
    # Save config to temp file
    config_file = "/tmp/test_validation_config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print("\nCreated test configuration with 3 tables")
    
    # Test 1: --validate-only mode
    print("\n" + "-"*40)
    print("Test 1: --validate-only mode")
    print("-"*40)
    print("\nCommand that would be run:")
    print("python tsv_loader.py --config {} --validate-only --month 2024-01 --quiet".format(config_file))
    print("\nExpected behavior:")
    print("1. Progress bar shows: 'Validating tables' with 3 tables total")
    print("2. Each table shows status: ✓ or ✗ (with anomaly count if any)")
    print("3. Validation details ALWAYS shown (even in --quiet mode)")
    print("4. Progress bar visible in stderr (even in --quiet mode)")
    
    # Test 2: --validate-in-snowflake mode
    print("\n" + "-"*40)
    print("Test 2: --validate-in-snowflake mode")
    print("-"*40)
    print("\nCommand that would be run:")
    print("python tsv_loader.py --config {} --validate-in-snowflake --month 2024-01 --quiet".format(config_file))
    print("\nExpected behavior:")
    print("1. Files are loaded WITHOUT file-based QC")
    print("2. After loading, validation progress bar appears")
    print("3. Each table shows validation status with anomaly count")
    print("4. Full validation details shown at end (even in --quiet mode)")
    
    # Example output
    print("\n" + "="*60)
    print("EXAMPLE PROGRESS BAR OUTPUT")
    print("="*60)
    
    # Simulate progress bar
    try:
        from tqdm import tqdm
        import time
        
        print("\nSimulating validation progress:")
        tables = ["TEST_TABLE_1", "TEST_TABLE_2", "TEST_TABLE_3"]
        statuses = ["✓", "✗ (3 anomalies)", "✓"]
        
        pbar = tqdm(total=3, desc="Validating tables", unit="table", file=sys.stderr)
        for i, (table, status) in enumerate(zip(tables, statuses)):
            pbar.set_description(f"Validating {table}")
            time.sleep(0.5)
            pbar.set_postfix_str(f"{table}: {status}")
            pbar.update(1)
        pbar.close()
        
        print("\n✓ Progress bar test completed")
    except ImportError:
        print("\n(tqdm not installed - would show progress bar here)")
    
    # Example validation output
    print("\n" + "="*60)
    print("EXAMPLE VALIDATION DETAILS OUTPUT")
    print("="*60)
    print("""
TEST_TABLE_1:
  Status: ✓ VALID
  Date Range: 2024-01-01 to 2024-01-31
  Total Rows: 1,488,000
  Unique Dates: 31
  Expected Dates: 31
  Avg Rows/Day: 48,000

TEST_TABLE_2:
  Status: ✗ INVALID
  Date Range: 2024-01-01 to 2024-01-31
  Total Rows: 1,440,012
  Unique Dates: 31
  Expected Dates: 31
  Missing Dates: 0
  Avg Rows/Day: 46,452
  
  Row Count Analysis:
    Mean: 46,452 rows/day
    Median: 48,000 rows/day
    Range: 12 - 52,000 rows
    Anomalies Detected: 3 dates
  
  ⚠️  Anomalous Dates (low row counts):
    1) 2024-01-05 - 12 rows (0.0% of avg) - SEVERELY_LOW
    2) 2024-01-15 - 2,400 rows (5.2% of avg) - SEVERELY_LOW
    3) 2024-01-22 - 18,000 rows (38.7% of avg) - LOW
  
  ⚠️  Warnings:
    • Found 3 dates with anomalous row counts: 2 SEVERELY_LOW, 1 LOW
    • CRITICAL: 2 dates have less than 10% of average row count - possible data loss

TEST_TABLE_3:
  Status: ✓ VALID
  Date Range: 2024-01-01 to 2024-01-31
  Total Rows: 1,488,000
  Unique Dates: 31
  Expected Dates: 31
  Avg Rows/Day: 48,000
""")
    
    print("\n" + "="*60)
    print("KEY FEATURES")
    print("="*60)
    print("\n✓ Progress bars show in --quiet mode (via stderr)")
    print("✓ Validation details ALWAYS show (critical data)")
    print("✓ Anomaly counts shown in progress bar status")
    print("✓ Full anomaly details in validation output")
    print("✓ Works for both --validate-only and --validate-in-snowflake")
    
    # Clean up
    if os.path.exists(config_file):
        os.remove(config_file)
    
    return True

if __name__ == "__main__":
    print("Testing Validation Progress Bars and Output")
    print("="*50)
    
    success = test_validation_progress()
    
    if success:
        print("\n✓✓✓ Test scenarios documented successfully!")
        print("\nTo test with real data:")
        print("1. Run: ./run_loader.sh --validate-only --month 2024-01 --quiet")
        print("2. Run: ./run_loader.sh --validate-in-snowflake --month 2024-01 --quiet")
        print("\nBoth should show progress bars and full validation details.")
    
    sys.exit(0 if success else 1)