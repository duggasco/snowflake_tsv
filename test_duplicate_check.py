#!/usr/bin/env python3
"""
Test script for duplicate detection feature
"""

import json
import sys
from datetime import datetime
from tsv_loader import SnowflakeDataValidator

def test_duplicate_check():
    """Test the duplicate detection feature"""
    
    # Load config
    config_path = "config/generated_config.json"
    print(f"Loading config from {config_path}")
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    snowflake_params = config['snowflake']
    
    # Initialize validator
    print("Connecting to Snowflake...")
    validator = SnowflakeDataValidator(snowflake_params)
    
    try:
        # Test parameters
        table_name = "TEST_CUSTOM_FACTLENDINGBENCHMARK"
        key_columns = ["recordDate", "assetId", "fundId"]
        date_column = "recordDate"
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        
        print(f"\nChecking for duplicates in {table_name}")
        print(f"Key columns: {key_columns}")
        print(f"Date range: {start_date} to {end_date}")
        print("-" * 60)
        
        # Run duplicate check
        result = validator.check_duplicates(
            table_name=table_name,
            key_columns=key_columns,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date,
            sample_limit=5
        )
        
        # Display results
        if result.get('error'):
            print(f"Error: {result['error']}")
        elif result.get('has_duplicates'):
            stats = result['statistics']
            print(f"\n⚠️  DUPLICATES FOUND!")
            print(f"Total rows in range: {stats['total_rows']:,}")
            print(f"Duplicate key combinations: {stats['duplicate_key_combinations']:,}")
            print(f"Total duplicate rows: {stats['total_duplicate_rows']:,}")
            print(f"Excess rows to remove: {stats['excess_rows']:,}")
            print(f"Duplicate percentage: {stats['duplicate_percentage']:.2f}%")
            print(f"Max duplicates per key: {stats['max_duplicates_per_key']}")
            print(f"Severity: {result['severity']}")
            
            # Show distribution
            if result.get('duplicate_distribution'):
                print("\nDuplicate distribution:")
                for dist in result['duplicate_distribution'][:5]:
                    print(f"  {dist['duplicates_per_key']} duplicates: {dist['key_combinations']} key combinations")
            
            # Show samples
            if result.get('sample_duplicates'):
                print("\nSample duplicate keys:")
                for sample in result['sample_duplicates']:
                    key_str = ', '.join([f"{k}={v}" for k, v in sample['key_values'].items()])
                    print(f"  • {key_str} (appears {sample['duplicate_count']} times)")
        else:
            print("✅ No duplicates found!")
            
    finally:
        validator.close()
        print("\nTest complete.")

if __name__ == "__main__":
    test_duplicate_check()