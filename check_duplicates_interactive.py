#!/usr/bin/env python3
"""
Interactive duplicate checker with progress feedback
"""

import json
import sys
import time
from datetime import datetime
from tsv_loader import SnowflakeDataValidator

def show_progress(message):
    """Show progress message to stderr so it's visible during execution"""
    print(f"\r{message}...", end='', file=sys.stderr, flush=True)

def clear_progress():
    """Clear the progress line"""
    print("\r" + " " * 80 + "\r", end='', file=sys.stderr, flush=True)

def main():
    if len(sys.argv) < 5:
        print("Usage: check_duplicates_interactive.py <config_file> <table> <key_columns> <start_date> <end_date>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    table = sys.argv[2]
    key_columns = sys.argv[3].split(',')
    key_columns = [col.strip() for col in key_columns]
    start_date = sys.argv[4] if sys.argv[4] != 'none' else None
    end_date = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != 'none' else None
    
    # Show initial status
    show_progress("Loading configuration")
    
    # Load config
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except Exception as e:
        clear_progress()
        print(f"Error loading config: {e}")
        sys.exit(1)
    
    snowflake_params = config['snowflake']
    
    # Print header
    clear_progress()
    print(f'Checking table: {table}')
    print(f'Key columns: {key_columns}')
    if start_date:
        print(f'Date range: {start_date} to {end_date}')
    else:
        print('Date range: All data')
    print('-' * 60)
    
    # Initialize validator
    show_progress("Connecting to Snowflake")
    validator = SnowflakeDataValidator(snowflake_params)
    
    try:
        # Determine date column (usually first key column)
        date_column = key_columns[0] if key_columns else 'recordDate'
        
        # Show progress
        show_progress("Analyzing table structure")
        time.sleep(0.5)  # Brief pause so message is visible
        
        show_progress("Scanning for duplicate keys")
        
        # Run duplicate check
        if start_date:
            result = validator.check_duplicates(
                table_name=table,
                key_columns=key_columns,
                date_column=date_column,
                start_date=start_date,
                end_date=end_date,
                sample_limit=5
            )
        else:
            # Check all data
            result = validator.check_duplicates(
                table_name=table,
                key_columns=key_columns,
                date_column=date_column,
                start_date=None,
                end_date=None,
                sample_limit=5
            )
        
        clear_progress()
        
        # Display results
        if result.get('error'):
            print(f'Error: {result["error"]}')
        elif result.get('has_duplicates'):
            stats = result['statistics']
            print(f'\nWARNING: DUPLICATES FOUND!')
            print(f'Total rows: {stats["total_rows"]:,}')
            print(f'Duplicate keys: {stats["duplicate_key_combinations"]:,}')
            print(f'Excess rows: {stats["excess_rows"]:,}')
            print(f'Duplicate %: {stats["duplicate_percentage"]:.2f}%')
            print(f'Severity: {result["severity"]}')
            
            # Show samples
            if result.get('sample_duplicates'):
                print('\nSample duplicate keys:')
                for sample in result['sample_duplicates'][:5]:
                    key_str = ', '.join([f'{k}={v}' for k, v in sample['key_values'].items()])
                    print(f'  - {key_str} (x{sample["duplicate_count"]})')
        else:
            print('SUCCESS: No duplicates found!')
            
    except KeyboardInterrupt:
        clear_progress()
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        clear_progress()
        print(f"Error during duplicate check: {e}")
        sys.exit(1)
    finally:
        show_progress("Closing connection")
        validator.close()
        clear_progress()

if __name__ == '__main__':
    main()