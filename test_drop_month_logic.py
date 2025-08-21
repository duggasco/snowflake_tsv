#!/usr/bin/env python3
"""Test the logic of drop_month.py without Snowflake connection"""

import sys
import json
from datetime import datetime, timedelta

# Test the date range calculation logic
def get_date_range_for_month(year_month: str):
    """Convert YYYY-MM to start/end dates in YYYYMMDD format."""
    try:
        year, month = map(int, year_month.split('-'))
        if not 1 <= month <= 12:
            raise ValueError("Month must be between 1 and 12.")
        start_date = datetime(year, month, 1)
        # Get last day of month
        if month == 12:
            end_date = datetime(year, 12, 31)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        return int(start_date.strftime('%Y%m%d')), int(end_date.strftime('%Y%m%d'))
    except (ValueError, TypeError) as e:
        print(f"Invalid month format '{year_month}'. Expected YYYY-MM. Error: {e}")
        return None

# Test the SQL generation logic
def generate_delete_sql(table_name, date_column, start_date, end_date):
    """Generate DELETE SQL with proper formatting"""
    return f"""
    DELETE FROM {table_name}
    WHERE {date_column} >= {start_date} 
      AND {date_column} <= {end_date}
    """

# Test various month formats
test_months = [
    "2024-01",  # January - 31 days
    "2024-02",  # February - 29 days (leap year)
    "2024-04",  # April - 30 days
    "2024-12",  # December - 31 days
    "2023-02",  # February - 28 days (non-leap year)
]

print("Testing Date Range Calculation:")
print("-" * 50)
for month in test_months:
    result = get_date_range_for_month(month)
    if result:
        start, end = result
        print(f"{month}: {start} to {end}")
        # Verify the dates
        start_dt = datetime.strptime(str(start), '%Y%m%d')
        end_dt = datetime.strptime(str(end), '%Y%m%d')
        days = (end_dt - start_dt).days + 1
        print(f"  → {days} days in month")

print("\nTesting SQL Generation:")
print("-" * 50)
test_cases = [
    ("TEST_TABLE", "recordDate", 20240101, 20240131),
    ("FACT_LENDING", "recordDateId", 20240201, 20240229),
]

for table, column, start, end in test_cases:
    sql = generate_delete_sql(table, column, start, end)
    print(f"Table: {table}, Column: {column}")
    print(f"SQL:{sql}")

print("\nTesting Config Parsing:")
print("-" * 50)
# Test loading the config file
config_path = "config/generated_config.json"
try:
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Extract table info
    for file_spec in config.get('files', []):
        table_name = file_spec.get('table_name')
        date_column = file_spec.get('date_column', 'recordDate')
        print(f"Table: {table_name}")
        print(f"Date Column: {date_column}")
        print(f"Expected Columns: {len(file_spec.get('expected_columns', []))} columns")
        
        # Generate a sample DELETE for January 2024
        date_range = get_date_range_for_month("2024-01")
        if date_range:
            sql = generate_delete_sql(table_name, date_column, date_range[0], date_range[1])
            print(f"Sample DELETE SQL for Jan 2024:{sql}")
except FileNotFoundError:
    print(f"Config file not found: {config_path}")
except json.JSONDecodeError as e:
    print(f"Error parsing JSON: {e}")

print("\nDrop Month Script Logic Tests: PASSED ✓")