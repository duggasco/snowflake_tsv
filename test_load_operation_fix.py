#!/usr/bin/env python3
"""Test that LoadOperation quality checks work after the fix"""

import sys
import os
import tempfile
from datetime import datetime

# Add project to path
sys.path.insert(0, '/root/snowflake')

from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.models.file_config import FileConfig

def test_load_operation_qc():
    """Test LoadOperation quality checks after fixing method name"""
    
    print("Testing LoadOperation quality checks...")
    
    # Create test file with valid data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
        # Write header (41 columns like the real file)
        columns = [f'COL{i}' for i in range(41)]
        columns[0] = 'RECORDDATE'
        columns[1] = 'RECORDDATEID'
        f.write('\t'.join(columns) + '\n')
        
        # Write data for date range
        dates = ['2024-01-01', '2024-01-02', '2024-01-03']
        for date in dates:
            date_id = date.replace('-', '')
            row = [date, date_id] + ['value' for _ in range(39)]
            f.write('\t'.join(row) + '\n')
        test_file = f.name
    
    try:
        # Create context and load operation
        context = ApplicationContext(config_path='/root/snowflake/config/factLendingBenchmark_config.json')
        load_op = LoadOperation(context)
        
        # Test 1: Valid file with correct dates
        print("\n1. Testing with valid file...")
        file_config = FileConfig(
            file_path=test_file,
            table_name="TEST_TABLE",
            date_column="RECORDDATE",
            expected_columns=columns,
            expected_date_range=(datetime(2024, 1, 1), datetime(2024, 1, 3))
        )
        
        result = load_op._run_quality_checks(file_config, max_workers=1)
        print(f"   Valid: {result.get('valid')}")
        print(f"   Error: {result.get('error', 'None')}")
        
        # Test 2: File with missing expected columns
        print("\n2. Testing with missing columns...")
        wrong_columns = columns + ['EXTRA_COLUMN']
        file_config2 = FileConfig(
            file_path=test_file,
            table_name="TEST_TABLE",
            date_column="RECORDDATE",
            expected_columns=wrong_columns,
            expected_date_range=(datetime(2024, 1, 1), datetime(2024, 1, 3))
        )
        
        result2 = load_op._run_quality_checks(file_config2, max_workers=1)
        print(f"   Valid: {result2.get('valid')}")
        print(f"   Error: {result2.get('error', 'None')}")
        
        # Test 3: File with wrong date range
        print("\n3. Testing with wrong date range...")
        file_config3 = FileConfig(
            file_path=test_file,
            table_name="TEST_TABLE",
            date_column="RECORDDATE",
            expected_columns=columns,
            expected_date_range=(datetime(2024, 1, 1), datetime(2024, 1, 10))  # Expects more dates
        )
        
        result3 = load_op._run_quality_checks(file_config3, max_workers=1)
        print(f"   Valid: {result3.get('valid')}")
        print(f"   Error: {result3.get('error', 'None')}")
        
        print("\n✅ LoadOperation quality checks are working correctly!")
        
    except AttributeError as e:
        if "check_data_quality" in str(e):
            print(f"\n❌ ERROR: The old method name is still being used: {e}")
            print("   The fix hasn't been applied correctly.")
        else:
            raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)

if __name__ == "__main__":
    test_load_operation_qc()