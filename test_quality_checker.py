#!/usr/bin/env python3
"""Test that DataQualityChecker methods are correctly called"""

import sys
import os
import tempfile
from datetime import datetime

# Add project to path
sys.path.insert(0, '/root/snowflake')

from snowflake_etl.validators.data_quality import DataQualityChecker
from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.models.file_config import FileConfig

def test_quality_checker_methods():
    """Test that DataQualityChecker methods work correctly"""
    
    print("Testing DataQualityChecker methods...")
    
    # Create test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
        # Write header
        columns = ['RECORDDATE', 'RECORDDATEID', 'VALUE', 'OTHER']
        f.write('\t'.join(columns) + '\n')
        # Write data
        f.write('2024-01-01\t20240101\t100\ttest\n')
        f.write('2024-01-02\t20240102\t200\ttest\n')
        test_file = f.name
    
    try:
        # Create quality checker
        checker = DataQualityChecker()
        
        # Test validate_file method
        print("\n1. Testing validate_file method...")
        result = checker.validate_file(
            file_path=test_file,
            expected_columns=columns,
            date_column='RECORDDATE',
            expected_start=datetime(2024, 1, 1),
            expected_end=datetime(2024, 1, 2),
            delimiter='\t'
        )
        
        print(f"   Result: {result}")
        print(f"   Validation passed: {result.get('validation_passed')}")
        
        # Test LoadOperation integration
        print("\n2. Testing LoadOperation integration...")
        context = ApplicationContext(config_path='/root/snowflake/config/factLendingBenchmark_config.json')
        load_op = LoadOperation(context)
        
        # Create file config
        file_config = FileConfig(
            file_path=test_file,
            table_name="TEST_TABLE",
            date_column="RECORDDATE",
            expected_columns=columns,
            expected_date_range=(datetime(2024, 1, 1), datetime(2024, 1, 2))
        )
        
        # Test _run_quality_checks
        print("\n3. Testing _run_quality_checks method...")
        qc_result = load_op._run_quality_checks(file_config, max_workers=1)
        
        print(f"   QC Result: {qc_result}")
        print(f"   Valid: {qc_result.get('valid')}")
        if qc_result.get('error'):
            print(f"   Error: {qc_result.get('error')}")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)

if __name__ == "__main__":
    test_quality_checker_methods()