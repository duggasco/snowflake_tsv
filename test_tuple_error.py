#!/usr/bin/env python3
"""Test script to verify the tuple formatting issue is resolved"""

import tempfile
import sys
import os

# Add project to path
sys.path.insert(0, '/root/snowflake')

from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.models.file_config import FileConfig

def test_count_rows_formatting():
    """Test that count_rows_fast properly unpacks and formats"""
    
    # Create a test TSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
        # Write header and some data
        f.write('\t'.join(['col' + str(i) for i in range(41)]) + '\n')  # 41 columns header
        for i in range(100):
            f.write('\t'.join(['val' + str(i) for _ in range(41)]) + '\n')
        test_file = f.name
    
    try:
        # Create a minimal context
        context = ApplicationContext(config_path='/root/snowflake/config/factLendingBenchmark_config.json')
        
        # Create load operation
        load_op = LoadOperation(context)
        
        # Test file analysis
        row_count, file_size_gb = load_op.file_analyzer.count_rows_fast(test_file)
        
        print(f"✓ count_rows_fast returned: ({row_count}, {file_size_gb})")
        print(f"✓ Type of row_count: {type(row_count)}")
        print(f"✓ Type of file_size_gb: {type(file_size_gb)}")
        
        # Test formatting
        file_size_mb = file_size_gb * 1024
        formatted_msg = f"File contains ~{row_count:,} rows ({file_size_mb:.1f} MB)"
        print(f"✓ Formatted message: {formatted_msg}")
        
        # Create a file config and test the actual load operation analysis
        file_config = FileConfig(
            file_path=test_file,
            file_pattern="test_{date_range}.tsv",
            table_name="TEST_TABLE",
            date_column="DATE",
            expected_columns=['col' + str(i) for i in range(41)]
        )
        
        # Test analyze_files method
        result = load_op.analyze_files(
            files=[file_config],
            base_path='/tmp',
            month='2024-01'
        )
        
        print(f"✓ analyze_files succeeded")
        print(f"✓ Result: {result}")
        
        print("\n✅ All tests passed! No tuple formatting errors.")
        return True
        
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        if os.path.exists(test_file):
            os.unlink(test_file)

if __name__ == "__main__":
    success = test_count_rows_formatting()
    sys.exit(0 if success else 1)