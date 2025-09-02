#!/usr/bin/env python3
"""Diagnose the tuple formatting error"""

import sys
import os

# Add project to path
sys.path.insert(0, '/root/snowflake')

def test_imports_and_methods():
    """Test that all the methods work correctly"""
    
    print("Testing imports and methods...")
    
    # Test 1: Import and check FileAnalyzer
    from snowflake_etl.core.file_analyzer import FileAnalyzer
    analyzer = FileAnalyzer()
    print("✓ FileAnalyzer imported")
    
    # Test 2: Check count_rows_fast signature
    import inspect
    sig = inspect.signature(analyzer.count_rows_fast)
    print(f"✓ count_rows_fast signature: {sig}")
    
    # Test 3: Check LoadOperation
    from snowflake_etl.operations.load_operation import LoadOperation
    print("✓ LoadOperation imported")
    
    # Test 4: Check the specific lines that might cause issues
    import ast
    load_op_file = '/root/snowflake/snowflake_etl/operations/load_operation.py'
    
    with open(load_op_file, 'r') as f:
        content = f.read()
    
    # Find all lines with count_rows_fast
    lines_with_count_rows = []
    for i, line in enumerate(content.split('\n'), 1):
        if 'count_rows_fast' in line:
            lines_with_count_rows.append((i, line.strip()))
    
    print("\nLines with count_rows_fast:")
    for line_no, line in lines_with_count_rows:
        print(f"  Line {line_no}: {line}")
    
    # Find all lines with formatting that could fail
    lines_with_format = []
    for i, line in enumerate(content.split('\n'), 1):
        if 'row_count' in line and ':,' in line:
            lines_with_format.append((i, line.strip()))
    
    print("\nLines with row_count formatting:")
    for line_no, line in lines_with_format:
        print(f"  Line {line_no}: {line}")
    
    # Test 5: Actually run the problematic code
    print("\n\nTesting actual execution:")
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
        f.write("col1\tcol2\tcol3\n")
        f.write("val1\tval2\tval3\n")
        test_file = f.name
    
    try:
        # Run count_rows_fast
        result = analyzer.count_rows_fast(test_file)
        print(f"count_rows_fast result: {result}")
        print(f"Result type: {type(result)}")
        
        # Test unpacking
        row_count, file_size_gb = result
        print(f"Unpacked - row_count: {row_count} (type: {type(row_count)})")
        print(f"Unpacked - file_size_gb: {file_size_gb} (type: {type(file_size_gb)})")
        
        # Test formatting
        file_size_mb = file_size_gb * 1024
        formatted = f"File contains ~{row_count:,} rows ({file_size_mb:.1f} MB)"
        print(f"✓ Formatted successfully: {formatted}")
        
    finally:
        os.unlink(test_file)
    
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_imports_and_methods()