#!/usr/bin/env python3
"""
Test script for CSV support implementation - Phase 2
Tests file discovery, config generation, and format detection
"""

import sys
import json
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from snowflake_etl.operations.config.generate_config_operation import GenerateConfigOperation
from snowflake_etl.operations.utilities.tsv_sampler_operation import FileSamplerOperation
from snowflake_etl.utils.format_detector import FormatDetector


def test_config_generation_with_csv():
    """Test config generation detects CSV format correctly"""
    print("Testing config generation with CSV files...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create test CSV file
        csv_file = tmpdir / "sales_2024-01.csv"
        csv_file.write_text(
            "sale_date,product_id,amount\n"
            "2024-01-01,P001,100.50\n"
            "2024-01-02,P002,200.75\n"
        )
        
        # Create test TSV file
        tsv_file = tmpdir / "inventory_2024-01.tsv"
        tsv_file.write_text(
            "inventory_date\tproduct_id\tquantity\n"
            "2024-01-01\tP001\t50\n"
            "2024-01-02\tP002\t75\n"
        )
        
        # Generate config
        gen_op = GenerateConfigOperation()
        result = gen_op.execute(
            files=[str(csv_file), str(tsv_file)],
            output_file=None,
            dry_run=True
        )
        
        # Check results
        assert 'files' in result
        assert len(result['files']) == 2
        
        # Find CSV config
        csv_config = next((f for f in result['files'] 
                          if 'sales' in f['file_pattern'].lower()), None)
        assert csv_config is not None
        assert csv_config['file_format'] == 'CSV'
        assert csv_config['delimiter'] == ','
        print("✓ CSV file detected and configured correctly")
        
        # Find TSV config
        tsv_config = next((f for f in result['files'] 
                          if 'inventory' in f['file_pattern'].lower()), None)
        assert tsv_config is not None
        assert tsv_config['file_format'] == 'TSV'
        assert tsv_config['delimiter'] == '\t'
        print("✓ TSV file detected and configured correctly")
        
        return result


def test_file_sampler_csv():
    """Test file sampler handles CSV files"""
    print("\nTesting file sampler with CSV...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create CSV file
        csv_file = tmpdir / "test.csv"
        csv_file.write_text(
            "name,age,city\n"
            "Alice,30,NYC\n"
            "Bob,25,LA\n"
            "Charlie,35,Chicago\n"
        )
        
        # Sample the file
        sampler = FileSamplerOperation()
        result = sampler.execute(str(csv_file), rows=2)
        
        assert result['file_format'] == 'CSV'
        assert result['delimiter'] == ','
        assert result['has_header'] == True
        assert result['column_count'] == 3
        assert 'headers' in result
        assert result['headers'] == ['name', 'age', 'city']
        print("✓ CSV file sampled correctly")
        print(f"✓ Detected {result['column_count']} columns with headers")


def test_mixed_format_pattern_detection():
    """Test pattern detection with mixed CSV/TSV files"""
    print("\nTesting pattern detection with mixed formats...")
    
    gen_op = GenerateConfigOperation()
    
    # Test CSV file pattern
    csv_pattern = gen_op._detect_pattern("sales_20240101-20240131.csv")
    assert csv_pattern == "sales_{date_range}.csv"
    print("✓ CSV date range pattern detected")
    
    # Test TSV file pattern  
    tsv_pattern = gen_op._detect_pattern("inventory_2024-01.tsv")
    assert tsv_pattern == "inventory_{month}.tsv"
    print("✓ TSV month pattern detected")
    
    # Test compressed CSV
    gz_pattern = gen_op._detect_pattern("data_202401.csv.gz")
    assert gz_pattern == "data_{month}.csv.gz"
    print("✓ Compressed CSV pattern detected")


def test_pipe_delimited_file():
    """Test handling of pipe-delimited files"""
    print("\nTesting pipe-delimited files...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create pipe-delimited file
        pipe_file = tmpdir / "data.txt"
        pipe_file.write_text(
            "id|name|value\n"
            "1|Alice|100\n"
            "2|Bob|200\n"
        )
        
        # Detect format
        format_info = FormatDetector.detect_format(str(pipe_file))
        assert format_info['delimiter'] == '|'
        assert format_info['format'] == 'CSV'  # Non-tab delimiters are CSV
        print("✓ Pipe delimiter detected correctly")
        
        # Generate config
        gen_op = GenerateConfigOperation()
        result = gen_op.execute(
            files=[str(pipe_file)],
            output_file=None,
            dry_run=True
        )
        
        file_config = result['files'][0]
        assert file_config['delimiter'] == '|'
        assert file_config['file_format'] == 'CSV'
        print("✓ Pipe-delimited file configured correctly")


def test_compressed_file_handling():
    """Test handling of compressed files"""
    print("\nTesting compressed file handling...")
    
    import gzip
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create compressed CSV
        csv_content = b"product,price,quantity\nP001,10.50,100\nP002,20.75,50"
        gz_file = tmpdir / "products.csv.gz"
        
        with gzip.open(gz_file, 'wb') as f:
            f.write(csv_content)
        
        # Detect format
        format_info = FormatDetector.detect_format(str(gz_file))
        assert format_info['format'] == 'CSV'
        assert format_info['delimiter'] == ','
        print("✓ Compressed CSV format detected")
        
        # Sample the file
        sampler = FileSamplerOperation()
        result = sampler.execute(str(gz_file), rows=10)
        
        assert result['file_format'] == 'CSV'
        assert result['column_count'] == 3
        print("✓ Compressed file sampled correctly")


def test_table_name_extraction():
    """Test table name extraction from various file names"""
    print("\nTesting table name extraction...")
    
    gen_op = GenerateConfigOperation()
    
    test_cases = [
        ("sales_data_20240101-20240131.csv", "SALES_DATA"),
        ("factInventory_2024-01.tsv", "INVENTORY"),
        ("dimCustomer.csv.gz", "CUSTOMER"),
        ("test_products_202401.txt", "PRODUCTS"),
        ("orders_data.csv", "ORDERS_DATA"),
    ]
    
    for filename, expected in test_cases:
        result = gen_op._extract_table_name(filename)
        assert result == expected, f"Expected {expected} but got {result} for {filename}"
        print(f"✓ {filename} → {result}")


def test_column_detection():
    """Test column detection from files"""
    print("\nTesting column detection...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # CSV with headers
        csv_file = tmpdir / "with_headers.csv"
        csv_file.write_text("ID,Name,Amount\n1,Alice,100\n2,Bob,200")
        
        gen_op = GenerateConfigOperation()
        columns = gen_op._detect_columns_from_file(csv_file, ',')
        
        assert columns == ['ID', 'Name', 'Amount']
        print("✓ CSV headers detected correctly")
        
        # CSV without headers (numeric first row)
        no_header_file = tmpdir / "no_headers.csv"
        no_header_file.write_text("1,2,3\n4,5,6\n7,8,9")
        
        columns = gen_op._detect_columns_from_file(no_header_file, ',')
        assert columns == ['COLUMN_1', 'COLUMN_2', 'COLUMN_3']
        print("✓ Generated generic column names for headerless file")


def main():
    """Run all Phase 2 tests"""
    print("=" * 60)
    print("CSV Support Phase 2 Tests")
    print("=" * 60)
    
    try:
        # Run tests
        config_result = test_config_generation_with_csv()
        test_file_sampler_csv()
        test_mixed_format_pattern_detection()
        test_pipe_delimited_file()
        test_compressed_file_handling()
        test_table_name_extraction()
        test_column_detection()
        
        print("\n" + "=" * 60)
        print("✅ All Phase 2 tests passed!")
        print("=" * 60)
        
        # Display sample generated config
        print("\nSample Generated Configuration:")
        print("-" * 40)
        print(json.dumps(config_result['files'][0], indent=2))
        
        return 0
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())