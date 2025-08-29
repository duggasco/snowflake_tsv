#!/usr/bin/env python3
"""
Diagnose column mismatch issues between TSV files and Snowflake tables.
Helps identify why '1 - GC' is being parsed as numeric.
"""

import sys
import json
import argparse
from pathlib import Path

def analyze_tsv_sample(file_path, num_lines=5):
    """Analyze TSV file structure and identify potential issues."""
    print(f"\n=== Analyzing TSV File: {file_path} ===")
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = []
        for i, line in enumerate(f):
            if i >= num_lines:
                break
            lines.append(line.rstrip('\n'))
    
    if not lines:
        print("ERROR: File is empty")
        return
    
    # Analyze each line
    for line_num, line in enumerate(lines, 1):
        columns = line.split('\t')
        print(f"\nLine {line_num}: {len(columns)} columns")
        
        # Show first 10 columns with data type detection
        for col_idx, value in enumerate(columns[:10]):
            # Truncate long values
            display_val = value[:30] + '...' if len(value) > 30 else value
            
            # Detect likely data type
            data_type = detect_data_type(value)
            
            print(f"  Col {col_idx + 1}: [{data_type}] '{display_val}'")
            
            # Flag potential issues
            if '1 - GC' in value:
                print(f"    >>> FOUND PROBLEMATIC VALUE: This appears to be in column {col_idx + 1}")
    
    return columns

def detect_data_type(value):
    """Detect the likely data type of a value."""
    if not value or value.upper() in ('NULL', '\\N', 'NA'):
        return "NULL"
    
    # Try numeric detection
    try:
        float(value)
        if '.' in value:
            return "FLOAT"
        return "INTEGER"
    except ValueError:
        pass
    
    # Check for date patterns
    if len(value) == 8 and value.isdigit():
        return "DATE(YYYYMMDD)"
    if len(value) == 10 and value[4] == '-' and value[7] == '-':
        return "DATE(YYYY-MM-DD)"
    
    # Check for codes/categories
    if ' - ' in value:
        return "CODE/CATEGORY"
    
    return "STRING"

def check_config_alignment(config_path, tsv_path):
    """Check if config columns align with TSV data."""
    print(f"\n=== Checking Config Alignment ===")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Find matching file pattern
        tsv_name = Path(tsv_path).name
        matching_config = None
        
        for file_config in config.get('files', []):
            pattern = file_config.get('file_pattern', '')
            # Simple pattern matching (could be improved)
            if pattern.replace('{date_range}', '').replace('{month}', '') in tsv_name:
                matching_config = file_config
                break
        
        if not matching_config:
            print(f"WARNING: No matching config found for {tsv_name}")
            return
        
        expected_cols = matching_config.get('expected_columns', [])
        table_name = matching_config.get('table_name', 'UNKNOWN')
        
        print(f"Table: {table_name}")
        print(f"Expected columns: {len(expected_cols)}")
        
        # Analyze first line of TSV
        with open(tsv_path, 'r') as f:
            first_line = f.readline().rstrip('\n')
            actual_cols = first_line.split('\t')
        
        print(f"Actual columns in TSV: {len(actual_cols)}")
        
        if len(expected_cols) != len(actual_cols):
            print(f"ERROR: Column count mismatch! Expected {len(expected_cols)}, got {len(actual_cols)}")
            print("This will cause data to shift into wrong columns!")
        
        # Look for the problematic value
        for idx, value in enumerate(actual_cols):
            if '1 - GC' in value:
                print(f"\nProblematic value '1 - GC' found in TSV column {idx + 1}")
                if idx < len(expected_cols):
                    print(f"This maps to Snowflake column: {expected_cols[idx]}")
                    print("Snowflake likely expects this column to be NUMERIC")
                
    except Exception as e:
        print(f"Error reading config: {e}")

def suggest_fixes():
    """Suggest potential fixes for the issue."""
    print("\n=== SUGGESTED FIXES ===")
    print("""
1. COLUMN COUNT MISMATCH:
   - Verify TSV has same number of columns as Snowflake table
   - Check for extra/missing tab characters in the data
   - Use: head -1 your_file.tsv | tr '\\t' '\\n' | wc -l

2. COLUMN ORDER MISMATCH:
   - Regenerate config with correct column order from Snowflake:
     ./generate_config.sh -t TEST_CUSTOM_BENCHMARK_REPORTING -c config/your_config.json file.tsv

3. DATA TYPE MISMATCH:
   - The value '1 - GC' suggests a category/code field
   - Check if Snowflake table has correct data types
   - May need to modify table schema or transform data

4. IMMEDIATE WORKAROUND:
   - Identify which column contains '1 - GC'
   - Check Snowflake table definition for that column
   - If column should be VARCHAR, alter table:
     ALTER TABLE TEST_CUSTOM_BENCHMARK_REPORTING 
     ALTER COLUMN <column_name> SET DATA TYPE VARCHAR(100);
""")

def main():
    parser = argparse.ArgumentParser(description='Diagnose TSV column mismatch issues')
    parser.add_argument('tsv_file', help='Path to TSV file with issues')
    parser.add_argument('--config', help='Path to config.json file')
    parser.add_argument('--lines', type=int, default=5, help='Number of lines to analyze')
    
    args = parser.parse_args()
    
    if not Path(args.tsv_file).exists():
        print(f"ERROR: File not found: {args.tsv_file}")
        sys.exit(1)
    
    # Analyze TSV structure
    analyze_tsv_sample(args.tsv_file, args.lines)
    
    # Check config alignment if provided
    if args.config and Path(args.config).exists():
        check_config_alignment(args.config, args.tsv_file)
    
    # Suggest fixes
    suggest_fixes()

if __name__ == '__main__':
    main()