#!/usr/bin/env python3
"""
TSV File Validator - Comprehensive checks to identify file issues
Compares problematic files against successful ones
"""

import sys
import os
import csv
import gzip
import json
import chardet
import re
from collections import Counter
from datetime import datetime
import snowflake.connector

def load_config(config_path):
    """Load configuration from JSON file"""
    with open(config_path, 'r') as f:
        return json.load(f)

def detect_encoding(file_path, sample_size=1000000):
    """Detect file encoding"""
    print("\n1. CHECKING FILE ENCODING...")
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            print(f"   Detected encoding: {encoding} (confidence: {confidence:.2%})")
            
            # Check for BOM
            if raw_data.startswith(b'\xef\xbb\xbf'):
                print("   ⚠️  WARNING: UTF-8 BOM detected - may cause issues")
            elif raw_data.startswith(b'\xff\xfe'):
                print("   ⚠️  WARNING: UTF-16 LE BOM detected")
            elif raw_data.startswith(b'\xfe\xff'):
                print("   ⚠️  WARNING: UTF-16 BE BOM detected")
                
            return encoding
    except Exception as e:
        print(f"   Error detecting encoding: {e}")
        return 'utf-8'

def analyze_delimiters(file_path, num_lines=100):
    """Analyze delimiter consistency"""
    print("\n2. ANALYZING DELIMITERS...")
    
    delimiter_counts = []
    line_lengths = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                tab_count = line.count('\t')
                delimiter_counts.append(tab_count)
                line_lengths.append(len(line))
                
                # Check for other potential delimiters
                if i == 0:
                    print(f"   First line has {tab_count} tabs")
                    if '|' in line:
                        print(f"   ⚠️  Found pipe delimiters: {line.count('|')} pipes")
                    if ',' in line and tab_count == 0:
                        print(f"   ⚠️  Found commas but no tabs: {line.count(',')} commas")
        
        if delimiter_counts:
            min_tabs = min(delimiter_counts)
            max_tabs = max(delimiter_counts)
            avg_tabs = sum(delimiter_counts) / len(delimiter_counts)
            
            print(f"   Tab count range: {min_tabs} to {max_tabs} (avg: {avg_tabs:.1f})")
            print(f"   Expected columns: {min_tabs + 1} to {max_tabs + 1}")
            
            if min_tabs != max_tabs:
                print(f"   ⚠️  WARNING: Inconsistent column count!")
                # Show distribution
                tab_distribution = Counter(delimiter_counts)
                print("   Tab count distribution:")
                for count, freq in sorted(tab_distribution.items())[:5]:
                    print(f"      {count} tabs: {freq} lines")
                    
            return min_tabs + 1, max_tabs + 1
    except Exception as e:
        print(f"   Error analyzing delimiters: {e}")
        return 0, 0

def check_special_characters(file_path, num_lines=1000):
    """Check for problematic special characters"""
    print("\n3. CHECKING FOR SPECIAL CHARACTERS...")
    
    issues_found = []
    null_bytes = 0
    control_chars = 0
    high_ascii = 0
    invalid_utf8 = 0
    
    try:
        with open(file_path, 'rb') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                    
                # Check for null bytes
                if b'\x00' in line:
                    null_bytes += 1
                    if null_bytes == 1:
                        issues_found.append(f"Line {i+1}: Contains NULL bytes")
                
                # Check for control characters
                for byte in line:
                    if byte < 32 and byte not in [9, 10, 13]:  # Allow tab, newline, carriage return
                        control_chars += 1
                        break
                
                # Check for high ASCII
                if any(byte > 127 for byte in line):
                    high_ascii += 1
                    
                # Try to decode as UTF-8
                try:
                    line.decode('utf-8')
                except UnicodeDecodeError:
                    invalid_utf8 += 1
                    if invalid_utf8 == 1:
                        issues_found.append(f"Line {i+1}: Invalid UTF-8 encoding")
        
        if null_bytes:
            print(f"   ⚠️  Found NULL bytes in {null_bytes} lines")
        if control_chars:
            print(f"   ⚠️  Found control characters in {control_chars} lines")
        if high_ascii:
            print(f"   Found high ASCII in {high_ascii} lines (may be fine)")
        if invalid_utf8:
            print(f"   ⚠️  Found invalid UTF-8 in {invalid_utf8} lines")
            
        if not any([null_bytes, control_chars, invalid_utf8]):
            print("   ✓ No problematic special characters found")
            
    except Exception as e:
        print(f"   Error checking special characters: {e}")

def analyze_quotes(file_path, num_lines=100):
    """Check for quote issues"""
    print("\n4. ANALYZING QUOTE CHARACTERS...")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            unmatched_quotes = 0
            escaped_quotes = 0
            
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                    
                # Count quotes
                single_quotes = line.count("'")
                double_quotes = line.count('"')
                
                if double_quotes % 2 != 0:
                    unmatched_quotes += 1
                    if unmatched_quotes == 1:
                        print(f"   Line {i+1}: Unmatched quotes")
                        print(f"      Preview: {line[:100]}...")
                
                # Check for escaped quotes
                if '\\"' in line or "\\'" in line:
                    escaped_quotes += 1
        
        if unmatched_quotes:
            print(f"   ⚠️  Found {unmatched_quotes} lines with unmatched quotes")
        if escaped_quotes:
            print(f"   Found {escaped_quotes} lines with escaped quotes")
        if not unmatched_quotes and not escaped_quotes:
            print("   ✓ No quote issues found")
            
    except Exception as e:
        print(f"   Error analyzing quotes: {e}")

def validate_date_columns(file_path, num_lines=100):
    """Validate date formats in the file"""
    print("\n5. VALIDATING DATE FORMATS...")
    
    date_patterns = {
        'YYYY-MM-DD': re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        'YYYYMMDD': re.compile(r'^\d{8}$'),
        'MM/DD/YYYY': re.compile(r'^\d{2}/\d{2}/\d{4}$'),
        'DD-MON-YYYY': re.compile(r'^\d{2}-[A-Za-z]{3}-\d{4}$'),
        'Mon DD YYYY': re.compile(r'^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$')
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter='\t')
            
            # Sample first few rows to find date columns
            rows = []
            for i, row in enumerate(reader):
                if i >= num_lines:
                    break
                rows.append(row)
            
            if not rows:
                print("   No data to analyze")
                return
            
            # Check each column for date patterns
            for col_idx in range(len(rows[0])):
                col_values = [row[col_idx] if col_idx < len(row) else '' for row in rows[1:]]  # Skip header
                
                # Check if this might be a date column
                date_matches = {}
                for val in col_values[:10]:  # Check first 10 values
                    for pattern_name, pattern in date_patterns.items():
                        if pattern.match(val.strip()):
                            date_matches[pattern_name] = date_matches.get(pattern_name, 0) + 1
                
                if date_matches:
                    print(f"   Column {col_idx}: Possible date column")
                    for pattern_name, count in date_matches.items():
                        print(f"      {pattern_name}: {count} matches")
                    
                    # Check for inconsistent formats
                    if len(date_matches) > 1:
                        print(f"      ⚠️  WARNING: Mixed date formats in column {col_idx}")
                        
    except Exception as e:
        print(f"   Error validating dates: {e}")

def check_line_endings(file_path, sample_size=10000):
    """Check line ending consistency"""
    print("\n6. CHECKING LINE ENDINGS...")
    
    try:
        with open(file_path, 'rb') as f:
            sample = f.read(sample_size)
            
        crlf = sample.count(b'\r\n')
        lf = sample.count(b'\n') - crlf  # Subtract CRLF occurrences
        cr = sample.count(b'\r') - crlf  # Subtract CRLF occurrences
        
        print(f"   Line endings found:")
        if crlf > 0:
            print(f"      Windows (CRLF): {crlf}")
        if lf > 0:
            print(f"      Unix (LF): {lf}")
        if cr > 0:
            print(f"      Mac (CR): {cr}")
            
        if crlf > 0 and lf > 0:
            print("   ⚠️  WARNING: Mixed line endings detected!")
        elif crlf > 0:
            print("   Using Windows line endings (CRLF)")
        elif lf > 0:
            print("   Using Unix line endings (LF)")
            
    except Exception as e:
        print(f"   Error checking line endings: {e}")

def compare_with_table(config_path, table_name, file_path):
    """Compare file structure with Snowflake table"""
    print("\n7. COMPARING WITH SNOWFLAKE TABLE...")
    
    try:
        config = load_config(config_path)
        conn = snowflake.connector.connect(**config['snowflake'])
        cursor = conn.cursor()
        
        # Get table columns
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, 
               NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(query)
        table_columns = cursor.fetchall()
        
        print(f"   Table has {len(table_columns)} columns")
        
        # Get file column count
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline()
            file_columns = first_line.count('\t') + 1
            
        print(f"   File has {file_columns} columns")
        
        if len(table_columns) != file_columns:
            print(f"   ⚠️  WARNING: Column count mismatch!")
            print(f"      Table: {len(table_columns)}, File: {file_columns}")
            
            if file_columns > len(table_columns):
                print(f"      File has {file_columns - len(table_columns)} extra columns")
            else:
                print(f"      File is missing {len(table_columns) - file_columns} columns")
        else:
            print("   ✓ Column count matches")
            
        # Show first few columns for reference
        print("\n   First 5 table columns:")
        for col in table_columns[:5]:
            print(f"      {col[0]}: {col[1]}")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   Error comparing with table: {e}")

def check_file_size_and_rows(file_path):
    """Check file size and estimate row count"""
    print("\n8. FILE SIZE AND ROW COUNT...")
    
    try:
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        file_size_gb = file_size / (1024 * 1024 * 1024)
        
        print(f"   File size: {file_size_gb:.2f} GB ({file_size_mb:.1f} MB)")
        
        # Count actual rows
        print("   Counting rows (this may take a moment)...")
        row_count = 0
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for _ in f:
                row_count += 1
                if row_count % 1000000 == 0:
                    print(f"      Counted {row_count:,} rows so far...")
        
        print(f"   Total rows: {row_count:,}")
        
        if row_count > 0:
            avg_row_size = file_size / row_count
            print(f"   Average row size: {avg_row_size:.0f} bytes")
            
    except Exception as e:
        print(f"   Error checking file size: {e}")

def find_problematic_rows(file_path, expected_columns, num_samples=10):
    """Find specific problematic rows"""
    print(f"\n9. LOOKING FOR PROBLEMATIC ROWS (expecting {expected_columns} columns)...")
    
    problems = {
        'too_few': [],
        'too_many': [],
        'very_long': [],
        'special_chars': []
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                col_count = line.count('\t') + 1
                
                # Check column count
                if col_count < expected_columns and len(problems['too_few']) < num_samples:
                    problems['too_few'].append((line_num, col_count))
                elif col_count > expected_columns and len(problems['too_many']) < num_samples:
                    problems['too_many'].append((line_num, col_count))
                
                # Check for very long lines
                if len(line) > 10000 and len(problems['very_long']) < num_samples:
                    problems['very_long'].append((line_num, len(line)))
                
                # Check for null bytes or other special characters
                if '\x00' in line and len(problems['special_chars']) < num_samples:
                    problems['special_chars'].append((line_num, 'contains NULL byte'))
        
        # Report findings
        if problems['too_few']:
            print(f"   ⚠️  Found {len(problems['too_few'])} rows with too few columns:")
            for line_num, col_count in problems['too_few'][:3]:
                print(f"      Line {line_num}: {col_count} columns")
        
        if problems['too_many']:
            print(f"   ⚠️  Found {len(problems['too_many'])} rows with too many columns:")
            for line_num, col_count in problems['too_many'][:3]:
                print(f"      Line {line_num}: {col_count} columns")
        
        if problems['very_long']:
            print(f"   ⚠️  Found {len(problems['very_long'])} very long rows:")
            for line_num, length in problems['very_long'][:3]:
                print(f"      Line {line_num}: {length:,} characters")
        
        if problems['special_chars']:
            print(f"   ⚠️  Found {len(problems['special_chars'])} rows with special characters:")
            for line_num, issue in problems['special_chars'][:3]:
                print(f"      Line {line_num}: {issue}")
        
        if not any(problems.values()):
            print("   ✓ No obvious problematic rows found")
            
    except Exception as e:
        print(f"   Error finding problematic rows: {e}")

def generate_test_file(file_path, output_path, num_rows=1000):
    """Generate a test file with first N rows"""
    print(f"\n10. GENERATING TEST FILE ({num_rows} rows)...")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
            with open(output_path, 'w', encoding='utf-8') as outfile:
                for i, line in enumerate(infile):
                    if i >= num_rows:
                        break
                    outfile.write(line)
        
        test_size = os.path.getsize(output_path) / 1024
        print(f"   Created test file: {output_path}")
        print(f"   Test file size: {test_size:.1f} KB")
        print(f"\n   You can test load this smaller file with:")
        print(f"   ./run_loader.sh --direct-file {output_path} --skip-qc")
        
    except Exception as e:
        print(f"   Error generating test file: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_tsv_file.py <tsv_file> [config.json] [table_name]")
        print("\nThis tool performs comprehensive validation of TSV files to identify issues")
        print("that might prevent successful loading into Snowflake.")
        sys.exit(1)
    
    file_path = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else None
    table_name = sys.argv[3] if len(sys.argv) > 3 else 'TEST_CUSTOM_FACTMARKITEXBLKBENCHMARK'
    
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    print("="*60)
    print("TSV FILE VALIDATION REPORT")
    print("="*60)
    print(f"File: {file_path}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run all checks
    encoding = detect_encoding(file_path)
    min_cols, max_cols = analyze_delimiters(file_path)
    check_special_characters(file_path)
    analyze_quotes(file_path)
    validate_date_columns(file_path)
    check_line_endings(file_path)
    
    if config_path and os.path.exists(config_path):
        compare_with_table(config_path, table_name, file_path)
    
    check_file_size_and_rows(file_path)
    
    # Use the most common column count
    expected_cols = min_cols if min_cols == max_cols else 59  # Use 59 as default based on your table
    find_problematic_rows(file_path, expected_cols)
    
    # Generate test file
    test_file = file_path.replace('.tsv', '_test_1000.tsv')
    generate_test_file(file_path, test_file)
    
    print("\n" + "="*60)
    print("VALIDATION COMPLETE")
    print("="*60)
    print("\nNEXT STEPS:")
    print("1. Review any warnings (⚠️) above")
    print("2. Test with the generated 1000-row file first")
    print("3. If test succeeds, try loading the full file with robust settings")
    print("4. If test fails, the issue is in the first 1000 rows - investigate further")

if __name__ == "__main__":
    main()