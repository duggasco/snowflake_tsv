#!/usr/bin/env python3
"""
Compare TSV files to identify differences between working and failing files
"""

import sys
import os
import csv
from collections import Counter
import chardet

def compare_files(good_file, bad_file):
    """Compare a working file with a problematic file"""
    
    print("="*60)
    print("TSV FILE COMPARISON")
    print("="*60)
    print(f"Good file: {os.path.basename(good_file)}")
    print(f"Bad file:  {os.path.basename(bad_file)}")
    print()
    
    results = {}
    
    # 1. File sizes
    print("1. FILE SIZES:")
    good_size = os.path.getsize(good_file)
    bad_size = os.path.getsize(bad_file)
    print(f"   Good: {good_size/1024/1024:.1f} MB")
    print(f"   Bad:  {bad_size/1024/1024:.1f} MB")
    if abs(good_size - bad_size) / good_size > 0.1:
        print("   ⚠️  Significant size difference (>10%)")
    print()
    
    # 2. Encoding
    print("2. ENCODING:")
    with open(good_file, 'rb') as f:
        good_encoding = chardet.detect(f.read(100000))
    with open(bad_file, 'rb') as f:
        bad_encoding = chardet.detect(f.read(100000))
    
    print(f"   Good: {good_encoding['encoding']} (confidence: {good_encoding['confidence']:.2%})")
    print(f"   Bad:  {bad_encoding['encoding']} (confidence: {bad_encoding['confidence']:.2%})")
    if good_encoding['encoding'] != bad_encoding['encoding']:
        print(f"   ⚠️  Different encodings detected!")
    print()
    
    # 3. Line endings
    print("3. LINE ENDINGS:")
    with open(good_file, 'rb') as f:
        good_sample = f.read(10000)
    with open(bad_file, 'rb') as f:
        bad_sample = f.read(10000)
    
    good_crlf = b'\r\n' in good_sample
    good_lf = b'\n' in good_sample and not good_crlf
    bad_crlf = b'\r\n' in bad_sample
    bad_lf = b'\n' in bad_sample and not bad_crlf
    
    print(f"   Good: {'CRLF (Windows)' if good_crlf else 'LF (Unix)' if good_lf else 'Unknown'}")
    print(f"   Bad:  {'CRLF (Windows)' if bad_crlf else 'LF (Unix)' if bad_lf else 'Unknown'}")
    if (good_crlf != bad_crlf) or (good_lf != bad_lf):
        print("   ⚠️  Different line endings!")
    print()
    
    # 4. Column counts
    print("4. COLUMN ANALYSIS:")
    good_cols = []
    bad_cols = []
    
    with open(good_file, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            if i >= 100:
                break
            good_cols.append(line.count('\t') + 1)
    
    with open(bad_file, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            if i >= 100:
                break
            bad_cols.append(line.count('\t') + 1)
    
    print(f"   Good: {min(good_cols)} to {max(good_cols)} columns")
    print(f"   Bad:  {min(bad_cols)} to {max(bad_cols)} columns")
    
    if min(good_cols) != max(good_cols):
        print(f"   ⚠️  Good file has inconsistent columns!")
    if min(bad_cols) != max(bad_cols):
        print(f"   ⚠️  Bad file has inconsistent columns!")
    if max(good_cols) != max(bad_cols):
        print(f"   ⚠️  Different column counts between files!")
    print()
    
    # 5. Special characters
    print("5. SPECIAL CHARACTERS:")
    
    def check_special(file_path, label):
        nulls = 0
        controls = 0
        with open(file_path, 'rb') as f:
            for i, line in enumerate(f):
                if i >= 1000:
                    break
                if b'\x00' in line:
                    nulls += 1
                if any(b < 32 and b not in [9, 10, 13] for b in line):
                    controls += 1
        
        print(f"   {label}:")
        if nulls > 0:
            print(f"      ⚠️  NULL bytes in {nulls} lines")
        if controls > 0:
            print(f"      ⚠️  Control chars in {controls} lines")
        if nulls == 0 and controls == 0:
            print(f"      ✓ Clean")
        return nulls, controls
    
    good_nulls, good_controls = check_special(good_file, "Good")
    bad_nulls, bad_controls = check_special(bad_file, "Bad")
    print()
    
    # 6. Row count
    print("6. ROW COUNT:")
    print("   Counting rows...")
    
    def count_rows(file_path):
        count = 0
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for _ in f:
                count += 1
        return count
    
    good_rows = count_rows(good_file)
    bad_rows = count_rows(bad_file)
    
    print(f"   Good: {good_rows:,} rows")
    print(f"   Bad:  {bad_rows:,} rows")
    print()
    
    # 7. First line comparison
    print("7. FIRST LINE COMPARISON:")
    with open(good_file, 'r', encoding='utf-8', errors='ignore') as f:
        good_first = f.readline().strip()
    with open(bad_file, 'r', encoding='utf-8', errors='ignore') as f:
        bad_first = f.readline().strip()
    
    good_first_cols = good_first.count('\t') + 1
    bad_first_cols = bad_first.count('\t') + 1
    
    print(f"   Good first line: {good_first_cols} columns, {len(good_first)} chars")
    print(f"   Bad first line:  {bad_first_cols} columns, {len(bad_first)} chars")
    
    if good_first_cols != bad_first_cols:
        print(f"   ⚠️  Different column count in first line!")
        print(f"   Difference: {abs(good_first_cols - bad_first_cols)} columns")
    print()
    
    # 8. Date format check (sample column 1 and 2)
    print("8. DATE FORMAT CHECK (first 2 columns):")
    
    def check_date_formats(file_path, label):
        formats = {0: Counter(), 1: Counter()}
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter='\t')
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                if i == 0:
                    continue  # Skip header if exists
                
                for col in [0, 1]:
                    if col < len(row):
                        val = row[col].strip()
                        if len(val) == 8 and val.isdigit():
                            formats[col]['YYYYMMDD'] += 1
                        elif len(val) == 10 and '-' in val:
                            formats[col]['YYYY-MM-DD'] += 1
                        elif '/' in val:
                            formats[col]['MM/DD/YYYY'] += 1
                        else:
                            formats[col]['Other'] += 1
        
        print(f"   {label}:")
        for col in [0, 1]:
            if formats[col]:
                most_common = formats[col].most_common(1)[0]
                print(f"      Column {col}: {most_common[0]}")
    
    check_date_formats(good_file, "Good")
    check_date_formats(bad_file, "Bad")
    print()
    
    # Summary
    print("="*60)
    print("SUMMARY OF DIFFERENCES")
    print("="*60)
    
    differences = []
    
    if good_encoding['encoding'] != bad_encoding['encoding']:
        differences.append("Different character encodings")
    
    if (good_crlf != bad_crlf) or (good_lf != bad_lf):
        differences.append("Different line endings")
    
    if max(good_cols) != max(bad_cols):
        differences.append(f"Different column counts ({max(good_cols)} vs {max(bad_cols)})")
    
    if bad_nulls > good_nulls:
        differences.append("Bad file has NULL bytes")
    
    if bad_controls > good_controls:
        differences.append("Bad file has more control characters")
    
    if differences:
        print("Key differences found:")
        for i, diff in enumerate(differences, 1):
            print(f"   {i}. {diff}")
    else:
        print("No significant structural differences found.")
        print("The issue may be in the data content rather than structure.")
    
    print("\nRECOMMENDATIONS:")
    if bad_nulls > 0:
        print("• Remove NULL bytes: tr -d '\\000' < bad_file.tsv > cleaned.tsv")
    if good_encoding['encoding'] != bad_encoding['encoding']:
        print(f"• Convert encoding: iconv -f {bad_encoding['encoding']} -t {good_encoding['encoding']} bad_file.tsv > converted.tsv")
    if max(bad_cols) != max(good_cols):
        print("• Check for extra/missing columns or delimiter issues")
    
    return differences

def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_tsv_files.py <good_file.tsv> <bad_file.tsv>")
        print("\nCompares a working TSV file with a problematic one to identify differences.")
        sys.exit(1)
    
    good_file = sys.argv[1]
    bad_file = sys.argv[2]
    
    if not os.path.exists(good_file):
        print(f"Error: Good file not found: {good_file}")
        sys.exit(1)
    
    if not os.path.exists(bad_file):
        print(f"Error: Bad file not found: {bad_file}")
        sys.exit(1)
    
    compare_files(good_file, bad_file)

if __name__ == "__main__":
    main()