#!/usr/bin/env python3
"""
Compare TSV files to identify differences between working and failing files
Optimized for large files (12GB+) with fast counting methods
"""

import sys
import os
import csv
import time
import subprocess
from collections import Counter
import chardet

def fast_line_count(file_path):
    """Use wc -l for fast line counting on Unix systems"""
    try:
        result = subprocess.run(['wc', '-l', file_path], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            count = int(result.stdout.split()[0])
            return count
    except (subprocess.TimeoutExpired, Exception):
        pass
    return None

def buffered_line_count(file_path, show_progress=True, buffer_size=8*1024*1024):
    """Count lines with large buffer for better performance"""
    count = 0
    last_update = time.time()
    mb_read = 0
    file_size = os.path.getsize(file_path) / (1024*1024)  # Size in MB
    
    with open(file_path, 'rb') as f:
        while True:
            buffer = f.read(buffer_size)
            if not buffer:
                break
            count += buffer.count(b'\n')
            mb_read += len(buffer) / (1024*1024)
            
            # Show progress every 2 seconds for large files
            if show_progress and file_size > 100:  # Only show progress for files > 100MB
                current_time = time.time()
                if current_time - last_update > 2:
                    progress = (mb_read / file_size) * 100
                    print(f"      Progress: {progress:.1f}% ({mb_read:.0f}/{file_size:.0f} MB)", end='\r')
                    last_update = current_time
    
    if show_progress and file_size > 100:
        print()  # Clear progress line
    return count

def sample_line_count(file_path, sample_size_mb=100):
    """Estimate row count by sampling first N MB of file"""
    file_size = os.path.getsize(file_path)
    sample_bytes = min(sample_size_mb * 1024 * 1024, file_size)
    
    with open(file_path, 'rb') as f:
        sample = f.read(sample_bytes)
        lines_in_sample = sample.count(b'\n')
    
    # Extrapolate to full file
    if sample_bytes < file_size:
        estimated_lines = int(lines_in_sample * (file_size / sample_bytes))
        return estimated_lines, True  # True indicates this is an estimate
    else:
        return lines_in_sample, False

def compare_files(good_file, bad_file, quick_mode=False):
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
    
    if quick_mode:
        print("   Using sampling method (--quick mode)...")
        good_rows, good_estimated = sample_line_count(good_file, sample_size_mb=100)
        bad_rows, bad_estimated = sample_line_count(bad_file, sample_size_mb=100)
        
        good_suffix = " (estimated)" if good_estimated else ""
        bad_suffix = " (estimated)" if bad_estimated else ""
        print(f"   Good: ~{good_rows:,} rows{good_suffix}")
        print(f"   Bad:  ~{bad_rows:,} rows{bad_suffix}")
    else:
        # For files > 1GB, inform user
        if good_size > 1024*1024*1024 or bad_size > 1024*1024*1024:
            print("   Large files detected. Using optimized counting...")
            print("   Tip: Use --quick flag for instant sampling-based count")
        
        # Try fast wc -l first
        print("   Counting good file...")
        start = time.time()
        good_rows = fast_line_count(good_file)
        if good_rows is None:
            # Fallback to buffered counting
            good_rows = buffered_line_count(good_file, show_progress=True)
        good_time = time.time() - start
        print(f"   Good: {good_rows:,} rows (took {good_time:.1f}s)")
        
        print("   Counting bad file...")
        start = time.time()
        bad_rows = fast_line_count(bad_file)
        if bad_rows is None:
            # Fallback to buffered counting
            bad_rows = buffered_line_count(bad_file, show_progress=True)
        bad_time = time.time() - start
        print(f"   Bad:  {bad_rows:,} rows (took {bad_time:.1f}s)")
    
    if isinstance(good_rows, int) and isinstance(bad_rows, int):
        if abs(good_rows - bad_rows) / good_rows > 0.1:
            print(f"   ⚠️  Significant row count difference (>10%)")
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
    # Check for --quick flag
    quick_mode = '--quick' in sys.argv
    
    # Remove --quick from args if present
    args = [arg for arg in sys.argv if arg != '--quick']
    
    if len(args) < 3:
        print("Usage: python compare_tsv_files.py [--quick] <good_file.tsv> <bad_file.tsv>")
        print("\nCompares a working TSV file with a problematic one to identify differences.")
        print("Optimized for large files (12GB+) with fast counting methods.")
        print("\nOptions:")
        print("  --quick    Use sampling for row count (instant results for large files)")
        sys.exit(1)
    
    good_file = args[1]
    bad_file = args[2]
    
    if not os.path.exists(good_file):
        print(f"Error: Good file not found: {good_file}")
        sys.exit(1)
    
    if not os.path.exists(bad_file):
        print(f"Error: Bad file not found: {bad_file}")
        sys.exit(1)
    
    compare_files(good_file, bad_file, quick_mode)

if __name__ == "__main__":
    main()