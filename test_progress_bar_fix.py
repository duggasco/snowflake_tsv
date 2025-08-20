#!/usr/bin/env python3
"""
Test script to validate the progress bar fix for parallel processing.
This simulates processing multiple files in parallel to ensure no static bars accumulate.
"""

import os
import sys
import time
import tempfile
import multiprocessing
from pathlib import Path

# Add current directory to path to import tsv_loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Mock the snowflake connector import
sys.modules['snowflake.connector'] = type(sys)('snowflake.connector')

from tsv_loader import ProgressTracker

def process_files_for_month(args):
    """Simulate processing multiple files for a single month"""
    month, job_position = args
    
    # Set environment variable for job position
    os.environ['TSV_JOB_POSITION'] = str(job_position)
    
    # Create progress tracker
    total_files = 3  # Simulate 3 files per month
    total_rows = 1000000
    total_size_gb = 3.0
    
    tracker = ProgressTracker(
        total_files=total_files,
        total_rows=total_rows,
        total_size_gb=total_size_gb,
        month=month,
        show_qc_progress=False  # Skip QC for this test
    )
    
    try:
        # Simulate processing 3 files
        for file_num in range(1, 4):
            filename = f"file_{month}_{file_num}.tsv"
            file_size_mb = 1024.0  # 1GB per file
            
            # Start compression
            tracker.start_file_compression(filename, file_size_mb)
            
            # Simulate compression progress
            for i in range(10):
                time.sleep(0.1)  # Simulate work
                tracker.update(compressed_mb=file_size_mb/10)
            
            # Start upload
            compressed_size_mb = file_size_mb * 0.3  # Compressed to 30%
            tracker.start_file_upload(filename + ".gz", compressed_size_mb)
            
            # Simulate upload progress
            for i in range(10):
                time.sleep(0.1)  # Simulate work
                tracker.update(uploaded_mb=compressed_size_mb/10)
            
            # Start COPY operation
            row_count = 300000
            tracker.start_copy_operation(f"TABLE_{month}", row_count)
            
            # Simulate COPY progress
            for i in range(10):
                time.sleep(0.1)  # Simulate work
                tracker.update(copied_rows=row_count/10)
            
            # Update file counter
            tracker.update(files=1)
            
            # Clear file-specific bars between files (except for the last file)
            if file_num < 3:
                tracker.clear_file_bars()
                time.sleep(0.5)  # Brief pause between files
        
        # Final pause to see completed state
        time.sleep(1)
        
    finally:
        tracker.close()
    
    return f"Completed {month}"

def test_parallel_progress_bars():
    """Test parallel processing with multiple progress bars"""
    print("\n" + "="*60)
    print("TESTING PARALLEL PROGRESS BAR FIX")
    print("="*60)
    print("\nThis test simulates processing 3 files each for 3 months in parallel.")
    print("Watch for any static/dead progress bars accumulating...\n")
    
    # Test data
    months = ["2022-09", "2022-10", "2022-11"]
    
    # Create process pool
    with multiprocessing.Pool(processes=3) as pool:
        # Process months in parallel
        args = [(month, idx) for idx, month in enumerate(months)]
        results = pool.map(process_files_for_month, args)
    
    print("\n" + "="*60)
    print("TEST COMPLETED")
    print("="*60)
    for result in results:
        print(f"  {result}")
    print("\nIf you saw clean progress bars without accumulation, the fix is working!")
    print("Each month should have shown compression, upload, and COPY bars")
    print("that updated cleanly without leaving static bars behind.\n")

def test_sequential_files():
    """Test sequential file processing (single process)"""
    print("\n" + "="*60)
    print("TESTING SEQUENTIAL FILE PROCESSING")
    print("="*60)
    print("\nThis test processes 5 files sequentially in a single process.")
    print("Progress bars should reuse and update cleanly.\n")
    
    # Create progress tracker
    tracker = ProgressTracker(
        total_files=5,
        total_rows=5000000,
        total_size_gb=5.0,
        month="2022-12",
        show_qc_progress=False
    )
    
    try:
        for file_num in range(1, 6):
            filename = f"test_file_{file_num}.tsv"
            file_size_mb = 1024.0
            
            print(f"\nProcessing {filename}...")
            
            # Compression
            tracker.start_file_compression(filename, file_size_mb)
            for i in range(20):
                time.sleep(0.05)
                tracker.update(compressed_mb=file_size_mb/20)
            
            # Upload
            compressed_size_mb = file_size_mb * 0.3
            tracker.start_file_upload(filename + ".gz", compressed_size_mb)
            for i in range(20):
                time.sleep(0.05)
                tracker.update(uploaded_mb=compressed_size_mb/20)
            
            # COPY
            row_count = 1000000
            tracker.start_copy_operation("TEST_TABLE", row_count)
            for i in range(20):
                time.sleep(0.05)
                tracker.update(copied_rows=row_count/20)
            
            tracker.update(files=1)
            
            # Clear bars between files (except last)
            if file_num < 5:
                tracker.clear_file_bars()
                time.sleep(0.3)
    
    finally:
        tracker.close()
    
    print("\n" + "="*60)
    print("SEQUENTIAL TEST COMPLETED")
    print("="*60)
    print("Progress bars should have updated cleanly without accumulation.\n")

if __name__ == "__main__":
    # Check if tqdm is available
    try:
        import tqdm
        print("✓ tqdm is installed")
    except ImportError:
        print("✗ tqdm is not installed. Please run: pip install tqdm")
        sys.exit(1)
    
    # Run tests
    print("\nStarting progress bar tests...")
    print("Press Ctrl+C to stop at any time.\n")
    
    try:
        # Test 1: Sequential processing
        test_sequential_files()
        
        print("\nContinuing to parallel test in 2 seconds...")
        time.sleep(2)
        
        # Test 2: Parallel processing
        test_parallel_progress_bars()
        
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
    except Exception as e:
        print(f"\nError during test: {e}")
        import traceback
        traceback.print_exc()