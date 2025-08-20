#!/usr/bin/env python3
"""
Simple test to verify progress bar reuse works correctly.
Tests that bars are reused instead of creating new ones.
"""

import os
import sys
import time

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Mock snowflake connector
sys.modules['snowflake.connector'] = type(sys)('snowflake.connector')

from tsv_loader import ProgressTracker

def test_bar_reuse():
    """Test that progress bars are reused correctly"""
    print("\n" + "="*60)
    print("TESTING PROGRESS BAR REUSE")
    print("="*60)
    print("\nProcessing 3 files - bars should update cleanly without accumulation\n")
    
    # Create tracker
    tracker = ProgressTracker(
        total_files=3,
        total_rows=3000000,
        total_size_gb=3.0,
        month="2022-12",
        show_qc_progress=False
    )
    
    # Track bar object IDs to verify reuse
    bar_ids = {'compress': [], 'upload': [], 'copy': []}
    
    try:
        for i in range(1, 4):
            filename = f"test_file_{i}.tsv"
            print(f"\n--- Processing {filename} ---")
            
            # Start compression
            tracker.start_file_compression(filename, 1024.0)
            if tracker.compress_pbar:
                bar_id = id(tracker.compress_pbar)
                bar_ids['compress'].append(bar_id)
                print(f"Compression bar ID: {bar_id}")
            
            # Simulate compression
            for _ in range(5):
                time.sleep(0.1)
                tracker.update(compressed_mb=204.8)
            
            # Start upload
            tracker.start_file_upload(filename + ".gz", 307.2)
            if tracker.upload_pbar:
                bar_id = id(tracker.upload_pbar)
                bar_ids['upload'].append(bar_id)
                print(f"Upload bar ID: {bar_id}")
            
            # Simulate upload
            for _ in range(5):
                time.sleep(0.1)
                tracker.update(uploaded_mb=61.44)
            
            # Start COPY
            tracker.start_copy_operation("TEST_TABLE", 1000000)
            if tracker.copy_pbar:
                bar_id = id(tracker.copy_pbar)
                bar_ids['copy'].append(bar_id)
                print(f"COPY bar ID: {bar_id}")
            
            # Simulate COPY
            for _ in range(5):
                time.sleep(0.1)
                tracker.update(copied_rows=200000)
            
            # Update file count
            tracker.update(files=1)
            
            # Clear bars between files
            if i < 3:
                tracker.clear_file_bars()
                time.sleep(0.5)
        
        print("\n" + "="*60)
        print("RESULTS")
        print("="*60)
        
        # Check if bars were reused
        for bar_type, ids in bar_ids.items():
            unique_ids = set(ids)
            if len(unique_ids) == 1:
                print(f"✓ {bar_type.capitalize()} bar was REUSED (same object ID for all files)")
            else:
                print(f"✗ {bar_type.capitalize()} bar was RECREATED (different object IDs: {unique_ids})")
        
        # All IDs should be the same if reuse is working
        success = all(len(set(ids)) == 1 for ids in bar_ids.values())
        
        if success:
            print("\n✓✓✓ SUCCESS: All progress bars were properly reused!")
            print("This should prevent static bars from accumulating.")
        else:
            print("\n✗✗✗ FAILURE: Progress bars were recreated instead of reused.")
            print("Static bars may still accumulate.")
        
    finally:
        tracker.close()
    
    return success

if __name__ == "__main__":
    # Check tqdm
    try:
        import tqdm
        print("✓ tqdm is installed")
    except ImportError:
        print("✗ tqdm is not installed. Please run: pip install tqdm")
        sys.exit(1)
    
    # Run test
    success = test_bar_reuse()
    sys.exit(0 if success else 1)