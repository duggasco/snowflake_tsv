#!/usr/bin/env python3
"""Test script to verify the new 5-bar progress system"""

import os
import sys
import time
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the ProgressTracker class
from tsv_loader import ProgressTracker

def test_sequential_progress():
    """Test progress bars in sequential mode"""
    print("\n=== Testing Sequential Progress Bars ===\n")
    
    # Simulate a single job with all 5 progress bars
    tracker = ProgressTracker(
        total_files=3,
        total_rows=1000000,
        total_size_gb=2.0,
        month="2024-01",
        show_qc_progress=True
    )
    
    try:
        # Simulate processing 3 files
        for i in range(3):
            filename = f"file_{i+1}.tsv"
            file_size_mb = 700.0
            
            # Update file progress
            tracker.update(files=1)
            
            # Simulate QC progress
            for _ in range(10):
                tracker.update(rows=100000)
                time.sleep(0.1)
            
            # Start and simulate compression
            tracker.start_file_compression(filename, file_size_mb)
            for _ in range(10):
                tracker.update(compressed_mb=70)
                time.sleep(0.1)
            
            # Start and simulate upload
            tracker.start_file_upload(f"{filename}.gz", file_size_mb * 0.3)
            for _ in range(10):
                tracker.update(uploaded_mb=21)
                time.sleep(0.1)
            
            # Start and simulate COPY
            tracker.start_copy_operation(f"TABLE_{i+1}", 330000)
            for _ in range(10):
                tracker.update(copied_rows=33000)
                time.sleep(0.1)
            
            time.sleep(0.5)
    finally:
        tracker.close()
    
    print("\n=== Sequential test completed ===\n")

def test_parallel_progress():
    """Test progress bars in parallel mode with multiple jobs"""
    print("\n=== Testing Parallel Progress Bars (3 jobs) ===\n")
    
    # Add initial spacing for 3 parallel jobs (5 bars each = 15 lines)
    for _ in range(15):
        print()
    
    trackers = []
    
    # Create 3 trackers for parallel jobs
    for job_id in range(3):
        os.environ['TSV_JOB_POSITION'] = str(job_id)
        tracker = ProgressTracker(
            total_files=2,
            total_rows=500000,
            total_size_gb=1.0,
            month=f"2024-0{job_id+1}",
            show_qc_progress=True
        )
        trackers.append(tracker)
    
    try:
        # Simulate parallel processing
        for step in range(10):
            for job_id, tracker in enumerate(trackers):
                # Update each tracker's progress
                tracker.update(files=0, rows=50000)
                
                if step == 3:
                    # Start compression for each job
                    tracker.start_file_compression(f"job{job_id}_file.tsv", 500)
                
                if step >= 3:
                    tracker.update(compressed_mb=50)
                
                if step == 6:
                    # Start upload for each job
                    tracker.start_file_upload(f"job{job_id}_file.tsv.gz", 150)
                
                if step >= 6:
                    tracker.update(uploaded_mb=15)
                
                if step == 8:
                    # Start COPY for each job
                    tracker.start_copy_operation(f"TABLE_{job_id}", 250000)
                
                if step >= 8:
                    tracker.update(copied_rows=25000)
            
            time.sleep(0.3)
    finally:
        for tracker in trackers:
            tracker.close()
    
    print("\n=== Parallel test completed ===\n")

def test_no_qc_progress():
    """Test progress bars when skipping QC (4 bars instead of 5)"""
    print("\n=== Testing Without QC Progress (4 bars) ===\n")
    
    # Clear job position
    os.environ['TSV_JOB_POSITION'] = '0'
    
    tracker = ProgressTracker(
        total_files=2,
        total_rows=500000,
        total_size_gb=1.0,
        month="2024-01",
        show_qc_progress=False  # Skip QC progress bar
    )
    
    try:
        for i in range(2):
            filename = f"file_{i+1}.tsv"
            
            # Update file progress
            tracker.update(files=1)
            
            # Start and simulate compression
            tracker.start_file_compression(filename, 500)
            for _ in range(5):
                tracker.update(compressed_mb=100)
                time.sleep(0.2)
            
            # Start and simulate upload
            tracker.start_file_upload(f"{filename}.gz", 150)
            for _ in range(5):
                tracker.update(uploaded_mb=30)
                time.sleep(0.2)
            
            # Start and simulate COPY
            tracker.start_copy_operation(f"TABLE_{i+1}", 250000)
            for _ in range(5):
                tracker.update(copied_rows=50000)
                time.sleep(0.2)
    finally:
        tracker.close()
    
    print("\n=== No-QC test completed ===\n")

if __name__ == "__main__":
    print("Testing New 5-Bar Progress System")
    print("==================================")
    
    # Check if tqdm is available
    try:
        import tqdm
        print("✓ tqdm is installed")
    except ImportError:
        print("✗ tqdm not installed - progress bars won't be visible")
        print("  Install with: pip install tqdm")
        sys.exit(1)
    
    # Run tests
    test_sequential_progress()
    time.sleep(2)
    
    test_parallel_progress()
    time.sleep(2)
    
    test_no_qc_progress()
    
    print("\nAll tests completed!")