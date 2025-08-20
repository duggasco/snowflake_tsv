#!/usr/bin/env python3
"""Test script to investigate multiple compression progress bar instances"""

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import logging

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(message)s')

# Import the ProgressTracker class
from tsv_loader import ProgressTracker

def test_sequential_compression():
    """Test compression bars with sequential file processing"""
    print("\n=== Testing Sequential Compression Bars ===\n")
    
    tracker = ProgressTracker(
        total_files=3,
        total_rows=1000000,
        total_size_gb=2.0,
        month="2024-01",
        show_qc_progress=True
    )
    
    try:
        # Simulate processing 3 files sequentially
        for i in range(3):
            filename = f"file_{i+1}.tsv"
            file_size_mb = 700.0
            
            print(f"\nStarting file {i+1}")
            
            # Start compression for this file
            tracker.start_file_compression(filename, file_size_mb)
            
            # Simulate compression progress
            for j in range(10):
                tracker.update(compressed_mb=70)
                time.sleep(0.2)
            
            print(f"Completed file {i+1}")
            time.sleep(0.5)
    finally:
        tracker.close()
    
    print("\n=== Sequential test completed ===\n")

def test_parallel_compression():
    """Test compression bars with parallel file processing"""
    print("\n=== Testing Parallel Compression Bars ===\n")
    
    # Add spacing for visibility
    for _ in range(10):
        print()
    
    tracker = ProgressTracker(
        total_files=3,
        total_rows=1000000,
        total_size_gb=2.0,
        month="2024-01",
        show_qc_progress=True
    )
    
    def compress_file(file_num):
        """Simulate file compression"""
        filename = f"file_{file_num}.tsv"
        file_size_mb = 700.0
        
        print(f"\nThread {threading.current_thread().name}: Starting {filename}")
        
        # Start compression for this file
        tracker.start_file_compression(filename, file_size_mb)
        
        # Simulate compression progress
        for j in range(10):
            tracker.update(compressed_mb=70)
            time.sleep(0.2)
        
        print(f"\nThread {threading.current_thread().name}: Completed {filename}")
    
    try:
        # Test with ThreadPoolExecutor (simulating what happens in the actual code)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            for i in range(3):
                future = executor.submit(compress_file, i+1)
                futures.append(future)
                time.sleep(0.5)  # Stagger the starts slightly
            
            # Wait for all to complete
            for future in futures:
                future.result()
    finally:
        tracker.close()
    
    print("\n=== Parallel test completed ===\n")

def test_rapid_file_switching():
    """Test rapid switching between files"""
    print("\n=== Testing Rapid File Switching ===\n")
    
    tracker = ProgressTracker(
        total_files=5,
        total_rows=500000,
        total_size_gb=1.0,
        month="2024-01",
        show_qc_progress=False
    )
    
    try:
        for i in range(5):
            filename = f"file_{i+1}.tsv"
            
            # Start compression for new file
            tracker.start_file_compression(filename, 200)
            
            # Very short compression simulation
            for j in range(5):
                tracker.update(compressed_mb=40)
                time.sleep(0.1)
            
            # Minimal delay between files
            time.sleep(0.1)
    finally:
        tracker.close()
    
    print("\n=== Rapid switching test completed ===\n")

def test_bar_cleanup():
    """Test if progress bars are properly cleaned up"""
    print("\n=== Testing Progress Bar Cleanup ===\n")
    
    # Test creating and destroying multiple trackers
    for round_num in range(3):
        print(f"\nRound {round_num + 1}")
        
        tracker = ProgressTracker(
            total_files=2,
            total_rows=100000,
            total_size_gb=0.5,
            month=f"2024-0{round_num+1}",
            show_qc_progress=False
        )
        
        try:
            for i in range(2):
                tracker.start_file_compression(f"round{round_num}_file{i}.tsv", 250)
                for j in range(5):
                    tracker.update(compressed_mb=50)
                    time.sleep(0.1)
        finally:
            tracker.close()
            print(f"Tracker closed for round {round_num + 1}")
        
        time.sleep(1)
    
    print("\n=== Cleanup test completed ===\n")

if __name__ == "__main__":
    print("Investigating Multiple Compression Progress Bar Issue")
    print("="*50)
    
    # Check if tqdm is available
    try:
        import tqdm
        print("✓ tqdm version:", tqdm.__version__)
    except ImportError:
        print("✗ tqdm not installed")
        sys.exit(1)
    
    # Run tests
    print("\nTest 1: Sequential Processing")
    test_sequential_compression()
    time.sleep(2)
    
    print("\nTest 2: Parallel Processing")
    test_parallel_compression()
    time.sleep(2)
    
    print("\nTest 3: Rapid File Switching")
    test_rapid_file_switching()
    time.sleep(2)
    
    print("\nTest 4: Bar Cleanup")
    test_bar_cleanup()
    
    print("\n" + "="*50)
    print("All tests completed!")