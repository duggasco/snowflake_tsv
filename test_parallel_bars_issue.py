#!/usr/bin/env python3
"""
Test to reproduce the static/duplicate progress bar issue in parallel mode.
This simulates what happens when multiple Python processes run in parallel.
"""

import os
import sys
import time
import subprocess
from concurrent.futures import ProcessPoolExecutor

def simulate_single_job(job_id):
    """Simulate what one Python process does"""
    # Each process creates its own set of progress bars
    env = os.environ.copy()
    env['TSV_JOB_POSITION'] = str(job_id)
    
    # Simulate running tsv_loader.py for one month
    python_code = f"""
import sys
import time
sys.path.insert(0, '.')
from tsv_loader import ProgressTracker

# Each process creates its own tracker
tracker = ProgressTracker(
    total_files=2,
    total_rows=100000,
    total_size_gb=1.0,
    month="2024-0{job_id+1}",
    show_qc_progress=True
)

# Simulate processing
for i in range(2):
    # File progress
    tracker.update(files=1)
    
    # QC progress
    for _ in range(3):
        tracker.update(rows=30000)
        time.sleep(0.2)
    
    # Compression
    tracker.start_file_compression(f"file_{job_id}_{i}.tsv", 500)
    for _ in range(3):
        tracker.update(compressed_mb=150)
        time.sleep(0.2)
    
    # Upload
    tracker.start_file_upload(f"file_{job_id}_{i}.tsv.gz", 150)
    for _ in range(3):
        tracker.update(uploaded_mb=50)
        time.sleep(0.2)
    
    # COPY
    tracker.start_copy_operation(f"TABLE_{job_id}", 50000)
    for _ in range(3):
        tracker.update(copied_rows=15000)
        time.sleep(0.2)

tracker.close()
"""
    
    # Run the Python code as a subprocess (simulating what bash does)
    result = subprocess.run(
        [sys.executable, '-c', python_code],
        env=env,
        capture_output=False,
        text=True
    )
    return f"Job {job_id} completed"

def test_parallel_processes():
    """Test with multiple Python processes (like bash --parallel does)"""
    print("Testing Parallel Processes (simulating bash --parallel 3 --quiet)")
    print("="*60)
    
    # Add initial spacing for 3 jobs × 5 bars = 15 lines
    for _ in range(15):
        print()
    
    # Run 3 processes in parallel
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = []
        for i in range(3):
            future = executor.submit(simulate_single_job, i)
            futures.append(future)
            time.sleep(0.5)  # Slight stagger like real execution
        
        # Wait for all to complete
        for future in futures:
            result = future.result()
            print(f"\n{result}")
    
    print("\n" + "="*60)
    print("Test complete - check for static/duplicate bars above")

if __name__ == "__main__":
    print("\nReproducing the parallel progress bar issue...")
    print("When each Python process creates its own progress bars,")
    print("they don't know about each other's positions.\n")
    
    test_parallel_processes()