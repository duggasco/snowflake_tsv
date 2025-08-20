#!/usr/bin/env python3
"""Visual test of parallel progress bars"""

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# Import tqdm
try:
    from tqdm import tqdm
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm"])
    from tqdm import tqdm

def process_month(month, position):
    """Simulate processing a month with progress bars"""
    
    # Set environment variable for position
    os.environ['TSV_JOB_POSITION'] = str(position)
    
    desc_prefix = f"[{month}] "
    lines_per_job = 2  # Files and Compression (no QC)
    position_offset = position * lines_per_job
    
    # Create progress bars
    file_pbar = tqdm(total=3, 
                     desc=f"{desc_prefix}Processing", 
                     unit="file",
                     position=position_offset,
                     leave=True,
                     file=sys.stderr,
                     ncols=100)
    
    # Process 3 files
    for i in range(3):
        filename = f"file_{month}_{i}.tsv"
        file_size_mb = 30 + i * 10
        
        # Create compression bar for this file
        compress_pbar = tqdm(total=file_size_mb,
                           desc=f"{desc_prefix}Compressing {filename}",
                           unit="MB",
                           position=position_offset + 1,
                           leave=False,  # Clear after each file
                           file=sys.stderr,
                           ncols=100)
        
        # Simulate compression
        for j in range(int(file_size_mb / 10)):
            time.sleep(0.05)
            compress_pbar.update(10)
        
        compress_pbar.close()
        file_pbar.update(1)
        time.sleep(0.2)
    
    file_pbar.close()
    return f"Month {month} complete"

def main():
    """Test parallel processing with 3 months"""
    print("Testing parallel progress bars (3 jobs, 2 bars each)...")
    print()
    
    months = ['2024-01', '2024-02', '2024-03']
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i, month in enumerate(months):
            future = executor.submit(process_month, month, i)
            futures.append(future)
        
        # Wait for completion
        for future in futures:
            result = future.result()
            print(result)
    
    print("\nAll jobs complete!")

if __name__ == "__main__":
    main()