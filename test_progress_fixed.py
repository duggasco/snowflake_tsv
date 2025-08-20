#!/usr/bin/env python3
"""Fixed visual test of parallel progress bars"""

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

# Global lock for progress bar creation
pbar_lock = threading.Lock()

def process_month(month, position):
    """Simulate processing a month with progress bars"""
    
    desc_prefix = f"[{month}] "
    lines_per_job = 2  # Files and Compression (no QC)
    position_offset = position * (lines_per_job + 1)  # Add extra line for spacing
    
    # Create progress bars with lock to avoid race conditions
    with pbar_lock:
        # Main processing bar
        file_pbar = tqdm(total=3, 
                         desc=f"{desc_prefix}Processing", 
                         unit="file",
                         position=position_offset,
                         leave=True,
                         file=sys.stderr,
                         ncols=100,
                         bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        
        # Placeholder for compression bar
        compress_position = position_offset + 1
    
    # Process 3 files
    for i in range(3):
        filename = f"file_{i}.tsv"
        file_size_mb = 30 + i * 10
        
        # Create compression bar for this file
        with pbar_lock:
            compress_pbar = tqdm(total=file_size_mb,
                               desc=f"{desc_prefix}Compress {filename}",
                               unit="MB",
                               position=compress_position,
                               leave=False,  # Don't leave after completion
                               file=sys.stderr,
                               ncols=100,
                               bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} MB')
        
        # Simulate compression
        for j in range(int(file_size_mb / 10)):
            time.sleep(0.05)
            compress_pbar.update(10)
        
        compress_pbar.close()
        
        # Clear the compression line
        print(f"\033[{compress_position + 1};1H\033[K", end='', file=sys.stderr)
        
        file_pbar.update(1)
        time.sleep(0.1)
    
    # Keep the final state visible
    file_pbar.refresh()
    time.sleep(0.5)
    file_pbar.close()
    
    return f"Month {month} complete"

def main():
    """Test parallel processing with 3 months"""
    print("Testing parallel progress bars (3 jobs, 2 bars each)...")
    print()
    
    # Reserve space for progress bars
    total_lines = 3 * 3  # 3 jobs, 3 lines each (2 bars + 1 spacing)
    for _ in range(total_lines):
        print("", file=sys.stderr)
    
    # Move cursor back up
    print(f"\033[{total_lines}A", end='', file=sys.stderr)
    
    months = ['2024-01', '2024-02', '2024-03']
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i, month in enumerate(months):
            future = executor.submit(process_month, month, i)
            futures.append(future)
            time.sleep(0.1)  # Small delay to avoid race conditions
        
        # Wait for completion
        for future in futures:
            result = future.result()
    
    # Move cursor below progress bars
    print(f"\033[{total_lines + 1};1H", file=sys.stderr)
    print("\nAll jobs complete!")

if __name__ == "__main__":
    main()