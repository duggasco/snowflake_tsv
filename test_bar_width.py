#!/usr/bin/env python3
"""Test progress bar width consistency"""

import os
import sys
import time

# Set up path for imports
sys.path.insert(0, '/root/snowflake')

# Use test_venv's tqdm
import subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "tqdm"])

from tqdm import tqdm

def test_progress_bar_widths():
    """Test that all progress bars use full terminal width"""
    
    # Simulate a month
    month = "2024-01"
    desc_prefix = f"[{month}] "
    
    print("Testing progress bar widths (should all be full terminal width):")
    print("-" * 80)
    
    # Files progress bar
    file_pbar = tqdm(total=10, 
                     desc=f"{desc_prefix}Files", 
                     unit="file",
                     position=0,
                     leave=True,
                     file=sys.stderr)
    
    # QC Progress bar
    qc_pbar = tqdm(total=1000, 
                   desc=f"{desc_prefix}QC Progress",
                   unit="rows", 
                   unit_scale=True,
                   position=1,
                   leave=True,
                   file=sys.stderr)
    
    # Compression bar (the one we fixed)
    compress_pbar = tqdm(total=100,
                        desc=f"{desc_prefix}Compressing test_file.tsv",
                        unit="MB",
                        unit_scale=True,
                        position=2,
                        leave=True,
                        file=sys.stderr)
    
    # Simulate some progress
    for i in range(10):
        file_pbar.update(1)
        qc_pbar.update(100)
        compress_pbar.update(10)
        time.sleep(0.1)
    
    file_pbar.close()
    qc_pbar.close()
    compress_pbar.close()
    
    print("\n" * 3)
    print("All three bars should have the same width and extend to the right edge of the terminal.")

if __name__ == "__main__":
    test_progress_bar_widths()