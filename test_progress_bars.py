#!/usr/bin/env python3
"""Test script to verify parallel progress bar positioning"""

import os
import sys
import time

# Try to import tqdm
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    print("tqdm not installed, installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm"])
    from tqdm import tqdm
    TQDM_AVAILABLE = True

def test_stacked_progress_bars():
    """Test stacked progress bars with position offsets"""
    
    # Get position from environment
    job_position = os.environ.get('TSV_JOB_POSITION', '0')
    position_offset = int(job_position) * 4
    
    month = os.environ.get('TEST_MONTH', '2024-01')
    
    print(f"Testing progress bars for job position {job_position}, month {month}")
    print(f"Position offset: {position_offset}")
    
    # Create stacked progress bars
    desc_prefix = f"[{month}] "
    
    file_pbar = tqdm(total=10, 
                     desc=f"{desc_prefix}Files", 
                     unit="file",
                     position=position_offset,
                     leave=True,
                     file=sys.stderr)
    
    row_pbar = tqdm(total=1000, 
                    desc=f"{desc_prefix}Rows", 
                    unit="rows",
                    position=position_offset + 1,
                    leave=True,
                    file=sys.stderr)
    
    compress_pbar = tqdm(total=100,
                        desc=f"{desc_prefix}Compression",
                        unit="MB",
                        position=position_offset + 2,
                        leave=True,
                        file=sys.stderr)
    
    # Simulate processing
    for i in range(10):
        file_pbar.update(1)
        row_pbar.update(100)
        compress_pbar.update(10)
        time.sleep(0.2)
    
    file_pbar.close()
    row_pbar.close()
    compress_pbar.close()
    
    print(f"Test complete for job {job_position}")

if __name__ == "__main__":
    test_stacked_progress_bars()