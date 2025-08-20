#!/usr/bin/env python3
"""Test script to verify progress bars adapt based on QC mode"""

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

def test_progress_bars_with_qc(show_qc: bool = True):
    """Test progress bars with or without QC progress"""
    
    # Get position from environment
    job_position = os.environ.get('TSV_JOB_POSITION', '0')
    lines_per_job = 3 if show_qc else 2
    position_offset = int(job_position) * lines_per_job
    
    month = os.environ.get('TEST_MONTH', '2024-01')
    
    print(f"Testing progress bars for job position {job_position}, month {month}")
    print(f"QC Mode: {'Enabled' if show_qc else 'Disabled (--skip-qc or --validate-in-snowflake)'}")
    print(f"Position offset: {position_offset}, Lines per job: {lines_per_job}")
    
    # Create stacked progress bars
    desc_prefix = f"[{month}] "
    
    file_pbar = tqdm(total=10, 
                     desc=f"{desc_prefix}Files", 
                     unit="file",
                     position=position_offset,
                     leave=True,
                     file=sys.stderr)
    
    if show_qc:
        row_pbar = tqdm(total=1000, 
                        desc=f"{desc_prefix}QC Rows", 
                        unit="rows",
                        position=position_offset + 1,
                        leave=True,
                        file=sys.stderr)
        compress_position = position_offset + 2
    else:
        row_pbar = None
        compress_position = position_offset + 1
    
    compress_pbar = tqdm(total=100,
                        desc=f"{desc_prefix}Compression",
                        unit="MB",
                        position=compress_position,
                        leave=True,
                        file=sys.stderr)
    
    # Simulate processing
    for i in range(10):
        file_pbar.update(1)
        if row_pbar:
            row_pbar.update(100)
        compress_pbar.update(10)
        time.sleep(0.2)
    
    file_pbar.close()
    if row_pbar:
        row_pbar.close()
    compress_pbar.close()
    
    print(f"Test complete for job {job_position}")

if __name__ == "__main__":
    # Check if QC should be shown (default: yes)
    show_qc = os.environ.get('SHOW_QC', '1') == '1'
    test_progress_bars_with_qc(show_qc)