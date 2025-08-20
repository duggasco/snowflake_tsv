#!/usr/bin/env python3
"""
Demonstration of the progress bar problem when running parallel processes.
This shows what happens with the current implementation.
"""

import sys
import time
import os

# Mock tqdm to show the problem
class MockProgressBar:
    active_bars = []
    
    def __init__(self, desc, position):
        self.desc = desc
        self.position = position
        self.value = 0
        MockProgressBar.active_bars.append(self)
        self.display()
    
    def display(self):
        # ANSI escape to move to position
        sys.stderr.write(f"\033[{self.position};0H{self.desc}: {self.value}%\033[K\n")
        sys.stderr.flush()
    
    def update(self, val):
        self.value = min(100, self.value + val)
        self.display()
    
    def close(self):
        # Problem: This doesn't actually clear the line in a different process!
        pass

print("\nDemonstrating the problem with parallel processes:")
print("=" * 60)

# Clear screen area for demo
for i in range(20):
    print()

# Simulate what Process 1 does (month 2024-01)
print("\033[0;0H### Process 1 starts (2024-01):")
bar1_files = MockProgressBar("[2024-01] Files", position=1)
bar1_qc = MockProgressBar("[2024-01] QC Progress", position=2)
bar1_compress = MockProgressBar("[2024-01] Compressing file_1.tsv", position=3)
bar1_upload = MockProgressBar("[2024-01] Uploading file_1.tsv.gz", position=4)
bar1_copy = MockProgressBar("[2024-01] Loading into TABLE_1", position=5)

# Update some progress
bar1_compress.update(100)
bar1_upload.update(100)
bar1_copy.update(100)

# Now process file 2 - creates NEW bars at SAME positions
time.sleep(0.5)
bar1_compress_2 = MockProgressBar("[2024-01] Compressing file_2.tsv", position=3)
bar1_upload_2 = MockProgressBar("[2024-01] Uploading file_2.tsv.gz", position=4)
bar1_copy_2 = MockProgressBar("[2024-01] Loading into TABLE_2", position=5)

bar1_compress_2.update(45)
bar1_upload_2.update(20)

# Simulate Process 2 (month 2024-02) - starts at position 6
print("\033[7;0H### Process 2 starts (2024-02):")
bar2_files = MockProgressBar("[2024-02] Files", position=6)
bar2_qc = MockProgressBar("[2024-02] QC Progress", position=7)
bar2_compress = MockProgressBar("[2024-02] Compressing file_3.tsv", position=8)

bar2_compress.update(100)
# Switch to file 4
bar2_compress_2 = MockProgressBar("[2024-02] Compressing file_4.tsv", position=8)
bar2_compress_2.update(60)

# Simulate Process 3 (month 2024-03)
print("\033[12;0H### Process 3 starts (2024-03):")
bar3_compress = MockProgressBar("[2024-03] Compressing file_5.tsv", position=11)
bar3_compress.update(100)
bar3_compress_2 = MockProgressBar("[2024-03] Compressing file_6.tsv", position=11)
bar3_compress_2.update(30)

print("\033[16;0H")
print("\nPROBLEM VISIBLE ABOVE:")
print("- Multiple 'Compressing' bars at same position")
print("- Old bars show 100% but don't get cleared")
print("- Each process can't clean up its previous bars")
print("- Result: Static/dead bars accumulate")

print("\nActive bars in memory:", len(MockProgressBar.active_bars))
for bar in MockProgressBar.active_bars:
    print(f"  - {bar.desc}: {bar.value}%")