#!/usr/bin/env python3
"""
Test script to verify the parent directory bug is fixed
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.insert(0, '.')

from tsv_file_browser import TSVFileBrowser, FileItem

def test_parent_directory_bug():
    """Test that parent directory isn't duplicated in cache"""
    
    # Create a browser instance
    browser = TSVFileBrowser(start_dir="/tmp")
    
    # Get directory contents
    items1 = browser._get_directory_contents(browser.current_dir)
    print(f"First call - items count: {len(items1)}")
    
    # Apply filter (no filter text, should return a copy)
    filtered1 = browser._apply_filter(items1)
    print(f"Filtered (no filter) - count: {len(filtered1)}")
    
    # Simulate what the UI does - add parent directory
    if browser.current_dir.parent != browser.current_dir:
        parent_item = FileItem(
            path=browser.current_dir.parent,
            name="..",
            is_dir=True,
            is_symlink=False,
            size=0,
            mtime=0
        )
        filtered1.insert(0, parent_item)
    
    print(f"After adding parent - filtered count: {len(filtered1)}")
    
    # Get directory contents again (should be from cache)
    items2 = browser._get_directory_contents(browser.current_dir)
    print(f"Second call - items count: {len(items2)}")
    
    # Check if items have been modified
    has_parent_in_cache = any(item.name == ".." for item in items2)
    
    if has_parent_in_cache:
        print("❌ BUG STILL EXISTS: Parent directory found in cached items!")
        return False
    else:
        print("✅ BUG FIXED: Cache not modified by UI operations")
        return True
    
    # Also verify that filtered list is independent
    filtered2 = browser._apply_filter(items2)
    print(f"Second filter - count: {len(filtered2)}")
    
    if len(filtered2) != len(items2):
        print("❌ Filter not returning correct copy")
        return False
    
    # Verify with actual filter text
    browser.filter_text = "test"
    filtered3 = browser._apply_filter(items2)
    print(f"Filtered with 'test' - count: {len(filtered3)}")
    
    return True

if __name__ == "__main__":
    success = test_parent_directory_bug()
    sys.exit(0 if success else 1)