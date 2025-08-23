#!/usr/bin/env python3
"""
Validate TSV file structure and content
"""

import logging
import csv
from pathlib import Path
from typing import Optional, Dict, Any


class ValidateFileOperation:
    """
    Operation to validate TSV file structure
    """
    
    def __init__(self, context):
        """
        Initialize validate file operation
        
        Args:
            context: ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, file_path: str, 
                expected_columns: Optional[int] = None,
                sample_rows: int = 10) -> bool:
        """
        Validate TSV file structure and content
        
        Args:
            file_path: Path to TSV file
            expected_columns: Expected number of columns
            sample_rows: Number of rows to sample
            
        Returns:
            True if file is valid
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            return False
        
        print(f"\nValidating TSV file: {file_path}")
        print("="*60)
        
        try:
            # Get file stats
            file_size = file_path.stat().st_size / (1024*1024)  # MB
            print(f"File size: {file_size:.2f} MB")
            
            # Analyze file structure
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read first few lines
                sample_lines = []
                for i, line in enumerate(f):
                    if i >= sample_rows:
                        break
                    sample_lines.append(line)
                
                # Detect delimiter
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample_lines[0]).delimiter
                print(f"Detected delimiter: {repr(delimiter)}")
                
                # Count columns
                reader = csv.reader(sample_lines, delimiter=delimiter)
                rows = list(reader)
                
                if rows:
                    col_count = len(rows[0])
                    print(f"Number of columns: {col_count}")
                    
                    if expected_columns and col_count != expected_columns:
                        print(f"⚠ Warning: Expected {expected_columns} columns, found {col_count}")
                    
                    # Check consistency
                    inconsistent = []
                    for i, row in enumerate(rows[1:], 2):
                        if len(row) != col_count:
                            inconsistent.append((i, len(row)))
                    
                    if inconsistent:
                        print(f"\n⚠ Inconsistent row lengths found:")
                        for row_num, length in inconsistent[:5]:
                            print(f"  Row {row_num}: {length} columns")
                    else:
                        print("✓ All sampled rows have consistent column count")
                    
                    # Show sample data
                    print(f"\nFirst {min(3, len(rows))} rows:")
                    for i, row in enumerate(rows[:3], 1):
                        print(f"  Row {i}: {row[:5]}..." if len(row) > 5 else f"  Row {i}: {row}")
                    
                    return len(inconsistent) == 0
                else:
                    print("Error: File appears to be empty")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error validating file: {e}")
            print(f"Error during validation: {e}")
            return False