"""
File format and delimiter detection utilities for CSV/TSV files.

This module provides intelligent detection of file formats and delimiters,
supporting CSV, TSV, and custom-delimited files with various quote characters.
Includes confidence scoring and automatic header detection.
"""

import csv
import os
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from collections import Counter


class FormatDetector:
    """
    Intelligent file format and delimiter detection for CSV/TSV files.
    
    This module provides automatic detection of file formats and delimiters
    based on file extension, content analysis, and statistical methods.
    """
    
    # Common delimiters to check
    COMMON_DELIMITERS = [',', '\t', '|', ';', ':']
    
    # File extension mappings
    EXTENSION_MAP = {
        '.csv': ('CSV', ','),
        '.tsv': ('TSV', '\t'),
        '.txt': ('AUTO', None),  # Need content analysis
        '.dat': ('AUTO', None),  # Need content analysis
    }
    
    @staticmethod
    def detect_format(file_path: str, sample_lines: int = 10, encoding: str = 'utf-8') -> Dict:
        """
        Detect file format and delimiter from a file.
        
        Args:
            file_path: Path to the file to analyze
            sample_lines: Number of lines to sample for detection
            encoding: File encoding to use
            
        Returns:
            Dictionary containing:
                - format: 'CSV' or 'TSV'
                - delimiter: Detected delimiter character
                - has_header: Whether file appears to have a header row
                - quote_char: Quote character used (if any)
                - confidence: Detection confidence score (0.0-1.0)
                - method: Detection method used
        """
        file_path = Path(file_path)
        
        # Step 1: Check file extension
        extension = file_path.suffix.lower()
        base_extension = extension.replace('.gz', '')  # Handle compressed files
        
        if base_extension in FormatDetector.EXTENSION_MAP:
            format_hint, delimiter_hint = FormatDetector.EXTENSION_MAP[base_extension]
            if delimiter_hint:
                # High confidence based on standard extension
                return {
                    'format': format_hint,
                    'delimiter': delimiter_hint,
                    'has_header': FormatDetector._detect_header(file_path, delimiter_hint),
                    'quote_char': '"' if format_hint == 'CSV' else None,
                    'confidence': 0.9,
                    'method': 'extension'
                }
        
        # Step 2: Content-based detection
        delimiter, confidence = FormatDetector._detect_delimiter_from_content(file_path, sample_lines)
        
        if delimiter:
            format_type = 'TSV' if delimiter == '\t' else 'CSV'
            return {
                'format': format_type,
                'delimiter': delimiter,
                'has_header': FormatDetector._detect_header(file_path, delimiter),
                'quote_char': '"' if format_type == 'CSV' else None,
                'confidence': confidence,
                'method': 'content_analysis'
            }
        
        # Step 3: Fallback to most common format
        return {
            'format': 'CSV',
            'delimiter': ',',
            'has_header': False,
            'quote_char': '"',
            'confidence': 0.3,
            'method': 'fallback'
        }
    
    @staticmethod
    def _detect_delimiter_from_content(file_path: Path, sample_lines: int = 10) -> Tuple[Optional[str], float]:
        """
        Detect delimiter by analyzing file content.
        
        Returns:
            Tuple of (delimiter, confidence_score)
        """
        try:
            # Handle compressed files
            if file_path.suffix == '.gz':
                import gzip
                opener = gzip.open
                mode = 'rt'
            else:
                opener = open
                mode = 'r'
            
            with opener(file_path, mode, encoding='utf-8', errors='ignore') as f:
                lines = []
                for _ in range(sample_lines):
                    line = f.readline()
                    if not line:
                        break
                    lines.append(line.rstrip('\n\r'))
            
            if not lines:
                return None, 0.0
            
            # Count delimiter frequencies
            delimiter_scores = {}
            
            for delimiter in FormatDetector.COMMON_DELIMITERS:
                counts = []
                for line in lines:
                    counts.append(line.count(delimiter))
                
                # Good delimiter should have consistent count across lines
                if counts and min(counts) > 0:
                    # Calculate consistency score
                    avg_count = sum(counts) / len(counts)
                    variance = sum((c - avg_count) ** 2 for c in counts) / len(counts)
                    
                    # Lower variance = more consistent = better delimiter
                    consistency_score = 1 / (1 + variance)
                    
                    # Bonus for common delimiters
                    if delimiter in [',', '\t']:
                        consistency_score *= 1.2
                    
                    delimiter_scores[delimiter] = (consistency_score, avg_count)
            
            if delimiter_scores:
                # Pick delimiter with highest consistency score
                best_delimiter = max(delimiter_scores, key=lambda k: delimiter_scores[k][0])
                confidence = min(delimiter_scores[best_delimiter][0], 1.0)
                return best_delimiter, confidence
            
            return None, 0.0
            
        except Exception:
            return None, 0.0
    
    @staticmethod
    def _detect_header(file_path: Path, delimiter: str) -> bool:
        """
        Detect if file has a header row.
        
        Simple heuristic: If first row has any non-numeric values,
        it's likely a header.
        """
        try:
            # Handle compressed files
            if file_path.suffix == '.gz':
                import gzip
                opener = gzip.open
                mode = 'rt'
            else:
                opener = open
                mode = 'r'
            
            with opener(file_path, mode, encoding='utf-8', errors='ignore') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return False
                
                # Parse the first line
                reader = csv.reader([first_line], delimiter=delimiter)
                first_row = next(reader, [])
                
                # Check if any field is non-numeric
                for field in first_row:
                    try:
                        float(field.strip())
                    except ValueError:
                        # Found a non-numeric field, likely a header
                        return True
                
                return False
                
        except Exception:
            return False
    
    @staticmethod
    def validate_delimiter(file_path: str, delimiter: str, sample_lines: int = 10) -> bool:
        """
        Validate that a specified delimiter works for the file.
        
        Args:
            file_path: Path to the file
            delimiter: Delimiter to validate
            sample_lines: Number of lines to check
            
        Returns:
            True if delimiter appears to work, False otherwise
        """
        try:
            file_path = Path(file_path)
            
            # Handle compressed files
            if file_path.suffix == '.gz':
                import gzip
                opener = gzip.open
                mode = 'rt'
            else:
                opener = open
                mode = 'r'
            
            with opener(file_path, mode, encoding='utf-8', errors='ignore') as f:
                counts = []
                for _ in range(sample_lines):
                    line = f.readline()
                    if not line:
                        break
                    counts.append(line.count(delimiter))
                
                # Delimiter should appear at least once per line
                # and be relatively consistent
                if counts and min(counts) > 0:
                    avg_count = sum(counts) / len(counts)
                    variance = sum((c - avg_count) ** 2 for c in counts) / len(counts)
                    
                    # Low variance indicates consistent delimiter usage
                    return variance < avg_count  # Variance should be less than average
                
                return False
                
        except Exception:
            return False
    
    @staticmethod
    def get_format_from_extension(file_path: str) -> Optional[str]:
        """
        Get format based solely on file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            'CSV', 'TSV', or None if unknown
        """
        file_path = Path(file_path)
        extension = file_path.suffix.lower()
        base_extension = extension.replace('.gz', '')
        
        if base_extension == '.csv':
            return 'CSV'
        elif base_extension == '.tsv':
            return 'TSV'
        else:
            return None