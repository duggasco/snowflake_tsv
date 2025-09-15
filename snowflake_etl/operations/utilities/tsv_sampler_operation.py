#!/usr/bin/env python3
"""
Sample and analyze data files (TSV/CSV)
"""

import csv
import gzip
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from ...utils.format_detector import FormatDetector


class FileSamplerOperation:
    """
    Operation to sample and analyze data files (TSV/CSV/TXT)
    """
    
    def __init__(self, context=None):
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, file_path: str, rows: int = 100) -> Dict[str, Any]:
        """
        Sample and analyze data file
        
        Args:
            file_path: Path to data file
            rows: Number of rows to sample
            
        Returns:
            Dictionary with analysis results
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            self.logger.error(f"File not found: {file_path}")
            return {"error": "File not found"}
        
        # Detect file format
        format_info = FormatDetector.detect_format(str(file_path))
        file_format = format_info['format']
        delimiter = format_info['delimiter']
        has_header = format_info.get('has_header', False)
        
        self.logger.info(f"Analyzing {file_format} file with delimiter {repr(delimiter)}")
        
        # Analyze file
        analysis = self._analyze_file(file_path, delimiter, rows)
        
        # Add format info to results
        analysis.update({
            'file_format': file_format,
            'delimiter': delimiter,
            'delimiter_name': self._get_delimiter_name(delimiter),
            'has_header': has_header,
            'detection_confidence': format_info['confidence'],
            'detection_method': format_info['method']
        })
        
        # Display results
        self._display_results(analysis, rows)
        
        return analysis
    
    def _analyze_file(self, file_path: Path, delimiter: str, sample_rows: int) -> Dict[str, Any]:
        """
        Analyze file structure and content
        
        Args:
            file_path: Path to file
            delimiter: Field delimiter
            sample_rows: Number of rows to sample
            
        Returns:
            Analysis results dictionary
        """
        results = {
            'file_path': str(file_path),
            'file_size': file_path.stat().st_size,
            'total_rows': 0,
            'column_count': 0,
            'sample_data': [],
            'column_widths': [],
            'null_counts': []
        }
        
        # Handle compressed files
        if file_path.suffix == '.gz':
            opener = gzip.open
            mode = 'rt'
        else:
            opener = open
            mode = 'r'
        
        try:
            with opener(file_path, mode, encoding='utf-8', errors='ignore') as f:
                # Count total rows efficiently
                for line_count, _ in enumerate(f, 1):
                    pass
                results['total_rows'] = line_count
                
                # Reset and read sample
                f.seek(0)
                reader = csv.reader(f, delimiter=delimiter)
                
                # Read header or first row
                first_row = next(reader, None)
                if first_row:
                    results['column_count'] = len(first_row)
                    results['column_widths'] = [0] * len(first_row)
                    results['null_counts'] = [0] * len(first_row)
                    
                    # Check if it's a header
                    if all(self._looks_like_header(col) for col in first_row[:min(5, len(first_row))]):
                        results['headers'] = first_row
                    else:
                        results['headers'] = [f"Column_{i+1}" for i in range(len(first_row))]
                        results['sample_data'].append(first_row)
                    
                    # Read sample rows
                    for i, row in enumerate(reader):
                        if i >= sample_rows - (1 if results['sample_data'] else 0):
                            break
                        
                        results['sample_data'].append(row)
                        
                        # Update column statistics
                        for j, value in enumerate(row[:results['column_count']]):
                            if j < len(results['column_widths']):
                                results['column_widths'][j] = max(
                                    results['column_widths'][j], 
                                    len(str(value))
                                )
                                if not value or value.upper() in ['NULL', 'NONE', 'NA', '']:
                                    results['null_counts'][j] += 1
                                    
        except Exception as e:
            self.logger.error(f"Error analyzing file: {e}")
            results['error'] = str(e)
            
        return results
    
    def _looks_like_header(self, value: str) -> bool:
        """Check if value looks like a column header"""
        if not value:
            return False
        
        # Headers typically don't start with numbers
        if value[0].isdigit():
            return False
            
        # Headers often contain underscores or are all caps
        if '_' in value or value.isupper():
            return True
            
        # Headers are typically not pure numbers
        try:
            float(value)
            return False
        except ValueError:
            return True
    
    def _get_delimiter_name(self, delimiter: str) -> str:
        """Get human-readable delimiter name"""
        delimiter_names = {
            '\t': 'Tab',
            ',': 'Comma',
            '|': 'Pipe',
            ';': 'Semicolon',
            ':': 'Colon',
            ' ': 'Space'
        }
        return delimiter_names.get(delimiter, f"'{delimiter}'")
    
    def _display_results(self, analysis: Dict[str, Any], sample_rows: int) -> None:
        """Display analysis results"""
        print("\n" + "=" * 60)
        print("FILE ANALYSIS RESULTS")
        print("=" * 60)
        
        # Basic info
        print(f"File: {Path(analysis['file_path']).name}")
        print(f"Format: {analysis['file_format']}")
        print(f"Delimiter: {analysis['delimiter_name']} ({repr(analysis['delimiter'])})")
        print(f"Detection confidence: {analysis['detection_confidence']:.2%}")
        print(f"Has header row: {analysis['has_header']}")
        print(f"Size: {analysis['file_size']:,} bytes")
        print(f"Total rows: {analysis['total_rows']:,}")
        print(f"Columns: {analysis['column_count']}")
        
        # Column info
        if 'headers' in analysis:
            print(f"\nColumn Headers:")
            for i, header in enumerate(analysis['headers'][:10]):
                width = analysis['column_widths'][i] if i < len(analysis['column_widths']) else 0
                nulls = analysis['null_counts'][i] if i < len(analysis['null_counts']) else 0
                print(f"  {i+1:3}. {header:30} (width: {width:3}, nulls: {nulls})")
            
            if len(analysis['headers']) > 10:
                print(f"  ... and {len(analysis['headers']) - 10} more columns")
        
        # Sample data
        if analysis['sample_data']:
            print(f"\nSample Data (first {min(sample_rows, len(analysis['sample_data']))} rows):")
            print("-" * 60)
            
            # Display sample rows
            for i, row in enumerate(analysis['sample_data'][:10], 1):
                # Truncate long values
                display_row = [str(val)[:50] + ('...' if len(str(val)) > 50 else '') 
                             for val in row[:5]]
                print(f"{i:3}: {' | '.join(display_row)}")
                if len(row) > 5:
                    print(f"     ... and {len(row) - 5} more columns")
            
            if len(analysis['sample_data']) > 10:
                print(f"... and {len(analysis['sample_data']) - 10} more rows in sample")
        
        print("=" * 60)


# Alias for backward compatibility
TSVSamplerOperation = FileSamplerOperation