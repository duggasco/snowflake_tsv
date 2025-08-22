"""
Data quality validation for ETL operations
Memory-efficient streaming validators
"""

import csv
import logging
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from snowflake_etl.core.progress import ProgressTracker, ProgressPhase


class DataQualityChecker:
    """
    Memory-efficient streaming quality checks for TSV files
    Refactored to use dependency injection for progress tracking
    """
    
    def __init__(
        self,
        chunk_size: int = 100000,
        buffer_size: int = 8192,
        progress_tracker: Optional[ProgressTracker] = None
    ):
        """
        Initialize data quality checker
        
        Args:
            chunk_size: Number of rows to process in memory at once
            buffer_size: File read buffer size in KB (converted to bytes)
            progress_tracker: Optional progress tracker for reporting
        """
        self.chunk_size = chunk_size
        self.buffer_size = buffer_size * 1024  # Convert KB to bytes
        self.progress_tracker = progress_tracker
        self.logger = logging.getLogger(__name__)
    
    def check_date_completeness(
        self,
        file_path: str,
        date_column_index: int,
        expected_start: datetime,
        expected_end: datetime,
        delimiter: str = '\t'
    ) -> Dict:
        """
        Stream through file checking date completeness
        
        Args:
            file_path: Path to the TSV file
            date_column_index: Index of the date column (0-based)
            expected_start: Expected start date
            expected_end: Expected end date
            delimiter: Field delimiter
            
        Returns:
            Dictionary with validation results
        """
        self.logger.debug(f"Streaming date check for {file_path} (column {date_column_index})")
        self.logger.debug(f"Expected range: {expected_start.date()} to {expected_end.date()}")
        
        # Update progress tracker
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.QUALITY_CHECK)
        
        date_counts = defaultdict(int)
        invalid_dates = []
        total_rows = 0
        
        # Pre-calculate expected dates for O(1) lookup
        expected_dates_set = self._generate_expected_dates(expected_start, expected_end)
        
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return self._error_result('File not found')
            
            start_time = time.time()
            
            # Stream with larger buffer for better performance
            with open(file_path, 'r', encoding='utf-8', errors='ignore', 
                     buffering=self.buffer_size) as file:
                reader = csv.reader(file, delimiter=delimiter)
                
                # Process dates in batches for better performance
                batch_dates = []
                batch_size = 10000
                
                for row_num, row in enumerate(reader, start=1):
                    total_rows += 1
                    
                    # Log progress periodically
                    if total_rows % 100000 == 0:
                        self._log_progress(total_rows, start_time)
                        if self.progress_tracker:
                            self.progress_tracker.update_progress(rows_processed=100000)
                    
                    try:
                        if len(row) > date_column_index:
                            date_str = row[date_column_index].strip()
                            batch_dates.append((row_num, date_str))
                            
                            # Process batch when full
                            if len(batch_dates) >= batch_size:
                                self._process_date_batch(
                                    batch_dates, expected_start, expected_end,
                                    date_counts, invalid_dates
                                )
                                batch_dates = []
                    except Exception as e:
                        invalid_dates.append((row_num, 'ERROR'))
                
                # Process remaining dates
                if batch_dates:
                    self._process_date_batch(
                        batch_dates, expected_start, expected_end,
                        date_counts, invalid_dates
                    )
            
            # Find missing dates using set difference
            found_dates = set(date_counts.keys())
            missing_dates = sorted(expected_dates_set - found_dates)
            
            elapsed = time.time() - start_time
            self.logger.debug(
                f"Date check complete: {total_rows} rows in {elapsed:.1f}s "
                f"({total_rows/elapsed if elapsed > 0 else 0:,.0f} rows/sec)"
            )
            
            return {
                'success': True,
                'total_rows': total_rows,
                'unique_dates': len(date_counts),
                'missing_dates': missing_dates,
                'invalid_dates': invalid_dates[:100],  # First 100 only
                'date_distribution': dict(date_counts),
                'processing_rate': total_rows / elapsed if elapsed > 0 else 0,
                'validation_passed': len(missing_dates) == 0 and len(invalid_dates) == 0
            }
            
        except Exception as e:
            self.logger.error(f"Error in date check: {e}")
            self.logger.error(traceback.format_exc())
            return self._error_result(str(e))
    
    def check_schema(
        self,
        file_path: str,
        expected_columns: List[str],
        sample_size: int = 10000,
        delimiter: str = '\t'
    ) -> Dict:
        """
        Check schema by sampling file
        
        Args:
            file_path: Path to the TSV file
            expected_columns: List of expected column names
            sample_size: Number of rows to sample for type inference
            delimiter: Field delimiter
            
        Returns:
            Dictionary with schema validation results
        """
        self.logger.debug(f"Checking schema in {file_path}")
        self.logger.debug(f"Expected {len(expected_columns)} columns: {expected_columns[:5]}")
        
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return {'schema_match': False, 'error': 'File not found'}
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore',
                     buffering=self.buffer_size) as file:
                reader = csv.reader(file, delimiter=delimiter)
                
                try:
                    first_row = next(reader)
                except StopIteration:
                    return {'schema_match': False, 'error': 'File is empty'}
                
                actual_col_count = len(first_row)
                expected_col_count = len(expected_columns)
                schema_match = actual_col_count == expected_col_count
                
                self.logger.debug(
                    f"Schema: {actual_col_count} columns found, "
                    f"{expected_col_count} expected - Match: {schema_match}"
                )
                
                # Sample rows for type inference
                sample_rows = [first_row]
                for i, row in enumerate(reader):
                    if i >= sample_size - 1:
                        break
                    sample_rows.append(row)
                
                # Analyze columns
                column_analysis = self._analyze_columns(sample_rows, expected_columns)
                
                return {
                    'schema_match': schema_match,
                    'actual_columns': actual_col_count,
                    'expected_columns': expected_col_count,
                    'column_types': column_analysis['types'],
                    'null_counts': column_analysis['null_counts'],
                    'sample_row_count': len(sample_rows),
                    'validation_passed': schema_match
                }
                
        except Exception as e:
            self.logger.error(f"Error in schema check: {e}")
            self.logger.error(traceback.format_exc())
            return {'schema_match': False, 'error': str(e)}
    
    def validate_file(
        self,
        file_path: str,
        expected_columns: List[str],
        date_column: str,
        expected_start: datetime,
        expected_end: datetime,
        delimiter: str = '\t'
    ) -> Dict:
        """
        Comprehensive file validation combining schema and date checks
        
        Args:
            file_path: Path to the TSV file
            expected_columns: List of expected column names
            date_column: Name of the date column
            expected_start: Expected start date
            expected_end: Expected end date
            delimiter: Field delimiter
            
        Returns:
            Combined validation results
        """
        results = {
            'file_path': file_path,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        # Schema validation
        schema_result = self.check_schema(file_path, expected_columns, delimiter=delimiter)
        results['schema'] = schema_result
        
        # Date validation (only if schema is valid and date column exists)
        if schema_result.get('schema_match') and date_column in expected_columns:
            date_column_index = expected_columns.index(date_column)
            date_result = self.check_date_completeness(
                file_path, date_column_index,
                expected_start, expected_end,
                delimiter
            )
            results['dates'] = date_result
        else:
            results['dates'] = {'skipped': True, 'reason': 'Schema validation failed or date column not found'}
        
        # Overall validation result
        results['validation_passed'] = (
            results['schema'].get('validation_passed', False) and
            results['dates'].get('validation_passed', False)
        )
        
        return results
    
    def _generate_expected_dates(self, start: datetime, end: datetime) -> Set[str]:
        """Generate set of expected date strings"""
        expected_dates = set()
        current = start
        while current <= end:
            expected_dates.add(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return expected_dates
    
    def _process_date_batch(
        self,
        batch_dates: List[Tuple[int, str]],
        expected_start: datetime,
        expected_end: datetime,
        date_counts: Dict[str, int],
        invalid_dates: List[Tuple[int, str]]
    ):
        """Process a batch of dates efficiently"""
        date_formats = ['%Y-%m-%d', '%Y%m%d', '%m/%d/%Y']
        
        for row_num, date_str in batch_dates:
            parsed = False
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    if expected_start <= date_obj <= expected_end:
                        date_counts[date_obj.strftime('%Y-%m-%d')] += 1
                    parsed = True
                    break
                except:
                    continue
            
            if not parsed and len(invalid_dates) < 100:  # Limit invalid dates stored
                invalid_dates.append((row_num, date_str))
    
    def _analyze_columns(self, sample_rows: List[List[str]], expected_columns: List[str]) -> Dict:
        """Analyze column types and null counts from sample rows"""
        column_types = {}
        null_counts = defaultdict(int)
        
        for col_idx, col_name in enumerate(expected_columns):
            if col_idx < len(sample_rows[0]) if sample_rows else 0:
                values = [row[col_idx] if col_idx < len(row) else None
                         for row in sample_rows]
                
                # Count nulls
                null_counts[col_name] = sum(
                    1 for v in values
                    if v in ('', 'NULL', 'null', '\\N', None)
                )
                
                # Infer type from non-null values
                non_null_values = [
                    v for v in values
                    if v not in ('', 'NULL', 'null', '\\N', None)
                ]
                
                if non_null_values:
                    column_types[col_name] = self._infer_type(non_null_values[0])
                else:
                    column_types[col_name] = 'UNKNOWN'
        
        return {
            'types': column_types,
            'null_counts': dict(null_counts)
        }
    
    def _infer_type(self, value: str) -> str:
        """Infer data type from a sample value"""
        if value.isdigit():
            return 'INTEGER'
        elif value.replace('.', '').replace('-', '').isdigit():
            return 'FLOAT'
        elif self._is_date(value):
            return 'DATE'
        else:
            return 'VARCHAR'
    
    @staticmethod
    def _is_date(value: str) -> bool:
        """Check if a value is a date"""
        date_formats = ['%Y-%m-%d', '%Y%m%d', '%m/%d/%Y']
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except:
                continue
        return False
    
    def _log_progress(self, rows_processed: int, start_time: float):
        """Log processing progress"""
        elapsed = time.time() - start_time
        rate = rows_processed / elapsed if elapsed > 0 else 0
        self.logger.debug(f"  Processed {rows_processed:,} rows at {rate:,.0f} rows/sec")
    
    def _error_result(self, error_message: str) -> Dict:
        """Generate error result dictionary"""
        return {
            'success': False,
            'error': error_message,
            'total_rows': 0,
            'unique_dates': 0,
            'missing_dates': [],
            'invalid_dates': [],
            'validation_passed': False
        }