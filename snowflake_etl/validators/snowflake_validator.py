"""
Snowflake data validator with dependency injection.
Validates data completeness, detects anomalies, and identifies duplicates.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

from ..utils.snowflake_connection_v3 import SnowflakeConnectionManager
from ..core.progress import ProgressTracker, ProgressPhase


@dataclass
class ValidationResult:
    """Data class for validation results."""
    valid: bool
    table_name: str
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    total_rows: int = 0
    unique_dates: int = 0
    expected_dates: int = 0
    missing_dates: List[str] = None
    gaps: List[Dict] = None
    anomalous_dates: List[Dict] = None
    duplicate_info: Optional[Dict] = None
    avg_rows_per_day: float = 0.0
    validation_time: float = 0.0
    error_message: Optional[str] = None
    failure_reasons: List[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class SnowflakeDataValidator:
    """
    Validates data completeness and quality directly in Snowflake tables.
    Uses dependency injection for connection management.
    """
    
    # Anomaly detection thresholds
    SEVERELY_LOW_THRESHOLD = 0.10  # 10% of average
    LOW_THRESHOLD = 0.50  # 50% of average
    OUTLIER_IQR_MULTIPLIER = 1.5
    
    def __init__(self,
                 connection_manager: SnowflakeConnectionManager,
                 progress_tracker: Optional[ProgressTracker] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize validator with injected dependencies.
        
        Args:
            connection_manager: Manages Snowflake connections
            progress_tracker: Optional progress tracking
            logger: Optional logger instance
        """
        self.connection_manager = connection_manager
        self.progress_tracker = progress_tracker
        self.logger = logger or logging.getLogger(__name__)
    
    def validate_table_with_cursor(self,
                                   cursor,
                                   table_name: str,
                                   date_column: str,
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   duplicate_key_columns: Optional[List[str]] = None) -> ValidationResult:
        """
        Validate table using provided cursor (avoids connection pool issues).
        
        Args:
            cursor: Existing database cursor to use
            table_name: Table to validate
            date_column: Date column for completeness check
            start_date: Start date for validation (YYYY-MM-DD format)
            end_date: End date for validation (YYYY-MM-DD format)
            duplicate_key_columns: Columns for duplicate detection
            
        Returns:
            ValidationResult with comprehensive validation details
        """
        start_time = datetime.now()
        
        if start_date and end_date:
            self.logger.info(
                f"Validating {table_name} from {start_date} to {end_date}"
            )
        else:
            self.logger.info(f"Validating ALL data in {table_name}")
        
        # Update progress phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
        
        result = ValidationResult(
            valid=True,
            table_name=table_name,
            date_range_start=start_date,
            date_range_end=end_date,
            missing_dates=[],
            gaps=[],
            anomalous_dates=[],
            failure_reasons=[]
        )
        
        try:
            # 1. Check date completeness
            completeness_data = self._check_date_completeness(
                cursor, table_name, date_column, start_date, end_date
            )
            self._update_result_from_completeness(result, completeness_data)
            
            # 2. Detect anomalies
            if result.total_rows > 0:
                anomalies = self._detect_anomalies(
                    cursor, table_name, date_column, start_date, end_date
                )
                self._update_result_from_anomalies(result, anomalies)
            
            # 3. Check for duplicates
            if duplicate_key_columns and result.total_rows > 0:
                duplicate_info = self._check_duplicates(
                    cursor, table_name, duplicate_key_columns, start_date, end_date, date_column
                )
                result.duplicate_info = duplicate_info
                
                if duplicate_info['duplicate_keys'] > 0:
                    result.failure_reasons.append(
                        f"{duplicate_info['duplicate_keys']} duplicate keys found"
                    )
            
            # Determine overall validity
            result.valid = len(result.failure_reasons) == 0
            
        except Exception as e:
            self.logger.error(f"Validation error for {table_name}: {e}")
            result.valid = False
            result.failure_reasons.append(f"Validation error: {str(e)}")
        
        finally:
            result.execution_time = (datetime.now() - start_time).total_seconds()
            
            # Log summary
            if result.valid:
                self.logger.info(f"Validation PASSED for {table_name}")
            else:
                self.logger.warning(
                    f"Validation FAILED for {table_name}: {', '.join(result.failure_reasons)}"
                )
        
        return result
    
    def validate_table(self,
                      table_name: str,
                      date_column: str,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      duplicate_key_columns: Optional[List[str]] = None) -> ValidationResult:
        """
        Comprehensive validation of table data quality.
        
        Args:
            table_name: Table to validate
            date_column: Date column for completeness check
            start_date: Start date for validation (YYYY-MM-DD format)
            end_date: End date for validation (YYYY-MM-DD format)
            duplicate_key_columns: Columns for duplicate detection
            
        Returns:
            ValidationResult with comprehensive validation details
        """
        start_time = datetime.now()
        
        if start_date and end_date:
            self.logger.info(
                f"Validating {table_name} from {start_date} to {end_date}"
            )
        else:
            self.logger.info(f"Validating ALL data in {table_name}")
        
        # Update progress phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
        
        result = ValidationResult(
            valid=True,
            table_name=table_name,
            date_range_start=start_date,
            date_range_end=end_date,
            missing_dates=[],
            gaps=[],
            anomalous_dates=[],
            failure_reasons=[]
        )
        
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Delegate to the cursor-based method
                return self.validate_table_with_cursor(
                    cursor, table_name, date_column, 
                    start_date, end_date, duplicate_key_columns
                )
                
        except Exception as e:
            self.logger.error(f"Validation failed for {table_name}: {e}")
            result.valid = False
            result.error_message = str(e)
            result.failure_reasons.append(f"Validation error: {e}")
        
        # Record validation time
        result.validation_time = (datetime.now() - start_time).total_seconds()
        
        # Update progress
        if self.progress_tracker:
            self.progress_tracker.update_progress(
                validation_complete=True,
                issues_found=len(result.failure_reasons)
            )
        
        return result
    
    def _check_date_completeness(self,
                                 cursor,
                                 table_name: str,
                                 date_column: str,
                                 start_date: Optional[str],
                                 end_date: Optional[str]) -> Dict:
        """
        Check if all expected dates are present in the table.
        
        Returns:
            Dictionary with completeness statistics
        """
        # Build WHERE clause
        where_clause = self._build_date_where_clause(
            date_column, start_date, end_date
        )
        
        # Get date range summary
        range_query = f"""
        SELECT 
            MIN({date_column}) as min_date,
            MAX({date_column}) as max_date,
            COUNT(DISTINCT {date_column}) as unique_dates,
            COUNT(*) as total_rows
        FROM {table_name}
        {where_clause}
        """
        
        self.logger.debug(f"Executing range query: {range_query}")
        cursor.execute(range_query)
        range_result = cursor.fetchone()
        
        if not range_result or range_result[3] == 0:
            return {
                'total_rows': 0,
                'unique_dates': 0,
                'min_date': None,
                'max_date': None,
                'missing_dates': [],
                'gaps': []
            }
        
        min_date, max_date, unique_dates, total_rows = range_result
        
        # Convert dates to proper format
        min_date = self._format_date(min_date)
        max_date = self._format_date(max_date)
        
        # Calculate expected dates
        if start_date and end_date:
            expected_dates = self._calculate_expected_dates(start_date, end_date)
        else:
            expected_dates = self._calculate_expected_dates(min_date, max_date)
        
        # Find missing dates and gaps
        missing_dates, gaps = self._find_missing_dates_and_gaps(
            cursor, table_name, date_column, 
            start_date or min_date, 
            end_date or max_date
        )
        
        return {
            'total_rows': total_rows,
            'unique_dates': unique_dates,
            'expected_dates': expected_dates,
            'min_date': min_date,
            'max_date': max_date,
            'missing_dates': missing_dates,
            'gaps': gaps,
            'avg_rows_per_day': total_rows / unique_dates if unique_dates > 0 else 0
        }
    
    def _detect_anomalies(self,
                         cursor,
                         table_name: str,
                         date_column: str,
                         start_date: Optional[str],
                         end_date: Optional[str]) -> List[Dict]:
        """
        Detect dates with anomalous row counts using statistical analysis.
        
        Returns:
            List of anomalous dates with severity classification
        """
        where_clause = self._build_date_where_clause(
            date_column, start_date, end_date
        )
        
        # Query with statistical analysis
        anomaly_query = f"""
        WITH daily_counts AS (
            SELECT 
                {date_column} as date_value,
                COUNT(*) as row_count
            FROM {table_name}
            {where_clause}
            GROUP BY {date_column}
        ),
        stats AS (
            SELECT 
                AVG(row_count) as avg_count,
                STDDEV(row_count) as std_dev,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY row_count) as q1,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY row_count) as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY row_count) as q3
            FROM daily_counts
        ),
        anomalies AS (
            SELECT 
                dc.date_value,
                dc.row_count,
                s.avg_count,
                s.q1,
                s.q3,
                (s.q3 - s.q1) as iqr,
                CASE
                    WHEN dc.row_count < s.avg_count * {self.SEVERELY_LOW_THRESHOLD} THEN 'SEVERELY_LOW'
                    WHEN dc.row_count < s.q1 - {self.OUTLIER_IQR_MULTIPLIER} * (s.q3 - s.q1) THEN 'OUTLIER_LOW'
                    WHEN dc.row_count < s.avg_count * {self.LOW_THRESHOLD} THEN 'LOW'
                    WHEN dc.row_count > s.q3 + {self.OUTLIER_IQR_MULTIPLIER} * (s.q3 - s.q1) THEN 'OUTLIER_HIGH'
                    ELSE 'NORMAL'
                END as severity,
                (dc.row_count / s.avg_count * 100) as pct_of_avg
            FROM daily_counts dc
            CROSS JOIN stats s
        )
        SELECT 
            date_value,
            row_count,
            avg_count,
            severity,
            pct_of_avg
        FROM anomalies
        WHERE severity != 'NORMAL'
        ORDER BY 
            CASE severity
                WHEN 'SEVERELY_LOW' THEN 1
                WHEN 'OUTLIER_LOW' THEN 2
                WHEN 'LOW' THEN 3
                WHEN 'OUTLIER_HIGH' THEN 4
            END,
            date_value
        LIMIT 100
        """
        
        self.logger.debug("Executing anomaly detection query")
        cursor.execute(anomaly_query)
        anomalies = cursor.fetchall()
        
        results = []
        for date_val, row_count, avg_count, severity, pct_of_avg in anomalies:
            results.append({
                'date': self._format_date(date_val),
                'row_count': row_count,
                'expected_count': int(avg_count),
                'severity': severity,
                'percent_of_average': round(pct_of_avg, 1)
            })
        
        return results
    
    def _check_duplicates(self,
                         cursor,
                         table_name: str,
                         key_columns: List[str],
                         start_date: Optional[str],
                         end_date: Optional[str],
                         date_column: str) -> Dict:
        """
        Check for duplicate records based on composite key.
        
        Returns:
            Dictionary with duplicate statistics and samples
        """
        key_cols_str = ", ".join(key_columns)
        where_clause = self._build_date_where_clause(
            date_column, start_date, end_date
        )
        
        # Count duplicates efficiently
        duplicate_query = f"""
        WITH duplicate_keys AS (
            SELECT 
                {key_cols_str},
                COUNT(*) as dup_count
            FROM {table_name}
            {where_clause}
            GROUP BY {key_cols_str}
            HAVING COUNT(*) > 1
        )
        SELECT 
            COUNT(*) as duplicate_keys,
            SUM(dup_count - 1) as excess_rows,
            MAX(dup_count) as max_duplicates,
            AVG(dup_count) as avg_duplicates
        FROM duplicate_keys
        """
        
        self.logger.debug("Checking for duplicates")
        cursor.execute(duplicate_query)
        dup_result = cursor.fetchone()
        
        if not dup_result or dup_result[0] == 0:
            return {
                'duplicate_keys': 0,
                'excess_rows': 0,
                'severity': 'NONE',
                'samples': []
            }
        
        duplicate_keys, excess_rows, max_duplicates, avg_duplicates = dup_result
        
        # Get sample duplicate records
        sample_query = f"""
        WITH duplicate_keys AS (
            SELECT 
                {key_cols_str},
                COUNT(*) as dup_count
            FROM {table_name}
            {where_clause}
            GROUP BY {key_cols_str}
            HAVING COUNT(*) > 1
            ORDER BY dup_count DESC
            LIMIT 3
        )
        SELECT 
            {key_cols_str},
            dup_count
        FROM duplicate_keys
        """
        
        cursor.execute(sample_query)
        samples = []
        for row in cursor.fetchall():
            sample = {}
            for i, col in enumerate(key_columns):
                sample[col] = str(row[i])
            sample['count'] = row[-1]
            samples.append(sample)
        
        # Calculate severity
        severity = self._calculate_duplicate_severity(
            duplicate_keys, excess_rows, max_duplicates
        )
        
        return {
            'duplicate_keys': duplicate_keys,
            'excess_rows': excess_rows,
            'max_duplicates': max_duplicates,
            'avg_duplicates': round(avg_duplicates, 2),
            'severity': severity,
            'samples': samples
        }
    
    def _calculate_duplicate_severity(self,
                                     duplicate_keys: int,
                                     excess_rows: int,
                                     max_duplicates: int) -> str:
        """Determine severity level of duplicates."""
        if duplicate_keys == 0:
            return 'NONE'
        elif max_duplicates > 100 or duplicate_keys > 1000:
            return 'CRITICAL'
        elif max_duplicates > 50 or duplicate_keys > 100:
            return 'HIGH'
        elif max_duplicates > 10 or duplicate_keys > 10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _build_date_where_clause(self,
                                 date_column: str,
                                 start_date: Optional[str],
                                 end_date: Optional[str]) -> str:
        """Build WHERE clause for date filtering."""
        if not start_date or not end_date:
            return ""
        
        # Convert to YYYYMMDD format for comparison
        start_yyyymmdd = start_date.replace('-', '')
        end_yyyymmdd = end_date.replace('-', '')
        
        return f"WHERE {date_column} BETWEEN '{start_yyyymmdd}' AND '{end_yyyymmdd}'"
    
    def _format_date(self, date_value) -> str:
        """Format date value to YYYY-MM-DD string."""
        if not date_value:
            return None
        
        date_str = str(date_value)
        
        # Handle YYYYMMDD format
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # Already in correct format or other format
        return date_str
    
    def _calculate_expected_dates(self, start_date: str, end_date: str) -> int:
        """Calculate number of expected dates in range."""
        if not start_date or not end_date:
            return 0
        
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        return (end - start).days + 1
    
    def _find_missing_dates_and_gaps(self,
                                     cursor,
                                     table_name: str,
                                     date_column: str,
                                     start_date: str,
                                     end_date: str) -> Tuple[List[str], List[Dict]]:
        """
        Find missing dates and gaps in the sequence.
        
        Returns:
            Tuple of (missing_dates, gaps)
        """
        # Query to find gaps using window functions
        gap_query = f"""
        WITH date_sequence AS (
            SELECT DISTINCT {date_column} as date_value
            FROM {table_name}
            WHERE {date_column} BETWEEN '{start_date.replace("-", "")}' 
                AND '{end_date.replace("-", "")}'
            ORDER BY date_value
        ),
        gaps AS (
            SELECT 
                date_value as current_date,
                LAG(date_value) OVER (ORDER BY date_value) as prev_date,
                DATEDIFF('day', 
                    TO_DATE(LAG(date_value) OVER (ORDER BY date_value), 'YYYYMMDD'),
                    TO_DATE(date_value, 'YYYYMMDD')
                ) as gap_days
            FROM date_sequence
        )
        SELECT 
            prev_date,
            current_date,
            gap_days - 1 as missing_days
        FROM gaps
        WHERE gap_days > 1
        ORDER BY prev_date
        LIMIT 100
        """
        
        try:
            cursor.execute(gap_query)
            gap_results = cursor.fetchall()
            
            gaps = []
            missing_dates = []
            
            for prev_date, curr_date, missing_days in gap_results:
                gap_info = {
                    'start_date': self._format_date(prev_date),
                    'end_date': self._format_date(curr_date),
                    'missing_days': missing_days
                }
                gaps.append(gap_info)
                
                # Add individual missing dates (limit to prevent memory issues)
                if missing_days <= 31:  # Only list individual dates for small gaps
                    start = datetime.strptime(self._format_date(prev_date), '%Y-%m-%d')
                    for i in range(1, missing_days + 1):
                        missing_date = start + timedelta(days=i)
                        missing_dates.append(missing_date.strftime('%Y-%m-%d'))
            
            return missing_dates[:100], gaps  # Limit to prevent huge lists
            
        except Exception as e:
            self.logger.warning(f"Could not detect gaps: {e}")
            return [], []
    
    def _update_result_from_completeness(self, result: ValidationResult, data: Dict):
        """Update validation result with completeness data."""
        result.total_rows = data['total_rows']
        result.unique_dates = data['unique_dates']
        result.expected_dates = data['expected_dates']
        result.missing_dates = data['missing_dates']
        result.gaps = data['gaps']
        result.avg_rows_per_day = data['avg_rows_per_day']
        
        if data['missing_dates']:
            result.failure_reasons.append(
                f"{len(data['missing_dates'])} missing dates"
            )
        
        if data['gaps']:
            result.failure_reasons.append(
                f"{len(data['gaps'])} gaps in date sequence"
            )
    
    def _update_result_from_anomalies(self, result: ValidationResult, anomalies: List[Dict]):
        """Update validation result with anomaly data."""
        result.anomalous_dates = anomalies
        
        if anomalies:
            # Count by severity
            severity_counts = {}
            for anomaly in anomalies:
                severity = anomaly['severity']
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Add failure reasons for critical anomalies
            if severity_counts.get('SEVERELY_LOW', 0) > 0:
                result.failure_reasons.append(
                    f"{severity_counts['SEVERELY_LOW']} dates with critically low row counts (<10% of average)"
                )
            
            if severity_counts.get('OUTLIER_LOW', 0) > 0:
                result.failure_reasons.append(
                    f"{severity_counts['OUTLIER_LOW']} dates with outlier low row counts"
                )