"""
Duplicate check operation for identifying duplicate records in Snowflake tables.
Uses ApplicationContext for dependency injection.
"""

import time
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict

from ..core.application_context import ApplicationContext, BaseOperation
from ..core.progress import ProgressPhase


@dataclass
class DuplicateCheckResult:
    """Result from duplicate check operation"""
    table_name: str
    key_columns: List[str]
    date_column: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    has_duplicates: bool
    total_rows: int
    duplicate_key_combinations: int
    excess_rows: int
    duplicate_percentage: float
    severity: str  # NONE, LOW, MEDIUM, HIGH, CRITICAL
    sample_duplicates: List[Dict[str, Any]] = field(default_factory=list)
    execution_time: float = 0
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


class DuplicateCheckOperation(BaseOperation):
    """
    Checks for duplicate records in Snowflake tables based on key columns.
    Supports date filtering and provides severity assessment.
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
        """
        super().__init__(context)
    
    def check_duplicates(self,
                        table_name: str,
                        key_columns: List[str],
                        date_column: Optional[str] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        sample_limit: int = 10,
                        show_progress: bool = True) -> DuplicateCheckResult:
        """
        Check for duplicates in a table based on key columns.
        
        Args:
            table_name: Name of the table to check
            key_columns: List of columns that form the unique key
            date_column: Optional column for date filtering
            start_date: Optional start date for filtering (YYYY-MM-DD format)
            end_date: Optional end date for filtering (YYYY-MM-DD format)
            sample_limit: Number of sample duplicate records to return
            show_progress: Whether to show progress updates
            
        Returns:
            DuplicateCheckResult with analysis results
        """
        start_time = time.time()
        
        # Initialize result
        result = DuplicateCheckResult(
            table_name=table_name,
            key_columns=key_columns,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date,
            has_duplicates=False,
            total_rows=0,
            duplicate_key_combinations=0,
            excess_rows=0,
            duplicate_percentage=0.0,
            severity='NONE'
        )
        
        # Update progress phase
        if show_progress and self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
        
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if table exists
                if not self._table_exists(cursor, table_name):
                    result.error = f"Table {table_name} does not exist"
                    self.logger.error(result.error)
                    return result
                
                # Validate columns exist
                if not self._validate_columns(cursor, table_name, key_columns, date_column):
                    result.error = f"One or more specified columns do not exist in {table_name}"
                    self.logger.error(result.error)
                    return result
                
                # Get total row count
                self.logger.info(f"Getting total row count for {table_name}")
                result.total_rows = self._get_row_count(cursor, table_name, date_column, start_date, end_date)
                
                if result.total_rows == 0:
                    self.logger.info(f"Table {table_name} has no rows in specified range")
                    return result
                
                # Check for duplicates
                self.logger.info(f"Checking for duplicates on columns: {key_columns}")
                dup_stats = self._get_duplicate_statistics(
                    cursor, table_name, key_columns, date_column, start_date, end_date
                )
                
                result.duplicate_key_combinations = dup_stats['duplicate_keys']
                result.excess_rows = dup_stats['excess_rows']
                result.has_duplicates = result.duplicate_key_combinations > 0
                
                if result.has_duplicates:
                    # Calculate percentage
                    result.duplicate_percentage = (result.excess_rows / result.total_rows) * 100
                    
                    # Determine severity
                    result.severity = self._calculate_severity(
                        result.duplicate_percentage,
                        result.duplicate_key_combinations,
                        result.total_rows
                    )
                    
                    # Get sample duplicates
                    if sample_limit > 0:
                        result.sample_duplicates = self._get_sample_duplicates(
                            cursor, table_name, key_columns, date_column, 
                            start_date, end_date, sample_limit
                        )
                    
                    self.logger.warning(
                        f"Found {result.duplicate_key_combinations:,} duplicate keys "
                        f"with {result.excess_rows:,} excess rows "
                        f"({result.duplicate_percentage:.2f}% of total) - Severity: {result.severity}"
                    )
                else:
                    self.logger.info(f"No duplicates found in {table_name}")
                
        except Exception as e:
            result.error = str(e)
            self.logger.error(f"Error checking duplicates in {table_name}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
            # Update progress
            if show_progress and self.progress_tracker:
                self.progress_tracker.update_progress(
                    items_processed=1,
                    total_items=1
                )
        
        return result
    
    def check_multiple_tables(self,
                            tables_config: List[Dict[str, Any]],
                            show_progress: bool = True) -> List[DuplicateCheckResult]:
        """
        Check duplicates across multiple tables.
        
        Args:
            tables_config: List of table configurations, each containing:
                - table_name: Name of the table
                - key_columns: List of key columns
                - date_column: Optional date column
                - start_date: Optional start date
                - end_date: Optional end date
            show_progress: Whether to show progress updates
            
        Returns:
            List of DuplicateCheckResult objects
        """
        results = []
        total_tables = len(tables_config)
        
        for i, config in enumerate(tables_config, 1):
            if show_progress:
                self.logger.info(f"Checking table {i}/{total_tables}: {config['table_name']}")
            
            result = self.check_duplicates(
                table_name=config['table_name'],
                key_columns=config['key_columns'],
                date_column=config.get('date_column'),
                start_date=config.get('start_date'),
                end_date=config.get('end_date'),
                sample_limit=config.get('sample_limit', 10),
                show_progress=False  # Don't show sub-progress
            )
            
            results.append(result)
            
            if show_progress and self.progress_tracker:
                self.progress_tracker.update_progress(
                    items_processed=i,
                    total_items=total_tables
                )
        
        return results
    
    def _table_exists(self, cursor, table_name: str) -> bool:
        """Check if table exists in the current schema."""
        try:
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = CURRENT_SCHEMA()
                  AND table_name = %s
                """,
                (table_name.upper(),)
            )
            return cursor.fetchone()[0] > 0
        except Exception as e:
            self.logger.debug(f"Error checking table existence: {e}")
            return False
    
    def _validate_columns(self, cursor, table_name: str, 
                         key_columns: List[str], 
                         date_column: Optional[str]) -> bool:
        """Validate that all specified columns exist in the table."""
        try:
            # Get all columns in the table
            cursor.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = CURRENT_SCHEMA()
                  AND table_name = %s
                """,
                (table_name.upper(),)
            )
            
            existing_columns = {row[0].upper() for row in cursor.fetchall()}
            
            # Check key columns
            for col in key_columns:
                if col.upper() not in existing_columns:
                    self.logger.error(f"Column {col} does not exist in {table_name}")
                    return False
            
            # Check date column if specified
            if date_column and date_column.upper() not in existing_columns:
                self.logger.error(f"Date column {date_column} does not exist in {table_name}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating columns: {e}")
            return False
    
    def _get_row_count(self, cursor, table_name: str,
                      date_column: Optional[str],
                      start_date: Optional[str],
                      end_date: Optional[str]) -> int:
        """Get total row count with optional date filtering."""
        try:
            if date_column and start_date and end_date:
                # Count with date filter
                query = f"""
                    SELECT COUNT(*) 
                    FROM IDENTIFIER(%s)
                    WHERE IDENTIFIER(%s) BETWEEN %s AND %s
                """
                cursor.execute(query, (table_name, date_column, start_date, end_date))
            else:
                # Count all rows
                cursor.execute("SELECT COUNT(*) FROM IDENTIFIER(%s)", (table_name,))
            
            return cursor.fetchone()[0]
            
        except Exception as e:
            self.logger.error(f"Error getting row count: {e}")
            return 0
    
    def _get_duplicate_statistics(self, cursor, table_name: str,
                                 key_columns: List[str],
                                 date_column: Optional[str],
                                 start_date: Optional[str],
                                 end_date: Optional[str]) -> Dict[str, int]:
        """Get duplicate statistics using efficient GROUP BY query."""
        try:
            # Build key columns string for GROUP BY
            key_cols_str = ', '.join([f'IDENTIFIER(%s)' for _ in key_columns])
            
            # Build WHERE clause if date filtering is needed
            where_clause = ""
            params = list(key_columns) + [table_name]
            
            if date_column and start_date and end_date:
                where_clause = f" WHERE IDENTIFIER(%s) BETWEEN %s AND %s"
                params = list(key_columns) + [table_name, date_column, start_date, end_date]
            
            # Query to find duplicates
            query = f"""
                WITH duplicate_keys AS (
                    SELECT {key_cols_str}, COUNT(*) as cnt
                    FROM IDENTIFIER(%s)
                    {where_clause}
                    GROUP BY {', '.join([str(i+1) for i in range(len(key_columns))])}
                    HAVING COUNT(*) > 1
                )
                SELECT 
                    COUNT(*) as duplicate_keys,
                    SUM(cnt - 1) as excess_rows
                FROM duplicate_keys
            """
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            return {
                'duplicate_keys': result[0] or 0,
                'excess_rows': result[1] or 0
            }
            
        except Exception as e:
            self.logger.error(f"Error getting duplicate statistics: {e}")
            return {'duplicate_keys': 0, 'excess_rows': 0}
    
    def _get_sample_duplicates(self, cursor, table_name: str,
                              key_columns: List[str],
                              date_column: Optional[str],
                              start_date: Optional[str],
                              end_date: Optional[str],
                              limit: int) -> List[Dict[str, Any]]:
        """Get sample duplicate records for investigation."""
        try:
            # Build key columns string
            key_cols_str = ', '.join([f'IDENTIFIER(%s)' for _ in key_columns])
            key_cols_select = ', '.join([f'{col} as key_{i}' for i, col in enumerate(key_columns)])
            
            # Build WHERE clause
            where_clause = ""
            params = list(key_columns) + [table_name]
            
            if date_column and start_date and end_date:
                where_clause = f" WHERE IDENTIFIER(%s) BETWEEN %s AND %s"
                params = list(key_columns) + [table_name, date_column, start_date, end_date]
            
            # Query to get sample duplicates
            query = f"""
                WITH duplicate_keys AS (
                    SELECT {key_cols_str}, COUNT(*) as duplicate_count
                    FROM IDENTIFIER(%s)
                    {where_clause}
                    GROUP BY {', '.join([str(i+1) for i in range(len(key_columns))])}
                    HAVING COUNT(*) > 1
                    ORDER BY COUNT(*) DESC
                    LIMIT %s
                )
                SELECT {key_cols_select}, duplicate_count
                FROM duplicate_keys
            """
            
            params.append(limit)
            cursor.execute(query, params)
            
            samples = []
            for row in cursor.fetchall():
                sample = {}
                for i, col in enumerate(key_columns):
                    sample[col] = row[i]
                sample['duplicate_count'] = row[-1]
                samples.append(sample)
            
            return samples
            
        except Exception as e:
            self.logger.error(f"Error getting sample duplicates: {e}")
            return []
    
    def _calculate_severity(self, duplicate_percentage: float,
                          duplicate_keys: int,
                          total_rows: int) -> str:
        """
        Calculate severity of duplicate issue.
        
        Severity levels:
        - CRITICAL: >10% duplicates or >100 duplicates per key on average
        - HIGH: >5% duplicates or >50 duplicates per key on average
        - MEDIUM: >1% duplicates or >10 duplicates per key on average
        - LOW: Any duplicates below medium threshold
        - NONE: No duplicates
        """
        if duplicate_keys == 0:
            return 'NONE'
        
        avg_duplicates_per_key = total_rows / duplicate_keys if duplicate_keys > 0 else 0
        
        if duplicate_percentage > 10 or avg_duplicates_per_key > 100:
            return 'CRITICAL'
        elif duplicate_percentage > 5 or avg_duplicates_per_key > 50:
            return 'HIGH'
        elif duplicate_percentage > 1 or avg_duplicates_per_key > 10:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def format_result(self, result: DuplicateCheckResult) -> str:
        """
        Format a duplicate check result for display.
        
        Args:
            result: The duplicate check result to format
            
        Returns:
            Formatted string for display
        """
        lines = []
        lines.append("=" * 60)
        lines.append(f"DUPLICATE CHECK RESULTS: {result.table_name}")
        lines.append("=" * 60)
        
        # Key information
        lines.append(f"Key Columns: {', '.join(result.key_columns)}")
        if result.date_column:
            if result.start_date and result.end_date:
                lines.append(f"Date Range: {result.start_date} to {result.end_date}")
            else:
                lines.append("Date Range: All data")
        
        lines.append(f"Total Rows: {result.total_rows:,}")
        lines.append("")
        
        # Results
        if result.error:
            lines.append(f"ERROR: {result.error}")
        elif result.has_duplicates:
            lines.append("WARNING: DUPLICATES FOUND!")
            lines.append(f"Duplicate Keys: {result.duplicate_key_combinations:,}")
            lines.append(f"Excess Rows: {result.excess_rows:,}")
            lines.append(f"Duplicate Percentage: {result.duplicate_percentage:.2f}%")
            lines.append(f"Severity: {result.severity}")
            
            if result.sample_duplicates:
                lines.append("")
                lines.append("Sample Duplicate Keys (top duplicates):")
                for i, sample in enumerate(result.sample_duplicates[:5], 1):
                    key_parts = []
                    for col in result.key_columns:
                        if col in sample:
                            key_parts.append(f"{col}={sample[col]}")
                    lines.append(f"  {i}. {', '.join(key_parts)} (appears {sample['duplicate_count']} times)")
        else:
            lines.append("SUCCESS: No duplicates found")
        
        lines.append("")
        lines.append(f"Execution Time: {result.execution_time:.2f} seconds")
        lines.append("=" * 60)
        
        return '\n'.join(lines)