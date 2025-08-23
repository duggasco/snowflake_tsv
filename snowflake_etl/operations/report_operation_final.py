"""
Report operation for generating comprehensive table reports - Final Version.
Optimized for performance with proper connection pooling and security fixes.
"""

import json
import logging
import time
import threading
from typing import List, Dict, Optional, Any, Tuple, Protocol
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
from abc import ABC, abstractmethod
import fnmatch

from ..core.application_context import ApplicationContext, BaseOperation
from ..validators.snowflake_validator import SnowflakeDataValidator
from ..core.progress import ProgressPhase


@dataclass
class TableReport:
    """Report data for a single table"""
    config_file: str
    table_name: str
    status: str  # SUCCESS, ERROR, TABLE_NOT_FOUND, EMPTY
    row_count: int = 0
    column_count: int = 0
    columns: List[str] = field(default_factory=list)
    date_column: Optional[str] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    unique_dates: int = 0
    expected_dates: int = 0
    missing_dates: List[str] = field(default_factory=list)
    gaps: int = 0
    anomalous_dates: int = 0
    anomaly_severity: Optional[str] = None
    duplicate_keys: int = 0
    duplicate_rows: int = 0
    duplicate_severity: Optional[str] = None
    avg_rows_per_day: float = 0
    validation_status: str = "NOT_RUN"  # NOT_RUN, PASSED, WARNING, FAILED
    validation_issues: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    execution_time: float = 0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class SeverityConfig:
    """Configuration for severity mappings"""
    anomaly_critical_threshold: float = 0.1  # <10% of average
    anomaly_high_threshold: float = 0.5      # <50% of average
    duplicate_critical_threshold: float = 0.1  # >10% duplicates
    duplicate_high_threshold: float = 0.05     # >5% duplicates
    duplicate_medium_threshold: float = 0.01   # >1% duplicates
    
    def get_anomaly_severity(self, severities: List[str]) -> str:
        """Determine overall anomaly severity from list of severities."""
        if 'SEVERELY_LOW' in severities:
            return 'CRITICAL'
        elif 'OUTLIER_LOW' in severities or 'OUTLIER_HIGH' in severities:
            return 'HIGH'
        else:
            return 'MEDIUM'
    
    def get_duplicate_severity(self, duplicate_ratio: float) -> str:
        """Determine duplicate severity based on ratio."""
        if duplicate_ratio > self.duplicate_critical_threshold:
            return 'CRITICAL'
        elif duplicate_ratio > self.duplicate_high_threshold:
            return 'HIGH'
        elif duplicate_ratio > self.duplicate_medium_threshold:
            return 'MEDIUM'
        else:
            return 'LOW'


class ReportFormatter(ABC):
    """Abstract base class for report formatters"""
    
    @abstractmethod
    def format(self, result: Dict[str, Any]) -> str:
        """Format the report results."""
        pass
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get the appropriate file extension for this format."""
        pass


class TextReportFormatter(ReportFormatter):
    """Formats reports as human-readable text"""
    
    def format(self, result: Dict[str, Any]) -> str:
        """Format results as text report."""
        lines = []
        summary = result['summary']
        
        lines.append("=" * 80)
        lines.append("SNOWFLAKE TABLES COMPREHENSIVE REPORT")
        lines.append(f"Generated: {result['timestamp']}")
        lines.append(f"Execution Time: {result['execution_time']:.1f} seconds")
        lines.append("=" * 80)
        lines.append("")
        
        # Summary statistics
        lines.append("SUMMARY STATISTICS")
        lines.append("-" * 40)
        lines.append(f"Total Tables: {summary['total_tables']}")
        lines.append(f"Total Rows: {summary['total_rows']:,}")
        lines.append("")
        
        # Status breakdown
        lines.append("Table Status:")
        for status, count in summary['status_counts'].items():
            lines.append(f"  {status}: {count}")
        lines.append("")
        
        # Validation breakdown
        lines.append("Validation Status:")
        for status, count in summary['validation_counts'].items():
            lines.append(f"  {status}: {count}")
        lines.append("")
        
        # Critical issues
        if summary['critical_issues']:
            lines.append("CRITICAL ISSUES")
            lines.append("-" * 40)
            for issue in summary['critical_issues']:
                lines.append(f"- {issue['table']}:")
                for reason in issue['reason']:
                    lines.append(f"  - {reason}")
            lines.append("")
        
        # Tables with issues
        if summary['tables_with_issues']:
            lines.append("TABLES WITH WARNINGS")
            lines.append("-" * 40)
            for issue in summary['tables_with_issues']:
                if issue['severity'] != 'FAILED':  # Skip critical, already shown
                    lines.append(f"- {issue['table']} ({issue['severity']}):")
                    for i in issue['issues']:
                        lines.append(f"  - {i}")
            lines.append("")
        
        # Largest tables
        if summary['largest_tables']:
            lines.append("LARGEST TABLES")
            lines.append("-" * 40)
            for table in summary['largest_tables']:
                lines.append(f"  {table['table']}: {table['rows']:,} rows")
            lines.append("")
        
        # Empty tables
        if summary['empty_tables']:
            lines.append("EMPTY TABLES")
            lines.append("-" * 40)
            for table in summary['empty_tables']:
                lines.append(f"  - {table}")
            lines.append("")
        
        # Detailed reports
        lines.append("=" * 80)
        lines.append("DETAILED TABLE REPORTS")
        lines.append("=" * 80)
        
        for report in result['reports']:
            lines.append("")
            lines.append(f"[{report['config_file']}] {report['table_name']}")
            lines.append("-" * 60)
            lines.append(f"Status: {report['status']}")
            
            if report['status'] == 'SUCCESS':
                lines.append(f"Rows: {report['row_count']:,}")
                lines.append(f"Columns: {report['column_count']}")
                
                if report['date_range_start']:
                    lines.append(f"Date Range: {report['date_range_start']} to {report['date_range_end']}")
                    lines.append(f"Unique Dates: {report['unique_dates']}")
                    lines.append(f"Avg Rows/Day: {report['avg_rows_per_day']:.0f}")
                
                lines.append(f"Validation: {report['validation_status']}")
                
                if report['validation_issues']:
                    lines.append("Issues:")
                    for issue in report['validation_issues']:
                        lines.append(f"  - {issue}")
            
            elif report['status'] == 'ERROR':
                lines.append(f"Error: {report['error_message']}")
            
            elif report['status'] == 'EMPTY':
                lines.append("Table is empty")
            
            elif report['status'] == 'TABLE_NOT_FOUND':
                lines.append("Table does not exist")
        
        lines.append("")
        lines.append("=" * 80)
        lines.append("Report Complete")
        lines.append("=" * 80)
        
        return '\n'.join(lines)
    
    def get_file_extension(self) -> str:
        return '.txt'


class JsonReportFormatter(ReportFormatter):
    """Formats reports as JSON"""
    
    def format(self, result: Dict[str, Any]) -> str:
        """Format results as JSON."""
        return json.dumps(result, indent=2, default=str)
    
    def get_file_extension(self) -> str:
        return '.json'


class CsvReportFormatter(ReportFormatter):
    """Formats reports as CSV"""
    
    def format(self, result: Dict[str, Any]) -> str:
        """Format results as CSV."""
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Config File', 'Table Name', 'Status', 'Row Count', 'Column Count',
            'Date Column', 'Date Range Start', 'Date Range End', 'Unique Dates',
            'Expected Dates', 'Missing Dates Count', 'Gaps', 'Anomalous Dates',
            'Anomaly Severity', 'Duplicate Keys', 'Duplicate Rows', 
            'Duplicate Severity', 'Avg Rows/Day', 'Validation Status',
            'Validation Issues', 'Error Message', 'Execution Time'
        ])
        
        # Write data rows
        for report in result['reports']:
            writer.writerow([
                report.get('config_file'),
                report.get('table_name'),
                report.get('status'),
                report.get('row_count'),
                report.get('column_count'),
                report.get('date_column'),
                report.get('date_range_start'),
                report.get('date_range_end'),
                report.get('unique_dates'),
                report.get('expected_dates'),
                len(report.get('missing_dates', [])),
                report.get('gaps'),
                report.get('anomalous_dates'),
                report.get('anomaly_severity'),
                report.get('duplicate_keys'),
                report.get('duplicate_rows'),
                report.get('duplicate_severity'),
                report.get('avg_rows_per_day'),
                report.get('validation_status'),
                '; '.join(report.get('validation_issues', [])),
                report.get('error_message'),
                report.get('execution_time')
            ])
        
        return output.getvalue()
    
    def get_file_extension(self) -> str:
        return '.csv'


class ReportFormatterFactory:
    """Factory for creating report formatters"""
    
    _formatters = {
        'text': TextReportFormatter,
        'json': JsonReportFormatter,
        'csv': CsvReportFormatter,
    }
    
    @classmethod
    def create(cls, format_type: str) -> ReportFormatter:
        """Create a formatter of the specified type."""
        formatter_class = cls._formatters.get(format_type.lower())
        if not formatter_class:
            raise ValueError(f"Unknown format type: {format_type}")
        return formatter_class()
    
    @classmethod
    def register(cls, format_type: str, formatter_class: type):
        """Register a new formatter type."""
        cls._formatters[format_type.lower()] = formatter_class


class OptimizedConnectionPool:
    """
    Thread-safe connection pool that properly tracks and manages all connections.
    This is the CORRECT approach - reuse connections across many operations.
    """
    
    def __init__(self, connection_manager, logger):
        self.connection_manager = connection_manager
        self.logger = logger
        self._local = local()
        self._all_connections = []  # Track all connections for cleanup
        self._lock = threading.Lock()
    
    def get_connection(self):
        """Get or create a connection for the current thread."""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            thread_name = threading.current_thread().name
            self.logger.debug(f"Creating new connection for thread {thread_name}")
            
            # Create new connection
            conn = self.connection_manager.get_connection()
            self._local.connection = conn
            
            # Track this connection for cleanup
            with self._lock:
                self._all_connections.append(conn)
                
        return self._local.connection
    
    def close_all(self):
        """Close ALL connections in the pool across all threads."""
        with self._lock:
            self.logger.debug(f"Closing {len(self._all_connections)} pooled connections")
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception as e:
                    self.logger.debug(f"Error closing connection: {e}")
            self._all_connections.clear()


class ReportOperation(BaseOperation):
    """
    Generates comprehensive reports for all tables across all configurations.
    Final version with optimized connection pooling and security fixes.
    """
    
    def __init__(self, context: ApplicationContext, severity_config: Optional[SeverityConfig] = None):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
            severity_config: Optional custom severity configuration
        """
        super().__init__(context)
        
        # Initialize validator with injected dependencies
        self.validator = SnowflakeDataValidator(
            self.connection_manager,
            self.progress_tracker,
            self.logger
        )
        
        self.severity_config = severity_config or SeverityConfig()
        self.reports: List[TableReport] = []
        self.start_time = None
        self._connection_pool = None
    
    def generate_full_report(self,
                            config_filter: Optional[str] = None,
                            table_filter: Optional[str] = None,
                            max_workers: int = 4,
                            output_format: str = 'both',
                            output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate comprehensive report for all tables.
        
        Args:
            config_filter: Optional filter for config files (glob pattern)
            table_filter: Optional filter for table names (glob pattern)
            max_workers: Number of parallel workers for analysis
            output_format: 'json', 'text', 'csv', or 'both'
            output_file: Optional file path to save report
            
        Returns:
            Dictionary with report results and summary
        """
        self.start_time = time.time()
        self.reports = []
        
        # Update progress phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.ANALYSIS)
        
        # Collect all tables to analyze
        tables_to_analyze = self._collect_tables(config_filter, table_filter)
        
        if not tables_to_analyze:
            self.logger.warning("No tables found to analyze")
            return {
                'status': 'NO_TABLES',
                'message': 'No tables found matching filters',
                'reports': [],
                'summary': {}
            }
        
        self.logger.info(f"Analyzing {len(tables_to_analyze)} table(s)")
        
        # Analyze tables (parallel or sequential based on max_workers)
        if max_workers > 1:
            self._analyze_tables_parallel(tables_to_analyze, max_workers)
        else:
            self._analyze_tables_sequential(tables_to_analyze)
        
        # Generate summary
        summary = self._generate_summary()
        
        # Prepare result
        result = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(self.reports),
            'execution_time': time.time() - self.start_time,
            'reports': [r.to_dict() for r in self.reports],
            'summary': summary
        }
        
        # Display and/or save results
        self._output_results(result, output_format, output_file)
        
        return result
    
    def _collect_tables(self,
                       config_filter: Optional[str],
                       table_filter: Optional[str]) -> List[Tuple[str, str, Dict]]:
        """
        Collect all tables to analyze from configurations.
        
        Returns:
            List of tuples (config_path, table_name, file_config)
        """
        tables = []
        
        # Get all config files
        config_files = self._get_config_files(config_filter)
        
        for config_file in config_files:
            try:
                # Load config
                with open(config_file) as f:
                    config_data = json.load(f)
                
                # Extract tables
                for file_config in config_data.get('files', []):
                    table_name = file_config.get('table_name')
                    if not table_name:
                        continue
                    
                    # Apply table filter if specified
                    if table_filter and not fnmatch.fnmatch(table_name, table_filter):
                        continue
                    
                    tables.append((
                        str(config_file),
                        table_name,
                        file_config
                    ))
                    
            except Exception as e:
                self.logger.error(f"Failed to load config {config_file}: {e}")
        
        return tables
    
    def _get_config_files(self, config_filter: Optional[str]) -> List[Path]:
        """Get list of config files to process."""
        config_dir = Path(self.context.config_manager.config_dir)
        
        if config_filter:
            # Use glob pattern
            return list(config_dir.glob(config_filter))
        else:
            # All JSON files in config directory
            return list(config_dir.glob("*.json"))
    
    def _analyze_tables_sequential(self, tables: List[Tuple[str, str, Dict]]):
        """Analyze tables sequentially with a single connection."""
        total = len(tables)
        
        # Use single connection for all sequential operations
        with self.connection_manager.get_connection() as conn:
            for i, (config_path, table_name, file_config) in enumerate(tables, 1):
                self.logger.info(f"Analyzing {i}/{total}: {table_name}")
                
                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_progress(
                        items_processed=i,
                        total_items=total
                    )
                
                report = self._analyze_single_table(config_path, table_name, file_config, conn)
                self.reports.append(report)
    
    def _analyze_tables_parallel(self, tables: List[Tuple[str, str, Dict]], max_workers: int):
        """
        Analyze tables in parallel with optimized connection pooling.
        Each worker thread reuses a single connection for all its tasks.
        """
        total = len(tables)
        completed = 0
        
        # Create optimized connection pool
        self._connection_pool = OptimizedConnectionPool(self.connection_manager, self.logger)
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                futures = {}
                for config_path, table_name, file_config in tables:
                    future = executor.submit(
                        self._analyze_single_table_pooled,
                        config_path, table_name, file_config
                    )
                    futures[future] = (config_path, table_name)
                
                # Collect results as they complete
                for future in as_completed(futures):
                    config_path, table_name = futures[future]
                    completed += 1
                    
                    try:
                        report = future.result()
                        self.reports.append(report)
                        self.logger.info(f"Completed {completed}/{total}: {table_name}")
                        
                        # Update progress
                        if self.progress_tracker:
                            self.progress_tracker.update_progress(
                                items_processed=completed,
                                total_items=total
                            )
                            
                    except Exception as e:
                        self.logger.error(f"Failed to analyze {table_name}: {e}")
                        # Create error report
                        self.reports.append(TableReport(
                            config_file=Path(config_path).name,
                            table_name=table_name,
                            status='ERROR',
                            error_message=str(e)
                        ))
        finally:
            # Properly clean up ALL connections
            if self._connection_pool:
                self._connection_pool.close_all()
                self._connection_pool = None
    
    def _analyze_single_table_pooled(self,
                                    config_path: str,
                                    table_name: str,
                                    file_config: Dict) -> TableReport:
        """
        Analyze a single table using pooled connection.
        The connection is reused for many tables by the same thread.
        """
        # Get thread-local connection from pool (reused across many tables)
        conn = self._connection_pool.get_connection()
        return self._analyze_single_table(config_path, table_name, file_config, conn)
    
    def _analyze_single_table(self,
                             config_path: str,
                             table_name: str,
                             file_config: Dict,
                             connection) -> TableReport:
        """
        Analyze a single table and generate report.
        Uses parameterized queries to prevent SQL injection.
        
        Returns:
            TableReport with analysis results
        """
        start_time = time.time()
        
        report = TableReport(
            config_file=Path(config_path).name,
            table_name=table_name,
            date_column=file_config.get('date_column')
        )
        
        try:
            cursor = connection.cursor()
            
            # Check if table exists in current schema
            if not self._table_exists(cursor, table_name):
                report.status = 'TABLE_NOT_FOUND'
                report.validation_status = 'FAILED'
                report.validation_issues.append('Table does not exist in current schema')
                return report
            
            # Get basic table info with safe queries
            self._get_table_info(cursor, table_name, report)
            
            # If table is empty, skip validation
            if report.row_count == 0:
                report.status = 'EMPTY'
                report.validation_status = 'WARNING'
                report.validation_issues.append('Table is empty')
                return report
            
            # Run validation using our validator
            if report.date_column:
                validation_result = self.validator.validate_table(
                    table_name=table_name,
                    date_column=report.date_column,
                    duplicate_key_columns=file_config.get('duplicate_key_columns')
                )
                
                # Update report from validation
                self._update_report_from_validation(report, validation_result)
            
            report.status = 'SUCCESS'
            
        except Exception as e:
            report.status = 'ERROR'
            report.error_message = str(e)
            report.validation_status = 'FAILED'
            self.logger.error(f"Error analyzing {table_name}: {e}")
        
        finally:
            report.execution_time = time.time() - start_time
        
        return report
    
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
    
    def _get_table_info(self, cursor, table_name: str, report: TableReport):
        """Get basic table information using safe parameterized queries."""
        # Get row count using IDENTIFIER for safe table name handling
        cursor.execute(
            "SELECT COUNT(*) FROM IDENTIFIER(%s)",
            (table_name,)
        )
        report.row_count = cursor.fetchone()[0]
        
        # Get columns
        cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = CURRENT_SCHEMA()
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name.upper(),)
        )
        report.columns = [row[0] for row in cursor.fetchall()]
        report.column_count = len(report.columns)
        
        # Get date range if date column exists
        if report.date_column and report.row_count > 0:
            try:
                # Use IDENTIFIER for safe column and table name handling
                cursor.execute(
                    "SELECT MIN(IDENTIFIER(%s)), MAX(IDENTIFIER(%s)) FROM IDENTIFIER(%s)",
                    (report.date_column, report.date_column, table_name)
                )
                min_date, max_date = cursor.fetchone()
                report.date_range_start = self._format_date(min_date)
                report.date_range_end = self._format_date(max_date)
            except Exception as e:
                self.logger.warning(f"Could not get date range for {table_name}: {e}")
    
    def _update_report_from_validation(self, report: TableReport, validation_result):
        """Update report with validation results using configurable severity mapping."""
        report.unique_dates = validation_result.unique_dates
        report.expected_dates = validation_result.expected_dates
        report.avg_rows_per_day = validation_result.avg_rows_per_day
        
        # Missing dates and gaps
        if validation_result.missing_dates:
            report.missing_dates = validation_result.missing_dates[:10]  # Limit to 10
            report.validation_issues.append(
                f"{len(validation_result.missing_dates)} missing dates"
            )
        
        if validation_result.gaps:
            report.gaps = len(validation_result.gaps)
            report.validation_issues.append(f"{report.gaps} gaps in date sequence")
        
        # Anomalies with configurable severity
        if validation_result.anomalous_dates:
            report.anomalous_dates = len(validation_result.anomalous_dates)
            severities = [a['severity'] for a in validation_result.anomalous_dates]
            report.anomaly_severity = self.severity_config.get_anomaly_severity(severities)
            report.validation_issues.append(
                f"{report.anomalous_dates} anomalous dates ({report.anomaly_severity})"
            )
        
        # Duplicates with configurable severity
        if validation_result.duplicate_info:
            dup_info = validation_result.duplicate_info
            report.duplicate_keys = dup_info.get('duplicate_keys', 0)
            report.duplicate_rows = dup_info.get('excess_rows', 0)
            
            # Calculate duplicate ratio
            if report.row_count > 0:
                duplicate_ratio = report.duplicate_rows / report.row_count
                report.duplicate_severity = self.severity_config.get_duplicate_severity(duplicate_ratio)
            else:
                report.duplicate_severity = 'NONE'
            
            if report.duplicate_keys > 0:
                report.validation_issues.append(
                    f"{report.duplicate_keys} duplicate keys ({report.duplicate_severity})"
                )
        
        # Overall validation status
        if validation_result.valid:
            report.validation_status = 'PASSED'
        elif len(report.validation_issues) > 0:
            if report.anomaly_severity == 'CRITICAL' or report.duplicate_severity == 'CRITICAL':
                report.validation_status = 'FAILED'
            else:
                report.validation_status = 'WARNING'
        else:
            report.validation_status = 'PASSED'
    
    def _format_date(self, date_value) -> str:
        """Format date value to YYYY-MM-DD string."""
        if not date_value:
            return None
        
        date_str = str(date_value)
        
        # Handle YYYYMMDD format
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        return date_str
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics from all reports."""
        summary = {
            'total_tables': len(self.reports),
            'total_rows': sum(r.row_count for r in self.reports),
            'status_counts': {},
            'validation_counts': {},
            'tables_with_issues': [],
            'critical_issues': [],
            'largest_tables': [],
            'empty_tables': []
        }
        
        # Count by status
        for report in self.reports:
            status = report.status
            summary['status_counts'][status] = summary['status_counts'].get(status, 0) + 1
            
            val_status = report.validation_status
            summary['validation_counts'][val_status] = summary['validation_counts'].get(val_status, 0) + 1
            
            # Track issues
            if report.validation_issues:
                summary['tables_with_issues'].append({
                    'table': report.table_name,
                    'issues': report.validation_issues,
                    'severity': report.validation_status
                })
            
            # Track critical issues
            if report.validation_status == 'FAILED':
                summary['critical_issues'].append({
                    'table': report.table_name,
                    'reason': report.validation_issues or [report.error_message]
                })
            
            # Track empty tables
            if report.status == 'EMPTY':
                summary['empty_tables'].append(report.table_name)
        
        # Find largest tables
        sorted_by_size = sorted(
            [r for r in self.reports if r.row_count > 0],
            key=lambda x: x.row_count,
            reverse=True
        )
        summary['largest_tables'] = [
            {'table': r.table_name, 'rows': r.row_count}
            for r in sorted_by_size[:5]
        ]
        
        return summary
    
    def _output_results(self, result: Dict, output_format: str, output_file: Optional[str]):
        """
        Output results using formatter strategy pattern.
        Handles file extensions intelligently.
        """
        formats_to_generate = []
        
        if output_format == 'both':
            formats_to_generate = ['text', 'json']
        else:
            formats_to_generate = [output_format]
        
        for fmt in formats_to_generate:
            try:
                formatter = ReportFormatterFactory.create(fmt)
                formatted_output = formatter.format(result)
                
                # Display to console/logs for text format
                if fmt == 'text':
                    for line in formatted_output.split('\n'):
                        self.logger.info(line)
                
                # Save to file if specified
                if output_file:
                    output_path = Path(output_file)
                    
                    # Handle file extensions intelligently
                    if len(formats_to_generate) > 1:
                        # Multiple formats: ensure each has correct extension
                        output_path = output_path.with_suffix(formatter.get_file_extension())
                    else:
                        # Single format: warn if extension doesn't match
                        expected_ext = formatter.get_file_extension()
                        if output_path.suffix != expected_ext:
                            self.logger.warning(
                                f"File extension {output_path.suffix} doesn't match "
                                f"format {fmt} (expected {expected_ext})"
                            )
                    
                    output_path.write_text(formatted_output)
                    self.logger.info(f"{fmt.upper()} report saved to {output_path}")
                    
            except Exception as e:
                self.logger.error(f"Failed to generate {fmt} report: {e}")