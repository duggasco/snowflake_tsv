#!/usr/bin/env python3
"""
Generate comprehensive report for all tables across all configs
Reuses existing SnowflakeDataValidator for consistency and performance
"""

import json
import os
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Import existing validator from tsv_loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tsv_loader import SnowflakeDataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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


class TableReportGenerator:
    """Generate comprehensive reports for all tables in all configs"""
    
    def __init__(self, config_dir: str = "config", creds_file: Optional[str] = None,
                 max_workers: int = 4):
        self.config_dir = Path(config_dir)
        self.creds_file = creds_file
        self.max_workers = max_workers
        self.reports: List[TableReport] = []
        self.start_time = time.time()
        self.validator = None
        self.conn = None
        
    def generate_all_reports(self, config_filter: Optional[str] = None,
                           table_filter: Optional[str] = None) -> Tuple[List[TableReport], str]:
        """
        Main method to generate reports for all tables
        Returns tuple of (reports_list, formatted_summary_string)
        """
        # Load credentials once
        sf_creds = self._load_credentials()
        if not sf_creds:
            logger.error("Failed to load Snowflake credentials")
            return [], "ERROR: No credentials available"
        
        # Establish single connection for reuse
        try:
            logger.info("Connecting to Snowflake...")
            self.conn = snowflake.connector.connect(**sf_creds)
            self.validator = SnowflakeDataValidator(sf_creds)
            logger.info("Snowflake connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return [], f"ERROR: Connection failed - {str(e)}"
        
        try:
            # Load all configs
            configs = self._load_all_configs(config_filter)
            logger.info(f"Loaded {len(configs)} configuration file(s)")
            
            # Build list of all tables to process
            all_tables = []
            for config_path, config_data in configs.items():
                for file_config in config_data.get('files', []):
                    table_name = file_config.get('table_name')
                    if table_name:
                        # Apply table filter if specified
                        if table_filter:
                            import fnmatch
                            if not fnmatch.fnmatch(table_name, table_filter):
                                continue
                        all_tables.append((config_path, table_name, file_config))
            
            logger.info(f"Found {len(all_tables)} table(s) to analyze")
            
            # Process tables in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                for config_path, table_name, file_config in all_tables:
                    future = executor.submit(
                        self._analyze_table_safe,
                        config_path, table_name, file_config
                    )
                    futures.append(future)
                
                # Collect results as they complete
                for future in as_completed(futures):
                    try:
                        report = future.result()
                        if report:
                            self.reports.append(report)
                    except Exception as e:
                        logger.error(f"Error processing table: {e}")
            
            # Generate summary
            summary = self._generate_summary()
            return self.reports, summary
            
        finally:
            # Clean up connections
            if self.validator:
                try:
                    self.validator.close()
                except:
                    pass
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
    
    def _load_credentials(self) -> Dict:
        """Load Snowflake credentials from file or environment"""
        # First try explicit credentials file
        if self.creds_file and os.path.exists(self.creds_file):
            logger.info(f"Loading credentials from {self.creds_file}")
            with open(self.creds_file) as f:
                creds = json.load(f)
                return creds.get('snowflake', creds)
        
        # Try default locations
        default_paths = [
            'snowflake_creds.json',
            'config/snowflake_creds.json',
            '.snowflake/credentials.json'
        ]
        
        for path in default_paths:
            if os.path.exists(path):
                logger.info(f"Loading credentials from {path}")
                with open(path) as f:
                    creds = json.load(f)
                    return creds.get('snowflake', creds)
        
        # Try environment variables
        env_creds = {}
        env_mapping = {
            'SNOWFLAKE_ACCOUNT': 'account',
            'SNOWFLAKE_USER': 'user',
            'SNOWFLAKE_PASSWORD': 'password',
            'SNOWFLAKE_WAREHOUSE': 'warehouse',
            'SNOWFLAKE_DATABASE': 'database',
            'SNOWFLAKE_SCHEMA': 'schema',
            'SNOWFLAKE_ROLE': 'role'
        }
        
        for env_var, key in env_mapping.items():
            value = os.environ.get(env_var)
            if value:
                env_creds[key] = value
        
        if env_creds and 'account' in env_creds and 'user' in env_creds:
            logger.info("Using credentials from environment variables")
            return env_creds
        
        # Last resort - check first config file for embedded creds (not recommended)
        for config_file in self.config_dir.glob("*.json"):
            with open(config_file) as f:
                config = json.load(f)
                if 'snowflake' in config:
                    logger.warning(f"Using embedded credentials from {config_file} (not recommended)")
                    return config['snowflake']
        
        return {}
    
    def _load_all_configs(self, config_filter: Optional[str] = None) -> Dict:
        """Load all JSON configs from config directory"""
        configs = {}
        
        pattern = config_filter if config_filter else "*.json"
        for config_file in self.config_dir.glob(pattern):
            # Skip credentials files
            if 'creds' in config_file.name.lower() or 'credentials' in config_file.name.lower():
                continue
                
            try:
                with open(config_file) as f:
                    config = json.load(f)
                    # Only include if it has file definitions
                    if 'files' in config:
                        configs[str(config_file)] = config
                        logger.debug(f"Loaded config: {config_file}")
            except Exception as e:
                logger.error(f"Failed to load {config_file}: {e}")
        
        return configs
    
    def _analyze_table_safe(self, config_path: str, table_name: str, 
                           file_config: Dict) -> Optional[TableReport]:
        """Safely analyze a table with error handling"""
        try:
            return self._analyze_table(config_path, table_name, file_config)
        except Exception as e:
            logger.error(f"Failed to analyze {table_name}: {e}")
            return TableReport(
                config_file=os.path.basename(config_path),
                table_name=table_name,
                status="ERROR",
                error_message=str(e)
            )
    
    def _analyze_table(self, config_path: str, table_name: str, 
                       file_config: Dict) -> TableReport:
        """Analyze a single table using existing validator"""
        start_time = time.time()
        
        report = TableReport(
            config_file=os.path.basename(config_path),
            table_name=table_name,
            status="PROCESSING"
        )
        
        try:
            cursor = self.conn.cursor()
            
            # Check if table exists and get column info
            column_query = f"""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE UPPER(table_name) = UPPER('{table_name}')
            AND table_catalog = CURRENT_DATABASE()
            AND table_schema = CURRENT_SCHEMA()
            ORDER BY ordinal_position
            """
            
            cursor.execute(column_query)
            columns_info = cursor.fetchall()
            
            if not columns_info:
                report.status = "TABLE_NOT_FOUND"
                report.error_message = f"Table {table_name} not found in current schema"
                return report
            
            # Extract column names
            report.columns = [row[0] for row in columns_info]
            report.column_count = len(report.columns)
            
            # Get date column from config
            date_column = file_config.get('date_column')
            report.date_column = date_column
            
            # Use existing validator for comprehensive validation
            if date_column:
                try:
                    # Run validation for ALL data (no date range specified)
                    validation_result = self.validator.validate_date_completeness(
                        table_name=table_name,
                        date_column=date_column,
                        start_date=None,  # Validate ALL data
                        end_date=None
                    )
                    
                    # Extract results from validation
                    if validation_result:
                        report.row_count = validation_result.get('total_rows', 0)
                        report.unique_dates = validation_result.get('unique_dates', 0)
                        
                        # Date range
                        date_range = validation_result.get('date_range', {})
                        if date_range:
                            report.date_range_start = date_range.get('start')
                            report.date_range_end = date_range.get('end')
                        
                        # Gaps and missing dates
                        gaps = validation_result.get('gaps', [])
                        report.gaps = len(gaps)
                        if gaps:
                            # Extract first few missing dates for report
                            for gap in gaps[:5]:  # Limit to first 5 gaps
                                if 'missing_dates' in gap:
                                    report.missing_dates.extend(gap['missing_dates'][:3])
                        
                        # Row count analysis
                        row_analysis = validation_result.get('row_count_analysis', {})
                        if row_analysis:
                            report.avg_rows_per_day = row_analysis.get('mean', 0)
                            
                            # Anomalies
                            anomalies = row_analysis.get('anomalous_dates', [])
                            report.anomalous_dates = len(anomalies)
                            
                            # Determine severity
                            if anomalies:
                                severely_low = sum(1 for a in anomalies 
                                                 if a.get('classification') == 'SEVERELY_LOW')
                                if severely_low > 10:
                                    report.anomaly_severity = "CRITICAL"
                                elif severely_low > 5:
                                    report.anomaly_severity = "HIGH"
                                elif len(anomalies) > 10:
                                    report.anomaly_severity = "MEDIUM"
                                else:
                                    report.anomaly_severity = "LOW"
                        
                        # Check for duplicates if config specifies key columns
                        duplicate_key_columns = file_config.get('duplicate_key_columns', [])
                        if duplicate_key_columns:
                            dup_result = self.validator.check_duplicates(
                                table_name=table_name,
                                key_columns=duplicate_key_columns,
                                date_column=date_column,
                                start_date=None,
                                end_date=None
                            )
                            
                            if dup_result and dup_result.get('has_duplicates'):
                                report.duplicate_keys = dup_result.get('duplicate_keys', 0)
                                report.duplicate_rows = dup_result.get('total_duplicates', 0)
                                report.duplicate_severity = dup_result.get('severity', 'LOW')
                        
                        # Determine overall validation status
                        if validation_result.get('valid'):
                            if report.anomalous_dates > 0 or report.duplicate_keys > 0:
                                report.validation_status = "WARNING"
                                if report.anomalous_dates > 0:
                                    report.validation_issues.append(
                                        f"{report.anomalous_dates} dates with anomalous row counts"
                                    )
                                if report.duplicate_keys > 0:
                                    report.validation_issues.append(
                                        f"{report.duplicate_keys} duplicate keys found"
                                    )
                            else:
                                report.validation_status = "PASSED"
                        else:
                            report.validation_status = "FAILED"
                            failure_reasons = validation_result.get('failure_reasons', [])
                            report.validation_issues.extend(failure_reasons)
                        
                except Exception as e:
                    logger.warning(f"Validation failed for {table_name}: {e}")
                    report.validation_status = "ERROR"
                    report.validation_issues.append(str(e))
            else:
                # No date column - just get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                report.row_count = cursor.fetchone()[0]
                report.validation_status = "SKIPPED"
                report.validation_issues.append("No date column configured")
            
            # Mark as successful if we got here
            if report.row_count == 0:
                report.status = "EMPTY"
            else:
                report.status = "SUCCESS"
            
        except ProgrammingError as e:
            # Specific Snowflake SQL errors
            error_msg = str(e)
            if 'does not exist' in error_msg.lower():
                report.status = "TABLE_NOT_FOUND"
                report.error_message = "Table does not exist"
            else:
                report.status = "ERROR"
                report.error_message = error_msg
        except Exception as e:
            report.status = "ERROR"
            report.error_message = str(e)
        
        finally:
            report.execution_time = time.time() - start_time
        
        return report
    
    def _generate_summary(self) -> str:
        """Generate formatted summary report"""
        total_time = time.time() - self.start_time
        
        # Calculate statistics
        total_configs = len(set(r.config_file for r in self.reports))
        total_tables = len(self.reports)
        successful = sum(1 for r in self.reports if r.status == "SUCCESS")
        failed = sum(1 for r in self.reports if r.status == "ERROR")
        not_found = sum(1 for r in self.reports if r.status == "TABLE_NOT_FOUND")
        empty = sum(1 for r in self.reports if r.status == "EMPTY")
        total_rows = sum(r.row_count for r in self.reports)
        
        # Count validation issues
        validation_passed = sum(1 for r in self.reports if r.validation_status == "PASSED")
        validation_warnings = sum(1 for r in self.reports if r.validation_status == "WARNING")
        validation_failed = sum(1 for r in self.reports if r.validation_status == "FAILED")
        
        # Build report
        lines = []
        lines.append("=" * 80)
        lines.append("                     SNOWFLAKE TABLES COMPREHENSIVE REPORT")
        lines.append(f"                          Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 80)
        lines.append("")
        lines.append("SUMMARY STATISTICS")
        lines.append("-" * 18)
        lines.append(f"Total Configurations: {total_configs}")
        lines.append(f"Total Tables Analyzed: {total_tables}")
        lines.append(f"  Successful: {successful}")
        lines.append(f"  Failed: {failed}")
        lines.append(f"  Not Found: {not_found}")
        lines.append(f"  Empty: {empty}")
        lines.append(f"Total Rows Processed: {total_rows:,}")
        lines.append("")
        lines.append(f"Validation Results:")
        lines.append(f"  Passed: {validation_passed}")
        lines.append(f"  Warnings: {validation_warnings}")
        lines.append(f"  Failed: {validation_failed}")
        lines.append("")
        lines.append("=" * 80)
        lines.append("DETAILED TABLE REPORTS")
        lines.append("=" * 80)
        
        # Group reports by config
        configs_dict = {}
        for report in self.reports:
            if report.config_file not in configs_dict:
                configs_dict[report.config_file] = []
            configs_dict[report.config_file].append(report)
        
        # Generate detailed reports
        config_num = 0
        for config_file, reports in sorted(configs_dict.items()):
            config_num += 1
            lines.append("")
            lines.append(f"[{config_num}] Config: {config_file}")
            lines.append("-" * 80)
            
            for report in sorted(reports, key=lambda r: r.table_name):
                lines.append("")
                lines.append(f"Table: {report.table_name}")
                lines.append(f"Status: {report.status}")
                
                if report.status == "SUCCESS":
                    lines.append("Statistics:")
                    lines.append(f"  - Total Rows: {report.row_count:,}")
                    lines.append(f"  - Columns: {report.column_count}")
                    
                    if report.date_range_start:
                        lines.append(f"  - Date Range: {report.date_range_start} to {report.date_range_end}")
                        lines.append(f"  - Unique Dates: {report.unique_dates}")
                        if report.avg_rows_per_day > 0:
                            lines.append(f"  - Avg Rows/Day: {report.avg_rows_per_day:,.0f}")
                    
                    lines.append("")
                    if report.validation_status == "PASSED":
                        lines.append(f"Validation Results: [PASSED]")
                    elif report.validation_status == "WARNING":
                        lines.append(f"Validation Results: [WARNING]")
                        for issue in report.validation_issues:
                            lines.append(f"  - {issue}")
                    elif report.validation_status == "FAILED":
                        lines.append(f"Validation Results: [FAILED]")
                        for issue in report.validation_issues:
                            lines.append(f"  - {issue}")
                    elif report.validation_status == "SKIPPED":
                        lines.append(f"Validation Results: [SKIPPED] - {report.validation_issues[0] if report.validation_issues else 'No date column'}")
                    
                    if report.gaps > 0:
                        lines.append(f"  - Gaps Detected: {report.gaps}")
                        if report.missing_dates:
                            lines.append(f"  - Sample Missing Dates: {', '.join(report.missing_dates[:5])}")
                    
                    if report.anomalous_dates > 0:
                        lines.append(f"  - Anomalous Dates: {report.anomalous_dates} (Severity: {report.anomaly_severity or 'N/A'})")
                    
                    if report.duplicate_keys > 0:
                        lines.append(f"  - Duplicate Keys: {report.duplicate_keys} (affecting {report.duplicate_rows} rows)")
                        lines.append(f"  - Duplicate Severity: {report.duplicate_severity}")
                    
                    # Show first few columns
                    if report.columns:
                        col_preview = report.columns[:5]
                        more = f"... ({len(report.columns) - 5} more)" if len(report.columns) > 5 else ""
                        lines.append(f"  Column Preview: {', '.join(col_preview)} {more}")
                
                elif report.status == "TABLE_NOT_FOUND":
                    lines.append(f"Error: {report.error_message or 'Table not found'}")
                
                elif report.status == "ERROR":
                    lines.append(f"Error: {report.error_message}")
                
                elif report.status == "EMPTY":
                    lines.append("Warning: Table exists but contains no data")
                
                lines.append("-" * 40)
        
        # Validation summary
        lines.append("")
        lines.append("=" * 80)
        lines.append("VALIDATION SUMMARY")
        lines.append("=" * 80)
        
        # Tables with issues
        issues = []
        for report in self.reports:
            if report.validation_status in ["WARNING", "FAILED"]:
                issue_summary = f"{report.table_name}: "
                issue_parts = []
                if report.gaps > 0:
                    issue_parts.append(f"{report.gaps} gaps")
                if report.anomalous_dates > 0:
                    issue_parts.append(f"{report.anomalous_dates} anomalies")
                if report.duplicate_keys > 0:
                    issue_parts.append(f"{report.duplicate_keys} duplicates")
                if report.validation_issues and report.validation_status == "FAILED":
                    issue_parts.append("validation failed")
                issue_summary += ", ".join(issue_parts)
                issues.append(issue_summary)
        
        if issues:
            lines.append(f"Tables with Issues ({len(issues)}):")
            for i, issue in enumerate(issues[:10], 1):  # Limit to first 10
                lines.append(f"  {i}. {issue}")
            if len(issues) > 10:
                lines.append(f"  ... and {len(issues) - 10} more")
        else:
            lines.append("No validation issues found - all tables healthy!")
        
        lines.append("")
        lines.append("Recommended Actions:")
        
        if validation_failed > 0:
            lines.append("  - Investigate tables with failed validation immediately")
        if sum(1 for r in self.reports if r.gaps > 0) > 0:
            lines.append("  - Review date gaps - may indicate missing data loads")
        if sum(1 for r in self.reports if r.duplicate_severity in ["HIGH", "CRITICAL"]) > 0:
            lines.append("  - Address critical duplicate issues")
        if sum(1 for r in self.reports if r.anomaly_severity in ["HIGH", "CRITICAL"]) > 0:
            lines.append("  - Investigate critical row count anomalies")
        if not_found > 0:
            lines.append("  - Verify table creation for missing tables")
        if empty > 0:
            lines.append("  - Check data loading for empty tables")
        
        lines.append("")
        lines.append("=" * 80)
        lines.append(f"Report Complete - Execution Time: {total_time:.1f} seconds")
        lines.append("=" * 80)
        
        return "\n".join(lines)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generate comprehensive report for all Snowflake tables'
    )
    parser.add_argument('--config-dir', default='config',
                       help='Directory containing config files (default: config)')
    parser.add_argument('--creds', '--credentials',
                       help='Path to Snowflake credentials file')
    parser.add_argument('--config-filter',
                       help='Filter config files (e.g., "fact*.json")')
    parser.add_argument('--table-filter',
                       help='Filter table names (e.g., "FACT*")')
    parser.add_argument('--output', '-o',
                       help='Output file for report (default: stdout)')
    parser.add_argument('--format', choices=['text', 'json', 'both'],
                       default='text',
                       help='Output format (default: text)')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Maximum parallel workers (default: 4)')
    
    args = parser.parse_args()
    
    # Create generator
    generator = TableReportGenerator(
        config_dir=args.config_dir,
        creds_file=args.creds,
        max_workers=args.max_workers
    )
    
    # Generate reports
    print("Generating table reports...", file=sys.stderr)
    reports, summary = generator.generate_all_reports(
        config_filter=args.config_filter,
        table_filter=args.table_filter
    )
    
    # Output results
    if args.format in ['text', 'both']:
        if args.output:
            with open(args.output, 'w') as f:
                f.write(summary)
            print(f"Report written to {args.output}", file=sys.stderr)
        else:
            print(summary)
    
    if args.format in ['json', 'both']:
        json_output = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_tables': len(reports),
                'successful': sum(1 for r in reports if r.status == "SUCCESS"),
                'failed': sum(1 for r in reports if r.status in ["ERROR", "TABLE_NOT_FOUND"]),
                'total_rows': sum(r.row_count for r in reports)
            },
            'reports': [asdict(r) for r in reports]
        }
        
        json_file = args.output + '.json' if args.output and args.format == 'both' else args.output
        if json_file:
            with open(json_file, 'w') as f:
                json.dump(json_output, f, indent=2)
            print(f"JSON report written to {json_file}", file=sys.stderr)
        else:
            print(json.dumps(json_output, indent=2))
    
    # Return exit code based on failures
    if any(r.status == "ERROR" for r in reports):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())