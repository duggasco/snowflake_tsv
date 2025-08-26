"""
Validation operation for checking data quality in Snowflake tables.
Uses ApplicationContext for dependency injection.
"""

import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from pathlib import Path

from ..core.application_context import ApplicationContext, BaseOperation
from ..validators.snowflake_validator import SnowflakeDataValidator, ValidationResult
from ..core.progress import ProgressPhase


class ValidateOperation(BaseOperation):
    """
    Orchestrates data validation in Snowflake tables.
    Checks for completeness, anomalies, and duplicates.
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
        """
        super().__init__(context)
        
        # Initialize validator with injected dependencies
        self.validator = SnowflakeDataValidator(
            self.connection_manager,
            self.progress_tracker,
            self.logger
        )
    
    def validate_tables(self,
                       tables: Optional[List[str]] = None,
                       month: Optional[str] = None,
                       output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate data quality in specified tables.
        
        Args:
            tables: Optional list of specific tables to validate
            month: Optional month to validate (YYYY-MM format)
            output_file: Optional file path to save results
            
        Returns:
            Dictionary with validation results
        """
        results = {
            'timestamp': datetime.now().isoformat(),
            'month': month,
            'tables_validated': 0,
            'tables_valid': 0,
            'tables_invalid': 0,
            'validation_results': [],
            'summary': {}
        }
        
        # Get tables from config
        validation_targets = self._build_validation_targets(tables, month)
        
        if not validation_targets:
            self.logger.warning("No tables found to validate")
            return results
        
        self.logger.info(
            f"Validating {len(validation_targets)} table(s)"
            f"{f' for month {month}' if month else ' (all data)'}"
        )
        
        # Update progress phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
        
        # Validate each table
        for i, target in enumerate(validation_targets, 1):
            self.logger.info(
                f"Validating {i}/{len(validation_targets)}: {target['table_name']}"
            )
            
            try:
                validation_result = self.validator.validate_table(
                    table_name=target['table_name'],
                    date_column=target['date_column'],
                    start_date=target.get('start_date'),
                    end_date=target.get('end_date'),
                    duplicate_key_columns=target.get('duplicate_key_columns')
                )
                
                results['validation_results'].append(validation_result.to_dict())
                results['tables_validated'] += 1
                
                if validation_result.valid:
                    results['tables_valid'] += 1
                    self.logger.info(f"[VALID] {target['table_name']} is valid")
                else:
                    results['tables_invalid'] += 1
                    self.logger.warning(
                        f"[INVALID] {target['table_name']} has issues: "
                        f"{', '.join(validation_result.failure_reasons)}"
                    )
                
                # Display detailed validation results for this table
                self._display_table_validation_details(validation_result)
                
                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_progress(
                        items_processed=i,
                        total_items=len(validation_targets)
                    )
                    
            except Exception as e:
                self.logger.error(f"Failed to validate {target['table_name']}: {e}")
                results['validation_results'].append({
                    'table_name': target['table_name'],
                    'valid': False,
                    'error_message': str(e),
                    'failure_reasons': [f"Validation error: {e}"]
                })
                results['tables_invalid'] += 1
        
        # Generate summary
        results['summary'] = self._generate_summary(results['validation_results'])
        
        # Display summary
        self._display_summary(results)
        
        # Save to file if requested
        if output_file:
            self._save_results(results, output_file)
        
        return results
    
    def _build_validation_targets(self,
                                 tables: Optional[List[str]],
                                 month: Optional[str]) -> List[Dict]:
        """
        Build list of tables to validate from configuration.
        
        Returns:
            List of validation target dictionaries
        """
        targets = []
        config_data = self.context.config_manager.get_config()
        
        # Parse month to get date range if provided
        start_date = None
        end_date = None
        if month:
            year, mon = month.split('-')
            import calendar
            last_day = calendar.monthrange(int(year), int(mon))[1]
            start_date = f"{year}-{mon}-01"
            end_date = f"{year}-{mon}-{last_day:02d}"
        
        # Build targets from config
        for file_config in config_data.get('files', []):
            table_name = file_config.get('table_name')
            date_column = file_config.get('date_column')
            
            if not table_name or not date_column:
                continue
            
            # Filter by specific tables if provided
            if tables and table_name not in tables:
                continue
            
            target = {
                'table_name': table_name,
                'date_column': date_column,
                'duplicate_key_columns': file_config.get('duplicate_key_columns'),
                'start_date': start_date,
                'end_date': end_date
            }
            targets.append(target)
        
        return targets
    
    def _generate_summary(self, validation_results: List[Dict]) -> Dict:
        """
        Generate summary statistics from validation results.
        
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_rows': 0,
            'total_missing_dates': 0,
            'total_gaps': 0,
            'total_anomalies': 0,
            'total_duplicates': 0,
            'tables_with_issues': []
        }
        
        for result in validation_results:
            if 'total_rows' in result:
                summary['total_rows'] += result['total_rows']
            
            if result.get('missing_dates'):
                summary['total_missing_dates'] += len(result['missing_dates'])
            
            if result.get('gaps'):
                summary['total_gaps'] += len(result['gaps'])
            
            if result.get('anomalous_dates'):
                summary['total_anomalies'] += len(result['anomalous_dates'])
            
            if result.get('duplicate_info'):
                dup_info = result['duplicate_info']
                if dup_info.get('duplicate_keys', 0) > 0:
                    summary['total_duplicates'] += dup_info['duplicate_keys']
            
            # Track tables with issues
            if not result.get('valid', True):
                issue_summary = {
                    'table': result.get('table_name'),
                    'issues': result.get('failure_reasons', [])
                }
                summary['tables_with_issues'].append(issue_summary)
        
        return summary
    
    def _display_table_validation_details(self, validation_result):
        """Display detailed validation results for a single table."""
        # Display date range information
        if validation_result.date_range_start and validation_result.date_range_end:
            self.logger.info(f"  Date Range: {validation_result.date_range_start} to {validation_result.date_range_end}")
        
        # Display row statistics
        if hasattr(validation_result, 'total_rows') and validation_result.total_rows:
            self.logger.info(f"  Total Rows: {validation_result.total_rows:,}")
            
        if hasattr(validation_result, 'unique_dates') and validation_result.unique_dates:
            self.logger.info(f"  Unique Dates: {validation_result.unique_dates}")
            
        if hasattr(validation_result, 'avg_rows_per_day') and validation_result.avg_rows_per_day:
            self.logger.info(f"  Average Rows/Day: {validation_result.avg_rows_per_day:,.0f}")
        
        # Display anomalies if any
        if validation_result.anomalous_dates:
            self.logger.info(f"  Anomalous Dates: {len(validation_result.anomalous_dates)}")
            # Show first few anomalies
            for anomaly in validation_result.anomalous_dates[:5]:
                self.logger.info(
                    f"    - {anomaly['date']}: {anomaly['row_count']:,} rows "
                    f"({anomaly['severity']}, {anomaly['pct_of_avg']:.1f}% of average)"
                )
            if len(validation_result.anomalous_dates) > 5:
                self.logger.info(f"    ... and {len(validation_result.anomalous_dates) - 5} more")
        
        # Display gaps if any
        if validation_result.gaps:
            self.logger.info(f"  Date Gaps: {len(validation_result.gaps)}")
            for gap in validation_result.gaps[:3]:
                self.logger.info(f"    - {gap['gap_start']} to {gap['gap_end']} ({gap['days_missing']} days)")
            if len(validation_result.gaps) > 3:
                self.logger.info(f"    ... and {len(validation_result.gaps) - 3} more gaps")
        
        # Display duplicate info if available
        if hasattr(validation_result, 'duplicate_info') and validation_result.duplicate_info:
            dup_info = validation_result.duplicate_info
            if dup_info.get('duplicate_keys', 0) > 0:
                self.logger.info(
                    f"  Duplicates: {dup_info['duplicate_keys']} duplicate keys, "
                    f"{dup_info['excess_rows']} excess rows ({dup_info['duplicate_percentage']:.2f}%)"
                )
                # Show sample duplicate keys
                if dup_info.get('sample_keys'):
                    self.logger.info("    Sample duplicate keys:")
                    for sample in dup_info['sample_keys'][:3]:
                        key_str = ', '.join([f"{k}={v}" for k, v in sample.items() if k != 'duplicate_count'])
                        self.logger.info(f"      - {key_str} (appears {sample.get('duplicate_count', 2)} times)")
    
    def _display_summary(self, results: Dict):
        """Display validation summary to console/logs."""
        summary = results['summary']
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("VALIDATION SUMMARY")
        self.logger.info("=" * 60)
        
        self.logger.info(
            f"Tables Validated: {results['tables_validated']}\n"
            f"  Valid: {results['tables_valid']}\n"
            f"  Invalid: {results['tables_invalid']}"
        )
        
        if summary['total_rows'] > 0:
            self.logger.info(f"\nTotal Rows Analyzed: {summary['total_rows']:,}")
        
        if summary['total_missing_dates'] > 0:
            self.logger.info(f"Missing Dates Found: {summary['total_missing_dates']}")
        
        if summary['total_gaps'] > 0:
            self.logger.info(f"Date Gaps Found: {summary['total_gaps']}")
        
        if summary['total_anomalies'] > 0:
            self.logger.info(f"Anomalous Dates Found: {summary['total_anomalies']}")
        
        if summary['total_duplicates'] > 0:
            self.logger.info(f"Duplicate Keys Found: {summary['total_duplicates']}")
        
        if summary['tables_with_issues']:
            self.logger.info("\nTables with Issues:")
            for issue in summary['tables_with_issues']:
                self.logger.info(f"  - {issue['table']}:")
                for reason in issue['issues']:
                    self.logger.info(f"    - {reason}")
        
        self.logger.info("=" * 60)
    
    def _save_results(self, results: Dict, output_file: str):
        """Save validation results to file."""
        output_path = Path(output_file)
        
        try:
            # Create directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            self.logger.info(f"Validation results saved to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
    
    def validate_all_tables(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate all tables defined in configuration.
        
        Args:
            output_file: Optional file path to save results
            
        Returns:
            Dictionary with validation results
        """
        return self.validate_tables(
            tables=None,  # All tables
            month=None,   # All data
            output_file=output_file
        )
    
    def validate_month(self,
                      month: str,
                      tables: Optional[List[str]] = None,
                      output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate specific month of data.
        
        Args:
            month: Month to validate (YYYY-MM format)
            tables: Optional list of specific tables
            output_file: Optional file path to save results
            
        Returns:
            Dictionary with validation results
        """
        return self.validate_tables(
            tables=tables,
            month=month,
            output_file=output_file
        )
    
    def quick_validate(self, table_name: str) -> ValidationResult:
        """
        Quick validation of a single table.
        
        Args:
            table_name: Table to validate
            
        Returns:
            ValidationResult object
        """
        # Find table config
        config_data = self.context.config_manager.get_config()
        
        for file_config in config_data.get('files', []):
            if file_config.get('table_name') == table_name:
                return self.validator.validate_table(
                    table_name=table_name,
                    date_column=file_config.get('date_column'),
                    duplicate_key_columns=file_config.get('duplicate_key_columns')
                )
        
        # Table not in config, try basic validation
        self.logger.warning(f"Table {table_name} not in config, using basic validation")
        return self.validator.validate_table(
            table_name=table_name,
            date_column='recordDate',  # Common default
            duplicate_key_columns=None
        )