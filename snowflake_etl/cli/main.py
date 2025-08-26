#!/usr/bin/env python3
"""
Main CLI Entry Point for Snowflake ETL Pipeline
Single entry point for all operations, replacing individual script calls
"""

import argparse
import calendar
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from snowflake_etl.core.application_context import ApplicationContext


class SnowflakeETLCLI:
    """
    Main CLI handler for Snowflake ETL operations
    """
    
    def __init__(self):
        """Initialize CLI"""
        self.context = None
        self.logger = None
        
    def parse_args(self, args=None):
        """
        Parse command line arguments
        
        Args:
            args: Arguments to parse (defaults to sys.argv)
            
        Returns:
            Parsed arguments
        """
        parser = argparse.ArgumentParser(
            description='Snowflake ETL Pipeline - Unified CLI',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        # Global options
        parser.add_argument(
            '--config', '-c',
            type=str,
            required=True,
            help='Path to configuration file'
        )
        parser.add_argument(
            '--log-dir',
            type=str,
            default='logs',
            help='Directory for log files (default: logs)'
        )
        parser.add_argument(
            '--log-level',
            type=str,
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            default='INFO',
            help='Logging level (default: INFO)'
        )
        parser.add_argument(
            '--quiet', '-q',
            action='store_true',
            help='Suppress console output'
        )
        
        # Create subparsers for different operations
        subparsers = parser.add_subparsers(
            dest='operation',
            help='Operation to perform'
        )
        
        # Load operation
        load_parser = subparsers.add_parser('load', help='Load TSV files to Snowflake')
        load_parser.add_argument('--base-path', type=str, help='Base path for TSV files')
        load_parser.add_argument('--month', type=str, help='Month to process (YYYY-MM)')
        load_parser.add_argument('--skip-qc', action='store_true', help='Skip quality checks')
        load_parser.add_argument('--validate-in-snowflake', action='store_true', 
                                help='Validate in Snowflake instead of file QC')
        load_parser.add_argument('--max-workers', type=int, help='Maximum parallel workers')
        
        # Delete operation
        delete_parser = subparsers.add_parser('delete', help='Delete data from Snowflake')
        delete_parser.add_argument('--table', type=str, required=True, help='Table name')
        delete_parser.add_argument('--month', type=str, required=True, help='Month to delete (YYYY-MM)')
        delete_parser.add_argument('--dry-run', action='store_true', help='Preview without deleting')
        delete_parser.add_argument('--yes', action='store_true', help='Skip confirmation')
        
        # Validate operation
        validate_parser = subparsers.add_parser('validate', help='Validate data in Snowflake')
        validate_parser.add_argument('--table', type=str, help='Table to validate')
        validate_parser.add_argument('--month', type=str, help='Month to validate')
        validate_parser.add_argument('--output', type=str, help='Output file for results')
        
        # Report operation
        report_parser = subparsers.add_parser('report', help='Generate table report')
        report_parser.add_argument('--tables', type=str, help='Comma-separated table names')
        report_parser.add_argument('--output', type=str, help='Output file for report')
        
        # Check duplicates operation
        dup_parser = subparsers.add_parser('check-duplicates', help='Check for duplicate records')
        dup_parser.add_argument('--table', type=str, required=True, help='Table to check')
        dup_parser.add_argument('--key-columns', type=str, help='Comma-separated key columns')
        dup_parser.add_argument('--date-range', type=str, help='Date range to check')
        
        # Compare files operation
        compare_parser = subparsers.add_parser('compare', help='Compare TSV files')
        compare_parser.add_argument('file1', help='First file to compare')
        compare_parser.add_argument('file2', help='Second file to compare')
        compare_parser.add_argument('--quick', action='store_true', help='Quick comparison mode')
        
        return parser.parse_args(args)
    
    def initialize_context(self, args):
        """
        Initialize application context from arguments
        
        Args:
            args: Parsed command line arguments
        """
        self.context = ApplicationContext(
            config_path=args.config,
            log_dir=Path(args.log_dir),
            log_level=args.log_level,
            quiet=args.quiet
        )
        self.logger = logging.getLogger('snowflake_etl.cli')
        
    def execute_load(self, args) -> int:
        """Execute load operation"""
        self.logger.info(f"Executing load operation")
        
        # Import operation module
        from ..operations.load_operation import LoadOperation
        from ..models.file_config import FileConfig
        
        # Create operation with injected context
        operation = LoadOperation(self.context)
        
        # Build file configs from arguments
        files = self._build_file_configs(args.base_path, args.month)
        
        if not files:
            self.logger.error("No files found to process")
            return 1
        
        # Execute load operation
        result = operation.load_files(
            files=files,
            skip_qc=args.skip_qc,
            validate_in_snowflake=args.validate_in_snowflake,
            validate_only=False,
            max_workers=args.max_workers
        )
        
        # Display summary
        self.logger.info(
            f"Load complete: {result['files_processed']} processed, "
            f"{result['files_failed']} failed, "
            f"{result['total_rows_loaded']:,} rows loaded"
        )
        
        return 0 if result['files_failed'] == 0 else 1
    
    def _build_file_configs(self, base_path: str, month: str) -> list:
        """Build file configurations from base path and month"""
        import re
        
        configs = []
        config_data = self.context.config_manager.get_config()
        
        if not base_path or not month:
            self.logger.warning("Base path and month required for load operation")
            return configs
        
        base = Path(base_path)
        
        # Find matching files for the month
        for file_config in config_data.get('files', []):
            pattern = file_config.get('file_pattern', '')
            
            # Parse month to get year and month integers
            year_str, mon_str = month.split('-')
            year_int = int(year_str)
            mon_int = int(mon_str)
            last_day = calendar.monthrange(year_int, mon_int)[1]
            
            # Replace placeholders with actual values
            if '{month}' in pattern:
                file_pattern = pattern.replace('{month}', month)
            elif '{date_range}' in pattern:
                # Convert month to date range (assuming full month)
                date_range = f"{year_str}{mon_str}01-{year_str}{mon_str}{last_day:02d}"
                file_pattern = pattern.replace('{date_range}', date_range)
            else:
                continue
            
            # Find matching files
            for file_path in base.glob(file_pattern):
                if file_path.is_file():
                    config = FileConfig(
                        file_path=str(file_path),
                        table_name=file_config['table_name'],
                        date_column=file_config.get('date_column'),
                        expected_columns=file_config.get('expected_columns', []),
                        duplicate_key_columns=file_config.get('duplicate_key_columns'),
                        expected_date_range=(
                            datetime(year_int, mon_int, 1),
                            datetime(year_int, mon_int, last_day)
                        )
                    )
                    configs.append(config)
        
        return configs
    
    def execute_delete(self, args) -> int:
        """Execute delete operation"""
        self.logger.info(f"Executing delete operation on {args.table} for {args.month}")
        
        # Import operation module
        from ..operations.delete_operation import DeleteOperation, DeletionTarget
        
        # Create operation with injected context
        operation = DeleteOperation(self.context)
        
        # Parse tables if provided
        tables = [args.table] if args.table else None
        
        # Execute deletion
        results = operation.delete_from_config(
            month=args.month,
            tables=tables,
            dry_run=args.dry_run,
            preview=False,  # Could add as CLI arg
            skip_confirmation=args.yes
        )
        
        # Check for failures
        failed = sum(1 for r in results if r.status == 'failed')
        return 0 if failed == 0 else 1
    
    def execute_validate(self, args) -> int:
        """Execute validate operation"""
        self.logger.info(f"Executing validation")
        
        # Import operation module
        from ..operations.validate_operation import ValidateOperation
        
        # Create operation with injected context
        operation = ValidateOperation(self.context)
        
        # Parse tables if provided
        tables = [args.table] if args.table else None
        
        # Execute validation
        result = operation.validate_tables(
            tables=tables,
            month=args.month,
            output_file=args.output
        )
        
        return 0 if result['tables_invalid'] == 0 else 1
    
    def execute_report(self, args) -> int:
        """Execute report operation"""
        self.logger.info(f"Executing report generation")
        
        # Import operation module
        from snowflake_etl.operations.reporter import ReportOperation
        
        # Create operation with injected context
        operation = ReportOperation(self.context)
        
        # Parse tables if provided
        tables = args.tables.split(',') if args.tables else None
        
        # Execute with parameters
        result = operation.execute(
            tables=tables,
            output_file=args.output
        )
        
        return 0 if result else 1
    
    def execute_check_duplicates(self, args) -> int:
        """Execute duplicate check operation"""
        self.logger.info(f"Checking duplicates in {args.table}")
        
        # Import operation module
        from snowflake_etl.operations.duplicate_checker import DuplicateCheckOperation
        
        # Create operation with injected context
        operation = DuplicateCheckOperation(self.context)
        
        # Parse key columns if provided
        key_columns = args.key_columns.split(',') if args.key_columns else None
        
        # Execute with parameters
        result = operation.execute(
            table=args.table,
            key_columns=key_columns,
            date_range=args.date_range
        )
        
        return 0 if result else 1
    
    def execute_compare(self, args) -> int:
        """Execute file comparison operation"""
        self.logger.info(f"Comparing {args.file1} and {args.file2}")
        
        # Import operation module
        from snowflake_etl.operations.file_comparator import CompareOperation
        
        # Create operation with injected context
        operation = CompareOperation(self.context)
        
        # Execute with parameters
        result = operation.execute(
            file1=args.file1,
            file2=args.file2,
            quick_mode=args.quick
        )
        
        return 0 if result else 1
    
    def run(self, args=None) -> int:
        """
        Main entry point
        
        Args:
            args: Command line arguments
            
        Returns:
            Exit code (0 for success)
        """
        try:
            # Parse arguments
            parsed_args = self.parse_args(args)
            
            # Check if operation was specified
            if not parsed_args.operation:
                print("Error: No operation specified. Use -h for help.")
                return 1
            
            # Initialize context
            self.initialize_context(parsed_args)
            
            # Use context manager for cleanup
            with self.context:
                # Route to appropriate operation
                operation_handlers = {
                    'load': self.execute_load,
                    'delete': self.execute_delete,
                    'validate': self.execute_validate,
                    'report': self.execute_report,
                    'check-duplicates': self.execute_check_duplicates,
                    'compare': self.execute_compare
                }
                
                handler = operation_handlers.get(parsed_args.operation)
                if handler:
                    return handler(parsed_args)
                else:
                    self.logger.error(f"Unknown operation: {parsed_args.operation}")
                    return 1
                    
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            return 130
        except Exception as e:
            if self.logger:
                self.logger.error(f"Operation failed: {e}", exc_info=True)
            else:
                print(f"Error: {e}")
            return 1


def main():
    """Main entry point"""
    cli = SnowflakeETLCLI()
    sys.exit(cli.run())


if __name__ == '__main__':
    main()