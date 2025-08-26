#!/usr/bin/env python3
"""
Package entry point for snowflake_etl.
Allows the package to be run as: python -m snowflake_etl
"""

import sys
import argparse
import logging
import calendar
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Import the application context and operations
from .core.application_context import ApplicationContext
from .models.file_config import FileConfig
from .operations.load_operation import LoadOperation
from .operations.delete_operation import DeleteOperation
from .operations.validate_operation import ValidateOperation
from .operations.report_operation import ReportOperation
from .operations.duplicate_check_operation import DuplicateCheckOperation
from .operations.compare_operation import CompareOperation
from .operations.utilities import (
    CheckTableOperation,
    DiagnoseErrorOperation,
    ValidateFileOperation,
    CheckStageOperation,
    FileBrowserOperation,
    GenerateReportOperation,
    TSVSamplerOperation
)
from .operations.config import (
    GenerateConfigOperation,
    ValidateConfigOperation,
    MigrateConfigOperation
)


def setup_logging(log_level: str, log_dir: str, quiet: bool) -> logging.Logger:
    """Set up logging configuration."""
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('snowflake_etl')
    logger.setLevel(getattr(logging, log_level))
    
    # File handler - always enabled
    file_handler = logging.FileHandler(
        log_dir_path / 'snowflake_etl.log'
    )
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(file_handler)
    
    # Console handler - only if not quiet
    if not quiet:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(levelname)s - %(message)s')
        )
        logger.addHandler(console_handler)
    
    return logger


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog='snowflake_etl',
        description='Snowflake ETL Pipeline - Unified CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global options
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration file (required for most operations)'
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
        help='Suppress console output (progress bars still shown)'
    )
    
    # Create subparsers for different operations
    subparsers = parser.add_subparsers(
        dest='operation',
        help='Operation to perform',
        required=True
    )
    
    # Load operation
    load_parser = subparsers.add_parser('load', help='Load TSV files to Snowflake')
    load_parser.add_argument('--base-path', type=str, help='Base path for TSV files')
    load_parser.add_argument('--month', type=str, help='Month to process (YYYY-MM)')
    load_parser.add_argument('--files', type=str, help='Comma-separated list of TSV files to process directly')
    load_parser.add_argument('--file-pattern', type=str, help='Pattern to match files')
    load_parser.add_argument('--skip-qc', action='store_true', help='Skip quality checks')
    load_parser.add_argument('--validate-in-snowflake', action='store_true', 
                            help='Validate in Snowflake instead of file QC')
    load_parser.add_argument('--validate-only', action='store_true',
                            help='Only validate existing data, no loading')
    load_parser.add_argument('--max-workers', type=int, default=4, help='Maximum parallel workers')
    
    # Delete operation
    delete_parser = subparsers.add_parser('delete', help='Delete data from Snowflake')
    delete_parser.add_argument('--table', type=str, required=True, help='Table name')
    delete_parser.add_argument('--month', type=str, help='Month to delete (YYYY-MM)')
    delete_parser.add_argument('--date-column', type=str, default='recordDate', 
                              help='Date column for filtering')
    delete_parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    delete_parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    delete_parser.add_argument('--dry-run', action='store_true', help='Preview without deleting')
    delete_parser.add_argument('--yes', action='store_true', help='Skip confirmation')
    
    # Validate operation
    validate_parser = subparsers.add_parser('validate', help='Validate data in Snowflake')
    validate_parser.add_argument('--table', type=str, help='Table to validate (optional, validates all if not specified)')
    validate_parser.add_argument('--date-column', type=str, default='recordDate',
                                help='Date column for validation')
    validate_parser.add_argument('--month', type=str, help='Month to validate (YYYY-MM)')
    validate_parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    validate_parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    validate_parser.add_argument('--output', type=str, help='Output file for results')
    
    # Report operation
    report_parser = subparsers.add_parser('report', help='Generate comprehensive table report')
    report_parser.add_argument('--config-filter', type=str, 
                              help='Filter config files (glob pattern)')
    report_parser.add_argument('--table-filter', type=str,
                              help='Filter table names (glob pattern)')
    report_parser.add_argument('--max-workers', type=int, default=4,
                              help='Number of parallel workers')
    report_parser.add_argument('--output-format', type=str, 
                              choices=['text', 'json', 'csv', 'both'],
                              default='both',
                              help='Output format (default: both text and json)')
    report_parser.add_argument('--output', type=str, help='Output file path')
    
    # Duplicate check operation
    dup_parser = subparsers.add_parser('check-duplicates', 
                                       help='Check for duplicate records')
    dup_parser.add_argument('--table', type=str, required=True, help='Table to check')
    dup_parser.add_argument('--key-columns', type=str, required=True,
                           help='Comma-separated list of key columns')
    dup_parser.add_argument('--date-column', type=str, help='Date column for filtering')
    dup_parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    dup_parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    dup_parser.add_argument('--sample-limit', type=int, default=10,
                           help='Number of sample duplicates to show')
    dup_parser.add_argument('--output', type=str, help='Output file for results')
    
    # Compare operation
    compare_parser = subparsers.add_parser('compare', help='Compare two TSV files')
    compare_parser.add_argument('--file1', type=str, required=True, 
                               help='First file path (good file)')
    compare_parser.add_argument('--file2', type=str, required=True,
                               help='Second file path (bad file)')
    compare_parser.add_argument('--quick', action='store_true',
                               help='Use sampling for faster analysis')
    compare_parser.add_argument('--sample-size', type=int, default=100,
                               help='Sample size in MB for quick mode')
    compare_parser.add_argument('--output', type=str, help='Output file for results')
    
    # Utilities subcommands
    # Check table utility
    check_table_parser = subparsers.add_parser('check-table', 
                                               help='Check if Snowflake table exists and show column info')
    check_table_parser.add_argument('table', type=str, help='Table name to check')
    
    # Diagnose error utility
    diagnose_parser = subparsers.add_parser('diagnose-error',
                                           help='Diagnose Snowflake COPY errors')
    diagnose_parser.add_argument('--table', type=str, help='Table name to filter errors')
    diagnose_parser.add_argument('--hours', type=int, default=24,
                                help='Hours to look back for errors (default: 24)')
    
    # Validate file utility
    validate_file_parser = subparsers.add_parser('validate-file',
                                                 help='Validate TSV file structure')
    validate_file_parser.add_argument('file', type=str, help='TSV file to validate')
    validate_file_parser.add_argument('--expected-columns', type=int,
                                     help='Expected number of columns')
    validate_file_parser.add_argument('--sample-rows', type=int, default=10,
                                     help='Number of rows to sample (default: 10)')
    
    # Check stage utility
    check_stage_parser = subparsers.add_parser('check-stage',
                                              help='Check and manage Snowflake stage files')
    check_stage_parser.add_argument('--pattern', type=str, default='*',
                                   help='File pattern to match (default: *)')
    check_stage_parser.add_argument('--clean', action='store_true',
                                   help='Clean old stage files')
    
    # File browser utility
    browse_parser = subparsers.add_parser('browse',
                                         help='Interactive TSV file browser')
    browse_parser.add_argument('--start-dir', type=str, default='.',
                             help='Starting directory (default: current)')
    
    # Generate report utility
    gen_report_parser = subparsers.add_parser('generate-report',
                                             help='Generate comprehensive table report')
    gen_report_parser.add_argument('--format', type=str, default='text',
                                  choices=['text', 'json', 'csv'],
                                  help='Output format (default: text)')
    
    # TSV sampler utility
    sample_parser = subparsers.add_parser('sample-file',
                                         help='Sample and analyze TSV file')
    sample_parser.add_argument('file', type=str, help='TSV file to sample')
    sample_parser.add_argument('--rows', type=int, default=100,
                             help='Number of rows to sample (default: 100)')
    
    # Configuration management subcommands
    # Generate config
    gen_config_parser = subparsers.add_parser('config-generate',
                                             help='Generate configuration from TSV files')
    gen_config_parser.add_argument('files', nargs='+', help='TSV files to analyze')
    gen_config_parser.add_argument('--output', '-o', type=str,
                                  help='Output configuration file')
    gen_config_parser.add_argument('--table', '-t', type=str,
                                  help='Snowflake table name for column info')
    gen_config_parser.add_argument('--headers', type=str,
                                  help='Comma-separated column headers')
    gen_config_parser.add_argument('--base-path', type=str, default='.',
                                  help='Base path for file patterns')
    gen_config_parser.add_argument('--date-column', type=str, default='RECORDDATEID',
                                  help='Date column name')
    gen_config_parser.add_argument('--merge', type=str,
                                  help='Merge with existing config file')
    gen_config_parser.add_argument('--interactive', '-i', action='store_true',
                                  help='Interactive mode for credentials')
    gen_config_parser.add_argument('--dry-run', action='store_true',
                                  help='Show what would be generated')
    
    # Validate config
    val_config_parser = subparsers.add_parser('config-validate',
                                             help='Validate configuration file')
    val_config_parser.add_argument('config_file', type=str,
                                  help='Configuration file to validate')
    val_config_parser.add_argument('--test-connection', action='store_true',
                                  help='Test Snowflake connection')
    
    # Migrate config
    mig_config_parser = subparsers.add_parser('config-migrate',
                                             help='Migrate configuration to new version')
    mig_config_parser.add_argument('config_file', type=str,
                                  help='Configuration file to migrate')
    mig_config_parser.add_argument('--target-version', type=str, default='3.0',
                                  help='Target version (default: 3.0)')
    mig_config_parser.add_argument('--no-backup', action='store_true',
                                  help='Do not create backup')
    
    return parser


def main(args=None):
    """Main entry point for the CLI."""
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args(args)
    
    # Setup logging
    logger = setup_logging(args.log_level, args.log_dir, args.quiet)
    
    try:
        # Determine if operation requires config
        config_optional_ops = ['config-generate', 'config-validate', 'config-migrate', 
                              'validate-file', 'sample-file']
        
        # Create application context if needed
        context = None
        if args.config or args.operation not in config_optional_ops:
            if not args.config:
                logger.error("Config file is required for this operation")
                parser.error("--config is required for this operation")
            
            logger.info(f"Initializing application context with config: {args.config}")
            context = ApplicationContext(
                config_path=args.config,
                log_dir=Path(args.log_dir),
                log_level=args.log_level,
                quiet=args.quiet
            )
        
        # Route to appropriate operation
        if args.operation == 'load':
            logger.info("Starting load operation")
            operation = LoadOperation(context)
            
            # Build file configurations
            files = []
            
            # Handle direct files if provided
            if args.files:
                # Process comma-separated list of files
                file_paths = [f.strip() for f in args.files.split(',')]
                config_data = context.config_manager.get_config()
                
                for file_path_str in file_paths:
                    file_path = Path(file_path_str)
                    if file_path.exists() and file_path.is_file():
                        # Find matching config for this file
                        for file_config in config_data.get('files', []):
                            pattern = file_config.get('file_pattern', '')
                            # Extract just the pattern part without placeholders for matching
                            base_pattern = pattern.replace('{date_range}', '*').replace('{month}', '*')
                            
                            if file_path.match(base_pattern):
                                # Extract date range from filename if possible
                                filename = file_path.name
                                expected_date_range = None
                                
                                # Try to extract dates from filename
                                date_range_match = re.search(r'(\d{8})-(\d{8})', filename)
                                if date_range_match:
                                    start_str, end_str = date_range_match.groups()
                                    start_year = start_str[:4]
                                    start_month = start_str[4:6]
                                    start_day = start_str[6:8]
                                    end_year = end_str[:4]
                                    end_month = end_str[4:6]
                                    end_day = end_str[6:8]
                                    expected_date_range = (
                                        f"{start_year}-{start_month}-{start_day}",
                                        f"{end_year}-{end_month}-{end_day}"
                                    )
                                
                                config = FileConfig(
                                    file_path=str(file_path),
                                    table_name=file_config['table_name'],
                                    date_column=file_config.get('date_column'),
                                    expected_columns=file_config.get('expected_columns', []),
                                    duplicate_key_columns=file_config.get('duplicate_key_columns'),
                                    expected_date_range=expected_date_range
                                )
                                files.append(config)
                                logger.info(f"Added direct file: {file_path}")
                                break
                    else:
                        logger.warning(f"File not found or not a file: {file_path_str}")
                        
            # Otherwise use base_path and month pattern matching
            elif args.base_path and args.month:
                config_data = context.config_manager.get_config()
                base = Path(args.base_path)
                
                # Find matching files for the month
                for file_config in config_data.get('files', []):
                    pattern = file_config.get('file_pattern', '')
                    
                    # Replace placeholders with actual values
                    if '{month}' in pattern:
                        file_pattern = pattern.replace('{month}', args.month)
                    elif '{date_range}' in pattern:
                        # Convert month to date range
                        year, mon = args.month.split('-')
                        year_int = int(year)
                        mon_int = int(mon)
                        last_day = calendar.monthrange(year_int, mon_int)[1]
                        date_range = f"{year}{mon}01-{year}{mon}{last_day:02d}"
                        file_pattern = pattern.replace('{date_range}', date_range)
                    else:
                        continue
                    
                    # Find matching files
                    for file_path in base.glob(file_pattern):
                        if file_path.is_file():
                            year, mon = args.month.split('-')
                            last_day = calendar.monthrange(int(year), int(mon))[1]
                            config = FileConfig(
                                file_path=str(file_path),
                                table_name=file_config['table_name'],
                                date_column=file_config.get('date_column'),
                                expected_columns=file_config.get('expected_columns', []),
                                duplicate_key_columns=file_config.get('duplicate_key_columns'),
                                expected_date_range=(
                                    f"{year}-{mon}-01",
                                    f"{year}-{mon}-{last_day:02d}"
                                )
                            )
                            files.append(config)
            
            if not files:
                logger.error("No files found to process")
                return 1
            
            result = operation.load_files(
                files=files,
                skip_qc=args.skip_qc,
                validate_in_snowflake=args.validate_in_snowflake,
                validate_only=args.validate_only,
                max_workers=args.max_workers
            )
            
            # Check for failures in load result
            if result.get('files_failed', 0) > 0:
                logger.warning(f"Load completed with {result['files_failed']} failures")
                return 1
            
        elif args.operation == 'delete':
            logger.info("Starting delete operation")
            operation = DeleteOperation(context)
            
            # Use delete_from_config which handles month conversion and gets date_column from config
            result = operation.delete_from_config(
                month=args.month,
                tables=[args.table] if args.table else None,
                dry_run=args.dry_run,
                skip_confirmation=args.yes
            )
            
        elif args.operation == 'validate':
            logger.info("Starting validate operation")
            operation = ValidateOperation(context)
            
            # If table specified, validate single table; otherwise validate all
            tables = [args.table] if args.table else None
            
            # Always generate output files for comprehensive validation details
            output_file = args.output
            if not output_file:
                # Auto-generate output filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                table_suffix = f"_{args.table}" if args.table else "_all"
                month_suffix = f"_{args.month.replace('-', '')}" if args.month else ""
                output_file = f"reports/validation{table_suffix}{month_suffix}_{timestamp}.json"
                Path("reports").mkdir(exist_ok=True)
                logger.info(f"Auto-generating detailed validation files: {output_file} and _issues.txt")
            
            # Use the validate_tables method which handles both single and multiple tables
            result = operation.validate_tables(
                tables=tables,
                month=args.month,
                output_file=output_file
            )
            
            # Return non-zero exit code if validation failed
            if result.get('tables_invalid', 0) > 0:
                return 1
            
        elif args.operation == 'report':
            logger.info("Starting report operation")
            operation = ReportOperation(context)
            
            # Always generate output files for comprehensive details
            output_file = args.output
            if not output_file:
                # Auto-generate output filename with timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f"reports/table_report_{timestamp}"
                Path("reports").mkdir(exist_ok=True)
                logger.info(f"Auto-generating detailed report files: {output_file}.[txt/json]")
            
            # Always use 'both' format for maximum detail when no output specified
            output_format = args.output_format
            if not args.output:
                output_format = 'both'  # Ensure we get both text and JSON
            
            result = operation.generate_full_report(
                config_filter=args.config_filter,
                table_filter=args.table_filter,
                max_workers=args.max_workers,
                output_format=output_format,
                output_file=output_file
            )
            
        elif args.operation == 'check-duplicates':
            logger.info("Starting duplicate check operation")
            operation = DuplicateCheckOperation(context)
            
            # Parse key columns
            key_columns = [col.strip() for col in args.key_columns.split(',')]
            
            result = operation.check_duplicates(
                table_name=args.table,
                key_columns=key_columns,
                date_column=args.date_column,
                start_date=args.start_date,
                end_date=args.end_date,
                sample_limit=args.sample_limit
            )
            
            # Display formatted result
            formatted = operation.format_result(result)
            print(formatted)
            
            # Save if output specified
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(result.to_dict(), f, indent=2, default=str)
                logger.info(f"Duplicate check results saved to {args.output}")
            
        elif args.operation == 'compare':
            logger.info("Starting file comparison")
            operation = CompareOperation(context)
            result = operation.compare_files(
                file1_path=args.file1,
                file2_path=args.file2,
                quick_mode=args.quick,
                sample_size_mb=args.sample_size
            )
            
            # Display formatted result
            formatted = operation.format_result(result)
            print(formatted)
            
            # Save if output specified
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(result.to_dict(), f, indent=2, default=str)
                logger.info(f"Comparison results saved to {args.output}")
        
        # Utility operations
        elif args.operation == 'check-table':
            logger.info(f"Checking table: {args.table}")
            operation = CheckTableOperation(context)
            result = operation.execute(table_name=args.table)
            
        elif args.operation == 'diagnose-error':
            logger.info("Diagnosing Snowflake errors")
            operation = DiagnoseErrorOperation(context)
            result = operation.execute(
                table_name=args.table,
                hours_back=args.hours
            )
            
        elif args.operation == 'validate-file':
            logger.info(f"Validating file: {args.file}")
            # Note: This operation doesn't need Snowflake connection
            # Could potentially skip context initialization
            operation = ValidateFileOperation(context)
            result = operation.execute(
                file_path=args.file,
                expected_columns=args.expected_columns,
                sample_rows=args.sample_rows
            )
            
        elif args.operation == 'check-stage':
            logger.info("Checking Snowflake stage files")
            operation = CheckStageOperation(context)
            result = operation.execute(
                pattern=args.pattern,
                clean=args.clean
            )
            
        elif args.operation == 'browse':
            logger.info("Launching file browser")
            operation = FileBrowserOperation(context)
            result = operation.execute(start_dir=args.start_dir)
            
        elif args.operation == 'generate-report':
            logger.info("Generating table report")
            operation = GenerateReportOperation(context)
            result = operation.execute(output_format=args.format)
            
        elif args.operation == 'sample-file':
            logger.info(f"Sampling file: {args.file}")
            operation = TSVSamplerOperation(context)
            result = operation.execute(
                file_path=args.file,
                rows=args.rows
            )
        
        # Configuration operations
        elif args.operation == 'config-generate':
            logger.info("Generating configuration")
            # This operation might not need full context
            operation = GenerateConfigOperation(context if args.table else None)
            result = operation.execute(
                files=args.files,
                output_file=args.output,
                table_name=args.table,
                column_headers=args.headers,
                base_path=args.base_path,
                date_column=args.date_column,
                merge_with=args.merge,
                interactive=args.interactive,
                dry_run=args.dry_run
            )
            
        elif args.operation == 'config-validate':
            logger.info(f"Validating configuration: {args.config_file}")
            operation = ValidateConfigOperation(context if args.test_connection else None)
            result = operation.execute(
                config_file=args.config_file,
                check_connection=args.test_connection
            )
            return 0 if result else 1
            
        elif args.operation == 'config-migrate':
            logger.info(f"Migrating configuration: {args.config_file}")
            operation = MigrateConfigOperation(context)
            result = operation.execute(
                config_file=args.config_file,
                target_version=args.target_version,
                backup=not args.no_backup
            )
            return 0 if result else 1
        
        else:
            logger.error(f"Unknown operation: {args.operation}")
            return 1
        
        logger.info(f"Operation {args.operation} completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Operation failed: {e}", exc_info=True)
        if not args.quiet:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1
    
    finally:
        # Clean up context if it was created
        if 'context' in locals():
            try:
                context.cleanup()
            except:
                pass


if __name__ == '__main__':
    sys.exit(main())