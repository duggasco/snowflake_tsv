#!/usr/bin/env python3
"""
Package entry point for snowflake_etl.
Allows the package to be run as: python -m snowflake_etl
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Import the application context and operations
from .core.application_context import ApplicationContext
from .operations.load_operation import LoadOperation
from .operations.delete_operation import DeleteOperation
from .operations.validate_operation import ValidateOperation
from .operations.report_operation_final import ReportOperation, SeverityConfig
from .operations.duplicate_check_operation import DuplicateCheckOperation
from .operations.compare_operation import CompareOperation


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
    load_parser.add_argument('--base-path', type=str, required=True, help='Base path for TSV files')
    load_parser.add_argument('--month', type=str, help='Month to process (YYYY-MM)')
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
    validate_parser.add_argument('--table', type=str, required=True, help='Table to validate')
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
    
    return parser


def main(args=None):
    """Main entry point for the CLI."""
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args(args)
    
    # Setup logging
    logger = setup_logging(args.log_level, args.log_dir, args.quiet)
    
    try:
        # Create application context
        logger.info(f"Initializing application context with config: {args.config}")
        context = ApplicationContext(
            config_file=args.config,
            log_dir=args.log_dir,
            quiet_mode=args.quiet
        )
        
        # Route to appropriate operation
        if args.operation == 'load':
            logger.info("Starting load operation")
            operation = LoadOperation(context)
            result = operation.load_files(
                base_path=args.base_path,
                month=args.month,
                file_pattern=args.file_pattern,
                skip_qc=args.skip_qc,
                validate_in_snowflake=args.validate_in_snowflake,
                validate_only=args.validate_only,
                max_workers=args.max_workers
            )
            
        elif args.operation == 'delete':
            logger.info("Starting delete operation")
            operation = DeleteOperation(context)
            
            # Handle month vs date range
            if args.month:
                # Convert month to date range
                from datetime import datetime
                month_date = datetime.strptime(args.month, '%Y-%m')
                start_date = month_date.strftime('%Y-%m-01')
                # Calculate last day of month
                if month_date.month == 12:
                    end_date = f"{month_date.year}-12-31"
                else:
                    next_month = month_date.replace(month=month_date.month + 1)
                    from datetime import timedelta
                    last_day = next_month - timedelta(days=1)
                    end_date = last_day.strftime('%Y-%m-%d')
            else:
                start_date = args.start_date
                end_date = args.end_date
            
            result = operation.delete_data(
                table_name=args.table,
                date_column=args.date_column,
                start_date=start_date,
                end_date=end_date,
                dry_run=args.dry_run,
                skip_confirmation=args.yes
            )
            
        elif args.operation == 'validate':
            logger.info("Starting validate operation")
            operation = ValidateOperation(context)
            
            # Handle month vs date range
            if args.month:
                from datetime import datetime
                month_date = datetime.strptime(args.month, '%Y-%m')
                start_date = month_date.strftime('%Y-%m-01')
                if month_date.month == 12:
                    end_date = f"{month_date.year}-12-31"
                else:
                    next_month = month_date.replace(month=month_date.month + 1)
                    from datetime import timedelta
                    last_day = next_month - timedelta(days=1)
                    end_date = last_day.strftime('%Y-%m-%d')
            else:
                start_date = args.start_date
                end_date = args.end_date
            
            result = operation.validate_table(
                table_name=args.table,
                date_column=args.date_column,
                start_date=start_date,
                end_date=end_date
            )
            
            # Save results if output specified
            if args.output:
                import json
                with open(args.output, 'w') as f:
                    json.dump(result.to_dict() if hasattr(result, 'to_dict') else result, 
                             f, indent=2, default=str)
                logger.info(f"Validation results saved to {args.output}")
            
        elif args.operation == 'report':
            logger.info("Starting report operation")
            operation = ReportOperation(context)
            result = operation.generate_full_report(
                config_filter=args.config_filter,
                table_filter=args.table_filter,
                max_workers=args.max_workers,
                output_format=args.output_format,
                output_file=args.output
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
                import json
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
                import json
                with open(args.output, 'w') as f:
                    json.dump(result.to_dict(), f, indent=2, default=str)
                logger.info(f"Comparison results saved to {args.output}")
        
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