#!/usr/bin/env python3
"""
Snowflake Month Data Dropper - Safe deletion of monthly data from Snowflake tables.
Follows a safety-first design with multiple confirmation layers, comprehensive logging,
and secure coding practices.
"""

import argparse
import json
import logging
import os
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

# --- Pre-execution Setup ---

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# --- Data Classes for Structure ---

@dataclass
class DeletionTarget:
    """Represents a single deletion operation."""
    table_name: str
    date_column: str
    year_month: str
    start_date: int  # YYYYMMDD format
    end_date: int    # YYYYMMDD format

@dataclass
class DeletionResult:
    """Result of a deletion operation."""
    target: DeletionTarget
    rows_affected: int
    total_rows: int
    deletion_percentage: float
    status: str  # 'success', 'failed', 'skipped'
    error_message: Optional[str] = None
    execution_time: Optional[float] = None

# --- Core Snowflake Interaction Classes ---

class SnowflakeManager:
    """Manages the Snowflake connection lifecycle using a context manager."""
    def __init__(self, connection_params: Dict):
        self._connection_params = connection_params
        self._conn: Optional[SnowflakeConnection] = None
        self.logger = logging.getLogger(self.__class__.__name__)

    @contextmanager
    def connect(self) -> SnowflakeConnection:
        """Provides a managed Snowflake connection."""
        try:
            self.logger.info("Establishing Snowflake connection...")
            self._conn = snowflake.connector.connect(**self._connection_params)
            
            with self._conn.cursor(DictCursor) as cur:
                cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER(), CURRENT_ROLE()")
                db_info = cur.fetchone()
                self.logger.info(
                    f"Connected to {db_info['CURRENT_DATABASE()']}.{db_info['CURRENT_SCHEMA()']} "
                    f"as {db_info['CURRENT_USER()']} with role {db_info['CURRENT_ROLE()']}"
                )
            yield self._conn
        except ProgrammingError as e:
            self.logger.error(f"Snowflake connection error: {e}")
            raise
        finally:
            if self._conn and not self._conn.is_closed():
                self.logger.info("Closing Snowflake connection.")
                self._conn.close()

class SnowflakeMetadata:
    """Handles caching of Snowflake table metadata to reduce redundant queries."""
    def __init__(self, conn: SnowflakeConnection):
        self._conn = conn
        self._cache: Dict[str, List[str]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_columns(self, table_name: str) -> Optional[List[str]]:
        """Gets column names for a table, using a cache."""
        if table_name not in self._cache:
            self.logger.info(f"Fetching metadata for table: {table_name}")
            try:
                with self._conn.cursor(DictCursor) as cur:
                    # Parameterized query to prevent injection in identifiers
                    cur.execute("SHOW COLUMNS IN TABLE identifier(%s)", (table_name,))
                    columns = cur.fetchall()
                    if not columns:
                        self.logger.error(f"Table '{table_name}' not found or has no columns.")
                        self._cache[table_name] = None
                    else:
                        self._cache[table_name] = [col['column_name'].upper() for col in columns]
            except ProgrammingError as e:
                self.logger.error(f"Error fetching metadata for '{table_name}': {e}")
                self._cache[table_name] = None
        return self._cache.get(table_name)

class SnowflakeDeleter:
    """Manages the analysis and deletion of data with proper transactions."""
    def __init__(self, conn: SnowflakeConnection, metadata: SnowflakeMetadata, dry_run: bool = False):
        self.conn = conn
        self.metadata = metadata
        self.dry_run = dry_run
        self.logger = logging.getLogger(self.__class__.__name__)

    def _execute_query(self, cursor, query: str, params: tuple = None, fetch: str = None):
        """Helper for executing queries with parameters."""
        cursor.execute(query, params)
        if fetch == 'one':
            return cursor.fetchone()
        if fetch == 'all':
            return cursor.fetchall()
        return cursor.rowcount

    def validate_target(self, target: DeletionTarget) -> bool:
        """Validates that the table and column exist."""
        columns = self.metadata.get_columns(target.table_name)
        if columns is None:
            return False
        if target.date_column.upper() not in columns:
            self.logger.error(f"Column '{target.date_column}' not found in table '{target.table_name}'.")
            self.logger.error(f"Available columns: {', '.join(columns)}")
            return False
        return True

    def analyze_deletion(self, target: DeletionTarget, preview: bool = False) -> Optional[Dict]:
        """Analyzes the impact of a deletion using parameterized queries."""
        self.logger.info(f"Analyzing deletion for {target.table_name} ({target.year_month})")
        try:
            with self.conn.cursor(DictCursor) as cur:
                # Get total row count
                total_query = f"SELECT COUNT(*) as total FROM {target.table_name}"
                cur.execute(total_query)
                total_rows = cur.fetchone()['TOTAL']
                
                # Get deletion row count - using parameterized values for dates
                delete_count_query = f"""
                SELECT COUNT(*) as delete_count 
                FROM {target.table_name}
                WHERE {target.date_column} >= %s
                  AND {target.date_column} <= %s
                """
                cur.execute(delete_count_query, (target.start_date, target.end_date))
                delete_rows = cur.fetchone()['DELETE_COUNT']

                deletion_percentage = (delete_rows / total_rows * 100) if total_rows > 0 else 0
                
                analysis = {
                    'total_rows': total_rows,
                    'rows_to_delete': delete_rows,
                    'deletion_percentage': deletion_percentage,
                }

                if preview and delete_rows > 0:
                    self.logger.info("Previewing 10 sample rows for deletion:")
                    preview_query = f"""
                    SELECT * 
                    FROM {target.table_name}
                    WHERE {target.date_column} >= %s
                      AND {target.date_column} <= %s
                    LIMIT 10
                    """
                    cur.execute(preview_query, (target.start_date, target.end_date))
                    preview_rows = cur.fetchall()
                    analysis['preview'] = preview_rows
                    for row in preview_rows[:3]:
                        self.logger.info(f"  {row}")

                self.logger.info(f"  Total rows: {total_rows:,}, Rows to delete: {delete_rows:,} ({deletion_percentage:.2f}%)")
                if deletion_percentage > 20:
                    self.logger.warning(f"  ⚠️  Large deletion ({deletion_percentage:.1f}%) - Consider CTAS method.")
                
                return analysis
        except ProgrammingError as e:
            self.logger.error(f"Error analyzing deletion for '{target.table_name}': {e}")
            return None

    def delete_month_data(self, target: DeletionTarget, analysis: Dict) -> DeletionResult:
        """Executes deletion within a managed transaction."""
        start_time = time.time()
        
        if not self.validate_target(target):
            return DeletionResult(target, 0, 0, 0, 'failed', 'Table or column validation failed')

        if analysis['rows_to_delete'] == 0:
            self.logger.warning(f"No rows to delete for {target.table_name} ({target.year_month}). Skipping.")
            return DeletionResult(target, 0, analysis['total_rows'], 0, 'success', execution_time=time.time() - start_time)

        if self.dry_run:
            self.logger.info(f"🔍 DRY RUN: Would delete {analysis['rows_to_delete']:,} rows from {target.table_name}.")
            return DeletionResult(target, analysis['rows_to_delete'], analysis['total_rows'], analysis['deletion_percentage'], 'skipped', execution_time=time.time() - start_time)

        self.logger.info(f"Executing deletion for {target.table_name} ({target.year_month})...")
        
        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute("BEGIN TRANSACTION")
                self.logger.info(f"Recovery timestamp (for Time Travel): {datetime.now().isoformat()}")
                
                # Execute deletion with parameterized query
                delete_sql = f"""
                DELETE FROM {target.table_name}
                WHERE {target.date_column} >= %s
                  AND {target.date_column} <= %s
                """
                cur.execute(delete_sql, (target.start_date, target.end_date))
                rows_deleted = cur.rowcount

                if rows_deleted != analysis['rows_to_delete']:
                    self.logger.error(f"Row count mismatch! Expected: {analysis['rows_to_delete']}, Deleted: {rows_deleted}. Rolling back.")
                    self.conn.rollback()
                    return DeletionResult(target, 0, analysis['total_rows'], 0, 'failed', f"Row count mismatch: expected {analysis['rows_to_delete']}, got {rows_deleted}")

                self.conn.commit()
                self.logger.info(f"✅ Transaction committed. Deleted {rows_deleted:,} rows.")
                return DeletionResult(target, rows_deleted, analysis['total_rows'], analysis['deletion_percentage'], 'success', execution_time=time.time() - start_time)

        except ProgrammingError as e:
            self.logger.error(f"Error during deletion for '{target.table_name}': {e}. Rolling back.")
            try:
                self.conn.rollback()
            except Exception as rb_e:
                self.logger.error(f"Failed to rollback transaction: {rb_e}")
            return DeletionResult(target, 0, analysis.get('total_rows', 0), 0, 'failed', str(e), execution_time=time.time() - start_time)

# --- Utility and Main Application Logic ---

def setup_logging(dry_run=False, quiet=False):
    """Setup logging configuration."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/drop_month_{timestamp}.log'
    
    handlers = [logging.FileHandler(log_file)]
    if not quiet:
        handlers.append(logging.StreamHandler(sys.stdout))
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s', handlers=handlers, force=True)
    
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info("SNOWFLAKE MONTH DATA DROPPER STARTING")
    logger.info(f"Dry Run Mode: {dry_run}")
    logger.info(f"Log File: {log_file}")
    logger.info("="*80)
    return logger

def get_date_range_for_month(year_month: str) -> Optional[Tuple[int, int]]:
    """Convert YYYY-MM to start/end dates in YYYYMMDD format."""
    try:
        year, month = map(int, year_month.split('-'))
        if not 1 <= month <= 12:
            raise ValueError("Month must be between 1 and 12.")
        start_date = datetime(year, month, 1)
        end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return int(start_date.strftime('%Y%m%d')), int(end_date.strftime('%Y%m%d'))
    except (ValueError, TypeError) as e:
        logging.getLogger(__name__).error(f"Invalid month format '{year_month}'. Expected YYYY-MM. Error: {e}")
        return None

def load_config(config_path: str) -> Dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def parse_table_specs(config: Dict, table_names: List[str] = None) -> List[Dict]:
    """Parse table specifications from config."""
    all_specs = config.get('files', [])
    if not table_names:
        return all_specs
    
    name_set = set(table_names)
    return [spec for spec in all_specs if spec.get('table_name') in name_set]

def confirm_deletion(targets: List[DeletionTarget], analysis_results: Dict) -> bool:
    """Interactive confirmation prompt."""
    print("\n" + "="*80 + "\n⚠️  DELETION CONFIRMATION REQUIRED ⚠️\n" + "="*80)
    print("\nYou are about to delete data from the following tables:")
    for target in targets:
        analysis = analysis_results.get(target.table_name, {})
        print(
            f"\n  Table: {target.table_name}\n"
            f"  Month: {target.year_month}\n"
            f"  Date Range: {target.start_date} to {target.end_date}\n"
            f"  Rows to Delete: {analysis.get('rows_to_delete', 'N/A'):,}\n"
            f"  Deletion %: {analysis.get('deletion_percentage', 0):.2f}%"
        )
    print("\n" + "="*80 + "\n⚠️  THIS CANNOT BE UNDONE (except via Snowflake Time Travel) ⚠️\n" + "="*80)
    
    response = input("\nType 'yes' to confirm deletion, or any other key to cancel: ")
    return response.lower() == 'yes'

def generate_summary_report(results: List[DeletionResult], dry_run: bool) -> Dict:
    """Generate summary report of all deletions."""
    summary = {
        'execution_time': datetime.now().isoformat(),
        'dry_run': dry_run,
        'total_operations': len(results),
        'successful_deletions': sum(1 for r in results if r.status == 'success'),
        'skipped_deletions': sum(1 for r in results if r.status == 'skipped'),
        'failed_deletions': sum(1 for r in results if r.status == 'failed'),
        'total_rows_deleted': sum(r.rows_affected for r in results if r.status == 'success'),
        'total_rows_analyzed': sum(r.rows_affected for r in results if r.status == 'skipped'),
        'operations': [
            {
                'table': r.target.table_name,
                'month': r.target.year_month,
                'status': r.status,
                'rows_affected': r.rows_affected,
                'deletion_percentage': f"{r.deletion_percentage:.2f}%",
                'execution_time': f"{r.execution_time:.2f}s" if r.execution_time else "N/A",
                'error': r.error_message
            } for r in results
        ]
    }
    return summary

def main():
    parser = argparse.ArgumentParser(
        description='Safely delete monthly data from Snowflake tables.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for a single table and month
  python drop_month.py --config config.json --table MY_TABLE --month 2024-01 --dry-run
  
  # Delete data with an interactive preview and confirmation
  python drop_month.py --config config.json --table MY_TABLE --month 2024-01 --preview
  
  # Delete data for multiple months from multiple tables, skipping confirmation
  python drop_month.py --config config.json --tables T1,T2 --months 2024-01,2024-02 --yes
"""
    )
    parser.add_argument('--config', required=True, help='Path to configuration JSON file.')
    parser.add_argument('--table', help='Single table name to process.')
    parser.add_argument('--tables', help='Comma-separated list of table names.')
    parser.add_argument('--all-tables', action='store_true', help='Process all tables in config.')
    parser.add_argument('--month', help='Single month to delete (YYYY-MM format).')
    parser.add_argument('--months', help='Comma-separated list of months (YYYY-MM format).')
    parser.add_argument('--dry-run', action='store_true', help='Analyze without deleting.')
    parser.add_argument('--preview', action='store_true', help='Show sample rows to be deleted.')
    parser.add_argument('--yes', '-y', action='store_true', help='Skip confirmation prompt.')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress console output (log file only).')
    parser.add_argument('--output-json', help='Output summary report to a JSON file.')
    
    args = parser.parse_args()
    logger = setup_logging(dry_run=args.dry_run, quiet=args.quiet)

    try:
        config = load_config(args.config)
        
        table_names = []
        if args.table: table_names.append(args.table)
        if args.tables: table_names.extend(t.strip() for t in args.tables.split(','))
        
        if not args.all_tables and not table_names:
            logger.error("No tables specified. Use --table, --tables, or --all-tables.")
            sys.exit(1)
            
        table_specs = parse_table_specs(config, table_names if not args.all_tables else None)
        if not table_specs:
            logger.error("No matching tables found in configuration.")
            sys.exit(1)

        months = []
        if args.month: months.append(args.month)
        if args.months: months.extend(m.strip() for m in args.months.split(','))
        if not months:
            logger.error("No months specified. Use --month or --months.")
            sys.exit(1)

        targets = []
        for spec in table_specs:
            for month in months:
                date_range = get_date_range_for_month(month)
                if date_range:
                    targets.append(DeletionTarget(
                        table_name=spec['table_name'],
                        date_column=spec.get('date_column', 'RECORDDATEID'),
                        year_month=month,
                        start_date=date_range[0],
                        end_date=date_range[1]
                    ))
        
        if not targets:
            logger.error("No valid deletion targets created.")
            sys.exit(1)

        logger.info(f"Created {len(targets)} deletion target(s).")

        manager = SnowflakeManager(config['snowflake'])
        results = []
        
        with manager.connect() as conn:
            metadata = SnowflakeMetadata(conn)
            deleter = SnowflakeDeleter(conn, metadata, args.dry_run)
            
            analysis_results = {}
            if not args.dry_run and not args.yes:
                logger.info("--- Deletion Impact Analysis ---")
                for target in targets:
                    if not deleter.validate_target(target): continue
                    analysis = deleter.analyze_deletion(target, preview=args.preview)
                    if analysis: analysis_results[target.table_name] = analysis
                
                if not confirm_deletion(targets, analysis_results):
                    logger.info("Deletion cancelled by user.")
                    sys.exit(0)

            logger.info("--- Processing Deletions ---")
            
            progress_bar = tqdm(targets, desc="Processing deletions", unit="table") if TQDM_AVAILABLE else targets
            for target in progress_bar:
                if TQDM_AVAILABLE: progress_bar.set_description(f"Processing {target.table_name}")
                
                # Re-analyze if needed (or use prior analysis)
                analysis = analysis_results.get(target.table_name)
                if not analysis:
                    if not deleter.validate_target(target):
                        results.append(DeletionResult(target, 0, 0, 0, 'failed', 'Table/column validation failed'))
                        continue
                    analysis = deleter.analyze_deletion(target)
                    if not analysis:
                        results.append(DeletionResult(target, 0, 0, 0, 'failed', 'Analysis failed'))
                        continue
                
                result = deleter.delete_month_data(target, analysis)
                results.append(result)

        summary = generate_summary_report(results, args.dry_run)
        
        logger.info("\n" + "="*80 + "\nDELETION SUMMARY\n" + "="*80)
        logger.info(f"Total Operations: {summary['total_operations']}")
        logger.info(f"Successful: {summary['successful_deletions']}")
        logger.info(f"Skipped (Dry Run): {summary['skipped_deletions']}")
        logger.info(f"Failed: {summary['failed_deletions']}")
        if args.dry_run:
            logger.info(f"Rows Analyzed (Dry Run): {summary['total_rows_analyzed']:,}")
        else:
            logger.info(f"Total Rows Deleted: {summary['total_rows_deleted']:,}")
        
        if args.output_json:
            with open(args.output_json, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Summary report written to {args.output_json}")

        if summary['failed_deletions'] > 0:
            sys.exit(1)

    except Exception as e:
        logging.getLogger(__name__).error(f"A fatal error occurred: {e}")
        logging.getLogger(__name__).error(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    main()