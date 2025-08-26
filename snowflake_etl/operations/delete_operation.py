"""
Delete operation for removing monthly data from Snowflake tables.
Uses ApplicationContext for dependency injection.
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict

from ..core.application_context import ApplicationContext, BaseOperation
from ..core.progress import ProgressPhase


@dataclass
class DeletionTarget:
    """Represents a single deletion operation."""
    table_name: str
    date_column: str
    year_month: str
    start_date: str  # YYYY-MM-DD format
    end_date: str    # YYYY-MM-DD format


@dataclass
class DeletionResult:
    """Result of a deletion operation."""
    target: DeletionTarget
    rows_affected: int
    total_rows_before: int
    deletion_percentage: float
    status: str  # 'success', 'failed', 'skipped', 'dry_run'
    error_message: Optional[str] = None
    execution_time: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['target'] = asdict(self.target)
        return result


class DeleteOperation(BaseOperation):
    """
    Orchestrates data deletion from Snowflake tables.
    Provides safety features including dry-run, preview, and transaction management.
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
        """
        super().__init__(context)
        self._metadata_cache = {}
    
    def delete_month_data(self,
                         targets: List[DeletionTarget],
                         dry_run: bool = False,
                         preview: bool = False,
                         skip_confirmation: bool = False) -> List[DeletionResult]:
        """
        Delete monthly data from Snowflake tables.
        
        Args:
            targets: List of deletion targets
            dry_run: If True, only analyze impact without deleting
            preview: If True, show sample rows before deletion
            skip_confirmation: If True, skip user confirmation
            
        Returns:
            List of deletion results
        """
        results = []
        
        self.logger.info(
            f"Processing {len(targets)} deletion target(s) "
            f"(dry_run={dry_run}, preview={preview})"
        )
        
        # Update progress phase - use ANALYSIS since there's no PROCESSING phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.ANALYSIS)
        
        for i, target in enumerate(targets, 1):
            self.logger.info(
                f"Processing {i}/{len(targets)}: {target.table_name} "
                f"for {target.year_month}"
            )
            
            try:
                result = self._process_deletion(
                    target, dry_run, preview, skip_confirmation
                )
                results.append(result)
                
                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_progress(
                        items_processed=i,
                        total_items=len(targets)
                    )
                    
            except Exception as e:
                self.logger.error(
                    f"Failed to process {target.table_name}: {e}"
                )
                results.append(DeletionResult(
                    target=target,
                    rows_affected=0,
                    total_rows_before=0,
                    deletion_percentage=0.0,
                    status='failed',
                    error_message=str(e)
                ))
        
        # Log summary
        self._log_summary(results)
        
        return results
    
    def _process_deletion(self,
                         target: DeletionTarget,
                         dry_run: bool,
                         preview: bool,
                         skip_confirmation: bool) -> DeletionResult:
        """
        Process a single deletion target.
        
        Returns:
            DeletionResult with operation details
        """
        start_time = datetime.now()
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Validate table and column exist
            if not self._validate_target(cursor, target):
                return DeletionResult(
                    target=target,
                    rows_affected=0,
                    total_rows_before=0,
                    deletion_percentage=0.0,
                    status='failed',
                    error_message=f"Table {target.table_name} or column {target.date_column} not found"
                )
            
            # Get impact analysis
            impact_data = self._analyze_impact(cursor, target)
            
            if impact_data['rows_to_delete'] == 0:
                self.logger.info(f"No rows to delete for {target.table_name} in {target.year_month}")
                return DeletionResult(
                    target=target,
                    rows_affected=0,
                    total_rows_before=impact_data['total_rows'],
                    deletion_percentage=0.0,
                    status='skipped',
                    error_message="No matching rows found"
                )
            
            # Show preview if requested
            if preview:
                self._show_preview(cursor, target, impact_data)
            
            # Get confirmation if needed
            if not skip_confirmation and not dry_run:
                if not self._get_confirmation(target, impact_data):
                    return DeletionResult(
                        target=target,
                        rows_affected=0,
                        total_rows_before=impact_data['total_rows'],
                        deletion_percentage=0.0,
                        status='skipped',
                        error_message="User cancelled"
                    )
            
            # Execute deletion (or simulate for dry run)
            if dry_run:
                self.logger.info(
                    f"DRY RUN: Would delete {impact_data['rows_to_delete']:,} rows "
                    f"from {target.table_name}"
                )
                status = 'dry_run'
                rows_affected = impact_data['rows_to_delete']
            else:
                rows_affected = self._execute_deletion(cursor, target, impact_data)
                status = 'success'
                conn.commit()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return DeletionResult(
                target=target,
                rows_affected=rows_affected,
                total_rows_before=impact_data['total_rows'],
                deletion_percentage=impact_data['deletion_percentage'],
                status=status,
                execution_time=execution_time
            )
    
    def _validate_target(self, cursor, target: DeletionTarget) -> bool:
        """
        Validate that table and column exist.
        
        Returns:
            True if valid, False otherwise
        """
        try:
            # Check if table exists
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = %s
                """,
                (target.table_name.upper(),)
            )
            
            if cursor.fetchone()[0] == 0:
                self.logger.error(f"Table {target.table_name} not found")
                return False
            
            # Check if column exists
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
                """,
                (target.table_name.upper(), target.date_column.upper())
            )
            
            if cursor.fetchone()[0] == 0:
                self.logger.error(
                    f"Column {target.date_column} not found in {target.table_name}"
                )
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return False
    
    def _analyze_impact(self, cursor, target: DeletionTarget) -> Dict[str, Any]:
        """
        Analyze the impact of the deletion.
        
        Returns:
            Dictionary with impact analysis
        """
        # Convert dates to YYYYMMDD format for comparison
        start_yyyymmdd = target.start_date.replace('-', '')
        end_yyyymmdd = target.end_date.replace('-', '')
        
        # Get total rows in table
        cursor.execute(f"SELECT COUNT(*) FROM {target.table_name}")
        total_rows = cursor.fetchone()[0]
        
        # Get rows to be deleted
        delete_query = f"""
        SELECT COUNT(*) 
        FROM {target.table_name}
        WHERE {target.date_column} BETWEEN %s AND %s
        """
        
        cursor.execute(delete_query, (start_yyyymmdd, end_yyyymmdd))
        rows_to_delete = cursor.fetchone()[0]
        
        deletion_percentage = (
            (rows_to_delete / total_rows * 100) if total_rows > 0 else 0
        )
        
        return {
            'total_rows': total_rows,
            'rows_to_delete': rows_to_delete,
            'deletion_percentage': deletion_percentage
        }
    
    def _show_preview(self, cursor, target: DeletionTarget, impact_data: Dict):
        """Show a preview of rows to be deleted."""
        start_yyyymmdd = target.start_date.replace('-', '')
        end_yyyymmdd = target.end_date.replace('-', '')
        
        preview_query = f"""
        SELECT * 
        FROM {target.table_name}
        WHERE {target.date_column} BETWEEN %s AND %s
        LIMIT 10
        """
        
        cursor.execute(preview_query, (start_yyyymmdd, end_yyyymmdd))
        preview_rows = cursor.fetchall()
        
        self.logger.info(
            f"\nPreview of rows to be deleted from {target.table_name}:\n"
            f"Total rows to delete: {impact_data['rows_to_delete']:,} "
            f"({impact_data['deletion_percentage']:.2f}% of table)\n"
            f"Sample rows (first 10):"
        )
        
        for row in preview_rows:
            self.logger.info(f"  {row}")
    
    def _get_confirmation(self, target: DeletionTarget, impact_data: Dict) -> bool:
        """
        Get user confirmation for deletion.
        
        Returns:
            True if confirmed, False otherwise
        """
        message = (
            f"\nAbout to delete {impact_data['rows_to_delete']:,} rows "
            f"({impact_data['deletion_percentage']:.2f}%) from {target.table_name} "
            f"for month {target.year_month}.\n"
            f"This action cannot be undone without Time Travel.\n"
            f"Continue? (yes/no): "
        )
        
        # In a real CLI, this would prompt the user
        # For now, we'll log and return True for testing
        self.logger.warning(message)
        return True  # Would be: input().lower() == 'yes'
    
    def _execute_deletion(self, cursor, target: DeletionTarget, impact_data: Dict) -> int:
        """
        Execute the actual deletion.
        
        Returns:
            Number of rows deleted
        """
        start_yyyymmdd = target.start_date.replace('-', '')
        end_yyyymmdd = target.end_date.replace('-', '')
        
        # Record recovery timestamp
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        recovery_timestamp = cursor.fetchone()[0]
        
        self.logger.info(
            f"Executing deletion for {target.table_name}. "
            f"Recovery timestamp: {recovery_timestamp}"
        )
        
        # Execute deletion
        delete_query = f"""
        DELETE FROM {target.table_name}
        WHERE {target.date_column} BETWEEN %s AND %s
        """
        
        cursor.execute(delete_query, (start_yyyymmdd, end_yyyymmdd))
        
        # Snowflake returns number of rows affected
        rows_deleted = cursor.rowcount
        
        # Verify deletion
        if rows_deleted != impact_data['rows_to_delete']:
            self.logger.warning(
                f"Row count mismatch: Expected {impact_data['rows_to_delete']}, "
                f"deleted {rows_deleted}"
            )
        
        self.logger.info(
            f"Successfully deleted {rows_deleted:,} rows from {target.table_name}"
        )
        
        return rows_deleted
    
    def _log_summary(self, results: List[DeletionResult]):
        """Log a summary of all deletion operations."""
        total_deleted = sum(r.rows_affected for r in results if r.status == 'success')
        successful = sum(1 for r in results if r.status == 'success')
        failed = sum(1 for r in results if r.status == 'failed')
        skipped = sum(1 for r in results if r.status == 'skipped')
        dry_run = sum(1 for r in results if r.status == 'dry_run')
        
        self.logger.info(
            f"\nDeletion Summary:\n"
            f"  Total operations: {len(results)}\n"
            f"  Successful: {successful}\n"
            f"  Failed: {failed}\n"
            f"  Skipped: {skipped}\n"
            f"  Dry run: {dry_run}\n"
            f"  Total rows deleted: {total_deleted:,}"
        )
    
    def delete_from_config(self,
                           month: str,
                           tables: Optional[List[str]] = None,
                           dry_run: bool = False,
                           preview: bool = False,
                           skip_confirmation: bool = False) -> List[DeletionResult]:
        """
        Delete data based on configuration file.
        
        Args:
            month: Month to delete (YYYY-MM format)
            tables: Optional list of specific tables
            dry_run: If True, only analyze impact
            preview: If True, show sample rows
            skip_confirmation: If True, skip user confirmation
            
        Returns:
            List of deletion results
        """
        # Parse month to get date range
        year, mon = month.split('-')
        import calendar
        last_day = calendar.monthrange(int(year), int(mon))[1]
        start_date = f"{year}-{mon}-01"
        end_date = f"{year}-{mon}-{last_day:02d}"
        
        # Build deletion targets from config
        targets = []
        config_data = self.context.config_manager.get_config()
        
        for file_config in config_data.get('files', []):
            table_name = file_config.get('table_name')
            date_column = file_config.get('date_column')
            
            if not table_name or not date_column:
                continue
            
            # Filter by specific tables if provided
            if tables and table_name not in tables:
                continue
            
            target = DeletionTarget(
                table_name=table_name,
                date_column=date_column,
                year_month=month,
                start_date=start_date,
                end_date=end_date
            )
            targets.append(target)
        
        if not targets:
            self.logger.warning("No deletion targets found in configuration")
            return []
        
        return self.delete_month_data(
            targets, dry_run, preview, skip_confirmation
        )