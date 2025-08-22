"""
Optimal SnowflakeLoader implementation combining best practices.
This version merges insights from both implementations with Gemini's feedback.
"""

import os
import re
import gzip
import time
import uuid
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass, field

from ..models.file_config import FileConfig
from ..core.progress import ProgressTracker, ProgressPhase
from ..utils.snowflake_connection_v3 import SnowflakeConnectionManager


@dataclass
class LoaderConfig:
    """
    Configuration for SnowflakeLoader.
    Provides sensible defaults while allowing full customization.
    """
    
    # Compression settings
    chunk_size_mb: int = 10
    compression_level: int = 1
    
    # Upload settings
    parallel_uploads: int = 4
    
    # Async settings
    async_threshold_mb: int = 100
    keepalive_interval_sec: int = 240
    poll_interval_sec: int = 30
    max_wait_time_sec: int = 7200
    
    # File format settings
    file_format_options: Dict[str, Any] = field(default_factory=lambda: {
        'TYPE': 'CSV',
        'FIELD_DELIMITER': '\\t',
        'SKIP_HEADER': 0,
        'FIELD_OPTIONALLY_ENCLOSED_BY': '"',
        'ESCAPE_UNENCLOSED_FIELD': 'NONE',
        'ERROR_ON_COLUMN_COUNT_MISMATCH': False,
        'REPLACE_INVALID_CHARACTERS': True,
        'DATE_FORMAT': 'YYYY-MM-DD',
        'TIMESTAMP_FORMAT': 'YYYY-MM-DD HH24:MI:SS',
        'NULL_IF': ['', 'NULL', 'null', '\\\\N']
    })
    
    # Copy settings
    on_error: str = 'ABORT_STATEMENT'
    purge: bool = True
    size_limit: int = 5368709120
    
    # Stage settings
    stage_prefix: str = 'TSV_LOADER'
    cleanup_old_stages: bool = True


class SnowflakeLoader:
    """
    Manages Snowflake loading operations with streaming compression and async support.
    
    This optimal implementation combines:
    - Externalized configuration via LoaderConfig
    - Pure logging (no print statements)
    - UUID-based stage management with guaranteed cleanup
    - Consistent pathlib usage internally
    - Flexible path input (str or Path)
    - Dynamic SQL generation from config
    """
    
    def __init__(self,
                 connection_manager: SnowflakeConnectionManager,
                 config: LoaderConfig,
                 progress_tracker: Optional[ProgressTracker] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize loader with injected dependencies and configuration.
        
        Args:
            connection_manager: Manages Snowflake connections
            config: Loader configuration (required for explicitness)
            progress_tracker: Optional progress tracking implementation
            logger: Optional logger instance
        """
        self.connection_manager = connection_manager
        self.config = config
        self.progress_tracker = progress_tracker
        self.logger = logger or logging.getLogger(__name__)
        
        # Check warehouse size on initialization
        self._check_warehouse_size()
    
    def load_file(self, 
                  file_config: FileConfig,
                  file_path: Optional[Union[str, os.PathLike]] = None) -> int:
        """
        Load a TSV file to Snowflake with streaming compression.
        
        Args:
            file_config: File configuration with table details
            file_path: Optional override for file path (accepts str or Path)
            
        Returns:
            Number of rows loaded
            
        Raises:
            FileNotFoundError: If input file doesn't exist
            Exception: For Snowflake or processing errors
        """
        # Convert to Path object for internal use
        input_path = Path(file_path or file_config.file_path)
        
        if not input_path.exists():
            raise FileNotFoundError(f"File not found: {input_path}")
        
        self.logger.info(f"Loading {input_path} to {file_config.table_name}")
        
        compressed_file = None
        stage_name = None
        
        try:
            # Report progress phase
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.COMPRESSION)
            
            # Compress file
            compressed_file = self._compress_file(input_path)
            
            # Create unique stage
            stage_name = self._create_unique_stage(file_config.table_name)
            
            # Upload to stage
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.UPLOAD)
            self._upload_to_stage(compressed_file, stage_name)
            
            # Validate and copy data
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.COPY)
            rows_loaded = self._copy_to_table(
                stage_name, 
                file_config.table_name,
                compressed_file
            )
            
            self.logger.info(
                f"Successfully loaded {rows_loaded:,} rows to {file_config.table_name}"
            )
            return rows_loaded
            
        except Exception as e:
            self.logger.error(f"Failed to load {file_config.table_name}: {e}")
            raise
            
        finally:
            # Guaranteed cleanup
            if stage_name:
                self._cleanup_stage(stage_name)
            
            if compressed_file and compressed_file.exists():
                self.logger.debug(f"Removing compressed file: {compressed_file}")
                compressed_file.unlink()
    
    def _check_warehouse_size(self):
        """Check and warn about warehouse size for performance."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current warehouse
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                current_wh = cursor.fetchone()[0]
                
                # Get warehouse details
                cursor.execute("SHOW WAREHOUSES")
                warehouses = cursor.fetchall()
                
                for wh in warehouses:
                    if wh[0] == current_wh:
                        wh_size = wh[2]
                        self.logger.info(f"Using warehouse: {current_wh} (Size: {wh_size})")
                        
                        if wh_size in ['X-Small', 'Small']:
                            self.logger.warning(
                                f"Warehouse '{current_wh}' is {wh_size}. "
                                f"For files >100MB, consider using: "
                                f"ALTER WAREHOUSE {current_wh} SET WAREHOUSE_SIZE = 'MEDIUM';"
                            )
                        break
                        
        except Exception as e:
            self.logger.warning(f"Could not check warehouse size: {e}")
    
    def _compress_file(self, file_path: Path) -> Path:
        """
        Compress file using streaming gzip compression.
        
        Args:
            file_path: Path object to input file
            
        Returns:
            Path to compressed file
        """
        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
        
        # Check if already compressed
        if compressed_path.exists():
            if self._is_compression_valid(file_path, compressed_path):
                self.logger.info(f"Using existing compressed file: {compressed_path}")
                return compressed_path
            else:
                self.logger.warning("Invalid compressed file detected, recompressing")
                compressed_path.unlink()
        
        # Stream compress with progress tracking
        file_size = file_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        self.logger.info(f"Compressing {file_size_mb:.1f} MB file: {file_path.name}")
        
        start_time = time.time()
        bytes_processed = 0
        chunk_size = self.config.chunk_size_mb * 1024 * 1024
        
        with file_path.open('rb') as f_in:
            with gzip.open(compressed_path, 'wb', 
                          compresslevel=self.config.compression_level) as f_out:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    
                    f_out.write(chunk)
                    bytes_processed += len(chunk)
                    
                    # Update progress
                    if self.progress_tracker:
                        progress_pct = (bytes_processed / file_size) * 100
                        self.progress_tracker.update_progress(
                            compression_progress=progress_pct
                        )
        
        compression_time = time.time() - start_time
        compressed_size_mb = compressed_path.stat().st_size / (1024 * 1024)
        compression_ratio = (compressed_size_mb / file_size_mb) * 100
        
        self.logger.info(
            f"Compression complete in {compression_time:.1f}s: "
            f"{file_size_mb:.1f} MB -> {compressed_size_mb:.1f} MB "
            f"({compression_ratio:.1f}% of original)"
        )
        
        return compressed_path
    
    def _is_compression_valid(self, original_path: Path, compressed_path: Path) -> bool:
        """
        Check if existing compressed file has valid compression ratio.
        
        Args:
            original_path: Path to original file
            compressed_path: Path to compressed file
            
        Returns:
            True if compression ratio is reasonable (8-40%)
        """
        original_size = original_path.stat().st_size
        compressed_size = compressed_path.stat().st_size
        
        if original_size == 0:
            return False
        
        ratio = (compressed_size / original_size) * 100
        return 8 <= ratio <= 40
    
    def _create_unique_stage(self, table_name: str) -> str:
        """
        Create a unique stage name using UUID.
        
        Args:
            table_name: Target table name for stage directory
            
        Returns:
            Unique stage name/path
        """
        stage_id = uuid.uuid4().hex[:8]
        stage_name = f"@~/{self.config.stage_prefix}_{table_name}_{stage_id}/"
        
        self.logger.debug(f"Created unique stage: {stage_name}")
        return stage_name
    
    def _upload_to_stage(self, file_path: Path, stage_name: str):
        """
        Upload compressed file to Snowflake stage.
        
        Args:
            file_path: Path to compressed file
            stage_name: Unique stage name
        """
        self.logger.info(f"Uploading {file_path.name} to stage {stage_name}")
        
        # Execute PUT command
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        put_command = (
            f"PUT file://{file_path} {stage_name} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL={self.config.parallel_uploads}"
        )
        
        start_time = time.time()
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(put_command)
        
        upload_time = time.time() - start_time
        upload_rate = file_size_mb / upload_time if upload_time > 0 else 0
        
        self.logger.info(
            f"Upload complete in {upload_time:.1f}s ({upload_rate:.1f} MB/s)"
        )
        
        # Update progress
        if self.progress_tracker:
            self.progress_tracker.update_progress(upload_complete=True)
    
    def _build_file_format_clause(self) -> str:
        """Build FILE_FORMAT clause from configuration."""
        options = []
        
        for key, value in self.config.file_format_options.items():
            if isinstance(value, bool):
                options.append(f"{key} = {str(value).upper()}")
            elif isinstance(value, list):
                # Handle NULL_IF and similar list options
                if key == 'NULL_IF':
                    null_values = ', '.join(f"'{v}'" for v in value)
                    options.append(f"{key} = ({null_values})")
                else:
                    # Generic list handling
                    list_values = ', '.join(f"'{v}'" if isinstance(v, str) else str(v) 
                                           for v in value)
                    options.append(f"{key} = ({list_values})")
            elif isinstance(value, str):
                # Check if it's a keyword that shouldn't be quoted
                if value.upper() in ['NONE', 'AUTO', 'TRUE', 'FALSE']:
                    options.append(f"{key} = {value}")
                else:
                    options.append(f"{key} = '{value}'")
            else:
                options.append(f"{key} = {value}")
        
        return "FILE_FORMAT = (\n    " + "\n    ".join(options) + "\n)"
    
    def _build_copy_query(self, table_name: str, stage_name: str) -> str:
        """Build COPY INTO query with configuration."""
        file_format = self._build_file_format_clause()
        
        return f"""
        COPY INTO {table_name}
        FROM {stage_name}
        {file_format}
        ON_ERROR = '{self.config.on_error}'
        PURGE = {str(self.config.purge).upper()}
        VALIDATION_MODE = 'RETURN_ERRORS'
        SIZE_LIMIT = {self.config.size_limit}
        """
    
    def _copy_to_table(self, stage_name: str, table_name: str, compressed_file: Path) -> int:
        """
        Copy data from stage to table with validation.
        
        Args:
            stage_name: Snowflake stage path
            table_name: Target table name
            compressed_file: Path to compressed file for size check
            
        Returns:
            Number of rows loaded
        """
        # Build COPY query
        copy_query = self._build_copy_query(table_name, stage_name)
        
        # Validate first
        self._validate_data(copy_query, table_name)
        
        # Determine sync vs async based on file size
        compressed_size_mb = compressed_file.stat().st_size / (1024 * 1024)
        use_async = compressed_size_mb > self.config.async_threshold_mb
        
        # Estimate rows for progress tracking
        estimated_rows = int(compressed_size_mb * 50000)  # ~50K rows per MB
        
        # Execute COPY
        final_query = copy_query.replace("VALIDATION_MODE = 'RETURN_ERRORS'", "")
        
        if use_async:
            self.logger.info(
                f"Using async COPY for large file ({compressed_size_mb:.1f} MB)"
            )
            return self._execute_copy_async(final_query, table_name, estimated_rows)
        else:
            return self._execute_copy_sync(final_query, table_name, estimated_rows)
    
    def _validate_data(self, copy_query: str, table_name: str):
        """
        Run validation before actual copy.
        
        Raises:
            Exception: If validation finds errors
        """
        self.logger.info(f"Validating data for {table_name}")
        
        validation_query = copy_query.replace(f"ON_ERROR = '{self.config.on_error}'", "")
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            validation_result = cursor.execute(validation_query).fetchall()
        
        if validation_result:
            error_msg = f"Validation failed for {table_name}. First 10 errors:\n"
            for i, error in enumerate(validation_result[:10]):
                error_msg += f"  - {error}\n"
            if len(validation_result) > 10:
                error_msg += f"  ... and {len(validation_result) - 10} more errors"
            
            self.logger.error(error_msg)
            raise Exception(f"Data validation failed. Fix errors before loading.")
    
    def _execute_copy_sync(self, query: str, table_name: str, estimated_rows: int) -> int:
        """Execute COPY synchronously for smaller files."""
        self.logger.info(f"Copying data to {table_name} (sync mode)")
        
        start_time = time.time()
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            copy_result = cursor.execute(query)
            
            # Extract rows loaded from result
            rows_loaded = self._extract_rows_loaded(copy_result)
        
        copy_time = time.time() - start_time
        rows_per_sec = rows_loaded / copy_time if copy_time > 0 else 0
        
        self.logger.info(
            f"COPY completed in {copy_time:.1f}s "
            f"({rows_loaded:,} rows at {rows_per_sec:,.0f} rows/sec)"
        )
        
        if self.progress_tracker:
            self.progress_tracker.update_progress(copy_complete=True)
        
        return rows_loaded
    
    def _execute_copy_async(self, query: str, table_name: str, estimated_rows: int) -> int:
        """
        Execute COPY asynchronously with keepalive for large files.
        
        Args:
            query: COPY query to execute
            table_name: Table name for logging
            estimated_rows: Estimated row count for progress
            
        Returns:
            Number of rows loaded
        """
        self.logger.info(
            f"Starting async COPY for {table_name} "
            f"(estimated {estimated_rows:,} rows)"
        )
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Enable async queries
            cursor.execute("ALTER SESSION SET ABORT_DETACHED_QUERY = FALSE")
            
            # Start async query
            query_id = cursor.execute_async(query).get('queryId')
            self.logger.info(f"Async COPY started with query ID: {query_id}")
            
            # Poll for completion with keepalive
            rows_loaded = self._poll_async_query(conn, cursor, query_id, table_name)
            
            if self.progress_tracker:
                self.progress_tracker.update_progress(copy_complete=True)
            
            return rows_loaded
    
    def _poll_async_query(self, conn, cursor, query_id: str, table_name: str) -> int:
        """
        Poll async query status with keepalive.
        
        Returns:
            Number of rows loaded
        """
        start_time = time.time()
        last_keepalive = start_time
        last_status_update = start_time
        
        while True:
            elapsed = time.time() - start_time
            
            # Check timeout
            if elapsed > self.config.max_wait_time_sec:
                raise Exception(
                    f"COPY operation timed out after "
                    f"{self.config.max_wait_time_sec/60:.0f} minutes"
                )
            
            # Check query status
            status = conn.get_query_status(query_id)
            
            if not conn.is_still_running(status):
                break
            
            # Send keepalive to prevent timeout
            if time.time() - last_keepalive > self.config.keepalive_interval_sec:
                self.logger.debug(f"Sending keepalive for query {query_id}")
                try:
                    cursor.get_results_from_sfqid(query_id)
                except:
                    pass  # Expected to fail while running
                last_keepalive = time.time()
            
            # Status update
            if time.time() - last_status_update > self.config.poll_interval_sec:
                elapsed_mins = elapsed / 60
                self.logger.info(f"COPY still running after {elapsed_mins:.1f} minutes")
                last_status_update = time.time()
            
            time.sleep(5)
        
        # Check for errors and get results
        conn.get_query_status_throw_if_error(query_id)
        copy_result = cursor.get_results_from_sfqid(query_id)
        
        rows_loaded = self._extract_rows_loaded(copy_result)
        
        total_time = time.time() - start_time
        self.logger.info(
            f"Async COPY completed in {total_time:.1f}s ({rows_loaded:,} rows)"
        )
        
        return rows_loaded
    
    def _extract_rows_loaded(self, result) -> int:
        """Extract row count from COPY result."""
        for row in result:
            if row[0] and 'rows_loaded' in str(row[0]).lower():
                match = re.search(r'(\d+)', str(row[0]))
                if match:
                    return int(match.group(1))
        return 0
    
    def _cleanup_stage(self, stage_name: str):
        """Remove stage with guaranteed cleanup."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"REMOVE {stage_name}")
                self.logger.debug(f"Removed stage: {stage_name}")
        except Exception as e:
            self.logger.warning(f"Could not remove stage {stage_name}: {e}")