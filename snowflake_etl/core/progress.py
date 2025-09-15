"""
Progress tracking abstractions for Snowflake ETL Pipeline
Clean separation between progress reporting and display implementation
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional, Dict, Any
import time
import logging


class ProgressPhase(Enum):
    """Phases of ETL processing"""
    ANALYSIS = "analysis"
    QUALITY_CHECK = "quality_check"
    COMPRESSION = "compression"
    UPLOAD = "upload"
    COPY = "copy"
    VALIDATION = "validation"
    COMPLETE = "complete"


@dataclass
class ProgressStats:
    """Statistics for progress tracking"""
    total_files: int = 0
    processed_files: int = 0
    total_bytes: int = 0
    processed_bytes: int = 0
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = None
    current_phase: ProgressPhase = None
    current_file: str = None
    current_file_format: str = None  # CSV, TSV, or other format
    errors: int = 0
    
    def __post_init__(self):
        if self.start_time is None:
            self.start_time = time.time()
    
    @property
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds"""
        return time.time() - self.start_time
    
    @property
    def progress_percentage(self) -> float:
        """Calculate overall progress percentage"""
        if self.total_files > 0:
            return (self.processed_files / self.total_files) * 100
        return 0.0
    
    @property
    def estimated_time_remaining(self) -> Optional[float]:
        """Estimate time remaining in seconds"""
        if self.processed_files > 0 and self.total_files > 0:
            avg_time_per_file = self.elapsed_time / self.processed_files
            remaining_files = self.total_files - self.processed_files
            return avg_time_per_file * remaining_files
        return None


class ProgressTracker(ABC):
    """
    Abstract base class for progress tracking
    Implementations can use tqdm, logging, or any other mechanism
    """
    
    def __init__(self):
        self.stats = ProgressStats()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def initialize(self, total_files: int, total_bytes: int = 0, total_rows: int = 0, **kwargs):
        """
        Initialize progress tracking for a batch operation
        
        Args:
            total_files: Total number of files to process
            total_bytes: Total size in bytes (optional)
            total_rows: Total number of rows (optional)
            **kwargs: Implementation-specific options
        """
        pass
    
    @abstractmethod
    def start_file(self, filename: str, file_size: int = 0, row_count: int = 0, file_format: str = None):
        """
        Start processing a new file
        
        Args:
            filename: Name of the file being processed
            file_size: Size of file in bytes
            row_count: Number of rows in file
            file_format: Format of the file (CSV, TSV, etc.)
        """
        pass
    
    @abstractmethod
    def update_phase(self, phase: ProgressPhase, **kwargs):
        """
        Update the current processing phase
        
        Args:
            phase: Current phase of processing
            **kwargs: Phase-specific metadata
        """
        pass
    
    @abstractmethod
    def update_progress(self, bytes_processed: int = 0, rows_processed: int = 0, **kwargs):
        """
        Update progress within current phase
        
        Args:
            bytes_processed: Additional bytes processed
            rows_processed: Additional rows processed
            **kwargs: Additional progress metrics
        """
        pass
    
    @abstractmethod
    def complete_file(self, success: bool = True, error_message: str = None):
        """
        Mark current file as complete
        
        Args:
            success: Whether file was processed successfully
            error_message: Error message if failed
        """
        pass
    
    @abstractmethod
    def close(self):
        """Clean up any resources"""
        pass
    
    def get_stats(self) -> ProgressStats:
        """Get current progress statistics"""
        return self.stats


class NoOpProgressTracker(ProgressTracker):
    """
    No-operation progress tracker for quiet mode or testing
    Updates stats but doesn't display anything
    """
    
    def initialize(self, total_files: int, total_bytes: int = 0, total_rows: int = 0, **kwargs):
        self.stats.total_files = total_files
        self.stats.total_bytes = total_bytes
        self.stats.total_rows = total_rows
        self.logger.debug(f"Initialized for {total_files} files")
    
    def start_file(self, filename: str, file_size: int = 0, row_count: int = 0, file_format: str = None):
        self.stats.current_file = filename
        self.stats.current_file_format = file_format
        self.logger.debug(f"Starting file: {filename}")
    
    def update_phase(self, phase: ProgressPhase, **kwargs):
        self.stats.current_phase = phase
        self.logger.debug(f"Phase: {phase.value}")
    
    def update_progress(self, bytes_processed: int = 0, rows_processed: int = 0, **kwargs):
        self.stats.processed_bytes += bytes_processed
        self.stats.processed_rows += rows_processed
    
    def complete_file(self, success: bool = True, error_message: str = None):
        self.stats.processed_files += 1
        if not success:
            self.stats.errors += 1
            self.logger.warning(f"File failed: {error_message}")
        self.stats.current_file = None
    
    def close(self):
        self.logger.debug("Progress tracking complete")


class LoggingProgressTracker(ProgressTracker):
    """
    Progress tracker that uses logging instead of visual progress bars
    Good for non-interactive environments
    """
    
    def __init__(self, log_interval: int = 10):
        """
        Initialize logging progress tracker
        
        Args:
            log_interval: Seconds between progress log messages
        """
        super().__init__()
        self.log_interval = log_interval
        self.last_log_time = 0
    
    def initialize(self, total_files: int, total_bytes: int = 0, total_rows: int = 0, **kwargs):
        self.stats.total_files = total_files
        self.stats.total_bytes = total_bytes
        self.stats.total_rows = total_rows
        
        size_str = f", {total_bytes / (1024**3):.2f} GB" if total_bytes > 0 else ""
        rows_str = f", {total_rows:,} rows" if total_rows > 0 else ""
        self.logger.info(f"Starting processing: {total_files} files{size_str}{rows_str}")
    
    def start_file(self, filename: str, file_size: int = 0, row_count: int = 0, file_format: str = None):
        self.stats.current_file = filename
        self.stats.current_file_format = file_format
        size_str = f" ({file_size / (1024**2):.1f} MB)" if file_size > 0 else ""
        format_str = f" [{file_format}]" if file_format else ""
        self.logger.info(f"Processing file {self.stats.processed_files + 1}/{self.stats.total_files}: {filename}{format_str}{size_str}")
    
    def update_phase(self, phase: ProgressPhase, **kwargs):
        self.stats.current_phase = phase
        self.logger.info(f"  Phase: {phase.value}")
    
    def update_progress(self, bytes_processed: int = 0, rows_processed: int = 0, **kwargs):
        self.stats.processed_bytes += bytes_processed
        self.stats.processed_rows += rows_processed
        
        # Log progress at intervals
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval:
            self._log_progress()
            self.last_log_time = current_time
    
    def complete_file(self, success: bool = True, error_message: str = None):
        self.stats.processed_files += 1
        
        if success:
            self.logger.info(f"  Completed: {self.stats.current_file}")
        else:
            self.stats.errors += 1
            self.logger.error(f"  Failed: {self.stats.current_file} - {error_message}")
        
        self._log_progress()
        self.stats.current_file = None
    
    def close(self):
        elapsed = timedelta(seconds=int(self.stats.elapsed_time))
        self.logger.info(f"Processing complete: {self.stats.processed_files}/{self.stats.total_files} files in {elapsed}")
        if self.stats.errors > 0:
            self.logger.warning(f"Completed with {self.stats.errors} errors")
    
    def _log_progress(self):
        """Log current progress"""
        pct = self.stats.progress_percentage
        elapsed = timedelta(seconds=int(self.stats.elapsed_time))
        
        eta_str = ""
        if self.stats.estimated_time_remaining:
            eta = timedelta(seconds=int(self.stats.estimated_time_remaining))
            eta_str = f", ETA: {eta}"
        
        self.logger.info(
            f"Progress: {self.stats.processed_files}/{self.stats.total_files} files "
            f"({pct:.1f}%), Elapsed: {elapsed}{eta_str}"
        )