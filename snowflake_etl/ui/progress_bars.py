"""
Visual progress tracking using tqdm
Simplified version without bash parallelism complexity
"""

import sys
import logging
from typing import Optional, Dict, Any

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from snowflake_etl.core.progress import ProgressTracker, ProgressPhase, ProgressStats


class TqdmProgressTracker(ProgressTracker):
    """
    Progress tracker using tqdm for visual progress bars
    Simplified from original - no longer handles bash parallel jobs
    """
    
    def __init__(self, position: int = 0, leave: bool = True):
        """
        Initialize tqdm progress tracker
        
        Args:
            position: Starting position for progress bars
            leave: Whether to leave progress bars on screen after completion
        """
        super().__init__()
        self.position = position
        self.leave = leave
        self.bars = {}
        
        if not TQDM_AVAILABLE:
            self.logger.warning("tqdm not available, falling back to logging")
            self.fallback = LoggingProgressTracker()
        else:
            self.fallback = None
    
    def initialize(self, total_files: int, total_bytes: int = 0, total_rows: int = 0, **kwargs):
        """Initialize progress bars"""
        if self.fallback:
            return self.fallback.initialize(total_files, total_bytes, total_rows, **kwargs)
        
        self.stats.total_files = total_files
        self.stats.total_bytes = total_bytes
        self.stats.total_rows = total_rows
        
        # Main progress bar for files
        self.bars['files'] = tqdm(
            total=total_files,
            desc="Files",
            unit="file",
            position=self.position,
            leave=self.leave,
            file=sys.stderr
        )
        
        # Optional progress bar for data volume
        if total_bytes > 0:
            self.bars['data'] = tqdm(
                total=total_bytes,
                desc="Data",
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                position=self.position + 1,
                leave=self.leave,
                file=sys.stderr
            )
        
        # Optional progress bar for rows (only if doing QC)
        if total_rows > 0 and kwargs.get('show_row_progress', False):
            self.bars['rows'] = tqdm(
                total=total_rows,
                desc="Rows",
                unit="rows",
                unit_scale=True,
                position=self.position + 2,
                leave=self.leave,
                file=sys.stderr
            )
    
    def start_file(self, filename: str, file_size: int = 0, row_count: int = 0, file_format: str = None):
        """Start processing a new file"""
        if self.fallback:
            return self.fallback.start_file(filename, file_size, row_count, file_format)
        
        self.stats.current_file = filename
        self.stats.current_file_format = file_format
        
        # Update file bar description with format
        if 'files' in self.bars:
            format_str = f" [{file_format}]" if file_format else ""
            self.bars['files'].set_postfix_str(f"Current: {filename}{format_str}")
        
        # Create phase-specific bar for this file
        if 'phase' in self.bars:
            self.bars['phase'].close()
        
        self.bars['phase'] = tqdm(
            total=100,  # Percentage
            desc="File Progress",
            unit="%",
            position=self.position + len(self.bars),
            leave=False,  # Don't leave file-specific bars
            file=sys.stderr
        )
    
    def update_phase(self, phase: ProgressPhase, **kwargs):
        """Update current processing phase"""
        if self.fallback:
            return self.fallback.update_phase(phase, **kwargs)
        
        self.stats.current_phase = phase
        
        # Update phase bar description
        if 'phase' in self.bars:
            self.bars['phase'].set_description(f"{phase.value.title()}")
            
            # Set phase progress based on typical flow
            phase_progress = {
                ProgressPhase.ANALYSIS: 10,
                ProgressPhase.QUALITY_CHECK: 30,
                ProgressPhase.COMPRESSION: 50,
                ProgressPhase.UPLOAD: 70,
                ProgressPhase.COPY: 90,
                ProgressPhase.VALIDATION: 95,
                ProgressPhase.COMPLETE: 100
            }
            
            if phase in phase_progress:
                current = self.bars['phase'].n
                target = phase_progress[phase]
                if target > current:
                    self.bars['phase'].update(target - current)
    
    def update_progress(self, bytes_processed: int = 0, rows_processed: int = 0, **kwargs):
        """Update progress within current phase"""
        if self.fallback:
            return self.fallback.update_progress(bytes_processed, rows_processed, **kwargs)
        
        self.stats.processed_bytes += bytes_processed
        self.stats.processed_rows += rows_processed
        
        # Update data bar
        if 'data' in self.bars and bytes_processed > 0:
            self.bars['data'].update(bytes_processed)
        
        # Update rows bar
        if 'rows' in self.bars and rows_processed > 0:
            self.bars['rows'].update(rows_processed)
        
        # Update phase-specific progress if provided
        if 'phase' in self.bars and 'phase_percent' in kwargs:
            current = self.bars['phase'].n
            target = kwargs['phase_percent']
            if target > current:
                self.bars['phase'].update(target - current)
    
    def complete_file(self, success: bool = True, error_message: str = None):
        """Mark current file as complete"""
        if self.fallback:
            return self.fallback.complete_file(success, error_message)
        
        self.stats.processed_files += 1
        
        if not success:
            self.stats.errors += 1
            if 'files' in self.bars:
                self.bars['files'].set_postfix_str(f"Error: {error_message}")
        
        # Update files bar
        if 'files' in self.bars:
            self.bars['files'].update(1)
        
        # Close and remove phase bar
        if 'phase' in self.bars:
            self.bars['phase'].close()
            del self.bars['phase']
        
        self.stats.current_file = None
    
    def close(self):
        """Close all progress bars"""
        if self.fallback:
            return self.fallback.close()
        
        for bar in self.bars.values():
            bar.close()
        self.bars.clear()


class ParallelTqdmProgressTracker(TqdmProgressTracker):
    """
    Enhanced tqdm tracker for Python-native parallel processing
    Handles multiple workers updating the same progress bars
    """
    
    def __init__(self, num_workers: int = 1, **kwargs):
        """
        Initialize parallel progress tracker
        
        Args:
            num_workers: Number of parallel workers
            **kwargs: Arguments passed to parent class
        """
        super().__init__(**kwargs)
        self.num_workers = num_workers
        self.worker_bars = {}
    
    def start_worker(self, worker_id: int, task_description: str = None):
        """
        Create a progress bar for a specific worker
        
        Args:
            worker_id: Unique identifier for the worker
            task_description: Description of what worker is doing
        """
        if self.fallback or worker_id in self.worker_bars:
            return
        
        position = self.position + len(self.bars) + len(self.worker_bars) + 1
        desc = f"Worker {worker_id}" if task_description is None else task_description
        
        self.worker_bars[worker_id] = tqdm(
            desc=desc,
            unit="task",
            position=position,
            leave=False,
            file=sys.stderr
        )
    
    def update_worker(self, worker_id: int, description: str = None, progress: int = None):
        """
        Update a worker's progress bar
        
        Args:
            worker_id: Worker identifier
            description: New description for the bar
            progress: Progress amount to add
        """
        if self.fallback or worker_id not in self.worker_bars:
            return
        
        bar = self.worker_bars[worker_id]
        if description:
            bar.set_description(description)
        if progress:
            bar.update(progress)
    
    def complete_worker(self, worker_id: int):
        """
        Mark a worker as complete and remove its bar
        
        Args:
            worker_id: Worker identifier
        """
        if self.fallback or worker_id not in self.worker_bars:
            return
        
        self.worker_bars[worker_id].close()
        del self.worker_bars[worker_id]
    
    def close(self):
        """Close all progress bars including worker bars"""
        # Close worker bars first
        for bar in self.worker_bars.values():
            bar.close()
        self.worker_bars.clear()
        
        # Then close main bars
        super().close()


# Import at module level for backward compatibility
from snowflake_etl.core.progress import LoggingProgressTracker