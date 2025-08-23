"""
File comparison operation for identifying differences between TSV files.
Uses ApplicationContext for dependency injection.
"""

import os
import time
import subprocess
import chardet
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, field, asdict
from collections import Counter

from ..core.application_context import ApplicationContext, BaseOperation
from ..core.progress import ProgressPhase


@dataclass
class FileCharacteristics:
    """Characteristics of a file"""
    file_path: str
    file_name: str
    size_mb: float
    encoding: str
    encoding_confidence: float
    line_ending: str  # LF, CRLF, or Mixed
    line_count: int
    is_estimated: bool  # True if line count is estimated
    column_count_min: int
    column_count_max: int
    column_consistency: bool
    sample_rows: List[str] = field(default_factory=list)
    delimiter: str = '\t'
    has_header: bool = False
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class ComparisonResult:
    """Result from file comparison"""
    file1: FileCharacteristics
    file2: FileCharacteristics
    differences: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    is_compatible: bool = True
    execution_time: float = 0
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert FileCharacteristics to dict
        result['file1'] = self.file1.to_dict() if self.file1 else None
        result['file2'] = self.file2.to_dict() if self.file2 else None
        return result


class CompareOperation(BaseOperation):
    """
    Compares TSV files to identify structural differences.
    Optimized for large files with streaming analysis.
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
        """
        super().__init__(context)
        self.buffer_size = 8 * 1024 * 1024  # 8MB buffer for performance
    
    def compare_files(self,
                     file1_path: str,
                     file2_path: str,
                     quick_mode: bool = False,
                     sample_size_mb: int = 100) -> ComparisonResult:
        """
        Compare two TSV files for structural differences.
        
        Args:
            file1_path: Path to first file (typically the "good" file)
            file2_path: Path to second file (typically the "bad" file)
            quick_mode: If True, use sampling for faster analysis
            sample_size_mb: Size of sample in MB for quick mode
            
        Returns:
            ComparisonResult with detailed analysis
        """
        start_time = time.time()
        
        # Initialize result
        result = ComparisonResult(
            file1=None,
            file2=None
        )
        
        # Update progress phase
        if self.progress_tracker:
            self.progress_tracker.update_phase(ProgressPhase.ANALYSIS)
        
        try:
            # Validate files exist
            file1_path = Path(file1_path)
            file2_path = Path(file2_path)
            
            if not file1_path.exists():
                result.error = f"File not found: {file1_path}"
                return result
            
            if not file2_path.exists():
                result.error = f"File not found: {file2_path}"
                return result
            
            # Analyze both files
            self.logger.info(f"Analyzing file 1: {file1_path.name}")
            file1_chars = self._analyze_file(file1_path, quick_mode, sample_size_mb)
            
            self.logger.info(f"Analyzing file 2: {file2_path.name}")
            file2_chars = self._analyze_file(file2_path, quick_mode, sample_size_mb)
            
            result.file1 = file1_chars
            result.file2 = file2_chars
            
            # Compare characteristics
            self._compare_characteristics(result)
            
            # Determine compatibility
            result.is_compatible = len(result.differences) == 0
            
        except Exception as e:
            result.error = str(e)
            self.logger.error(f"Error comparing files: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
        
        return result
    
    def _analyze_file(self,
                     file_path: Path,
                     quick_mode: bool,
                     sample_size_mb: int) -> FileCharacteristics:
        """
        Analyze a single file's characteristics.
        
        Args:
            file_path: Path to the file
            quick_mode: Whether to use sampling
            sample_size_mb: Sample size for quick mode
            
        Returns:
            FileCharacteristics object
        """
        chars = FileCharacteristics(
            file_path=str(file_path),
            file_name=file_path.name,
            size_mb=file_path.stat().st_size / (1024 * 1024),
            encoding='unknown',
            encoding_confidence=0.0,
            line_ending='unknown',
            line_count=0,
            is_estimated=False,
            column_count_min=0,
            column_count_max=0,
            column_consistency=True
        )
        
        # Detect encoding
        self.logger.debug(f"Detecting encoding for {file_path.name}")
        with open(file_path, 'rb') as f:
            sample = f.read(min(100000, file_path.stat().st_size))
            encoding_info = chardet.detect(sample)
            chars.encoding = encoding_info.get('encoding', 'unknown')
            chars.encoding_confidence = encoding_info.get('confidence', 0.0)
        
        # Detect line endings
        self.logger.debug(f"Detecting line endings for {file_path.name}")
        chars.line_ending = self._detect_line_ending(sample)
        
        # Count lines
        self.logger.debug(f"Counting lines for {file_path.name}")
        if quick_mode and chars.size_mb > sample_size_mb:
            # Use sampling for large files
            chars.line_count, chars.is_estimated = self._sample_line_count(
                file_path, sample_size_mb
            )
        else:
            # Try fast method first (wc -l)
            line_count = self._fast_line_count(file_path)
            if line_count is not None:
                chars.line_count = line_count
            else:
                # Fall back to buffered counting
                chars.line_count = self._buffered_line_count(file_path)
        
        # Analyze column structure
        self.logger.debug(f"Analyzing columns for {file_path.name}")
        self._analyze_columns(file_path, chars)
        
        # Get sample rows
        self.logger.debug(f"Getting sample rows for {file_path.name}")
        chars.sample_rows = self._get_sample_rows(file_path, 5)
        
        # Detect if file has header
        if chars.sample_rows:
            chars.has_header = self._detect_header(chars.sample_rows[0], chars)
        
        return chars
    
    def _detect_line_ending(self, sample: bytes) -> str:
        """Detect line ending type from sample."""
        has_crlf = b'\r\n' in sample
        has_lf = b'\n' in sample
        has_cr = b'\r' in sample and not has_crlf
        
        if has_crlf and has_lf:
            # Check if all \n are preceded by \r
            if sample.count(b'\n') == sample.count(b'\r\n'):
                return 'CRLF'
            else:
                return 'Mixed'
        elif has_crlf:
            return 'CRLF'
        elif has_lf:
            return 'LF'
        elif has_cr:
            return 'CR'
        else:
            return 'None'
    
    def _fast_line_count(self, file_path: Path) -> Optional[int]:
        """Use wc -l for fast line counting on Unix systems."""
        try:
            result = subprocess.run(
                ['wc', '-l', str(file_path)],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                return int(result.stdout.split()[0])
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
            pass
        return None
    
    def _buffered_line_count(self, file_path: Path) -> int:
        """Count lines with large buffer for better performance."""
        count = 0
        with open(file_path, 'rb') as f:
            while True:
                buffer = f.read(self.buffer_size)
                if not buffer:
                    break
                count += buffer.count(b'\n')
        return count
    
    def _sample_line_count(self, file_path: Path, sample_size_mb: int) -> Tuple[int, bool]:
        """Estimate row count by sampling first N MB of file."""
        file_size = file_path.stat().st_size
        sample_bytes = min(sample_size_mb * 1024 * 1024, file_size)
        
        with open(file_path, 'rb') as f:
            sample = f.read(sample_bytes)
            lines_in_sample = sample.count(b'\n')
        
        # Extrapolate to full file
        if sample_bytes < file_size:
            estimated_lines = int(lines_in_sample * (file_size / sample_bytes))
            return estimated_lines, True  # True indicates estimate
        else:
            return lines_in_sample, False
    
    def _analyze_columns(self, file_path: Path, chars: FileCharacteristics):
        """Analyze column structure of the file."""
        column_counts = []
        
        try:
            with open(file_path, 'r', encoding=chars.encoding or 'utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= 1000:  # Sample first 1000 lines
                        break
                    
                    # Count delimiters
                    tab_count = line.count('\t')
                    comma_count = line.count(',')
                    pipe_count = line.count('|')
                    
                    # Determine delimiter if not set
                    if i == 0:
                        if tab_count > max(comma_count, pipe_count):
                            chars.delimiter = '\t'
                        elif comma_count > pipe_count:
                            chars.delimiter = ','
                        elif pipe_count > 0:
                            chars.delimiter = '|'
                        else:
                            chars.delimiter = '\t'  # Default
                    
                    # Count columns based on delimiter
                    if chars.delimiter == '\t':
                        col_count = tab_count + 1
                    elif chars.delimiter == ',':
                        col_count = comma_count + 1
                    else:
                        col_count = pipe_count + 1
                    
                    column_counts.append(col_count)
            
            if column_counts:
                chars.column_count_min = min(column_counts)
                chars.column_count_max = max(column_counts)
                chars.column_consistency = chars.column_count_min == chars.column_count_max
                
        except Exception as e:
            self.logger.warning(f"Error analyzing columns: {e}")
    
    def _get_sample_rows(self, file_path: Path, count: int) -> List[str]:
        """Get sample rows from the file."""
        samples = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if i >= count:
                        break
                    samples.append(line.strip())
        except Exception as e:
            self.logger.warning(f"Error getting sample rows: {e}")
        return samples
    
    def _detect_header(self, first_line: str, chars: FileCharacteristics) -> bool:
        """Detect if the first line is likely a header."""
        if not first_line:
            return False
        
        # Split by delimiter
        fields = first_line.split(chars.delimiter)
        
        # Heuristics for header detection
        # 1. Check if fields contain typical header keywords
        header_keywords = ['id', 'name', 'date', 'time', 'count', 'amount', 
                          'value', 'type', 'status', 'description']
        
        lowercase_fields = [f.lower().strip() for f in fields]
        keyword_matches = sum(1 for f in lowercase_fields 
                             if any(kw in f for kw in header_keywords))
        
        # 2. Check if fields are not numeric
        non_numeric_count = sum(1 for f in fields 
                               if not f.strip().replace('.', '').replace('-', '').isdigit())
        
        # If most fields are non-numeric and contain keywords, likely a header
        if non_numeric_count > len(fields) * 0.7 or keyword_matches > 2:
            return True
        
        return False
    
    def _compare_characteristics(self, result: ComparisonResult):
        """Compare file characteristics and identify differences."""
        file1 = result.file1
        file2 = result.file2
        
        # Size comparison
        size_diff_pct = abs(file1.size_mb - file2.size_mb) / max(file1.size_mb, 0.1) * 100
        if size_diff_pct > 10:
            result.differences.append(
                f"Significant size difference: {file1.size_mb:.1f}MB vs {file2.size_mb:.1f}MB "
                f"({size_diff_pct:.1f}% difference)"
            )
        
        # Encoding comparison
        if file1.encoding != file2.encoding:
            result.differences.append(
                f"Different encodings: {file1.encoding} vs {file2.encoding}"
            )
        
        # Line ending comparison
        if file1.line_ending != file2.line_ending:
            result.differences.append(
                f"Different line endings: {file1.line_ending} vs {file2.line_ending}"
            )
        
        # Column count comparison
        if file1.column_count_max != file2.column_count_max:
            result.differences.append(
                f"Different column counts: {file1.column_count_max} vs {file2.column_count_max}"
            )
        
        # Column consistency
        if not file1.column_consistency:
            result.warnings.append(f"File 1 has inconsistent column counts ({file1.column_count_min}-{file1.column_count_max})")
        if not file2.column_consistency:
            result.warnings.append(f"File 2 has inconsistent column counts ({file2.column_count_min}-{file2.column_count_max})")
        
        # Delimiter comparison
        if file1.delimiter != file2.delimiter:
            result.differences.append(
                f"Different delimiters: '{file1.delimiter}' vs '{file2.delimiter}'"
            )
        
        # Header detection
        if file1.has_header != file2.has_header:
            result.warnings.append(
                f"Header mismatch: File 1 {'has' if file1.has_header else 'no'} header, "
                f"File 2 {'has' if file2.has_header else 'no'} header"
            )
        
        # Line count comparison (if not estimated)
        if not (file1.is_estimated or file2.is_estimated):
            line_diff_pct = abs(file1.line_count - file2.line_count) / max(file1.line_count, 1) * 100
            if line_diff_pct > 5:
                result.warnings.append(
                    f"Line count difference: {file1.line_count:,} vs {file2.line_count:,} "
                    f"({line_diff_pct:.1f}% difference)"
                )
    
    def format_result(self, result: ComparisonResult) -> str:
        """
        Format comparison result for display.
        
        Args:
            result: The comparison result to format
            
        Returns:
            Formatted string for display
        """
        lines = []
        lines.append("=" * 60)
        lines.append("TSV FILE COMPARISON REPORT")
        lines.append("=" * 60)
        
        if result.error:
            lines.append(f"ERROR: {result.error}")
            return '\n'.join(lines)
        
        # File information
        lines.append("")
        lines.append("FILE 1 CHARACTERISTICS:")
        lines.append(f"  Name: {result.file1.file_name}")
        lines.append(f"  Size: {result.file1.size_mb:.1f} MB")
        lines.append(f"  Encoding: {result.file1.encoding} (confidence: {result.file1.encoding_confidence:.0%})")
        lines.append(f"  Line Ending: {result.file1.line_ending}")
        lines.append(f"  Lines: {result.file1.line_count:,}{' (estimated)' if result.file1.is_estimated else ''}")
        lines.append(f"  Columns: {result.file1.column_count_min}-{result.file1.column_count_max}")
        lines.append(f"  Delimiter: '{result.file1.delimiter}'")
        lines.append(f"  Has Header: {result.file1.has_header}")
        
        lines.append("")
        lines.append("FILE 2 CHARACTERISTICS:")
        lines.append(f"  Name: {result.file2.file_name}")
        lines.append(f"  Size: {result.file2.size_mb:.1f} MB")
        lines.append(f"  Encoding: {result.file2.encoding} (confidence: {result.file2.encoding_confidence:.0%})")
        lines.append(f"  Line Ending: {result.file2.line_ending}")
        lines.append(f"  Lines: {result.file2.line_count:,}{' (estimated)' if result.file2.is_estimated else ''}")
        lines.append(f"  Columns: {result.file2.column_count_min}-{result.file2.column_count_max}")
        lines.append(f"  Delimiter: '{result.file2.delimiter}'")
        lines.append(f"  Has Header: {result.file2.has_header}")
        
        # Differences
        if result.differences:
            lines.append("")
            lines.append("CRITICAL DIFFERENCES:")
            for diff in result.differences:
                lines.append(f"  - {diff}")
        
        # Warnings
        if result.warnings:
            lines.append("")
            lines.append("WARNINGS:")
            for warning in result.warnings:
                lines.append(f"  - {warning}")
        
        # Compatibility assessment
        lines.append("")
        if result.is_compatible:
            lines.append("ASSESSMENT: Files are structurally compatible")
        else:
            lines.append("ASSESSMENT: Files have structural incompatibilities")
        
        lines.append("")
        lines.append(f"Execution Time: {result.execution_time:.2f} seconds")
        lines.append("=" * 60)
        
        return '\n'.join(lines)