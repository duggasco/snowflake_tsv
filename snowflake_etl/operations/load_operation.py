"""
Load operation that orchestrates the complete ETL pipeline.
Uses ApplicationContext for dependency injection.
"""

import os
import logging
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime

from ..core.application_context import ApplicationContext, BaseOperation
from ..models.file_config import FileConfig
from ..core.file_analyzer import FileAnalyzer
from ..validators.data_quality import DataQualityChecker
from ..core.snowflake_loader import SnowflakeLoader
from ..validators.snowflake_validator import SnowflakeDataValidator, ValidationResult
from ..core.progress import ProgressPhase


class LoadOperation(BaseOperation):
    """
    Orchestrates the complete ETL loading process:
    1. File analysis
    2. Quality checks (optional)
    3. Compression and upload
    4. Snowflake loading
    5. Post-load validation (optional)
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize with application context.
        
        Args:
            context: Application context with shared resources
        """
        super().__init__(context)
        
        # Initialize components with injected dependencies
        self.file_analyzer = FileAnalyzer()
        self.quality_checker = DataQualityChecker(progress_tracker=self.progress_tracker)
        self.loader = SnowflakeLoader(
            self.connection_manager,
            self.progress_tracker,
            self.logger
        )
        self.validator = SnowflakeDataValidator(
            self.connection_manager,
            self.progress_tracker,
            self.logger
        )
    
    def load_files(self,
                   files: List[FileConfig],
                   skip_qc: bool = False,
                   validate_in_snowflake: bool = False,
                   validate_only: bool = False,
                   max_workers: Optional[int] = None) -> Dict[str, Any]:
        """
        Load multiple TSV files to Snowflake.
        
        Args:
            files: List of file configurations to process
            skip_qc: Skip file-based quality checks
            validate_in_snowflake: Validate in Snowflake instead of file QC
            validate_only: Only validate existing data, don't load
            max_workers: Maximum parallel workers for QC
            
        Returns:
            Dictionary with load results and statistics
        """
        results = {
            'files_processed': 0,
            'files_failed': 0,
            'total_rows_loaded': 0,
            'validation_results': [],
            'errors': [],
            'start_time': datetime.now(),
            'file_results': []
        }
        
        for file_config in files:
            try:
                if validate_only:
                    # Only validate existing data
                    result = self._validate_only(file_config)
                else:
                    # Full load process
                    result = self._process_file(
                        file_config,
                        skip_qc,
                        validate_in_snowflake,
                        max_workers
                    )
                
                results['file_results'].append(result)
                
                if result.get('success'):
                    results['files_processed'] += 1
                    results['total_rows_loaded'] += result.get('rows_loaded', 0)
                else:
                    results['files_failed'] += 1
                    results['errors'].append(result.get('error'))
                
                # Add validation results if available
                if result.get('validation_result'):
                    results['validation_results'].append(result['validation_result'])
                    
            except Exception as e:
                self.logger.error(f"Failed to process {file_config.file_path}: {e}")
                results['files_failed'] += 1
                results['errors'].append(str(e))
                results['file_results'].append({
                    'file': file_config.file_path,
                    'success': False,
                    'error': str(e)
                })
        
        results['end_time'] = datetime.now()
        results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
        
        return results
    
    def _process_file(self,
                     file_config: FileConfig,
                     skip_qc: bool,
                     validate_in_snowflake: bool,
                     max_workers: Optional[int]) -> Dict:
        """
        Process a single file through the ETL pipeline.
        
        Returns:
            Dictionary with processing results
        """
        result = {
            'file': file_config.file_path,
            'table': file_config.table_name,
            'success': False,
            'rows_loaded': 0,
            'validation_result': None
        }
        
        self.logger.info(f"Processing {file_config.file_path}")
        
        try:
            # Phase 1: File Analysis
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.ANALYSIS)
            
            self.logger.info(f"Analyzing {file_config.file_path}")
            row_count = self.file_analyzer.count_rows_fast(file_config.file_path)
            file_size_mb = os.path.getsize(file_config.file_path) / (1024 * 1024)
            
            self.logger.info(
                f"File contains ~{row_count:,} rows ({file_size_mb:.1f} MB)"
            )
            
            # Phase 2: Quality Checks (optional)
            if not skip_qc and not validate_in_snowflake:
                if self.progress_tracker:
                    self.progress_tracker.update_phase(ProgressPhase.QUALITY_CHECK)
                
                self.logger.info("Running file-based quality checks")
                qc_result = self._run_quality_checks(file_config, max_workers)
                
                if not qc_result['valid']:
                    result['success'] = False
                    result['error'] = f"Quality check failed: {qc_result.get('error')}"
                    return result
            
            # Phase 3: Load to Snowflake
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.UPLOAD)
            
            rows_loaded = self.loader.load_file(file_config)
            result['rows_loaded'] = rows_loaded
            result['success'] = True
            
            # Phase 4: Post-load validation (optional)
            if validate_in_snowflake:
                if self.progress_tracker:
                    self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
                
                self.logger.info("Validating data in Snowflake")
                validation_result = self._validate_in_snowflake(file_config)
                result['validation_result'] = validation_result.to_dict()
                
                if not validation_result.valid:
                    self.logger.warning(
                        f"Validation issues found: {validation_result.failure_reasons}"
                    )
            
        except Exception as e:
            self.logger.error(f"Failed to process {file_config.file_path}: {e}")
            result['success'] = False
            result['error'] = str(e)
        
        return result
    
    def _validate_only(self, file_config: FileConfig) -> Dict:
        """
        Only validate existing data without loading.
        
        Returns:
            Dictionary with validation results
        """
        result = {
            'file': file_config.file_path,
            'table': file_config.table_name,
            'success': True,
            'validation_result': None
        }
        
        self.logger.info(f"Validating existing data in {file_config.table_name}")
        
        try:
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.VALIDATION)
            
            validation_result = self._validate_in_snowflake(file_config)
            result['validation_result'] = validation_result.to_dict()
            
            if not validation_result.valid:
                self.logger.warning(
                    f"Validation failed for {file_config.table_name}: "
                    f"{validation_result.failure_reasons}"
                )
                result['success'] = False
                result['error'] = f"Validation failed: {validation_result.failure_reasons}"
            
        except Exception as e:
            self.logger.error(f"Validation failed for {file_config.table_name}: {e}")
            result['success'] = False
            result['error'] = str(e)
        
        return result
    
    def _run_quality_checks(self,
                           file_config: FileConfig,
                           max_workers: Optional[int]) -> Dict:
        """
        Run file-based quality checks.
        
        Returns:
            Dictionary with QC results
        """
        try:
            # Parse expected date range from file config
            expected_date_range = file_config.expected_date_range
            if expected_date_range:
                start_date, end_date = expected_date_range
            else:
                # Try to extract from filename
                start_date, end_date = self._extract_dates_from_filename(
                    file_config.file_path
                )
            
            # Run quality checks
            is_valid, stats = self.quality_checker.check_data_quality(
                file_config.file_path,
                file_config.date_column,
                file_config.expected_columns,
                expected_start_date=start_date,
                expected_end_date=end_date,
                max_workers=max_workers
            )
            
            return {
                'valid': is_valid,
                'stats': stats,
                'error': stats.get('error') if not is_valid else None
            }
            
        except Exception as e:
            self.logger.error(f"Quality check failed: {e}")
            return {
                'valid': False,
                'error': str(e)
            }
    
    def _validate_in_snowflake(self, file_config: FileConfig) -> ValidationResult:
        """
        Validate data in Snowflake table.
        
        Returns:
            ValidationResult with details
        """
        # Extract date range for validation
        expected_date_range = file_config.expected_date_range
        if expected_date_range:
            start_date, end_date = expected_date_range
        else:
            start_date, end_date = None, None
        
        # Run validation
        return self.validator.validate_table(
            table_name=file_config.table_name,
            date_column=file_config.date_column,
            start_date=start_date,
            end_date=end_date,
            duplicate_key_columns=file_config.duplicate_key_columns
        )
    
    def _extract_dates_from_filename(self, file_path: str) -> tuple:
        """
        Extract date range from filename pattern.
        
        Returns:
            Tuple of (start_date, end_date) or (None, None)
        """
        import re
        
        filename = Path(file_path).name
        
        # Try date range pattern (YYYYMMDD-YYYYMMDD)
        range_match = re.search(r'(\d{8})-(\d{8})', filename)
        if range_match:
            start = range_match.group(1)
            end = range_match.group(2)
            return (
                f"{start[:4]}-{start[4:6]}-{start[6:8]}",
                f"{end[:4]}-{end[4:6]}-{end[6:8]}"
            )
        
        # Try month pattern (YYYY-MM)
        month_match = re.search(r'(\d{4})-(\d{2})', filename)
        if month_match:
            year = month_match.group(1)
            month = month_match.group(2)
            # Calculate last day of month
            import calendar
            last_day = calendar.monthrange(int(year), int(month))[1]
            return (
                f"{year}-{month}-01",
                f"{year}-{month}-{last_day:02d}"
            )
        
        return (None, None)
    
    def analyze_files(self, files: List[FileConfig]) -> Dict[str, Any]:
        """
        Analyze files without loading them.
        
        Args:
            files: List of file configurations
            
        Returns:
            Dictionary with analysis results
        """
        results = {
            'total_files': len(files),
            'total_size_mb': 0,
            'estimated_rows': 0,
            'estimated_time_minutes': 0,
            'file_details': []
        }
        
        for file_config in files:
            try:
                if not os.path.exists(file_config.file_path):
                    self.logger.warning(f"File not found: {file_config.file_path}")
                    continue
                
                # Analyze file
                row_count = self.file_analyzer.count_rows_fast(file_config.file_path)
                file_size = os.path.getsize(file_config.file_path)
                file_size_mb = file_size / (1024 * 1024)
                
                # Estimate processing time
                estimated_time = self.file_analyzer.estimate_processing_time(
                    file_size, row_count
                )
                
                file_detail = {
                    'file': file_config.file_path,
                    'table': file_config.table_name,
                    'size_mb': round(file_size_mb, 2),
                    'estimated_rows': row_count,
                    'estimated_minutes': round(estimated_time / 60, 1)
                }
                
                results['file_details'].append(file_detail)
                results['total_size_mb'] += file_size_mb
                results['estimated_rows'] += row_count
                results['estimated_time_minutes'] += estimated_time / 60
                
            except Exception as e:
                self.logger.error(f"Failed to analyze {file_config.file_path}: {e}")
                results['file_details'].append({
                    'file': file_config.file_path,
                    'error': str(e)
                })
        
        results['total_size_mb'] = round(results['total_size_mb'], 2)
        results['estimated_time_minutes'] = round(results['estimated_time_minutes'], 1)
        
        return results