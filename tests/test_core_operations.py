"""
Unit tests for core operations in Snowflake ETL Pipeline
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from pathlib import Path
import json
from datetime import datetime
import pandas as pd

from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.operations.validate_operation import ValidateOperation
from snowflake_etl.operations.delete_operation import DeleteOperation
from snowflake_etl.operations.duplicate_check_operation import DuplicateCheckOperation
from snowflake_etl.operations.compare_operation import CompareOperation


class TestLoadOperation:
    """Test suite for LoadOperation"""
    
    def test_init(self, application_context):
        """Test LoadOperation initialization"""
        op = LoadOperation(application_context)
        assert op.context == application_context
        assert op.config is not None
        assert op.logger is not None
    
    def test_find_matching_files(self, application_context, temp_dir, sample_tsv_file):
        """Test file pattern matching"""
        op = LoadOperation(application_context)
        
        # Mock the file finding logic
        with patch.object(op, '_find_files_for_pattern') as mock_find:
            mock_find.return_value = [sample_tsv_file]
            
            files = op._find_matching_files(
                base_path=temp_dir,
                month="2024-01",
                file_pattern="test_{date_range}.tsv"
            )
            
            assert len(files) == 1
            assert files[0] == sample_tsv_file
    
    def test_validate_file_columns(self, application_context, sample_tsv_file):
        """Test column validation for TSV files"""
        op = LoadOperation(application_context)
        
        # Create a file config
        file_config = {
            "expected_columns": ["col1", "col2", "col3", "recordDate"]
        }
        
        # Should not raise an exception for valid columns
        result = op._validate_file_columns(sample_tsv_file, file_config)
        assert result is True
    
    @patch('snowflake_etl.core.file_analyzer.FileAnalyzer')
    def test_analyze_file(self, mock_analyzer_class, application_context, sample_tsv_file):
        """Test file analysis"""
        mock_analyzer = MagicMock()
        mock_analyzer.analyze.return_value = {
            "row_count": 3,
            "estimated_time": 0.1
        }
        mock_analyzer_class.return_value = mock_analyzer
        
        op = LoadOperation(application_context)
        result = op._analyze_file(sample_tsv_file)
        
        assert result["row_count"] == 3
        assert result["estimated_time"] == 0.1
        mock_analyzer.analyze.assert_called_once()
    
    @patch('snowflake_etl.core.snowflake_loader.SnowflakeLoader')
    def test_load_file_to_snowflake(self, mock_loader_class, application_context, sample_tsv_file):
        """Test loading file to Snowflake"""
        mock_loader = MagicMock()
        mock_loader.load.return_value = {
            "status": "success",
            "rows_loaded": 3
        }
        mock_loader_class.return_value = mock_loader
        
        op = LoadOperation(application_context)
        file_config = {"table_name": "TEST_TABLE"}
        
        result = op._load_file_to_snowflake(sample_tsv_file, file_config)
        
        assert result["status"] == "success"
        assert result["rows_loaded"] == 3
        mock_loader.load.assert_called_once()
    
    def test_execute_with_invalid_base_path(self, application_context):
        """Test execute with invalid base path"""
        op = LoadOperation(application_context)
        
        with pytest.raises(ValueError, match="Base path"):
            op.execute(base_path="/nonexistent/path", month="2024-01")


class TestValidateOperation:
    """Test suite for ValidateOperation"""
    
    def test_init(self, application_context):
        """Test ValidateOperation initialization"""
        op = ValidateOperation(application_context)
        assert op.context == application_context
        assert op.validator is not None
    
    @patch('snowflake_etl.core.snowflake_validator.SnowflakeDataValidator')
    def test_execute_successful_validation(self, mock_validator_class, application_context):
        """Test successful validation execution"""
        mock_validator = MagicMock()
        mock_validator.validate_date_completeness.return_value = {
            "status": "success",
            "missing_dates": [],
            "total_rows": 1000
        }
        mock_validator_class.return_value = mock_validator
        
        op = ValidateOperation(application_context)
        result = op.execute(
            table="TEST_TABLE",
            date_column="recordDate",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert result["status"] == "success"
        assert result["missing_dates"] == []
        mock_validator.validate_date_completeness.assert_called_once()
    
    def test_execute_with_missing_dates(self, application_context):
        """Test validation with missing dates"""
        op = ValidateOperation(application_context)
        
        with patch.object(op.validator, 'validate_date_completeness') as mock_validate:
            mock_validate.return_value = {
                "status": "warning",
                "missing_dates": ["2024-01-15", "2024-01-16"],
                "total_rows": 900
            }
            
            result = op.execute(
                table="TEST_TABLE",
                date_column="recordDate",
                month="2024-01"
            )
            
            assert result["status"] == "warning"
            assert len(result["missing_dates"]) == 2
            assert "2024-01-15" in result["missing_dates"]
    
    def test_format_validation_results(self, application_context):
        """Test formatting of validation results"""
        op = ValidateOperation(application_context)
        
        results = {
            "status": "success",
            "table": "TEST_TABLE",
            "date_range": {
                "start": "2024-01-01",
                "end": "2024-01-31"
            },
            "statistics": {
                "total_rows": 100000,
                "avg_rows_per_day": 3226
            }
        }
        
        formatted = op._format_results(results)
        assert "TEST_TABLE" in formatted
        assert "100000" in formatted or "100,000" in formatted
        assert "success" in formatted.lower()


class TestDeleteOperation:
    """Test suite for DeleteOperation"""
    
    def test_init(self, application_context):
        """Test DeleteOperation initialization"""
        op = DeleteOperation(application_context)
        assert op.context == application_context
        assert op.deleter is not None
    
    def test_execute_dry_run(self, application_context):
        """Test delete operation in dry-run mode"""
        op = DeleteOperation(application_context)
        
        with patch.object(op.deleter, 'delete_month') as mock_delete:
            mock_delete.return_value = {
                "status": "dry_run",
                "would_delete": 10000,
                "table": "TEST_TABLE"
            }
            
            result = op.execute(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=True
            )
            
            assert result["status"] == "dry_run"
            assert result["would_delete"] == 10000
            mock_delete.assert_called_once_with(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=True
            )
    
    def test_execute_actual_deletion(self, application_context):
        """Test actual deletion execution"""
        op = DeleteOperation(application_context)
        
        with patch.object(op.deleter, 'delete_month') as mock_delete:
            mock_delete.return_value = {
                "status": "success",
                "rows_deleted": 5000,
                "table": "TEST_TABLE"
            }
            
            result = op.execute(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=False,
                confirm=True  # Skip confirmation prompt
            )
            
            assert result["status"] == "success"
            assert result["rows_deleted"] == 5000
    
    def test_execute_with_preview(self, application_context):
        """Test deletion with preview"""
        op = DeleteOperation(application_context)
        
        with patch.object(op.deleter, 'preview_deletion') as mock_preview:
            mock_preview.return_value = pd.DataFrame({
                "recordDate": ["2024-01-01", "2024-01-02"],
                "col1": ["val1", "val2"]
            })
            
            with patch.object(op.deleter, 'delete_month'):
                result = op.execute(
                    table="TEST_TABLE",
                    month="2024-01",
                    preview=True,
                    confirm=True
                )
                
                mock_preview.assert_called_once()


class TestDuplicateCheckOperation:
    """Test suite for DuplicateCheckOperation"""
    
    def test_init(self, application_context):
        """Test DuplicateCheckOperation initialization"""
        op = DuplicateCheckOperation(application_context)
        assert op.context == application_context
    
    def test_execute_no_duplicates(self, application_context):
        """Test duplicate check with no duplicates found"""
        op = DuplicateCheckOperation(application_context)
        
        with patch.object(op, '_check_duplicates') as mock_check:
            mock_check.return_value = {
                "duplicate_keys": 0,
                "excess_rows": 0,
                "percentage": 0.0,
                "severity": "NONE"
            }
            
            result = op.execute(
                table="TEST_TABLE",
                key_columns=["recordDate", "col1"],
                date_start="2024-01-01",
                date_end="2024-01-31"
            )
            
            assert result["duplicate_keys"] == 0
            assert result["severity"] == "NONE"
    
    def test_execute_with_duplicates(self, application_context):
        """Test duplicate check with duplicates found"""
        op = DuplicateCheckOperation(application_context)
        
        with patch.object(op, '_check_duplicates') as mock_check:
            mock_check.return_value = {
                "duplicate_keys": 150,
                "excess_rows": 300,
                "percentage": 0.03,
                "severity": "MEDIUM",
                "sample_duplicates": [
                    {"recordDate": "2024-01-15", "col1": "dup1"},
                    {"recordDate": "2024-01-20", "col1": "dup2"}
                ]
            }
            
            result = op.execute(
                table="TEST_TABLE",
                key_columns=["recordDate", "col1"]
            )
            
            assert result["duplicate_keys"] == 150
            assert result["severity"] == "MEDIUM"
            assert len(result["sample_duplicates"]) == 2
    
    def test_severity_calculation(self, application_context):
        """Test duplicate severity calculation"""
        op = DuplicateCheckOperation(application_context)
        
        # Test different severity levels
        assert op._calculate_severity(0, 0, 0.0) == "NONE"
        assert op._calculate_severity(50, 100, 0.005) == "LOW"
        assert op._calculate_severity(500, 1000, 0.02) == "MEDIUM"
        assert op._calculate_severity(5000, 10000, 0.06) == "HIGH"
        assert op._calculate_severity(50000, 100000, 0.11) == "CRITICAL"


class TestCompareOperation:
    """Test suite for CompareOperation"""
    
    def test_init(self, application_context):
        """Test CompareOperation initialization"""
        op = CompareOperation(application_context)
        assert op.context == application_context
    
    def test_execute_identical_files(self, application_context, temp_dir):
        """Test comparing identical files"""
        # Create two identical files
        file1 = temp_dir / "file1.tsv"
        file2 = temp_dir / "file2.tsv"
        content = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        
        file1.write_text(content)
        file2.write_text(content)
        
        op = CompareOperation(application_context)
        result = op.execute(str(file1), str(file2))
        
        assert result["identical"] is True
        assert result["differences"] == 0
    
    def test_execute_different_files(self, application_context, temp_dir):
        """Test comparing different files"""
        file1 = temp_dir / "file1.tsv"
        file2 = temp_dir / "file2.tsv"
        
        file1.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        file2.write_text("col1\tcol2\tcol3\nval4\tval5\tval6\n")
        
        op = CompareOperation(application_context)
        result = op.execute(str(file1), str(file2))
        
        assert result["identical"] is False
        assert result["differences"] > 0
    
    def test_execute_with_quick_mode(self, application_context, temp_dir):
        """Test quick comparison mode"""
        file1 = temp_dir / "file1.tsv"
        file2 = temp_dir / "file2.tsv"
        
        # Create files with different sizes
        file1.write_text("col1\tcol2\n" + "val1\tval2\n" * 100)
        file2.write_text("col1\tcol2\n" + "val1\tval2\n" * 50)
        
        op = CompareOperation(application_context)
        result = op.execute(str(file1), str(file2), quick=True)
        
        assert result["identical"] is False
        assert "size" in result.get("reason", "").lower()
    
    def test_execute_with_nonexistent_file(self, application_context, temp_dir):
        """Test comparing with nonexistent file"""
        file1 = temp_dir / "file1.tsv"
        file1.write_text("col1\tcol2\n")
        
        op = CompareOperation(application_context)
        
        with pytest.raises(FileNotFoundError):
            op.execute(str(file1), "/nonexistent/file.tsv")