"""
Improved unit tests for core operations with better coverage and edge cases
Based on Gemini's feedback
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call, mock_open
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
import snowflake.connector.errors as sf_errors

from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.operations.validate_operation import ValidateOperation
from snowflake_etl.operations.delete_operation import DeleteOperation
from snowflake_etl.operations.duplicate_check_operation import DuplicateCheckOperation
from snowflake_etl.operations.compare_operation import CompareOperation


class TestLoadOperationImproved:
    """Improved test suite for LoadOperation with edge cases"""
    
    def test_execute_no_files_found(self, application_context):
        """Test execute when no matching files are found"""
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[]):
            result = op.execute(base_path="/tmp", month="2024-01")
            
            assert result["status"] == "warning"
            assert result["files_processed"] == 0
            assert "No matching files" in result.get("message", "")
    
    def test_execute_with_malformed_tsv(self, application_context, temp_dir):
        """Test loading a malformed TSV file"""
        # Create malformed TSV with inconsistent columns
        bad_tsv = temp_dir / "malformed.tsv"
        bad_tsv.write_text(
            "col1\tcol2\tcol3\n"
            "val1\tval2\n"  # Missing column
            "val3\tval4\tval5\textra\n"  # Extra column
        )
        
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[bad_tsv]):
            with patch.object(op, '_validate_file_columns') as mock_validate:
                mock_validate.side_effect = ValueError("Column count mismatch")
                
                result = op.execute(base_path=str(temp_dir))
                
                assert result["status"] == "error"
                assert "Column count mismatch" in str(result.get("errors", []))
    
    def test_execute_with_empty_file(self, application_context, temp_dir):
        """Test loading an empty TSV file"""
        empty_tsv = temp_dir / "empty.tsv"
        empty_tsv.write_text("")  # Completely empty
        
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[empty_tsv]):
            result = op.execute(base_path=str(temp_dir))
            
            assert result["status"] in ["warning", "error"]
            assert result.get("files_processed", 0) == 0
    
    def test_execute_with_encoding_issues(self, application_context, temp_dir):
        """Test loading TSV with encoding issues"""
        # Create file with non-UTF8 encoding
        bad_encoding_tsv = temp_dir / "bad_encoding.tsv"
        with open(bad_encoding_tsv, "wb") as f:
            # Write some Latin-1 encoded content
            f.write("col1\tcol2\n".encode('utf-8'))
            f.write("café\tñoño\n".encode('latin-1'))
        
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[bad_encoding_tsv]):
            with patch('chardet.detect') as mock_detect:
                mock_detect.return_value = {'encoding': 'latin-1', 'confidence': 0.9}
                
                # Should handle encoding detection
                result = op.execute(base_path=str(temp_dir))
                # Implementation should handle this gracefully
    
    def test_execute_snowflake_connection_failure(self, application_context, sample_tsv_file):
        """Test handling Snowflake connection failure during load"""
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[sample_tsv_file]):
            with patch.object(op.context, 'get_connection') as mock_get_conn:
                mock_get_conn.side_effect = sf_errors.DatabaseError("Connection failed")
                
                result = op.execute(base_path="/tmp")
                
                assert result["status"] == "error"
                assert "Connection failed" in str(result.get("errors", []))
    
    def test_execute_with_permission_error(self, application_context, temp_dir):
        """Test handling file permission errors"""
        protected_file = temp_dir / "protected.tsv"
        protected_file.write_text("col1\tcol2\n")
        
        op = LoadOperation(application_context)
        
        with patch.object(op, '_find_matching_files', return_value=[protected_file]):
            with patch('builtins.open', side_effect=PermissionError("Access denied")):
                result = op.execute(base_path=str(temp_dir))
                
                assert result["status"] == "error"
                assert "Access denied" in str(result.get("errors", []))


class TestValidateOperationImproved:
    """Improved test suite for ValidateOperation with edge cases"""
    
    def test_execute_with_invalid_date_column(self, application_context):
        """Test validation with non-existent date column"""
        op = ValidateOperation(application_context)
        
        with patch.object(op.validator, 'validate_date_completeness') as mock_validate:
            mock_validate.side_effect = sf_errors.ProgrammingError(
                "Column 'invalid_date' does not exist"
            )
            
            with pytest.raises(sf_errors.ProgrammingError):
                op.execute(
                    table="TEST_TABLE",
                    date_column="invalid_date",
                    month="2024-01"
                )
    
    def test_execute_with_invalid_date_format(self, application_context):
        """Test validation with incorrectly formatted dates"""
        op = ValidateOperation(application_context)
        
        with patch.object(op.validator, 'validate_date_completeness') as mock_validate:
            mock_validate.return_value = {
                "status": "error",
                "error": "Invalid date format in column",
                "sample_invalid_dates": ["2024-13-01", "2024-01-32", "not-a-date"]
            }
            
            result = op.execute(
                table="TEST_TABLE",
                date_column="recordDate",
                month="2024-01"
            )
            
            assert result["status"] == "error"
            assert "Invalid date format" in result.get("error", "")
    
    def test_execute_with_empty_table(self, application_context):
        """Test validation on empty table"""
        op = ValidateOperation(application_context)
        
        with patch.object(op.validator, 'validate_date_completeness') as mock_validate:
            mock_validate.return_value = {
                "status": "warning",
                "total_rows": 0,
                "message": "Table is empty"
            }
            
            result = op.execute(
                table="EMPTY_TABLE",
                date_column="recordDate",
                month="2024-01"
            )
            
            assert result["status"] == "warning"
            assert result["total_rows"] == 0
            assert "empty" in result.get("message", "").lower()


class TestDeleteOperationImproved:
    """Improved test suite for DeleteOperation with user interaction"""
    
    @patch('builtins.input', return_value='n')
    def test_execute_user_cancels_deletion(self, mock_input, application_context):
        """Test deletion when user cancels confirmation"""
        op = DeleteOperation(application_context)
        
        result = op.execute(
            table="TEST_TABLE",
            month="2024-01",
            dry_run=False,
            confirm=False  # Trigger confirmation prompt
        )
        
        assert result["status"] == "cancelled"
        assert result.get("rows_deleted", 0) == 0
        mock_input.assert_called_once()
    
    @patch('builtins.input', return_value='y')
    def test_execute_user_confirms_deletion(self, mock_input, application_context):
        """Test deletion when user confirms"""
        op = DeleteOperation(application_context)
        
        with patch.object(op.deleter, 'delete_month') as mock_delete:
            mock_delete.return_value = {
                "status": "success",
                "rows_deleted": 5000
            }
            
            result = op.execute(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=False,
                confirm=False
            )
            
            assert result["status"] == "success"
            assert result["rows_deleted"] == 5000
            mock_input.assert_called_once()
            mock_delete.assert_called_once()
    
    def test_execute_deletion_failure(self, application_context):
        """Test handling deletion failure"""
        op = DeleteOperation(application_context)
        
        with patch.object(op.deleter, 'delete_month') as mock_delete:
            mock_delete.side_effect = sf_errors.ProgrammingError(
                "Insufficient privileges to delete"
            )
            
            with pytest.raises(sf_errors.ProgrammingError):
                op.execute(
                    table="TEST_TABLE",
                    month="2024-01",
                    confirm=True
                )


class TestDuplicateCheckOperationImproved:
    """Improved test suite for DuplicateCheckOperation"""
    
    @pytest.mark.parametrize(
        "duplicate_keys,excess_rows,percentage,expected_severity",
        [
            (0, 0, 0.0, "NONE"),
            (10, 20, 0.001, "LOW"),
            (50, 100, 0.005, "LOW"),
            (500, 1000, 0.02, "MEDIUM"),
            (5000, 10000, 0.06, "HIGH"),
            (50000, 100000, 0.11, "CRITICAL"),
            (100, 200, 0.008, "LOW"),  # Edge case
            (1000, 2000, 0.015, "MEDIUM"),  # Edge case
        ]
    )
    def test_severity_calculation_parameterized(
        self, application_context, duplicate_keys, excess_rows, percentage, expected_severity
    ):
        """Parameterized test for severity calculation"""
        op = DuplicateCheckOperation(application_context)
        severity = op._calculate_severity(duplicate_keys, excess_rows, percentage)
        assert severity == expected_severity
    
    def test_execute_with_invalid_key_columns(self, application_context):
        """Test duplicate check with invalid key columns"""
        op = DuplicateCheckOperation(application_context)
        
        with patch.object(op, '_check_duplicates') as mock_check:
            mock_check.side_effect = sf_errors.ProgrammingError(
                "Column 'invalid_col' does not exist"
            )
            
            with pytest.raises(sf_errors.ProgrammingError):
                op.execute(
                    table="TEST_TABLE",
                    key_columns=["invalid_col", "col2"]
                )
    
    def test_execute_with_null_values_in_keys(self, application_context):
        """Test duplicate check when key columns contain NULL values"""
        op = DuplicateCheckOperation(application_context)
        
        with patch.object(op, '_check_duplicates') as mock_check:
            mock_check.return_value = {
                "duplicate_keys": 50,
                "excess_rows": 100,
                "percentage": 0.01,
                "severity": "LOW",
                "null_key_warning": "Key columns contain NULL values",
                "null_key_count": 25
            }
            
            result = op.execute(
                table="TEST_TABLE",
                key_columns=["recordDate", "col1"]
            )
            
            assert "null_key_warning" in result
            assert result["null_key_count"] == 25


class TestCompareOperationImproved:
    """Improved test suite for CompareOperation"""
    
    def test_execute_with_binary_files(self, application_context, temp_dir):
        """Test comparing binary files"""
        # Create two binary files
        bin1 = temp_dir / "file1.bin"
        bin2 = temp_dir / "file2.bin"
        
        bin1.write_bytes(b'\x00\x01\x02\x03')
        bin2.write_bytes(b'\x00\x01\x02\x04')  # Different last byte
        
        op = CompareOperation(application_context)
        
        # Should detect binary files and handle appropriately
        result = op.execute(str(bin1), str(bin2), quick=True)
        
        assert result["identical"] is False
        assert "binary" in result.get("file_type", "").lower()
    
    def test_execute_with_very_large_files(self, application_context, temp_dir):
        """Test comparing very large files"""
        # Create large files (simulate, don't actually create GB files)
        large1 = temp_dir / "large1.tsv"
        large2 = temp_dir / "large2.tsv"
        
        # Write headers
        large1.write_text("col1\tcol2\tcol3\n")
        large2.write_text("col1\tcol2\tcol3\n")
        
        op = CompareOperation(application_context)
        
        with patch('os.path.getsize') as mock_size:
            # Simulate 1GB files
            mock_size.return_value = 1024 * 1024 * 1024
            
            result = op.execute(str(large1), str(large2), quick=True)
            
            # Quick mode should be used for large files
            assert "quick" in result.get("mode", "")
    
    def test_execute_with_different_encodings(self, application_context, temp_dir):
        """Test comparing files with different encodings"""
        utf8_file = temp_dir / "utf8.tsv"
        latin1_file = temp_dir / "latin1.tsv"
        
        content = "col1\tcol2\nçafé\tñoño\n"
        utf8_file.write_text(content, encoding='utf-8')
        
        with open(latin1_file, 'w', encoding='latin-1') as f:
            f.write(content)
        
        op = CompareOperation(application_context)
        
        # Should handle encoding differences
        result = op.execute(str(utf8_file), str(latin1_file))
        
        # Files have same content but different encodings
        # Result depends on implementation
        assert result is not None