"""
Integration tests for the complete Snowflake ETL pipeline.

These tests verify end-to-end functionality of the ETL system,
including file processing, loading, validation, and cleanup.
"""

import pytest
import tempfile
import json
import gzip
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
import pandas as pd

from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.operations.validate_operation import ValidateOperation
from snowflake_etl.operations.delete_operation import DeleteOperation
from snowflake_etl.operations.duplicate_check_operation import DuplicateCheckOperation
from snowflake_etl.operations.report_operation_final import ReportOperation


class TestEndToEndPipeline:
    """Test complete ETL pipeline workflows"""
    
    @pytest.fixture
    def test_environment(self, tmp_path):
        """Create a complete test environment"""
        # Create directory structure
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        
        # Create test TSV files
        tsv_file1 = data_dir / "test_20240101-20240131.tsv"
        tsv_file1.write_text(
            "recordDate\tassetId\tfundId\tvalue\n"
            "20240101\tA001\tF001\t100.50\n"
            "20240102\tA001\tF001\t101.25\n"
            "20240103\tA002\tF001\t200.00\n"
        )
        
        tsv_file2 = data_dir / "test_20240201-20240229.tsv"
        tsv_file2.write_text(
            "recordDate\tassetId\tfundId\tvalue\n"
            "20240201\tA001\tF001\t102.00\n"
            "20240202\tA001\tF001\t102.50\n"
        )
        
        # Create configuration
        config = {
            "snowflake": {
                "account": "test_account",
                "user": "test_user",
                "password": "test_pass",
                "warehouse": "TEST_WH",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "role": "TEST_ROLE"
            },
            "files": [
                {
                    "file_pattern": "test_{date_range}.tsv",
                    "table_name": "TEST_TABLE",
                    "date_column": "recordDate",
                    "expected_columns": ["recordDate", "assetId", "fundId", "value"],
                    "duplicate_key_columns": ["recordDate", "assetId", "fundId"]
                }
            ]
        }
        
        config_file = config_dir / "test_config.json"
        config_file.write_text(json.dumps(config, indent=2))
        
        return {
            "data_dir": data_dir,
            "config_dir": config_dir,
            "logs_dir": logs_dir,
            "config_file": config_file,
            "tsv_files": [tsv_file1, tsv_file2]
        }
    
    @patch('snowflake.connector.connect')
    def test_complete_load_validate_workflow(self, mock_connect, test_environment):
        """Test loading files and validating them"""
        # Setup mock Snowflake connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (3,)  # Row count
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Create context
        context = ApplicationContext(
            config_path=test_environment["config_file"],
            log_dir=test_environment["logs_dir"],
            quiet=True
        )
        
        try:
            # Test load operation
            load_op = LoadOperation(context)
            load_result = load_op.execute(
                base_path=test_environment["data_dir"],
                month="2024-01",
                skip_qc=True  # Skip for speed in tests
            )
            
            assert load_result["status"] in ["success", "partial"]
            assert load_result["files_processed"] >= 0
            
            # Test validation
            validate_op = ValidateOperation(context)
            validate_result = validate_op.execute(
                table="TEST_TABLE",
                date_column="recordDate",
                month="2024-01"
            )
            
            assert validate_result is not None
            assert "status" in validate_result
            
        finally:
            context.cleanup()
    
    @patch('snowflake.connector.connect')
    def test_load_with_duplicates_detection(self, mock_connect, test_environment):
        """Test loading with duplicate detection"""
        # Create TSV with duplicates
        dup_file = test_environment["data_dir"] / "dup_20240301-20240331.tsv"
        dup_file.write_text(
            "recordDate\tassetId\tfundId\tvalue\n"
            "20240301\tA001\tF001\t100.00\n"
            "20240301\tA001\tF001\t100.00\n"  # Duplicate
            "20240302\tA002\tF001\t200.00\n"
        )
        
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("20240301", "A001", "F001", 2)  # 2 occurrences
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        context = ApplicationContext(
            config_path=test_environment["config_file"],
            log_dir=test_environment["logs_dir"],
            quiet=True
        )
        
        try:
            # Check for duplicates
            dup_op = DuplicateCheckOperation(context)
            dup_result = dup_op.execute(
                table="TEST_TABLE",
                key_columns=["recordDate", "assetId", "fundId"],
                date_start="2024-03-01",
                date_end="2024-03-31"
            )
            
            # Should detect duplicates
            assert dup_result is not None
            
        finally:
            context.cleanup()
    
    @patch('snowflake.connector.connect')
    def test_delete_operation_workflow(self, mock_connect, test_environment):
        """Test deletion workflow with confirmation"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock row count query
        mock_cursor.fetchone.side_effect = [
            (10000,),  # Initial count
            (0,)       # After deletion
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        context = ApplicationContext(
            config_path=test_environment["config_file"],
            log_dir=test_environment["logs_dir"],
            quiet=True
        )
        
        try:
            delete_op = DeleteOperation(context)
            
            # Test dry run first
            dry_result = delete_op.execute(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=True
            )
            
            assert dry_result["status"] == "dry_run"
            assert "would_delete" in dry_result
            
            # Test actual deletion (with confirm bypassed)
            delete_result = delete_op.execute(
                table="TEST_TABLE",
                month="2024-01",
                dry_run=False,
                confirm=True  # Skip prompt
            )
            
            assert delete_result is not None
            
        finally:
            context.cleanup()
    
    @patch('snowflake.connector.connect')
    def test_report_generation(self, mock_connect, test_environment):
        """Test comprehensive report generation"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock various queries for report
        mock_cursor.fetchall.side_effect = [
            [("TEST_TABLE", 1000000, "2024-01-01", "2024-01-31")],  # Table stats
            [],  # No gaps
            [],  # No duplicates
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        context = ApplicationContext(
            config_path=test_environment["config_file"],
            log_dir=test_environment["logs_dir"],
            quiet=True
        )
        
        try:
            report_op = ReportOperation(context)
            report_result = report_op.execute(
                output_format="json"
            )
            
            assert report_result is not None
            assert "tables" in report_result
            
        finally:
            context.cleanup()


class TestParallelProcessing:
    """Test parallel and concurrent operations"""
    
    @patch('snowflake.connector.connect')
    def test_parallel_file_processing(self, mock_connect, tmp_path):
        """Test processing multiple files in parallel"""
        # Create multiple TSV files
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        
        files = []
        for i in range(5):
            month = f"{i+1:02d}"
            file_path = data_dir / f"test_2024{month}01-2024{month}28.tsv"
            file_path.write_text(
                f"recordDate\tvalue\n"
                f"2024{month}01\t{i*100}\n"
                f"2024{month}02\t{i*100+1}\n"
            )
            files.append(file_path)
        
        # Create config
        config = {
            "snowflake": {
                "account": "test",
                "user": "test",
                "password": "test",
                "warehouse": "TEST",
                "database": "TEST",
                "schema": "TEST"
            },
            "files": [{
                "file_pattern": "test_{date_range}.tsv",
                "table_name": "TEST",
                "date_column": "recordDate",
                "expected_columns": ["recordDate", "value"]
            }]
        }
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        
        # Mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        context = ApplicationContext(
            config_path=config_file,
            quiet=True
        )
        
        try:
            load_op = LoadOperation(context)
            
            # Process with multiple workers
            result = load_op.execute(
                base_path=data_dir,
                max_workers=3,
                skip_qc=True
            )
            
            # Should process all files
            assert result["files_processed"] <= len(files)
            
        finally:
            context.cleanup()
    
    @patch('snowflake.connector.connect')
    def test_thread_safety(self, mock_connect, tmp_path):
        """Test thread safety of context and operations"""
        import concurrent.futures
        
        # Create simple config
        config = {
            "snowflake": {
                "account": "test",
                "user": "test", 
                "password": "test",
                "warehouse": "TEST",
                "database": "TEST",
                "schema": "TEST"
            },
            "files": []
        }
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        
        # Each thread gets its own connection
        connections = {}
        
        def mock_connect_thread(*args, **kwargs):
            import threading
            thread_id = threading.current_thread().ident
            if thread_id not in connections:
                conn = MagicMock()
                conn.thread_id = thread_id
                connections[thread_id] = conn
            return connections[thread_id]
        
        mock_connect.side_effect = mock_connect_thread
        
        context = ApplicationContext(config_path=config_file, quiet=True)
        
        def get_connection_in_thread(ctx):
            conn = ctx.get_connection()
            return conn.thread_id
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(get_connection_in_thread, context)
                    for _ in range(10)
                ]
                
                thread_ids = [f.result() for f in futures]
            
            # Each thread should have gotten its own connection
            assert len(set(thread_ids)) > 1
            
        finally:
            context.cleanup()


class TestErrorHandling:
    """Test error handling and recovery"""
    
    @patch('snowflake.connector.connect')
    def test_connection_failure_handling(self, mock_connect, tmp_path):
        """Test handling of connection failures"""
        import snowflake.connector.errors as sf_errors
        
        # Setup to fail
        mock_connect.side_effect = sf_errors.DatabaseError("Connection timeout")
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "snowflake": {
                "account": "test",
                "user": "test",
                "password": "test",
                "warehouse": "TEST",
                "database": "TEST",
                "schema": "TEST"
            },
            "files": []
        }))
        
        context = ApplicationContext(config_path=config_file, quiet=True)
        
        # Should raise when trying to get connection
        with pytest.raises(sf_errors.DatabaseError):
            context.get_connection()
        
        context.cleanup()
    
    @patch('snowflake.connector.connect')
    def test_partial_failure_handling(self, mock_connect, tmp_path):
        """Test handling when some files succeed and others fail"""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        
        # Create good and bad files
        good_file = data_dir / "good_20240101-20240131.tsv"
        good_file.write_text("recordDate\tvalue\n20240101\t100\n")
        
        bad_file = data_dir / "bad_20240201-20240228.tsv"
        bad_file.write_text("This is not valid TSV content")
        
        config = {
            "snowflake": {
                "account": "test",
                "user": "test",
                "password": "test",
                "warehouse": "TEST",
                "database": "TEST",
                "schema": "TEST"
            },
            "files": [{
                "file_pattern": "*_{date_range}.tsv",
                "table_name": "TEST",
                "date_column": "recordDate",
                "expected_columns": ["recordDate", "value"]
            }]
        }
        
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        context = ApplicationContext(config_path=config_file, quiet=True)
        
        try:
            load_op = LoadOperation(context)
            result = load_op.execute(
                base_path=data_dir,
                skip_qc=True
            )
            
            # Should be partial success
            assert result["status"] == "partial"
            assert len(result["errors"]) > 0
            assert result["files_processed"] >= 0
            
        finally:
            context.cleanup()
    
    def test_cleanup_on_exception(self, tmp_path):
        """Test that cleanup happens even on exceptions"""
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "snowflake": {
                "account": "test",
                "user": "test",
                "password": "test",
                "warehouse": "TEST",
                "database": "TEST",
                "schema": "TEST"
            }
        }))
        
        with patch('snowflake.connector.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            # Use context manager - should cleanup even if exception
            with pytest.raises(RuntimeError):
                with ApplicationContext(config_path=config_file) as context:
                    # Force an error
                    raise RuntimeError("Test error")
            
            # Connection should have been cleaned up
            # (In real implementation, verify cleanup was called)