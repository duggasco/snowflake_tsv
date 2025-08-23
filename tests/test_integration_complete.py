#!/usr/bin/env python3
"""
Comprehensive integration tests for the Snowflake ETL v3.0.0 architecture.
Tests all operations with dependency injection and new CLI.
"""

import unittest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.operations.report_operation_final import (
    ReportOperation, SeverityConfig, TableReport, 
    TextReportFormatter, JsonReportFormatter, CsvReportFormatter,
    ReportFormatterFactory
)
from snowflake_etl.operations.duplicate_check_operation import (
    DuplicateCheckOperation, DuplicateCheckResult
)
from snowflake_etl.operations.compare_operation import (
    CompareOperation, ComparisonResult, FileCharacteristics
)


class TestApplicationContext(unittest.TestCase):
    """Test ApplicationContext dependency injection."""
    
    def setUp(self):
        """Create temporary config file."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = Path(self.temp_dir) / 'test_config.json'
        
        # Create test config
        config_data = {
            'snowflake': {
                'account': 'test_account',
                'user': 'test_user',
                'password': 'test_pass',
                'warehouse': 'test_wh',
                'database': 'test_db',
                'schema': 'test_schema'
            },
            'files': [
                {
                    'table_name': 'TEST_TABLE',
                    'date_column': 'recordDate',
                    'duplicate_key_columns': ['recordDate', 'assetId']
                }
            ]
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(config_data, f)
    
    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('snowflake_etl.core.application_context.SnowflakeConnectionManager')
    @patch('snowflake_etl.core.application_context.ConfigManager')
    def test_context_initialization(self, mock_config, mock_conn):
        """Test context creates all required components."""
        context = ApplicationContext(
            config_file=str(self.config_file),
            log_dir=self.temp_dir,
            quiet_mode=True
        )
        
        # Verify components are created
        self.assertIsNotNone(context.config_manager)
        self.assertIsNotNone(context.connection_manager)
        self.assertIsNotNone(context.logger)
        self.assertIsNotNone(context.progress_tracker)
        
        # Verify cleanup
        context.cleanup()


class TestReportOperation(unittest.TestCase):
    """Test ReportOperation with all improvements."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_context = Mock()
        self.mock_context.config_manager = Mock()
        self.mock_context.config_manager.config_dir = self.temp_dir
        self.mock_context.connection_manager = Mock()
        self.mock_context.logger = Mock()
        self.mock_context.progress_tracker = Mock()
    
    def test_sql_injection_prevention(self):
        """Test that SQL queries use IDENTIFIER for table names."""
        operation = ReportOperation(self.mock_context)
        
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        self.mock_context.connection_manager.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Test table exists check
        operation._table_exists(mock_cursor, "TEST_TABLE'; DROP TABLE users; --")
        
        # Verify parameterized query was used
        mock_cursor.execute.assert_called_with(
            """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = CURRENT_SCHEMA()
                  AND table_name = %s
                """,
            ("TEST_TABLE'; DROP TABLE USERS; --",)  # Note: upper case conversion
        )
    
    def test_connection_pooling(self):
        """Test optimized connection pooling for parallel workers."""
        from snowflake_etl.operations.report_operation_final import OptimizedConnectionPool
        
        mock_logger = Mock()
        pool = OptimizedConnectionPool(self.mock_context.connection_manager, mock_logger)
        
        # Simulate multiple threads getting connections
        conn1 = pool.get_connection()
        conn2 = pool.get_connection()  # Same thread, should return same connection
        
        self.assertEqual(conn1, conn2)  # Should be the same connection
        
        # Test cleanup
        pool._all_connections = [Mock(), Mock(), Mock()]
        pool.close_all()
        
        # Verify all connections were closed
        for conn in pool._all_connections:
            conn.close.assert_called_once()
    
    def test_formatter_strategy_pattern(self):
        """Test formatter strategy pattern implementation."""
        # Test factory
        text_formatter = ReportFormatterFactory.create('text')
        json_formatter = ReportFormatterFactory.create('json')
        csv_formatter = ReportFormatterFactory.create('csv')
        
        self.assertIsInstance(text_formatter, TextReportFormatter)
        self.assertIsInstance(json_formatter, JsonReportFormatter)
        self.assertIsInstance(csv_formatter, CsvReportFormatter)
        
        # Test custom formatter registration
        class CustomFormatter(TextReportFormatter):
            def get_file_extension(self):
                return '.custom'
        
        ReportFormatterFactory.register('custom', CustomFormatter)
        custom_formatter = ReportFormatterFactory.create('custom')
        self.assertIsInstance(custom_formatter, CustomFormatter)
    
    def test_configurable_severity(self):
        """Test configurable severity mapping."""
        # Default config
        default_config = SeverityConfig()
        self.assertEqual(default_config.anomaly_critical_threshold, 0.1)
        self.assertEqual(default_config.duplicate_critical_threshold, 0.1)
        
        # Custom config
        custom_config = SeverityConfig(
            anomaly_critical_threshold=0.05,
            duplicate_critical_threshold=0.15
        )
        self.assertEqual(custom_config.anomaly_critical_threshold, 0.05)
        self.assertEqual(custom_config.duplicate_critical_threshold, 0.15)
        
        # Test severity calculation
        self.assertEqual(
            custom_config.get_anomaly_severity(['SEVERELY_LOW']), 
            'CRITICAL'
        )
        self.assertEqual(
            custom_config.get_duplicate_severity(0.16), 
            'CRITICAL'
        )


class TestDuplicateCheckOperation(unittest.TestCase):
    """Test DuplicateCheckOperation."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_context = Mock()
        self.mock_context.connection_manager = Mock()
        self.mock_context.logger = Mock()
        self.mock_context.progress_tracker = Mock()
    
    def test_duplicate_detection(self):
        """Test duplicate detection logic."""
        operation = DuplicateCheckOperation(self.mock_context)
        
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        self.mock_context.connection_manager.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Mock query results
        mock_cursor.fetchone.side_effect = [
            (1,),  # Table exists
            (1000,),  # Total rows
            (10, 20),  # Duplicate stats: 10 keys, 20 excess rows
        ]
        mock_cursor.fetchall.side_effect = [
            [('RECORDDATE',), ('ASSETID',)],  # Columns exist
            []  # No sample duplicates for simplicity
        ]
        
        result = operation.check_duplicates(
            table_name='TEST_TABLE',
            key_columns=['recordDate', 'assetId'],
            show_progress=False
        )
        
        # Verify results
        self.assertTrue(result.has_duplicates)
        self.assertEqual(result.duplicate_key_combinations, 10)
        self.assertEqual(result.excess_rows, 20)
        self.assertEqual(result.duplicate_percentage, 2.0)  # 20/1000 * 100
        self.assertEqual(result.severity, 'MEDIUM')  # 2% is MEDIUM
    
    def test_severity_calculation(self):
        """Test severity calculation logic."""
        operation = DuplicateCheckOperation(self.mock_context)
        
        # Test different severity levels
        self.assertEqual(operation._calculate_severity(0, 0, 1000), 'NONE')
        self.assertEqual(operation._calculate_severity(0.5, 10, 1000), 'LOW')
        self.assertEqual(operation._calculate_severity(2, 10, 1000), 'MEDIUM')
        self.assertEqual(operation._calculate_severity(6, 10, 1000), 'HIGH')
        self.assertEqual(operation._calculate_severity(11, 10, 1000), 'CRITICAL')


class TestCompareOperation(unittest.TestCase):
    """Test CompareOperation."""
    
    def setUp(self):
        """Set up test environment."""
        self.mock_context = Mock()
        self.mock_context.logger = Mock()
        self.mock_context.progress_tracker = Mock()
        
        # Create test files
        self.temp_dir = tempfile.mkdtemp()
        self.file1 = Path(self.temp_dir) / 'file1.tsv'
        self.file2 = Path(self.temp_dir) / 'file2.tsv'
        
        # Create test content
        self.file1.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
        self.file2.write_text("col1,col2,col3\nval1,val2,val3\n")
    
    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_file_comparison(self):
        """Test file comparison detects differences."""
        operation = CompareOperation(self.mock_context)
        
        result = operation.compare_files(
            str(self.file1),
            str(self.file2)
        )
        
        # Verify differences detected
        self.assertIsNotNone(result.file1)
        self.assertIsNotNone(result.file2)
        self.assertEqual(result.file1.delimiter, '\t')
        self.assertEqual(result.file2.delimiter, ',')
        
        # Should detect delimiter difference
        self.assertIn("Different delimiters", ' '.join(result.differences))
        self.assertFalse(result.is_compatible)
    
    def test_encoding_detection(self):
        """Test encoding detection."""
        operation = CompareOperation(self.mock_context)
        
        # Create file with different encoding
        utf8_file = Path(self.temp_dir) / 'utf8.tsv'
        utf8_file.write_text("col1\tcol2\nтест\tданные\n", encoding='utf-8')
        
        result = operation.compare_files(
            str(self.file1),
            str(utf8_file)
        )
        
        # Both should be detected as UTF-8 or ASCII-compatible
        self.assertIsNotNone(result.file1.encoding)
        self.assertIsNotNone(result.file2.encoding)


class TestCLIIntegration(unittest.TestCase):
    """Test CLI integration."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = Path(self.temp_dir) / 'config.json'
        
        # Create minimal config
        config = {
            'snowflake': {
                'account': 'test',
                'user': 'test',
                'password': 'test',
                'warehouse': 'test',
                'database': 'test',
                'schema': 'test'
            }
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(config, f)
    
    def tearDown(self):
        """Clean up."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    @patch('snowflake_etl.__main__.ReportOperation')
    def test_report_command(self, mock_report_op, mock_context):
        """Test report command through CLI."""
        from snowflake_etl.__main__ import main
        
        # Mock operation
        mock_op_instance = Mock()
        mock_report_op.return_value = mock_op_instance
        mock_op_instance.generate_full_report.return_value = {'status': 'SUCCESS'}
        
        # Test command
        args = [
            '--config', str(self.config_file),
            '--quiet',
            'report',
            '--output-format', 'json',
            '--max-workers', '2'
        ]
        
        result = main(args)
        
        # Verify operation was called
        self.assertEqual(result, 0)
        mock_op_instance.generate_full_report.assert_called_once()
        
        # Check arguments
        call_args = mock_op_instance.generate_full_report.call_args
        self.assertEqual(call_args.kwargs['output_format'], 'json')
        self.assertEqual(call_args.kwargs['max_workers'], 2)
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    @patch('snowflake_etl.__main__.DuplicateCheckOperation')
    def test_duplicate_check_command(self, mock_dup_op, mock_context):
        """Test duplicate check command."""
        from snowflake_etl.__main__ import main
        
        # Mock operation
        mock_op_instance = Mock()
        mock_dup_op.return_value = mock_op_instance
        
        # Create mock result
        mock_result = DuplicateCheckResult(
            table_name='TEST_TABLE',
            key_columns=['col1', 'col2'],
            date_column=None,
            start_date=None,
            end_date=None,
            has_duplicates=False,
            total_rows=1000,
            duplicate_key_combinations=0,
            excess_rows=0,
            duplicate_percentage=0.0,
            severity='NONE'
        )
        mock_op_instance.check_duplicates.return_value = mock_result
        mock_op_instance.format_result.return_value = "No duplicates found"
        
        # Test command
        args = [
            '--config', str(self.config_file),
            '--quiet',
            'check-duplicates',
            '--table', 'TEST_TABLE',
            '--key-columns', 'col1,col2'
        ]
        
        result = main(args)
        
        # Verify operation was called
        self.assertEqual(result, 0)
        mock_op_instance.check_duplicates.assert_called_once()
        
        # Check arguments
        call_args = mock_op_instance.check_duplicates.call_args
        self.assertEqual(call_args.kwargs['table_name'], 'TEST_TABLE')
        self.assertEqual(call_args.kwargs['key_columns'], ['col1', 'col2'])


class TestEndToEndWorkflow(unittest.TestCase):
    """Test complete workflow integration."""
    
    @patch('snowflake_etl.core.application_context.SnowflakeConnectionManager')
    @patch('snowflake_etl.core.application_context.ConfigManager')
    def test_full_workflow(self, mock_config_mgr, mock_conn_mgr):
        """Test a complete workflow from context creation to operation execution."""
        # Setup
        temp_dir = tempfile.mkdtemp()
        config_file = Path(temp_dir) / 'config.json'
        
        config = {
            'snowflake': {'account': 'test'},
            'files': [{'table_name': 'TEST_TABLE'}]
        }
        
        with open(config_file, 'w') as f:
            json.dump(config, f)
        
        try:
            # Create context
            context = ApplicationContext(
                config_file=str(config_file),
                log_dir=temp_dir,
                quiet_mode=True
            )
            
            # Create operations
            report_op = ReportOperation(context)
            dup_op = DuplicateCheckOperation(context)
            compare_op = CompareOperation(context)
            
            # Verify all operations can be instantiated
            self.assertIsNotNone(report_op)
            self.assertIsNotNone(dup_op)
            self.assertIsNotNone(compare_op)
            
            # Verify they share the same context
            self.assertEqual(report_op.context, context)
            self.assertEqual(dup_op.context, context)
            self.assertEqual(compare_op.context, context)
            
            # Cleanup
            context.cleanup()
            
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()