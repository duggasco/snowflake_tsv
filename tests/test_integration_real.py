#!/usr/bin/env python3
"""
Real integration tests that test the actual production code.
Uses mocks for external dependencies but tests real implementation.
"""

import unittest
import tempfile
import json
import threading
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRealSQLInjectionPrevention(unittest.TestCase):
    """Test SQL injection prevention in actual code."""
    
    @patch('snowflake.connector.connect')
    def test_report_operation_sql_injection(self, mock_connect):
        """Test ReportOperation prevents SQL injection."""
        # Import real code
        from snowflake_etl.operations.report_operation_final import ReportOperation
        from snowflake_etl.core.application_context import ApplicationContext
        
        # Mock the Snowflake connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Setup mock context
        with patch('snowflake_etl.core.application_context.SnowflakeConnectionManager') as mock_conn_mgr:
            with patch('snowflake_etl.core.application_context.ConfigManager') as mock_cfg_mgr:
                # Create temporary config
                temp_dir = tempfile.mkdtemp()
                config_file = Path(temp_dir) / 'config.json'
                config_file.write_text(json.dumps({
                    'snowflake': {'account': 'test'},
                    'files': []
                }))
                
                # Create real ApplicationContext
                context = ApplicationContext(
                    config_file=str(config_file),
                    log_dir=temp_dir,
                    quiet_mode=True
                )
                
                # Mock the connection manager's get_connection
                mock_conn_ctx = MagicMock()
                mock_conn_ctx.__enter__.return_value = mock_conn
                mock_conn_ctx.__exit__.return_value = None
                context.connection_manager.get_connection.return_value = mock_conn_ctx
                
                # Create real ReportOperation
                operation = ReportOperation(context)
                
                # Test with malicious table name
                malicious_table = "users'; DROP TABLE sensitive; --"
                
                # Call the real method
                operation._table_exists(mock_cursor, malicious_table)
                
                # Verify the query used IDENTIFIER and parameters
                mock_cursor.execute.assert_called_once()
                call_args = mock_cursor.execute.call_args
                query = call_args[0][0]
                params = call_args[0][1]
                
                # Check that table name was parameterized, not concatenated
                self.assertIn("table_name = %s", query)
                self.assertEqual(params[0], "USERS'; DROP TABLE SENSITIVE; --")  # Uppercased
                
                # Ensure no direct concatenation
                self.assertNotIn(malicious_table, query)
                self.assertNotIn("DROP TABLE", query)
    
    @patch('snowflake.connector.connect')
    def test_duplicate_check_sql_injection(self, mock_connect):
        """Test DuplicateCheckOperation prevents SQL injection."""
        from snowflake_etl.operations.duplicate_check_operation import DuplicateCheckOperation
        from snowflake_etl.core.application_context import ApplicationContext
        
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('snowflake_etl.core.application_context.SnowflakeConnectionManager'):
            with patch('snowflake_etl.core.application_context.ConfigManager'):
                # Setup
                temp_dir = tempfile.mkdtemp()
                config_file = Path(temp_dir) / 'config.json'
                config_file.write_text(json.dumps({
                    'snowflake': {'account': 'test'}
                }))
                
                context = ApplicationContext(
                    config_file=str(config_file),
                    log_dir=temp_dir,
                    quiet_mode=True
                )
                
                # Mock connection
                mock_conn_ctx = MagicMock()
                mock_conn_ctx.__enter__.return_value = mock_conn
                context.connection_manager.get_connection.return_value = mock_conn_ctx
                
                # Create operation
                operation = DuplicateCheckOperation(context)
                
                # Test with SQL injection attempt
                malicious_keys = ["id'; DELETE FROM users; --", "name"]
                
                # Mock returns
                mock_cursor.fetchone.side_effect = [
                    (1,),  # Table exists
                    (1000,),  # Row count
                    (0, 0)  # No duplicates
                ]
                mock_cursor.fetchall.return_value = [('ID',), ('NAME',)]
                
                # Run check
                operation.check_duplicates(
                    table_name="TEST_TABLE",
                    key_columns=malicious_keys,
                    show_progress=False
                )
                
                # Verify IDENTIFIER was used for dynamic columns
                calls = mock_cursor.execute.call_args_list
                
                # Check that malicious input wasn't concatenated
                for call in calls:
                    query = call[0][0]
                    # Should not contain the DELETE statement
                    self.assertNotIn("DELETE FROM users", query)


class TestRealConnectionPooling(unittest.TestCase):
    """Test real connection pooling implementation."""
    
    def test_connection_pool_threading(self):
        """Test that different threads get different connections."""
        from snowflake_etl.operations.report_operation_final import OptimizedConnectionPool
        
        # Mock connection manager
        mock_conn_mgr = Mock()
        mock_logger = Mock()
        
        # Each call to get_connection returns a new mock
        mock_conn_mgr.get_connection.side_effect = [
            Mock(name='conn1'),
            Mock(name='conn2'),
            Mock(name='conn3')
        ]
        
        # Create pool
        pool = OptimizedConnectionPool(mock_conn_mgr, mock_logger)
        
        # Collect connections from different threads
        connections = []
        lock = threading.Lock()
        
        def get_connection_in_thread():
            conn = pool.get_connection()
            with lock:
                connections.append(conn)
        
        # Create threads
        threads = []
        for _ in range(3):
            t = threading.Thread(target=get_connection_in_thread)
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Each thread should have gotten a different connection
        self.assertEqual(len(connections), 3)
        self.assertEqual(len(set(connections)), 3)  # All unique
        
        # Test close_all
        pool._all_connections = connections
        pool.close_all()
        
        # Verify all connections were closed
        for conn in connections:
            conn.close.assert_called_once()
        self.assertEqual(len(pool._all_connections), 0)
    
    def test_connection_reuse_same_thread(self):
        """Test that same thread reuses connection."""
        from snowflake_etl.operations.report_operation_final import OptimizedConnectionPool
        
        mock_conn_mgr = Mock()
        mock_logger = Mock()
        mock_conn = Mock(name='shared_conn')
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        pool = OptimizedConnectionPool(mock_conn_mgr, mock_logger)
        
        # Get connection multiple times in same thread
        conn1 = pool.get_connection()
        conn2 = pool.get_connection()
        conn3 = pool.get_connection()
        
        # Should be the same connection
        self.assertIs(conn1, conn2)
        self.assertIs(conn2, conn3)
        
        # Connection manager should only be called once
        mock_conn_mgr.get_connection.assert_called_once()


class TestRealFormatterStrategy(unittest.TestCase):
    """Test real formatter strategy implementation."""
    
    def test_formatter_factory_real_code(self):
        """Test the real ReportFormatterFactory."""
        from snowflake_etl.operations.report_operation_final import (
            ReportFormatterFactory,
            TextReportFormatter,
            JsonReportFormatter,
            CsvReportFormatter
        )
        
        # Test factory creates correct types
        text_formatter = ReportFormatterFactory.create('text')
        json_formatter = ReportFormatterFactory.create('json')
        csv_formatter = ReportFormatterFactory.create('csv')
        
        self.assertIsInstance(text_formatter, TextReportFormatter)
        self.assertIsInstance(json_formatter, JsonReportFormatter)
        self.assertIsInstance(csv_formatter, CsvReportFormatter)
        
        # Test extensions
        self.assertEqual(text_formatter.get_file_extension(), '.txt')
        self.assertEqual(json_formatter.get_file_extension(), '.json')
        self.assertEqual(csv_formatter.get_file_extension(), '.csv')
        
        # Test unknown format raises error
        with self.assertRaises(ValueError) as ctx:
            ReportFormatterFactory.create('xml')
        self.assertIn("Unknown format type: xml", str(ctx.exception))
    
    def test_custom_formatter_registration(self):
        """Test registering custom formatters."""
        from snowflake_etl.operations.report_operation_final import (
            ReportFormatterFactory,
            ReportFormatter
        )
        
        # Create custom formatter
        class HtmlFormatter(ReportFormatter):
            def format(self, result):
                return f"<html><body>{result}</body></html>"
            
            def get_file_extension(self):
                return '.html'
        
        # Register it
        ReportFormatterFactory.register('html', HtmlFormatter)
        
        # Test creation
        html_formatter = ReportFormatterFactory.create('html')
        self.assertIsInstance(html_formatter, HtmlFormatter)
        self.assertEqual(html_formatter.get_file_extension(), '.html')
        self.assertIn("<html>", html_formatter.format("test"))


class TestRealCLIParsing(unittest.TestCase):
    """Test real CLI argument parsing."""
    
    def test_cli_missing_required_args(self):
        """Test CLI handles missing required arguments."""
        from snowflake_etl.__main__ import create_parser
        
        parser = create_parser()
        
        # Test missing config
        with self.assertRaises(SystemExit):
            parser.parse_args(['report'])
        
        # Test missing operation
        with self.assertRaises(SystemExit):
            parser.parse_args(['--config', 'test.json'])
        
        # Test missing required operation args
        with self.assertRaises(SystemExit):
            parser.parse_args([
                '--config', 'test.json',
                'check-duplicates',
                '--table', 'TEST'
                # Missing --key-columns
            ])
    
    def test_cli_parse_complex_args(self):
        """Test CLI parses complex arguments correctly."""
        from snowflake_etl.__main__ import create_parser
        
        parser = create_parser()
        
        # Test report command with all options
        args = parser.parse_args([
            '--config', 'config.json',
            '--log-level', 'DEBUG',
            '--quiet',
            'report',
            '--config-filter', '*.json',
            '--table-filter', 'FACT*',
            '--max-workers', '8',
            '--output-format', 'csv',
            '--output', 'report.csv'
        ])
        
        self.assertEqual(args.config, 'config.json')
        self.assertEqual(args.log_level, 'DEBUG')
        self.assertTrue(args.quiet)
        self.assertEqual(args.operation, 'report')
        self.assertEqual(args.config_filter, '*.json')
        self.assertEqual(args.table_filter, 'FACT*')
        self.assertEqual(args.max_workers, 8)
        self.assertEqual(args.output_format, 'csv')
        self.assertEqual(args.output, 'report.csv')


class TestRealCompareOperation(unittest.TestCase):
    """Test real CompareOperation implementation."""
    
    def test_compare_files_with_differences(self):
        """Test real file comparison detects differences."""
        from snowflake_etl.operations.compare_operation import CompareOperation
        from snowflake_etl.core.application_context import ApplicationContext
        
        with patch('snowflake_etl.core.application_context.SnowflakeConnectionManager'):
            with patch('snowflake_etl.core.application_context.ConfigManager'):
                # Setup
                temp_dir = tempfile.mkdtemp()
                config_file = Path(temp_dir) / 'config.json'
                config_file.write_text(json.dumps({'snowflake': {}}))
                
                # Create test files
                file1 = Path(temp_dir) / 'file1.tsv'
                file2 = Path(temp_dir) / 'file2.csv'
                
                # Different delimiters and line endings
                file1.write_bytes(b"col1\tcol2\tcol3\r\nval1\tval2\tval3\r\n")
                file2.write_bytes(b"col1,col2,col3\nval1,val2,val3\n")
                
                # Create context and operation
                context = ApplicationContext(
                    config_file=str(config_file),
                    log_dir=temp_dir,
                    quiet_mode=True
                )
                operation = CompareOperation(context)
                
                # Compare files
                result = operation.compare_files(str(file1), str(file2))
                
                # Verify differences detected
                self.assertIsNotNone(result.file1)
                self.assertIsNotNone(result.file2)
                self.assertEqual(result.file1.delimiter, '\t')
                self.assertEqual(result.file2.delimiter, ',')
                self.assertEqual(result.file1.line_ending, 'CRLF')
                self.assertEqual(result.file2.line_ending, 'LF')
                
                # Should find delimiter and line ending differences
                self.assertFalse(result.is_compatible)
                self.assertTrue(any('delimiter' in d.lower() for d in result.differences))
                self.assertTrue(any('line ending' in d.lower() for d in result.differences))
    
    def test_compare_empty_files(self):
        """Test comparison handles empty files."""
        from snowflake_etl.operations.compare_operation import CompareOperation
        from snowflake_etl.core.application_context import ApplicationContext
        
        with patch('snowflake_etl.core.application_context.SnowflakeConnectionManager'):
            with patch('snowflake_etl.core.application_context.ConfigManager'):
                # Setup
                temp_dir = tempfile.mkdtemp()
                config_file = Path(temp_dir) / 'config.json'
                config_file.write_text(json.dumps({'snowflake': {}}))
                
                # Create empty files
                file1 = Path(temp_dir) / 'empty1.tsv'
                file2 = Path(temp_dir) / 'empty2.tsv'
                file1.write_text('')
                file2.write_text('')
                
                # Create context and operation
                context = ApplicationContext(
                    config_file=str(config_file),
                    log_dir=temp_dir,
                    quiet_mode=True
                )
                operation = CompareOperation(context)
                
                # Compare files
                result = operation.compare_files(str(file1), str(file2))
                
                # Should handle empty files gracefully
                self.assertIsNotNone(result)
                self.assertEqual(result.file1.line_count, 0)
                self.assertEqual(result.file2.line_count, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)