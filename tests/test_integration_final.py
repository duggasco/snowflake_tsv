#!/usr/bin/env python3
"""
Final integration tests that work with Snowflake connector installed.
Tests actual production code with proper mocking.
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


class TestSQLInjectionPrevention(unittest.TestCase):
    """Test SQL injection prevention in real code."""
    
    @patch('snowflake_etl.utils.snowflake_connection_v3.SnowflakeConnectionManager')
    def test_report_operation_prevents_injection(self, mock_conn_class):
        """Test ReportOperation properly parameterizes queries."""
        from snowflake_etl.operations.report_operation_final import ReportOperation
        
        # Setup mock connection
        mock_conn_instance = Mock()
        mock_conn_class.return_value = mock_conn_instance
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_instance.get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn_instance.get_connection.return_value.__exit__.return_value = None
        
        # Create mock context
        mock_context = Mock()
        mock_context.connection_manager = mock_conn_instance
        mock_context.logger = Mock()
        mock_context.progress_tracker = Mock()
        mock_context.config_manager = Mock()
        
        # Create operation
        operation = ReportOperation(mock_context)
        
        # Test with malicious input
        malicious_table = "users'; DROP TABLE sensitive; --"
        operation._table_exists(mock_cursor, malicious_table)
        
        # Verify parameterized query
        mock_cursor.execute.assert_called_once()
        query, params = mock_cursor.execute.call_args[0]
        
        # Check SQL injection prevention
        self.assertIn("table_name = %s", query)
        self.assertIn("CURRENT_SCHEMA()", query)  # Uses our improved version
        self.assertEqual(params[0], "USERS'; DROP TABLE SENSITIVE; --")
        self.assertNotIn("DROP TABLE", query)


class TestConnectionPooling(unittest.TestCase):
    """Test connection pooling implementation."""
    
    def test_optimized_connection_pool(self):
        """Test the OptimizedConnectionPool with threading."""
        from snowflake_etl.operations.report_operation_final import OptimizedConnectionPool
        
        # Mock dependencies
        mock_conn_mgr = Mock()
        mock_logger = Mock()
        
        # Each call returns a new connection
        connections = [Mock(name=f'conn{i}') for i in range(3)]
        mock_conn_mgr.get_connection.side_effect = connections
        
        # Create pool
        pool = OptimizedConnectionPool(mock_conn_mgr, mock_logger)
        
        # Test multi-threading
        thread_connections = []
        lock = threading.Lock()
        
        def get_conn():
            conn = pool.get_connection()
            with lock:
                thread_connections.append(conn)
        
        # Create threads
        threads = [threading.Thread(target=get_conn) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Each thread should have different connection
        self.assertEqual(len(thread_connections), 3)
        self.assertEqual(len(set(thread_connections)), 3)
        
        # Test cleanup
        pool._all_connections = thread_connections
        pool.close_all()
        
        for conn in thread_connections:
            conn.close.assert_called_once()


class TestFormatterStrategy(unittest.TestCase):
    """Test formatter strategy pattern."""
    
    def test_formatter_factory(self):
        """Test ReportFormatterFactory creates correct formatters."""
        from snowflake_etl.operations.report_operation_final import (
            ReportFormatterFactory,
            TextReportFormatter,
            JsonReportFormatter,
            CsvReportFormatter
        )
        
        # Test each format
        formatters = {
            'text': TextReportFormatter,
            'json': JsonReportFormatter,
            'csv': CsvReportFormatter
        }
        
        for format_name, expected_class in formatters.items():
            formatter = ReportFormatterFactory.create(format_name)
            self.assertIsInstance(formatter, expected_class)
        
        # Test unknown format
        with self.assertRaises(ValueError) as ctx:
            ReportFormatterFactory.create('xml')
        self.assertIn("Unknown format", str(ctx.exception))
    
    def test_csv_formatter_output(self):
        """Test CSV formatter produces valid CSV."""
        from snowflake_etl.operations.report_operation_final import CsvReportFormatter
        
        formatter = CsvReportFormatter()
        
        # Test data
        result = {
            'reports': [
                {
                    'config_file': 'test.json',
                    'table_name': 'TEST_TABLE',
                    'status': 'SUCCESS',
                    'row_count': 1000,
                    'column_count': 10,
                    'validation_issues': ['Issue 1', 'Issue 2']
                }
            ]
        }
        
        output = formatter.format(result)
        
        # Verify CSV structure
        lines = output.strip().split('\n')
        self.assertGreater(len(lines), 1)  # Header + data
        self.assertIn('Table Name', lines[0])  # Header
        self.assertIn('TEST_TABLE', lines[1])  # Data


class TestDuplicateCheckOperation(unittest.TestCase):
    """Test DuplicateCheckOperation."""
    
    @patch('snowflake_etl.utils.snowflake_connection_v3.SnowflakeConnectionManager')
    def test_duplicate_severity_calculation(self, mock_conn_class):
        """Test severity calculation logic."""
        from snowflake_etl.operations.duplicate_check_operation import DuplicateCheckOperation
        
        # Setup mocks
        mock_conn_instance = Mock()
        mock_conn_class.return_value = mock_conn_instance
        
        mock_context = Mock()
        mock_context.connection_manager = mock_conn_instance
        mock_context.logger = Mock()
        mock_context.progress_tracker = Mock()
        
        operation = DuplicateCheckOperation(mock_context)
        
        # Test severity levels
        test_cases = [
            (0, 0, 1000, 'NONE'),       # No duplicates
            (0.5, 10, 1000, 'LOW'),      # 0.5% duplicates
            (2, 20, 1000, 'MEDIUM'),     # 2% duplicates
            (6, 60, 1000, 'HIGH'),       # 6% duplicates
            (11, 110, 1000, 'CRITICAL'), # 11% duplicates
        ]
        
        for dup_pct, dup_keys, total_rows, expected_severity in test_cases:
            severity = operation._calculate_severity(dup_pct, dup_keys, total_rows)
            self.assertEqual(severity, expected_severity, 
                           f"Failed for {dup_pct}% duplicates")


class TestCompareOperation(unittest.TestCase):
    """Test CompareOperation file comparison."""
    
    def test_line_ending_detection(self):
        """Test line ending detection in CompareOperation."""
        from snowflake_etl.operations.compare_operation import CompareOperation
        
        # Mock context
        mock_context = Mock()
        mock_context.logger = Mock()
        mock_context.progress_tracker = Mock()
        
        operation = CompareOperation(mock_context)
        
        # Test different line endings
        test_cases = [
            (b'line1\r\nline2\r\n', 'CRLF'),
            (b'line1\nline2\n', 'LF'),
            (b'line1\rline2\r', 'CR'),
            (b'line1\r\nline2\nline3', 'Mixed'),
            (b'no line endings', 'None')
        ]
        
        for sample, expected in test_cases:
            result = operation._detect_line_ending(sample)
            self.assertEqual(result, expected, 
                           f"Failed for sample: {sample[:20]}")
    
    def test_delimiter_detection(self):
        """Test delimiter detection logic."""
        from snowflake_etl.operations.compare_operation import CompareOperation
        
        mock_context = Mock()
        mock_context.logger = Mock()
        mock_context.progress_tracker = Mock()
        
        operation = CompareOperation(mock_context)
        
        # Create temp file
        temp_dir = tempfile.mkdtemp()
        test_file = Path(temp_dir) / 'test.tsv'
        
        # Test tab-delimited
        test_file.write_text('col1\tcol2\tcol3\nval1\tval2\tval3\n')
        chars = Mock()
        chars.encoding = 'utf-8'
        chars.delimiter = None
        
        operation._analyze_columns(test_file, chars)
        self.assertEqual(chars.delimiter, '\t')
        
        # Test comma-delimited
        test_file.write_text('col1,col2,col3\nval1,val2,val3\n')
        chars.delimiter = None
        operation._analyze_columns(test_file, chars)
        self.assertEqual(chars.delimiter, ',')


class TestCLIIntegration(unittest.TestCase):
    """Test CLI command parsing."""
    
    def test_cli_argument_validation(self):
        """Test CLI validates arguments properly."""
        from snowflake_etl.__main__ import create_parser
        
        parser = create_parser()
        
        # Valid arguments
        args = parser.parse_args([
            '--config', 'config.json',
            'check-duplicates',
            '--table', 'TEST',
            '--key-columns', 'col1,col2'
        ])
        
        self.assertEqual(args.operation, 'check-duplicates')
        self.assertEqual(args.table, 'TEST')
        self.assertEqual(args.key_columns, 'col1,col2')
        
        # Test missing required args raises SystemExit
        with self.assertRaises(SystemExit):
            parser.parse_args(['report'])  # Missing --config
        
        with self.assertRaises(SystemExit):
            parser.parse_args([
                '--config', 'config.json',
                'check-duplicates',
                '--table', 'TEST'
                # Missing --key-columns
            ])
    
    def test_month_parsing(self):
        """Test month argument parsing for delete operation."""
        from snowflake_etl.__main__ import create_parser
        
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'delete',
            '--table', 'TEST',
            '--month', '2024-01',
            '--dry-run'
        ])
        
        self.assertEqual(args.month, '2024-01')
        self.assertTrue(args.dry_run)


class TestSeverityConfiguration(unittest.TestCase):
    """Test configurable severity thresholds."""
    
    def test_severity_config(self):
        """Test SeverityConfig with custom thresholds."""
        from snowflake_etl.operations.report_operation_final import SeverityConfig
        
        # Default config
        default = SeverityConfig()
        self.assertEqual(default.anomaly_critical_threshold, 0.1)
        self.assertEqual(default.duplicate_critical_threshold, 0.1)
        
        # Custom config
        custom = SeverityConfig(
            anomaly_critical_threshold=0.05,
            duplicate_critical_threshold=0.15,
            duplicate_high_threshold=0.08
        )
        
        # Test anomaly severity
        self.assertEqual(
            custom.get_anomaly_severity(['SEVERELY_LOW']),
            'CRITICAL'
        )
        self.assertEqual(
            custom.get_anomaly_severity(['OUTLIER_LOW']),
            'HIGH'
        )
        
        # Test duplicate severity
        self.assertEqual(custom.get_duplicate_severity(0.16), 'CRITICAL')
        self.assertEqual(custom.get_duplicate_severity(0.09), 'HIGH')
        self.assertEqual(custom.get_duplicate_severity(0.02), 'MEDIUM')
        self.assertEqual(custom.get_duplicate_severity(0.005), 'LOW')


if __name__ == '__main__':
    unittest.main(verbosity=2)