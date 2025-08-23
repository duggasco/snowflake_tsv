"""
Tests for CLI argument parsing and command execution.
"""

import pytest
import sys
from unittest.mock import Mock, MagicMock, patch
from io import StringIO
import argparse
import json
from pathlib import Path

# Import the main CLI module
from snowflake_etl.__main__ import main, create_parser, execute_command


class TestCLIArgumentParsing:
    """Test CLI argument parsing"""
    
    def test_create_parser(self):
        """Test parser creation and structure"""
        parser = create_parser()
        
        assert isinstance(parser, argparse.ArgumentParser)
        
        # Test global arguments
        args = parser.parse_args(['--config', 'test.json', 'load', '--base-path', '/data'])
        assert args.config == 'test.json'
        assert args.command == 'load'
        assert args.base_path == '/data'
    
    def test_load_command_arguments(self):
        """Test load subcommand arguments"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'load',
            '--base-path', '/data',
            '--month', '2024-01',
            '--skip-qc',
            '--validate-in-snowflake',
            '--max-workers', '8'
        ])
        
        assert args.command == 'load'
        assert args.base_path == '/data'
        assert args.month == '2024-01'
        assert args.skip_qc is True
        assert args.validate_in_snowflake is True
        assert args.max_workers == 8
    
    def test_validate_command_arguments(self):
        """Test validate subcommand arguments"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'validate',
            '--table', 'TEST_TABLE',
            '--date-column', 'recordDate',
            '--month', '2024-01',
            '--output', 'results.json'
        ])
        
        assert args.command == 'validate'
        assert args.table == 'TEST_TABLE'
        assert args.date_column == 'recordDate'
        assert args.month == '2024-01'
        assert args.output == 'results.json'
    
    def test_delete_command_arguments(self):
        """Test delete subcommand arguments"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'delete',
            '--table', 'TEST_TABLE',
            '--month', '2024-01',
            '--dry-run',
            '--yes'
        ])
        
        assert args.command == 'delete'
        assert args.table == 'TEST_TABLE'
        assert args.month == '2024-01'
        assert args.dry_run is True
        assert args.yes is True
    
    def test_report_command_arguments(self):
        """Test report subcommand arguments"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'report',
            '--output-format', 'json',
            '--output-file', 'report.json',
            '--tables', 'TABLE1,TABLE2'
        ])
        
        assert args.command == 'report'
        assert args.output_format == 'json'
        assert args.output_file == 'report.json'
        assert args.tables == 'TABLE1,TABLE2'
    
    def test_check_duplicates_arguments(self):
        """Test check-duplicates subcommand"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'check-duplicates',
            '--table', 'TEST_TABLE',
            '--key-columns', 'col1,col2,col3',
            '--date-start', '2024-01-01',
            '--date-end', '2024-01-31'
        ])
        
        assert args.command == 'check-duplicates'
        assert args.table == 'TEST_TABLE'
        assert args.key_columns == 'col1,col2,col3'
        assert args.date_start == '2024-01-01'
        assert args.date_end == '2024-01-31'
    
    def test_compare_command_arguments(self):
        """Test compare subcommand"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            'compare',
            'file1.tsv',
            'file2.tsv',
            '--quick'
        ])
        
        assert args.command == 'compare'
        assert args.file1 == 'file1.tsv'
        assert args.file2 == 'file2.tsv'
        assert args.quick is True
    
    def test_global_options(self):
        """Test global options like quiet and log level"""
        parser = create_parser()
        
        args = parser.parse_args([
            '--config', 'config.json',
            '--quiet',
            '--log-level', 'DEBUG',
            '--log-dir', '/var/log/etl',
            'validate',
            '--table', 'TEST'
        ])
        
        assert args.quiet is True
        assert args.log_level == 'DEBUG'
        assert args.log_dir == '/var/log/etl'
    
    def test_invalid_command(self):
        """Test handling of invalid commands"""
        parser = create_parser()
        
        with pytest.raises(SystemExit):
            # Capture stderr to avoid test output noise
            with patch('sys.stderr', new=StringIO()):
                parser.parse_args(['--config', 'config.json', 'invalid_command'])
    
    def test_missing_required_arguments(self):
        """Test handling of missing required arguments"""
        parser = create_parser()
        
        # Config is required
        with pytest.raises(SystemExit):
            with patch('sys.stderr', new=StringIO()):
                parser.parse_args(['load', '--base-path', '/data'])
        
        # Base path is required for load
        with pytest.raises(SystemExit):
            with patch('sys.stderr', new=StringIO()):
                parser.parse_args(['--config', 'config.json', 'load'])


class TestCLIExecution:
    """Test CLI command execution"""
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    @patch('snowflake_etl.__main__.LoadOperation')
    def test_execute_load_command(self, mock_load_op_class, mock_context_class, tmp_path):
        """Test executing load command"""
        # Create mock instances
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        
        mock_load_op = MagicMock()
        mock_load_op.execute.return_value = {
            "status": "success",
            "files_processed": 2,
            "total_rows": 1000
        }
        mock_load_op_class.return_value = mock_load_op
        
        # Create args
        args = argparse.Namespace(
            config='config.json',
            command='load',
            base_path='/data',
            month='2024-01',
            skip_qc=False,
            validate_in_snowflake=True,
            validate_only=False,
            max_workers=None,
            quiet=False,
            log_level='INFO',
            log_dir='logs'
        )
        
        # Execute
        result = execute_command(args)
        
        # Verify
        assert result == 0  # Success exit code
        mock_context_class.assert_called_once()
        mock_load_op_class.assert_called_once_with(mock_context)
        mock_load_op.execute.assert_called_once()
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    @patch('snowflake_etl.__main__.ValidateOperation')
    def test_execute_validate_command(self, mock_validate_op_class, mock_context_class):
        """Test executing validate command"""
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        
        mock_validate_op = MagicMock()
        mock_validate_op.execute.return_value = {
            "status": "success",
            "missing_dates": []
        }
        mock_validate_op_class.return_value = mock_validate_op
        
        args = argparse.Namespace(
            config='config.json',
            command='validate',
            table='TEST_TABLE',
            date_column='recordDate',
            month='2024-01',
            start_date=None,
            end_date=None,
            output=None,
            quiet=False,
            log_level='INFO',
            log_dir='logs'
        )
        
        result = execute_command(args)
        
        assert result == 0
        mock_validate_op.execute.assert_called_once()
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    def test_execute_with_invalid_config(self, mock_context_class):
        """Test execution with invalid configuration"""
        # Make context initialization fail
        mock_context_class.side_effect = FileNotFoundError("Config not found")
        
        args = argparse.Namespace(
            config='nonexistent.json',
            command='load',
            base_path='/data',
            quiet=False,
            log_level='INFO',
            log_dir='logs'
        )
        
        result = execute_command(args)
        
        assert result != 0  # Should return error code
    
    @patch('snowflake_etl.__main__.ApplicationContext')
    @patch('snowflake_etl.__main__.LoadOperation')
    def test_execute_with_operation_failure(self, mock_load_op_class, mock_context_class):
        """Test handling of operation failures"""
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        
        mock_load_op = MagicMock()
        mock_load_op.execute.side_effect = Exception("Load failed")
        mock_load_op_class.return_value = mock_load_op
        
        args = argparse.Namespace(
            config='config.json',
            command='load',
            base_path='/data',
            month='2024-01',
            skip_qc=False,
            validate_in_snowflake=False,
            validate_only=False,
            max_workers=None,
            quiet=False,
            log_level='INFO',
            log_dir='logs'
        )
        
        result = execute_command(args)
        
        assert result != 0  # Should return error code
    
    @patch('sys.argv')
    @patch('snowflake_etl.__main__.execute_command')
    def test_main_function(self, mock_execute, mock_argv):
        """Test main() entry point"""
        mock_argv.__getitem__.return_value = [
            'snowflake-etl',
            '--config', 'config.json',
            'validate',
            '--table', 'TEST'
        ]
        
        mock_execute.return_value = 0
        
        with pytest.raises(SystemExit) as exc_info:
            main()
        
        assert exc_info.value.code == 0
        mock_execute.assert_called_once()


class TestCLIHelp:
    """Test help text and documentation"""
    
    def test_main_help(self):
        """Test main help text"""
        parser = create_parser()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            with pytest.raises(SystemExit):
                parser.parse_args(['--help'])
            
            help_text = fake_out.getvalue()
            assert 'Snowflake ETL Pipeline' in help_text
            assert 'load' in help_text
            assert 'validate' in help_text
            assert 'delete' in help_text
    
    def test_subcommand_help(self):
        """Test subcommand help text"""
        parser = create_parser()
        
        # Test load help
        with patch('sys.stdout', new=StringIO()) as fake_out:
            with pytest.raises(SystemExit):
                parser.parse_args(['load', '--help'])
            
            help_text = fake_out.getvalue()
            assert '--base-path' in help_text
            assert '--month' in help_text
            assert '--skip-qc' in help_text
    
    def test_version_display(self):
        """Test version display"""
        parser = create_parser()
        
        # Add version argument if it exists
        with patch('sys.stdout', new=StringIO()) as fake_out:
            with pytest.raises(SystemExit):
                parser.parse_args(['--version'])
            
            # Version should be displayed
            # (Implementation may vary)