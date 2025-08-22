#!/usr/bin/env python3
"""
Integration test for the refactored ETL architecture.
Tests the complete flow with mocked components.
"""

import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

# Add project to path
sys.path.insert(0, '/root/snowflake')

def create_test_config():
    """Create a test configuration file."""
    config = {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "role": "TEST_ROLE"
        },
        "files": [
            {
                "file_pattern": "test_table_{date_range}.tsv",
                "table_name": "TEST_TABLE",
                "date_column": "recordDate",
                "expected_columns": ["col1", "col2", "col3"],
                "duplicate_key_columns": ["recordDate", "col1"]
            }
        ]
    }
    
    # Create temp config file
    config_file = tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', delete=False
    )
    json.dump(config, config_file)
    config_file.close()
    
    return Path(config_file.name)


def test_cli_operations():
    """Test CLI operations with mocked dependencies."""
    print("=" * 60)
    print("Testing CLI Operations")
    print("=" * 60)
    
    # Create test config
    config_path = create_test_config()
    print(f"✓ Created test config: {config_path}")
    
    try:
        # Import CLI
        from snowflake_etl.cli.main import SnowflakeETLCLI
        
        # Test parsing arguments
        cli = SnowflakeETLCLI()
        
        # Test load command parsing
        args = cli.parse_args([
            '--config', str(config_path),
            'load',
            '--base-path', '/tmp/data',
            '--month', '2024-01',
            '--skip-qc'
        ])
        
        assert args.config == str(config_path)
        assert args.operation == 'load'
        assert args.base_path == '/tmp/data'
        assert args.month == '2024-01'
        assert args.skip_qc is True
        print("✓ Load command parsing works")
        
        # Test delete command parsing
        args = cli.parse_args([
            '--config', str(config_path),
            'delete',
            '--table', 'TEST_TABLE',
            '--month', '2024-01',
            '--dry-run'
        ])
        
        assert args.operation == 'delete'
        assert args.table == 'TEST_TABLE'
        assert args.dry_run is True
        print("✓ Delete command parsing works")
        
        # Test validate command parsing
        args = cli.parse_args([
            '--config', str(config_path),
            'validate',
            '--month', '2024-01',
            '--output', 'results.json'
        ])
        
        assert args.operation == 'validate'
        assert args.output == 'results.json'
        print("✓ Validate command parsing works")
        
    finally:
        # Cleanup
        config_path.unlink()


def test_operation_classes():
    """Test operation classes with mocked context."""
    print("\n" + "=" * 60)
    print("Testing Operation Classes")
    print("=" * 60)
    
    # Mock snowflake module before importing our modules
    with patch.dict('sys.modules', {'snowflake': MagicMock(), 'snowflake.connector': MagicMock()}):
        # Mock the ApplicationContext
        mock_context = MagicMock()
        mock_context.connection_manager = MagicMock()
        mock_context.config_manager = MagicMock()
        mock_context.logger = MagicMock()
        mock_context.progress_tracker = MagicMock()
        
        # Test config data
        mock_context.config_manager.get_config.return_value = {
            'files': [
                {
                    'table_name': 'TEST_TABLE',
                    'date_column': 'recordDate',
                    'duplicate_key_columns': ['recordDate', 'id']
                }
            ]
        }
        
        # Test DeleteOperation
        from snowflake_etl.operations.delete_operation import DeleteOperation, DeletionTarget
        
        delete_op = DeleteOperation(mock_context)
        assert delete_op.context == mock_context
        print("✓ DeleteOperation initialization works")
        
        # Create a deletion target
        target = DeletionTarget(
            table_name='TEST_TABLE',
            date_column='recordDate',
            year_month='2024-01',
            start_date='2024-01-01',
            end_date='2024-01-31'
        )
        
        # Test target to dict conversion
        result_dict = target.__dict__
        assert result_dict['table_name'] == 'TEST_TABLE'
        assert result_dict['year_month'] == '2024-01'
        print("✓ DeletionTarget creation works")
        
        # Test ValidateOperation
        from snowflake_etl.operations.validate_operation import ValidateOperation
        
        validate_op = ValidateOperation(mock_context)
        assert validate_op.context == mock_context
        assert validate_op.validator is not None
        print("✓ ValidateOperation initialization works")
        
        # Test building validation targets
        targets = validate_op._build_validation_targets(None, '2024-01')
        assert isinstance(targets, list)
        print("✓ Validation target building works")


def test_dependency_injection_flow():
    """Test the complete dependency injection flow."""
    print("\n" + "=" * 60)
    print("Testing Dependency Injection Flow")
    print("=" * 60)
    
    # Create test config
    config_path = create_test_config()
    
    try:
        # Mock snowflake connector at import time
        with patch.dict('sys.modules', {'snowflake.connector': MagicMock()}):
            # Import after mocking
            from snowflake_etl.core.application_context import ApplicationContext
            from snowflake_etl.operations.load_operation import LoadOperation
            from snowflake_etl.models.file_config import FileConfig
            
            # Create context
            context = ApplicationContext(
                config_path=str(config_path),
                log_dir=Path('/tmp/logs'),
                log_level='INFO',
                quiet=True
            )
            
            print("✓ ApplicationContext created")
            
            # Create operation with context
            load_op = LoadOperation(context)
            
            # Verify dependencies are injected
            assert load_op.context == context
            assert load_op.connection_manager == context.connection_manager
            assert load_op.config_manager == context.config_manager
            print("✓ Dependencies properly injected")
            
            # Test file config creation
            file_config = FileConfig(
                file_path='/tmp/test.tsv',
                table_name='TEST_TABLE',
                date_column='recordDate',
                expected_columns=['col1', 'col2'],
                duplicate_key_columns=['recordDate', 'col1']
            )
            
            assert file_config.table_name == 'TEST_TABLE'
            print("✓ FileConfig creation works")
            
            # Test date extraction
            start, end = load_op._extract_dates_from_filename(
                'test_20240101-20240131.tsv'
            )
            assert start == '2024-01-01'
            assert end == '2024-01-31'
            print("✓ Date extraction works")
            
    finally:
        # Cleanup
        config_path.unlink()


def test_loader_config():
    """Test the LoaderConfig dataclass."""
    print("\n" + "=" * 60)
    print("Testing LoaderConfig")
    print("=" * 60)
    
    from snowflake_etl.core.snowflake_loader_optimal import LoaderConfig
    
    # Test default config
    config = LoaderConfig()
    assert config.chunk_size_mb == 10
    assert config.async_threshold_mb == 100
    assert config.stage_prefix == 'TSV_LOADER'
    print("✓ LoaderConfig defaults work")
    
    # Test custom config
    custom = LoaderConfig(
        chunk_size_mb=20,
        async_threshold_mb=200,
        stage_prefix='CUSTOM',
        on_error='CONTINUE'
    )
    assert custom.chunk_size_mb == 20
    assert custom.on_error == 'CONTINUE'
    print("✓ LoaderConfig customization works")
    
    # Test file format options
    assert 'TYPE' in config.file_format_options
    assert config.file_format_options['TYPE'] == 'CSV'
    assert config.file_format_options['FIELD_DELIMITER'] == '\\t'
    print("✓ File format options configured correctly")


def main():
    """Run all integration tests."""
    print("=" * 60)
    print("Integration Test Suite for Refactored ETL")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    
    try:
        test_cli_operations()
        test_operation_classes()
        test_dependency_injection_flow()
        test_loader_config()
        
        print("\n" + "=" * 60)
        print("✅ ALL INTEGRATION TESTS PASSED!")
        print("=" * 60)
        print(f"Completed at: {datetime.now()}")
        return 0
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())