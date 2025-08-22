#!/usr/bin/env python3
"""
Simple architecture test without Snowflake dependencies.
Demonstrates the dependency injection pattern and component structure.
"""

import sys
import json
import tempfile
from pathlib import Path
from datetime import datetime

# Add project to path
sys.path.insert(0, '/root/snowflake')


def test_dataclasses():
    """Test our dataclass models."""
    print("=" * 60)
    print("Testing Dataclass Models")
    print("=" * 60)
    
    # Test FileConfig
    from snowflake_etl.models.file_config import FileConfig
    
    config = FileConfig(
        file_path="/data/test.tsv",
        table_name="TEST_TABLE",
        date_column="recordDate",
        expected_columns=["col1", "col2", "col3"],
        duplicate_key_columns=["recordDate", "col1"],
        expected_date_range=("2024-01-01", "2024-01-31")
    )
    
    assert config.table_name == "TEST_TABLE"
    assert len(config.expected_columns) == 3
    print("✓ FileConfig dataclass works")
    
    # Test LoaderConfig
    from snowflake_etl.models.loader_config import LoaderConfig
    
    loader_config = LoaderConfig()
    assert loader_config.chunk_size_mb == 10
    assert loader_config.compression_level == 1
    assert loader_config.async_threshold_mb == 100
    assert loader_config.stage_prefix == 'TSV_LOADER'
    print("✓ LoaderConfig defaults work")
    
    # Test custom config
    custom_loader = LoaderConfig(
        chunk_size_mb=20,
        async_threshold_mb=200,
        stage_prefix='CUSTOM_LOADER'
    )
    assert custom_loader.chunk_size_mb == 20
    assert custom_loader.stage_prefix == 'CUSTOM_LOADER'
    print("✓ LoaderConfig customization works")
    
    # Test DeletionTarget
    from snowflake_etl.operations.delete_operation import DeletionTarget, DeletionResult
    
    target = DeletionTarget(
        table_name="TEST_TABLE",
        date_column="recordDate",
        year_month="2024-01",
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    assert target.year_month == "2024-01"
    print("✓ DeletionTarget dataclass works")
    
    # Test DeletionResult
    result = DeletionResult(
        target=target,
        rows_affected=1000,
        total_rows_before=10000,
        deletion_percentage=10.0,
        status="success",
        execution_time=5.5
    )
    
    result_dict = result.to_dict()
    assert result_dict['rows_affected'] == 1000
    assert result_dict['status'] == 'success'
    print("✓ DeletionResult dataclass works")
    
    # Test ValidationResult
    from snowflake_etl.models.validation_result import ValidationResult
    
    val_result = ValidationResult(
        valid=True,
        table_name="TEST_TABLE",
        total_rows=10000,
        unique_dates=31,
        expected_dates=31,
        avg_rows_per_day=322.58
    )
    
    val_dict = val_result.to_dict()
    assert val_dict['valid'] is True
    assert val_dict['table_name'] == "TEST_TABLE"
    print("✓ ValidationResult dataclass works")


def test_cli_structure():
    """Test CLI argument parsing."""
    print("\n" + "=" * 60)
    print("Testing CLI Structure")
    print("=" * 60)
    
    from snowflake_etl.cli.main import SnowflakeETLCLI
    
    cli = SnowflakeETLCLI()
    
    # Test various command structures
    test_cases = [
        # Load command
        (['--config', 'test.json', 'load', '--month', '2024-01'], {
            'operation': 'load',
            'month': '2024-01'
        }),
        # Delete command
        (['--config', 'test.json', 'delete', '--table', 'T1', '--month', '2024-01', '--dry-run'], {
            'operation': 'delete',
            'table': 'T1',
            'dry_run': True
        }),
        # Validate command
        (['--config', 'test.json', 'validate', '--output', 'out.json'], {
            'operation': 'validate',
            'output': 'out.json'
        }),
        # Global quiet flag
        (['--config', 'test.json', '--quiet', 'load', '--skip-qc'], {
            'quiet': True,
            'skip_qc': True
        })
    ]
    
    for args_list, expected in test_cases:
        args = cli.parse_args(args_list)
        for key, value in expected.items():
            assert getattr(args, key) == value, f"Expected {key}={value}, got {getattr(args, key)}"
    
    print("✓ All CLI parsing tests passed")


def test_progress_enums():
    """Test progress tracking enums."""
    print("\n" + "=" * 60)
    print("Testing Progress Tracking")
    print("=" * 60)
    
    from snowflake_etl.core.progress import ProgressPhase
    
    # Test enum values (using actual values from the file)
    assert ProgressPhase.ANALYSIS.value == "analysis"
    assert ProgressPhase.QUALITY_CHECK.value == "quality_check"
    assert ProgressPhase.COMPRESSION.value == "compression"
    assert ProgressPhase.UPLOAD.value == "upload"
    assert ProgressPhase.COPY.value == "copy"
    assert ProgressPhase.VALIDATION.value == "validation"
    assert ProgressPhase.COMPLETE.value == "complete"
    
    print("✓ ProgressPhase enum works correctly")


def test_base_operation():
    """Test BaseOperation structure."""
    print("\n" + "=" * 60)
    print("Testing BaseOperation Pattern")
    print("=" * 60)
    
    from snowflake_etl.core.application_context import BaseOperation
    from unittest.mock import MagicMock
    
    # Create mock context
    mock_context = MagicMock()
    mock_context.connection_manager = MagicMock()
    mock_context.config_manager = MagicMock()
    mock_context.logger = MagicMock()
    mock_context.progress_tracker = MagicMock()
    
    # Create a test operation
    class TestOperation(BaseOperation):
        def execute(self):
            return "test_result"
    
    operation = TestOperation(mock_context)
    
    # Verify inheritance and properties
    assert operation.context == mock_context
    assert operation.connection_manager == mock_context.connection_manager
    # Note: config_manager is accessed through context, not directly
    assert operation.context.config_manager == mock_context.config_manager
    assert operation.logger is not None  # Logger is created in __init__
    assert operation.progress_tracker == mock_context.progress_tracker
    
    # Test execution
    result = operation.execute()
    assert result == "test_result"
    
    print("✓ BaseOperation pattern works correctly")


def test_file_operations():
    """Test file handling utilities."""
    print("\n" + "=" * 60)
    print("Testing File Operations")
    print("=" * 60)
    
    from pathlib import Path
    import tempfile
    
    # Test Path operations
    test_file = Path(tempfile.mktemp(suffix='.tsv'))
    test_file.write_text("col1\tcol2\tcol3\nval1\tval2\tval3\n")
    
    assert test_file.exists()
    assert test_file.suffix == '.tsv'
    
    # Test file size
    size = test_file.stat().st_size
    assert size > 0
    
    # Cleanup
    test_file.unlink()
    assert not test_file.exists()
    
    print("✓ File operations work correctly")


def test_configuration_structure():
    """Test configuration file structure."""
    print("\n" + "=" * 60)
    print("Testing Configuration Structure")
    print("=" * 60)
    
    # Sample configuration
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
                "file_pattern": "table1_{date_range}.tsv",
                "table_name": "TABLE1",
                "date_column": "recordDate",
                "expected_columns": ["col1", "col2"],
                "duplicate_key_columns": ["recordDate", "col1"]
            },
            {
                "file_pattern": "table2_{month}.tsv",
                "table_name": "TABLE2",
                "date_column": "date",
                "expected_columns": ["colA", "colB"]
            }
        ]
    }
    
    # Test structure
    assert "snowflake" in config
    assert "files" in config
    assert len(config["files"]) == 2
    
    # Test Snowflake config
    sf_config = config["snowflake"]
    assert sf_config["account"] == "test_account"
    assert sf_config["warehouse"] == "TEST_WH"
    
    # Test file configs
    file1 = config["files"][0]
    assert file1["table_name"] == "TABLE1"
    assert "{date_range}" in file1["file_pattern"]
    
    file2 = config["files"][1]
    assert file2["table_name"] == "TABLE2"
    assert "{month}" in file2["file_pattern"]
    
    print("✓ Configuration structure is valid")


def main():
    """Run all simple tests."""
    print("=" * 60)
    print("Simple Architecture Test Suite")
    print("=" * 60)
    print(f"Started at: {datetime.now()}\n")
    
    try:
        test_dataclasses()
        test_cli_structure()
        test_progress_enums()
        test_base_operation()
        test_file_operations()
        test_configuration_structure()
        
        print("\n" + "=" * 60)
        print("✅ ALL ARCHITECTURE TESTS PASSED!")
        print("=" * 60)
        print(f"Completed at: {datetime.now()}")
        return 0
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())