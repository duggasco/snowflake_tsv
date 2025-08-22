#!/usr/bin/env python3
"""
Test script for the refactored Snowflake ETL architecture.
Tests dependency injection and component integration.
"""

import sys
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project to path
sys.path.insert(0, '/root/snowflake')

from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.core.snowflake_loader_optimal import SnowflakeLoader, LoaderConfig
from snowflake_etl.validators.snowflake_validator import SnowflakeDataValidator
from snowflake_etl.operations.load_operation import LoadOperation
from snowflake_etl.models.file_config import FileConfig


def test_loader_config():
    """Test LoaderConfig dataclass"""
    print("Testing LoaderConfig...")
    
    # Test default config
    config = LoaderConfig()
    assert config.chunk_size_mb == 10
    assert config.compression_level == 1
    assert config.async_threshold_mb == 100
    assert 'TYPE' in config.file_format_options
    assert config.file_format_options['TYPE'] == 'CSV'
    print("✓ LoaderConfig defaults work correctly")
    
    # Test custom config
    custom_config = LoaderConfig(
        chunk_size_mb=20,
        async_threshold_mb=200,
        stage_prefix='TEST'
    )
    assert custom_config.chunk_size_mb == 20
    assert custom_config.stage_prefix == 'TEST'
    print("✓ LoaderConfig customization works correctly")


def test_snowflake_loader():
    """Test SnowflakeLoader with mocked dependencies"""
    print("\nTesting SnowflakeLoader...")
    
    # Mock dependencies
    mock_conn_manager = MagicMock()
    mock_progress = MagicMock()
    config = LoaderConfig()
    
    # Create loader
    loader = SnowflakeLoader(
        connection_manager=mock_conn_manager,
        config=config,
        progress_tracker=mock_progress
    )
    
    # Verify initialization
    assert loader.config == config
    assert loader.connection_manager == mock_conn_manager
    assert loader.progress_tracker == mock_progress
    print("✓ SnowflakeLoader initialization works correctly")
    
    # Test stage name generation
    stage_name = loader._create_unique_stage("TEST_TABLE")
    assert "TEST_TABLE" in stage_name
    assert loader.config.stage_prefix in stage_name
    print(f"✓ Generated unique stage: {stage_name}")
    
    # Test file format clause generation
    file_format = loader._build_file_format_clause()
    assert "TYPE = 'CSV'" in file_format
    assert "FIELD_DELIMITER = '\\t'" in file_format
    print("✓ File format clause generation works correctly")


def test_snowflake_validator():
    """Test SnowflakeDataValidator with mocked dependencies"""
    print("\nTesting SnowflakeDataValidator...")
    
    # Mock dependencies
    mock_conn_manager = MagicMock()
    mock_progress = MagicMock()
    
    # Create validator
    validator = SnowflakeDataValidator(
        connection_manager=mock_conn_manager,
        progress_tracker=mock_progress
    )
    
    # Verify initialization
    assert validator.connection_manager == mock_conn_manager
    assert validator.progress_tracker == mock_progress
    print("✓ SnowflakeDataValidator initialization works correctly")
    
    # Test date formatting
    formatted = validator._format_date("20240115")
    assert formatted == "2024-01-15"
    print("✓ Date formatting works correctly")
    
    # Test expected dates calculation
    expected = validator._calculate_expected_dates("2024-01-01", "2024-01-31")
    assert expected == 31
    print("✓ Expected dates calculation works correctly")


def test_application_context():
    """Test ApplicationContext"""
    print("\nTesting ApplicationContext...")
    
    # Create mock config file
    config_path = Path("/tmp/test_config.json")
    config_data = {
        "snowflake": {
            "account": "test",
            "user": "test",
            "password": "test",
            "warehouse": "test",
            "database": "test",
            "schema": "test"
        },
        "files": []
    }
    
    import json
    with open(config_path, 'w') as f:
        json.dump(config_data, f)
    
    try:
        # Create context
        context = ApplicationContext(
            config_path=str(config_path),
            log_dir=Path("/tmp/logs"),
            log_level="INFO",
            quiet=True
        )
        
        # Verify components are initialized
        assert context.config_manager is not None
        assert context.connection_manager is not None
        assert context.logger is not None
        assert context.progress_tracker is not None
        print("✓ ApplicationContext initialization works correctly")
        
        # Test context manager
        with context:
            print("✓ ApplicationContext context manager works correctly")
            
    finally:
        # Cleanup
        if config_path.exists():
            config_path.unlink()


def test_load_operation():
    """Test LoadOperation with mocked context"""
    print("\nTesting LoadOperation...")
    
    # Mock context
    mock_context = MagicMock()
    mock_context.connection_manager = MagicMock()
    mock_context.config_manager = MagicMock()
    mock_context.logger = logging.getLogger('test')
    mock_context.progress_tracker = MagicMock()
    
    # Create operation
    operation = LoadOperation(mock_context)
    
    # Verify initialization
    assert operation.context == mock_context
    assert operation.file_analyzer is not None
    assert operation.quality_checker is not None
    assert operation.loader is not None
    assert operation.validator is not None
    print("✓ LoadOperation initialization works correctly")
    
    # Test date extraction
    start, end = operation._extract_dates_from_filename("file_20240101-20240131.tsv")
    assert start == "2024-01-01"
    assert end == "2024-01-31"
    print("✓ Date extraction from filename works correctly")


def test_integration():
    """Test component integration"""
    print("\nTesting Component Integration...")
    
    # This demonstrates how components work together
    config = LoaderConfig(
        chunk_size_mb=5,
        async_threshold_mb=50,
        stage_prefix='INTEGRATION_TEST'
    )
    
    mock_conn_manager = MagicMock()
    mock_progress = MagicMock()
    
    loader = SnowflakeLoader(
        connection_manager=mock_conn_manager,
        config=config,
        progress_tracker=mock_progress
    )
    
    validator = SnowflakeDataValidator(
        connection_manager=mock_conn_manager,
        progress_tracker=mock_progress
    )
    
    # Verify they share the same connection manager
    assert loader.connection_manager == validator.connection_manager
    print("✓ Components share connection manager correctly")
    
    # Verify configuration is properly used
    assert loader.config.chunk_size_mb == 5
    assert loader.config.stage_prefix == 'INTEGRATION_TEST'
    print("✓ Configuration is properly propagated")


def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing Refactored Snowflake ETL Architecture")
    print("=" * 60)
    
    try:
        test_loader_config()
        test_snowflake_loader()
        test_snowflake_validator()
        test_application_context()
        test_load_operation()
        test_integration()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED!")
        print("=" * 60)
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