"""
Pytest configuration and shared fixtures for Snowflake ETL tests
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any, List
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_config(temp_dir) -> Dict[str, Any]:
    """Create a sample configuration dictionary"""
    return {
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
                "file_pattern": "test_{date_range}.tsv",
                "table_name": "TEST_TABLE",
                "date_column": "recordDate",
                "expected_columns": ["col1", "col2", "col3"],
                "duplicate_key_columns": ["recordDate", "col1"]
            }
        ]
    }


@pytest.fixture
def config_file(temp_dir, sample_config) -> Path:
    """Create a temporary config file"""
    config_path = temp_dir / "test_config.json"
    with open(config_path, "w") as f:
        json.dump(sample_config, f)
    return config_path


@pytest.fixture
def sample_tsv_file(temp_dir) -> Path:
    """Create a sample TSV file for testing"""
    tsv_path = temp_dir / "test_20240101-20240131.tsv"
    with open(tsv_path, "w") as f:
        f.write("col1\tcol2\tcol3\trecordDate\n")
        f.write("val1\tval2\tval3\t20240101\n")
        f.write("val4\tval5\tval6\t20240102\n")
        f.write("val7\tval8\tval9\t20240103\n")
    return tsv_path


@pytest.fixture
def large_tsv_file(temp_dir) -> Path:
    """Create a larger TSV file for performance testing"""
    tsv_path = temp_dir / "large_test.tsv"
    with open(tsv_path, "w") as f:
        f.write("col1\tcol2\tcol3\trecordDate\n")
        for i in range(10000):
            date = 20240101 + (i % 31)
            f.write(f"val{i*3}\tval{i*3+1}\tval{i*3+2}\t{date}\n")
    return tsv_path


@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection for testing"""
    with patch("snowflake.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Setup mock cursor behavior
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [("count",), ("date",)]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.is_closed.return_value = False
        
        mock_connect.return_value = mock_conn
        
        yield mock_conn


@pytest.fixture
def mock_logger():
    """Mock logger for testing"""
    logger = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.debug = MagicMock()
    return logger


@pytest.fixture
def mock_progress_tracker():
    """Mock progress tracker for testing"""
    tracker = MagicMock()
    tracker.update_files = MagicMock()
    tracker.update_qc_rows = MagicMock()
    tracker.update_compression = MagicMock()
    tracker.close = MagicMock()
    return tracker


@pytest.fixture
def application_context(config_file, mock_snowflake_connection, mock_logger):
    """Create an ApplicationContext for testing"""
    from snowflake_etl.core.application_context import ApplicationContext
    
    with patch("snowflake_etl.utils.logging_config.get_logger") as mock_get_logger:
        mock_get_logger.return_value = mock_logger
        
        context = ApplicationContext(
            config_path=str(config_file),
            log_dir=Path("/tmp/logs"),
            quiet=False
        )
        
        # Mock the connection manager
        context._connection_manager = MagicMock()
        context._connection_manager.get_connection.return_value = mock_snowflake_connection
        
        yield context


@pytest.fixture
def sample_validation_results() -> Dict[str, Any]:
    """Sample validation results for testing"""
    return {
        "table": "TEST_TABLE",
        "status": "success",
        "date_range": {
            "requested_start": "2024-01-01",
            "requested_end": "2024-01-31",
            "actual_start": "2024-01-01",
            "actual_end": "2024-01-31",
            "missing_dates": []
        },
        "statistics": {
            "total_rows": 1000000,
            "total_days": 31,
            "avg_rows_per_day": 32258
        },
        "anomalies": {
            "outliers": [],
            "gaps": []
        },
        "duplicates": {
            "duplicate_keys": 0,
            "excess_rows": 0,
            "percentage": 0.0,
            "severity": "NONE"
        }
    }


# Utility functions for tests
def create_mock_file_config(file_pattern: str = "test_{date_range}.tsv",
                           table_name: str = "TEST_TABLE") -> Dict[str, Any]:
    """Create a mock file configuration"""
    return {
        "file_pattern": file_pattern,
        "table_name": table_name,
        "date_column": "recordDate",
        "expected_columns": ["col1", "col2", "col3", "recordDate"],
        "duplicate_key_columns": ["recordDate", "col1"]
    }


def create_test_tsv_content(rows: int = 100, columns: List[str] = None) -> str:
    """Generate TSV content for testing"""
    if columns is None:
        columns = ["col1", "col2", "col3", "recordDate"]
    
    lines = ["\t".join(columns)]
    for i in range(rows):
        date = 20240101 + (i % 31)
        row = [f"val{i}_{j}" for j in range(len(columns) - 1)]
        row.append(str(date))
        lines.append("\t".join(row))
    
    return "\n".join(lines)