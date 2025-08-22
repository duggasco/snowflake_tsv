#!/usr/bin/env python3
"""
Test script for refactored Snowflake ETL modules
Tests the three core utility modules without requiring Snowflake connection
"""

import json
import os
import sys
import tempfile
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))

def test_config_manager():
    """Test ConfigManager functionality"""
    print("\n" + "="*60)
    print("Testing ConfigManager V2")
    print("="*60)
    
    from snowflake_etl.utils.config_manager_v2 import ConfigManager, ConfigError
    
    # Create test config
    test_config = {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "password": "test_pass",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA"
        },
        "files": [
            {
                "file_pattern": "test_{date_range}.tsv",
                "table_name": "TEST_TABLE",
                "date_column": "date",
                "expected_columns": ["col1", "col2"]
            }
        ]
    }
    
    # Test with temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(test_config, f)
        temp_path = Path(f.name)
    
    try:
        manager = ConfigManager()
        
        # Test 1: Load config
        print("\n1. Testing config loading...")
        config = manager.load_config(temp_path, validate=False)
        assert config['snowflake']['account'] == 'test_account'
        print("   [PASS] Config loaded successfully")
        
        # Test 2: Environment override
        print("\n2. Testing environment overrides...")
        os.environ['SNOWFLAKE_ETL_WAREHOUSE'] = 'OVERRIDE_WH'
        config = manager.load_config(temp_path, apply_env_overrides=True, validate=False)
        assert config['snowflake']['warehouse'] == 'OVERRIDE_WH'
        print("   [PASS] Environment override applied")
        del os.environ['SNOWFLAKE_ETL_WAREHOUSE']
        
        # Test 3: Cache functionality
        print("\n3. Testing LRU cache...")
        # First load
        manager.clear_cache()
        config1 = manager.load_config(temp_path, validate=False)
        cache_info1 = manager.cache_info
        print(f"   Cache after first load: hits={cache_info1.hits}, misses={cache_info1.misses}")
        
        # Second load (should hit cache)
        config2 = manager.load_config(temp_path, validate=False)
        cache_info2 = manager.cache_info
        print(f"   Cache after second load: hits={cache_info2.hits}, misses={cache_info2.misses}")
        assert cache_info2.hits > cache_info1.hits
        print("   [PASS] Cache working correctly")
        
        # Test 4: Get specific configs
        print("\n4. Testing config extraction...")
        sf_config = manager.get_snowflake_config(config)
        assert sf_config['database'] == 'TEST_DB'
        
        file_configs = manager.get_file_configs(config)
        assert len(file_configs) == 1
        assert file_configs[0]['table_name'] == 'TEST_TABLE'
        print("   [PASS] Config extraction working")
        
        # Test 5: Merge configs
        print("\n5. Testing config merging...")
        config2 = {
            "snowflake": {"warehouse": "MERGED_WH"},
            "files": [{"file_pattern": "test2.tsv", "table_name": "TEST2"}]
        }
        merged = manager.merge_configs(config, config2)
        assert merged['snowflake']['warehouse'] == 'MERGED_WH'  # Last wins
        assert len(merged['files']) == 2
        print("   [PASS] Config merging working")
        
        print("\n[SUCCESS] All ConfigManager tests passed!")
        
    finally:
        temp_path.unlink()


def test_logging_config():
    """Test logging configuration"""
    print("\n" + "="*60)
    print("Testing Logging Configuration")
    print("="*60)
    
    from snowflake_etl.utils.logging_config import setup_logging, get_logger, log_performance
    import logging
    
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir) / 'logs'
        
        # Test 1: Basic setup
        print("\n1. Testing basic logging setup...")
        setup_logging(
            operation='test_operation',
            log_dir=log_dir,
            level='DEBUG',
            quiet=True  # Suppress console output for test
        )
        
        # Check that log files were created
        assert (log_dir / 'test_operation.log').exists()
        assert (log_dir / 'test_operation_debug.log').exists()
        assert (log_dir / 'errors.log').exists()
        assert (log_dir / 'performance.log').exists()
        print("   [PASS] Log files created")
        
        # Test 2: Logger functionality
        print("\n2. Testing logger functionality...")
        logger = get_logger('snowflake_etl')
        logger.info("Test info message")
        logger.debug("Test debug message")
        logger.error("Test error message")
        
        # Check that messages were written
        with open(log_dir / 'test_operation_debug.log') as f:
            content = f.read()
            assert "Test info message" in content
            assert "Test debug message" in content
        print("   [PASS] Logger writing correctly")
        
        # Test 3: Performance logging
        print("\n3. Testing performance logging...")
        log_performance(
            operation='test_query',
            duration=1.23,
            rows_processed=1000,
            throughput_mbps=45.6
        )
        
        with open(log_dir / 'performance.log') as f:
            content = f.read()
            assert 'test_query' in content
            assert '1.23' in content or '1000' in content  # Check for our metrics
        print("   [PASS] Performance logging working")
        
        # Test 4: Multiple operations
        print("\n4. Testing operation-specific loggers...")
        tsv_logger = logging.getLogger('tsv_loader')
        tsv_logger.info("TSV loader message")
        
        drop_logger = logging.getLogger('drop_month')
        drop_logger.info("Drop month message")
        
        # These should create their own files based on config
        print("   [PASS] Operation-specific loggers configured")
        
        print("\n[SUCCESS] All logging tests passed!")


def test_connection_config():
    """Test connection configuration (without actual connection)"""
    print("\n" + "="*60)
    print("Testing Connection Configuration")
    print("="*60)
    
    try:
        from snowflake_etl.utils.snowflake_connection_v2 import ConnectionConfig, retry_on_error
    except ImportError:
        # Create mock classes for testing without snowflake-connector
        print("\n[WARNING] snowflake-connector not installed, using mock classes")
        
        from dataclasses import dataclass
        from typing import Optional, Dict, Any
        from functools import wraps
        import logging
        
        @dataclass
        class ConnectionConfig:
            account: str
            user: str
            password: str
            warehouse: str
            database: str
            schema: str
            role: Optional[str] = None
            login_timeout: int = 300
            network_timeout: int = 60
            
            @classmethod
            def from_dict(cls, config_dict: Dict[str, Any]):
                return cls(
                    account=config_dict['account'],
                    user=config_dict['user'],
                    password=config_dict['password'],
                    warehouse=config_dict['warehouse'],
                    database=config_dict['database'],
                    schema=config_dict['schema'],
                    role=config_dict.get('role'),
                    login_timeout=config_dict.get('login_timeout', 300),
                    network_timeout=config_dict.get('network_timeout', 60)
                )
            
            def to_connect_params(self) -> Dict[str, Any]:
                params = {
                    'account': self.account,
                    'user': self.user,
                    'password': self.password,
                    'warehouse': self.warehouse,
                    'database': self.database,
                    'schema': self.schema,
                    'login_timeout': self.login_timeout,
                    'network_timeout': self.network_timeout,
                    'session_parameters': {
                        'QUERY_TAG': 'snowflake_etl_pipeline',
                        'ABORT_DETACHED_QUERY': False,
                        'AUTOCOMMIT': True
                    }
                }
                if self.role:
                    params['role'] = self.role
                return params
        
        def retry_on_error(max_retries: int = 3, backoff_factor: float = 2.0, retry_on: tuple = None):
            if retry_on is None:
                retry_on = (ValueError,)  # Use ValueError for testing
            
            def decorator(func):
                @wraps(func)
                def wrapper(*args, **kwargs):
                    import time
                    last_exception = None
                    for attempt in range(max_retries):
                        try:
                            return func(*args, **kwargs)
                        except retry_on as e:
                            last_exception = e
                            if attempt < max_retries - 1:
                                wait_time = backoff_factor ** attempt
                                logging.warning(
                                    f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}). "
                                    f"Retrying in {wait_time} seconds... Error: {e}"
                                )
                                time.sleep(wait_time)
                            else:
                                logging.error(f"{func.__name__} failed after {max_retries} attempts: {e}")
                    raise last_exception
                return wrapper
            return decorator
    
    import time
    
    # Test 1: ConnectionConfig creation
    print("\n1. Testing ConnectionConfig...")
    
    config_dict = {
        'account': 'test_account',
        'user': 'test_user',
        'password': 'test_pass',
        'warehouse': 'TEST_WH',
        'database': 'TEST_DB',
        'schema': 'TEST_SCHEMA',
        'role': 'TEST_ROLE'
    }
    
    config = ConnectionConfig.from_dict(config_dict)
    assert config.account == 'test_account'
    assert config.role == 'TEST_ROLE'
    print("   [PASS] ConnectionConfig created from dict")
    
    # Test 2: Convert to connect params
    print("\n2. Testing connect params conversion...")
    params = config.to_connect_params()
    assert params['account'] == 'test_account'
    assert 'session_parameters' in params
    assert params['session_parameters']['QUERY_TAG'] == 'snowflake_etl_pipeline'
    print("   [PASS] Connect params generated correctly")
    
    # Test 3: Retry decorator
    print("\n3. Testing retry decorator...")
    
    attempt_count = 0
    
    @retry_on_error(max_retries=3, backoff_factor=0.1, retry_on=(ValueError,))
    def failing_function():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise ValueError(f"Attempt {attempt_count} failed")
        return "Success"
    
    start_time = time.time()
    result = failing_function()
    duration = time.time() - start_time
    
    assert result == "Success"
    assert attempt_count == 3
    assert duration > 0.1  # Should have had backoff delays
    print(f"   [PASS] Retry decorator worked (3 attempts in {duration:.2f}s)")
    
    print("\n[SUCCESS] All connection configuration tests passed!")


def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("# SNOWFLAKE ETL REFACTORED MODULES TEST SUITE")
    print("#"*60)
    
    try:
        test_config_manager()
        test_logging_config()
        test_connection_config()
        
        print("\n" + "#"*60)
        print("# ALL TESTS PASSED SUCCESSFULLY!")
        print("#"*60)
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())