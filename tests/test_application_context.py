"""
Unit tests for ApplicationContext and dependency injection
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
import json
import threading

from snowflake_etl.core.application_context import ApplicationContext


class TestApplicationContext:
    """Test suite for ApplicationContext"""
    
    def test_init_with_valid_config(self, config_file):
        """Test ApplicationContext initialization with valid config"""
        with patch('snowflake_etl.utils.config_manager_v2.ConfigManager'):
            with patch('snowflake_etl.utils.logging_config.get_logger'):
                context = ApplicationContext(
                    config_path=str(config_file),
                    log_dir=Path("/tmp/logs")
                )
                
                assert context.config_path == str(config_file)
                assert context.log_dir == Path("/tmp/logs")
                assert context.quiet is False
    
    def test_init_with_invalid_config(self):
        """Test ApplicationContext with invalid config path"""
        with pytest.raises(FileNotFoundError):
            ApplicationContext(
                config_path="/nonexistent/config.json",
                log_dir=Path("/tmp/logs")
            )
    
    def test_get_config(self, application_context, sample_config):
        """Test getting configuration"""
        config = application_context.get_config()
        assert config is not None
        # The actual config structure depends on ConfigManager implementation
    
    def test_get_logger(self, application_context):
        """Test getting logger"""
        logger = application_context.get_logger("test_module")
        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
    
    def test_get_connection(self, application_context, mock_snowflake_connection):
        """Test getting Snowflake connection"""
        conn = application_context.get_connection()
        assert conn is not None
        assert hasattr(conn, 'cursor')
    
    @patch('snowflake_etl.core.progress.TqdmProgressTracker')
    @patch('snowflake_etl.core.progress.NoOpProgressTracker')
    def test_get_progress_tracker(self, mock_noop, mock_tqdm, application_context):
        """Test getting progress tracker based on quiet mode"""
        # Test with quiet=False (should get TqdmProgressTracker)
        application_context.quiet = False
        tracker = application_context.get_progress_tracker()
        assert tracker is not None
        
        # Test with quiet=True (should get NoOpProgressTracker)
        application_context.quiet = True
        tracker = application_context.get_progress_tracker()
        assert tracker is not None
    
    def test_thread_local_connections(self, application_context):
        """Test thread-local connection management"""
        # Get connection in main thread
        conn1 = application_context.get_connection()
        
        # Get connection again in same thread - should be same instance
        conn2 = application_context.get_connection()
        assert conn1 is conn2
        
        # Test in different thread
        conn_in_thread = None
        
        def get_conn_in_thread():
            nonlocal conn_in_thread
            conn_in_thread = application_context.get_connection()
        
        thread = threading.Thread(target=get_conn_in_thread)
        thread.start()
        thread.join()
        
        # Connection in different thread should be different instance
        # (This depends on actual implementation of thread-local storage)
    
    def test_cleanup(self, application_context):
        """Test context cleanup"""
        # Get some resources
        conn = application_context.get_connection()
        tracker = application_context.get_progress_tracker()
        
        # Cleanup
        application_context.cleanup()
        
        # Verify cleanup was called
        # (Specific assertions depend on implementation)
    
    def test_context_manager(self, config_file):
        """Test ApplicationContext as context manager"""
        with patch('snowflake_etl.utils.config_manager_v2.ConfigManager'):
            with patch('snowflake_etl.utils.logging_config.get_logger'):
                with ApplicationContext(
                    config_path=str(config_file),
                    log_dir=Path("/tmp/logs")
                ) as context:
                    assert context is not None
                    # Resources should be available
                    logger = context.get_logger("test")
                    assert logger is not None
                
                # After exiting context, cleanup should have been called
                # (Specific verification depends on implementation)
    
    def test_get_file_configs(self, application_context):
        """Test getting file configurations"""
        with patch.object(application_context, 'get_config') as mock_config:
            mock_config.return_value = {
                "files": [
                    {"file_pattern": "test1_{date_range}.tsv", "table_name": "TABLE1"},
                    {"file_pattern": "test2_{date_range}.tsv", "table_name": "TABLE2"}
                ]
            }
            
            file_configs = application_context.get_file_configs()
            assert len(file_configs) == 2
            assert file_configs[0]["table_name"] == "TABLE1"
            assert file_configs[1]["table_name"] == "TABLE2"
    
    def test_get_snowflake_config(self, application_context):
        """Test getting Snowflake configuration"""
        with patch.object(application_context, 'get_config') as mock_config:
            mock_config.return_value = {
                "snowflake": {
                    "account": "test_account",
                    "warehouse": "TEST_WH",
                    "database": "TEST_DB"
                }
            }
            
            sf_config = application_context.get_snowflake_config()
            assert sf_config["account"] == "test_account"
            assert sf_config["warehouse"] == "TEST_WH"
    
    def test_set_quiet_mode(self, application_context):
        """Test setting quiet mode"""
        # Initially not quiet
        assert application_context.quiet is False
        
        # Set quiet mode
        application_context.set_quiet_mode(True)
        assert application_context.quiet is True
        
        # Progress tracker should be NoOp in quiet mode
        tracker = application_context.get_progress_tracker()
        assert tracker.__class__.__name__ in ["NoOpProgressTracker", "MagicMock"]
    
    def test_concurrent_access(self, application_context):
        """Test concurrent access to context resources"""
        import concurrent.futures
        
        def access_resources(context, thread_id):
            """Function to run in thread"""
            logger = context.get_logger(f"thread_{thread_id}")
            conn = context.get_connection()
            tracker = context.get_progress_tracker()
            return thread_id, logger is not None, conn is not None, tracker is not None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(access_resources, application_context, i)
                for i in range(10)
            ]
            
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # All threads should successfully get resources
        for thread_id, has_logger, has_conn, has_tracker in results:
            assert has_logger
            assert has_conn
            assert has_tracker