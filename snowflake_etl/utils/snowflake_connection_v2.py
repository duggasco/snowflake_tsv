"""
Snowflake Connection Manager V2
Using built-in Snowflake connection pooling with enhanced error handling
"""

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Union

import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.errors import (
    DatabaseError,
    OperationalError, 
    ProgrammingError
)


@dataclass
class ConnectionConfig:
    """Snowflake connection configuration"""
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
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ConnectionConfig':
        """Create ConnectionConfig from dictionary"""
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
    
    @classmethod
    def from_config_file(cls, config_path: Union[str, Path]) -> 'ConnectionConfig':
        """Load configuration from JSON file"""
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls.from_dict(config.get('snowflake', config))
    
    def to_connect_params(self) -> Dict[str, Any]:
        """Convert to parameters for snowflake.connector.connect()"""
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
    """
    Decorator for retrying operations with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for exponential backoff
        retry_on: Tuple of exception types to retry on (defaults to Snowflake errors)
    """
    if retry_on is None:
        retry_on = (OperationalError, DatabaseError)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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


class SnowflakeConnectionManager:
    """
    Singleton connection manager using Snowflake's built-in pooling
    
    Features:
    - Uses snowflake.connector native connection pooling
    - Automatic retry with exponential backoff
    - Connection validation and recovery
    - Context manager support for transactions
    - Async query support
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """Ensure singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize connection manager"""
        if self._initialized:
            return
            
        self.logger = logging.getLogger(__name__)
        self._pool = None
        self._config = None
        self._pool_size = int(os.environ.get('SNOWFLAKE_POOL_SIZE', '5'))
        self._initialized = True
    
    def initialize_pool(
        self,
        config: Union[ConnectionConfig, Dict, str, Path],
        pool_size: Optional[int] = None
    ):
        """
        Initialize the connection pool
        
        Args:
            config: Connection configuration
            pool_size: Size of connection pool (uses env var or default if not provided)
        """
        # Parse configuration
        if isinstance(config, (str, Path)):
            config = ConnectionConfig.from_config_file(config)
        elif isinstance(config, dict):
            config = ConnectionConfig.from_dict(config)
        elif not isinstance(config, ConnectionConfig):
            raise ValueError(f"Invalid config type: {type(config)}")
        
        self._config = config
        pool_size = pool_size or self._pool_size
        
        # Close existing pool if any
        if self._pool:
            self.close_pool()
        
        # Create new pool using Snowflake's built-in pooling
        try:
            # Import here to avoid issues if connector not installed
            from snowflake.connector import pooling
            
            self._pool = pooling.SnowflakeConnectionPool(
                connection_name='etl_pool',
                max_size=pool_size,
                max_idle_time=3600,  # 1 hour idle timeout
                **config.to_connect_params()
            )
            self.logger.info(f"Initialized Snowflake connection pool with size {pool_size}")
        except Exception as e:
            self.logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self, validate: bool = True):
        """
        Get a connection from the pool with automatic cleanup
        
        Args:
            validate: Whether to validate connection health before returning
            
        Yields:
            SnowflakeConnection
        """
        if not self._pool:
            raise RuntimeError("Connection pool not initialized. Call initialize_pool() first.")
        
        conn = None
        try:
            conn = self._pool.get_connection()
            
            # Validate connection if requested
            if validate and not self._validate_connection(conn):
                self.logger.warning("Connection validation failed, getting new connection")
                conn.close()
                conn = self._pool.get_connection()
            
            yield conn
            
        except (OperationalError, DatabaseError) as e:
            self.logger.error(f"Database error occurred: {e}")
            raise
        finally:
            # Return connection to pool
            if conn and not conn.is_closed():
                conn.close()  # This returns it to the pool, doesn't actually close it
    
    def _validate_connection(self, conn: SnowflakeConnection) -> bool:
        """
        Validate that a connection is healthy
        
        Args:
            conn: Connection to validate
            
        Returns:
            True if connection is healthy
        """
        if conn.is_closed():
            return False
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            self.logger.debug(f"Connection validation failed: {e}")
            return False
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        """
        Get a cursor with automatic cleanup
        
        Args:
            dict_cursor: Use DictCursor if True
            
        Yields:
            Cursor object
        """
        with self.get_connection() as conn:
            cursor_class = DictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_class)
            try:
                yield cursor
            finally:
                cursor.close()
    
    @retry_on_error()
    def execute_query(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None,
        fetch: bool = True
    ) -> Optional[list]:
        """
        Execute a query with automatic retry
        
        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True
        """
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                return cursor.fetchall()
            return None
    
    def execute_async(self, query: str) -> str:
        """
        Execute query asynchronously
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query ID for status checking
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute_async(query)
                return cursor.sfqid
            finally:
                cursor.close()
    
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """
        Get status of async query
        
        Args:
            query_id: Query ID from execute_async
            
        Returns:
            Query status information
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Check if still running
                is_running = cursor.is_still_running(
                    lambda: cursor.get_results_from_sfqid(query_id)
                )
                
                # Get status
                if not is_running:
                    cursor.get_results_from_sfqid(query_id)
                    status = 'SUCCESS'
                else:
                    status = 'RUNNING'
                
                return {
                    'query_id': query_id,
                    'status': status,
                    'is_running': is_running
                }
            except ProgrammingError as e:
                if 'does not exist' in str(e):
                    return {
                        'query_id': query_id,
                        'status': 'NOT_FOUND',
                        'is_running': False,
                        'error': str(e)
                    }
                raise
            finally:
                cursor.close()
    
    def get_results_from_query_id(self, query_id: str) -> list:
        """
        Get results from a completed async query
        
        Args:
            query_id: Query ID from execute_async
            
        Returns:
            Query results
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.get_results_from_sfqid(query_id)
                return cursor.fetchall()
            finally:
                cursor.close()
    
    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transaction control
        
        Yields:
            Connection object with autocommit disabled
        """
        with self.get_connection() as conn:
            # Disable autocommit for transaction
            conn.autocommit(False)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                # Re-enable autocommit
                conn.autocommit(True)
    
    def close_pool(self):
        """Close the connection pool"""
        if self._pool:
            try:
                self._pool.close_connections()
                self.logger.info("Connection pool closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection pool: {e}")
            finally:
                self._pool = None
                self._config = None
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close pool"""
        self.close_pool()
    
    @property
    def is_initialized(self) -> bool:
        """Check if pool is initialized"""
        return self._pool is not None
    
    @property
    def pool_size(self) -> int:
        """Get configured pool size"""
        return self._pool_size if self._pool else 0


# Convenience function for simple queries
def execute_query(query: str, config_path: Optional[Union[str, Path]] = None) -> list:
    """
    Execute a simple query without managing connections
    
    Args:
        query: SQL query to execute
        config_path: Path to config file (uses default if not provided)
        
    Returns:
        Query results
    """
    manager = SnowflakeConnectionManager()
    
    if not manager.is_initialized:
        if not config_path:
            # Try default locations
            for path in ['config/config.json', 'config.json']:
                if Path(path).exists():
                    config_path = path
                    break
        
        if not config_path:
            raise ValueError("No configuration provided and no default config found")
        
        manager.initialize_pool(config_path)
    
    return manager.execute_query(query)