"""
Snowflake Connection Manager
Centralized connection management with pooling, retry logic, and health checks
"""

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from threading import Lock, local
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
    timeout: int = 300
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
            timeout=config_dict.get('timeout', 300),
            network_timeout=config_dict.get('network_timeout', 60)
        )
    
    @classmethod
    def from_config_file(cls, config_path: Union[str, Path]) -> 'ConnectionConfig':
        """Load configuration from JSON file"""
        with open(config_path, 'r') as f:
            config = json.load(f)
        return cls.from_dict(config.get('snowflake', config))


def retry_on_error(max_retries: int = 3, backoff_factor: float = 2.0):
    """
    Decorator for retrying operations with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for exponential backoff
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, DatabaseError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = backoff_factor ** attempt
                        logging.warning(
                            f"Operation failed (attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {wait_time} seconds... Error: {e}"
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Operation failed after {max_retries} attempts: {e}")
            raise last_exception
        return wrapper
    return decorator


class SnowflakeConnectionManager:
    """
    Singleton connection manager with pooling and retry logic
    
    Features:
    - Connection pooling with configurable size
    - Automatic retry with exponential backoff
    - Connection health checks
    - Thread-safe operations
    - Context manager support
    """
    
    _instance = None
    _lock = Lock()
    _thread_local = local()
    
    def __new__(cls):
        """Ensure singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize connection manager"""
        if self._initialized:
            return
            
        self.logger = logging.getLogger(__name__)
        self._connections = {}
        self._config_cache = {}
        self._connection_lock = Lock()
        self._max_pool_size = int(os.environ.get('SNOWFLAKE_POOL_SIZE', '5'))
        self._initialized = True
        
    def get_connection(
        self,
        config: Optional[Union[ConnectionConfig, Dict, str, Path]] = None,
        connection_name: str = 'default'
    ) -> SnowflakeConnection:
        """
        Get or create a Snowflake connection
        
        Args:
            config: Connection configuration (ConnectionConfig, dict, or path to config file)
            connection_name: Name for connection pooling
            
        Returns:
            Active Snowflake connection
        """
        # Parse configuration
        if config is None:
            config = self._get_cached_config(connection_name)
        elif isinstance(config, (str, Path)):
            config = ConnectionConfig.from_config_file(config)
        elif isinstance(config, dict):
            config = ConnectionConfig.from_dict(config)
        elif not isinstance(config, ConnectionConfig):
            raise ValueError(f"Invalid config type: {type(config)}")
        
        # Cache configuration
        self._config_cache[connection_name] = config
        
        # Check thread-local storage first
        thread_conn = getattr(self._thread_local, f'conn_{connection_name}', None)
        if thread_conn and self._is_connection_healthy(thread_conn):
            return thread_conn
            
        # Get or create connection with thread safety
        with self._connection_lock:
            conn = self._create_or_get_connection(config, connection_name)
            setattr(self._thread_local, f'conn_{connection_name}', conn)
            return conn
    
    def _get_cached_config(self, connection_name: str) -> ConnectionConfig:
        """Get cached configuration or raise error"""
        if connection_name not in self._config_cache:
            # Try to load from default locations
            default_paths = [
                Path('config/config.json'),
                Path('config.json'),
                Path.home() / '.snowflake' / 'config.json'
            ]
            for path in default_paths:
                if path.exists():
                    return ConnectionConfig.from_config_file(path)
            raise ValueError(
                f"No configuration provided and no cached config for '{connection_name}'. "
                f"Searched in: {', '.join(str(p) for p in default_paths)}"
            )
        return self._config_cache[connection_name]
    
    @retry_on_error(max_retries=3)
    def _create_or_get_connection(
        self,
        config: ConnectionConfig,
        connection_name: str
    ) -> SnowflakeConnection:
        """Create new connection or get existing healthy one"""
        # Check existing connection
        if connection_name in self._connections:
            conn = self._connections[connection_name]
            if self._is_connection_healthy(conn):
                return conn
            else:
                self.logger.info(f"Connection '{connection_name}' unhealthy, recreating...")
                self._close_connection(conn)
        
        # Create new connection
        self.logger.info(f"Creating new connection '{connection_name}'...")
        conn = snowflake.connector.connect(
            account=config.account,
            user=config.user,
            password=config.password,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema,
            role=config.role,
            login_timeout=config.timeout,
            network_timeout=config.network_timeout,
            session_parameters={
                'QUERY_TAG': 'snowflake_etl_pipeline',
                'ABORT_DETACHED_QUERY': False,
                'AUTOCOMMIT': True
            }
        )
        
        self._connections[connection_name] = conn
        self.logger.info(f"Successfully connected to Snowflake as '{connection_name}'")
        return conn
    
    def _is_connection_healthy(self, conn: SnowflakeConnection) -> bool:
        """Check if connection is healthy"""
        if conn is None or conn.is_closed():
            return False
            
        try:
            # Simple health check query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            self.logger.debug(f"Connection health check failed: {e}")
            return False
    
    def _close_connection(self, conn: SnowflakeConnection):
        """Safely close a connection"""
        try:
            if conn and not conn.is_closed():
                conn.close()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")
    
    @contextmanager
    def get_cursor(
        self,
        config: Optional[Union[ConnectionConfig, Dict, str, Path]] = None,
        connection_name: str = 'default',
        dict_cursor: bool = False
    ):
        """
        Context manager for cursor with automatic cleanup
        
        Args:
            config: Connection configuration
            connection_name: Name for connection pooling
            dict_cursor: Use DictCursor if True
            
        Yields:
            Snowflake cursor
        """
        conn = self.get_connection(config, connection_name)
        cursor_class = DictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_class)
        try:
            yield cursor
        finally:
            cursor.close()
    
    @retry_on_error(max_retries=3)
    def execute_query(
        self,
        query: str,
        params: Optional[Union[tuple, dict]] = None,
        config: Optional[Union[ConnectionConfig, Dict, str, Path]] = None,
        connection_name: str = 'default',
        fetch: bool = True
    ) -> Optional[list]:
        """
        Execute a query with automatic retry and error handling
        
        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries
            config: Connection configuration
            connection_name: Name for connection pooling
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, None otherwise
        """
        with self.get_cursor(config, connection_name) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                return cursor.fetchall()
            return None
    
    def execute_async(
        self,
        query: str,
        config: Optional[Union[ConnectionConfig, Dict, str, Path]] = None,
        connection_name: str = 'default'
    ) -> str:
        """
        Execute query asynchronously and return query ID
        
        Args:
            query: SQL query to execute
            config: Connection configuration
            connection_name: Name for connection pooling
            
        Returns:
            Query ID for status checking
        """
        conn = self.get_connection(config, connection_name)
        cursor = conn.cursor()
        cursor.execute_async(query)
        query_id = cursor.sfqid
        cursor.close()
        return query_id
    
    def get_query_status(
        self,
        query_id: str,
        config: Optional[Union[ConnectionConfig, Dict, str, Path]] = None,
        connection_name: str = 'default'
    ) -> Dict[str, Any]:
        """
        Get status of async query
        
        Args:
            query_id: Query ID from execute_async
            config: Connection configuration
            connection_name: Name for connection pooling
            
        Returns:
            Query status information
        """
        conn = self.get_connection(config, connection_name)
        cursor = conn.cursor()
        status = cursor.get_results_from_sfqid(query_id)
        cursor.close()
        return {
            'status': status,
            'is_running': cursor.is_still_running(lambda: cursor.get_results_from_sfqid(query_id))
        }
    
    def close_all(self):
        """Close all connections in the pool"""
        with self._connection_lock:
            for name, conn in self._connections.items():
                self.logger.info(f"Closing connection '{name}'")
                self._close_connection(conn)
            self._connections.clear()
            self._config_cache.clear()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close all connections"""
        self.close_all()
    
    @property
    def active_connections(self) -> int:
        """Get number of active connections"""
        return len([c for c in self._connections.values() if self._is_connection_healthy(c)])
    
    @property
    def pool_size(self) -> int:
        """Get current pool size"""
        return len(self._connections)