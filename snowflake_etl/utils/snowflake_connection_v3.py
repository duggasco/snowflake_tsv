"""
Snowflake Connection Manager V3
Non-singleton version for dependency injection
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Optional, Union

from snowflake_etl.utils.snowflake_connection_v2 import (
    ConnectionConfig,
    retry_on_error
)


class SnowflakeConnectionManager:
    """
    Connection manager without singleton pattern.
    Designed to be instantiated once and passed via dependency injection.
    
    Features:
    - Uses snowflake.connector native connection pooling
    - Connection validation and recovery
    - Context manager support for transactions
    - Async query support
    """
    
    def __init__(self):
        """Initialize connection manager"""
        self.logger = logging.getLogger(__name__)
        self._pool = None
        self._config = None
        self._is_initialized = False
    
    def initialize_pool(
        self,
        config: Union[ConnectionConfig, Dict],
        pool_size: int = 5
    ):
        """
        Initialize the connection pool
        
        Args:
            config: Connection configuration
            pool_size: Size of connection pool
        """
        # Parse configuration
        if isinstance(config, dict):
            config = ConnectionConfig.from_dict(config)
        elif not isinstance(config, ConnectionConfig):
            raise ValueError(f"Invalid config type: {type(config)}")
        
        self._config = config
        
        # Close existing pool if any
        if self._pool:
            self.close_pool()
        
        # Create new pool using Snowflake's built-in pooling
        try:
            # Import here to handle missing module gracefully
            import snowflake.connector
            from snowflake.connector import pooling
            
            self._pool = pooling.SnowflakeConnectionPool(
                connection_name='etl_pool',
                max_size=pool_size,
                max_idle_time=3600,  # 1 hour idle timeout
                **config.to_connect_params()
            )
            self._is_initialized = True
            self.logger.info(f"Initialized Snowflake connection pool with size {pool_size}")
            
        except ImportError:
            self.logger.warning("snowflake-connector-python not installed. Connection pool not created.")
            raise
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
            
        except Exception as e:
            self.logger.error(f"Database error occurred: {e}")
            raise
        finally:
            # Return connection to pool
            if conn and not conn.is_closed():
                conn.close()  # This returns it to the pool, doesn't actually close it
    
    def _validate_connection(self, conn) -> bool:
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
            # Import here to avoid issues if module not installed
            if dict_cursor:
                from snowflake.connector import DictCursor
                cursor = conn.cursor(DictCursor)
            else:
                cursor = conn.cursor()
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
            except Exception as e:
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
                self._is_initialized = False
    
    @property
    def is_initialized(self) -> bool:
        """Check if pool is initialized"""
        return self._is_initialized