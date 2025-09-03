#!/usr/bin/env python3
"""
Snowflake Connection Manager v3
Production-ready connection management with pooling, async support, and proper lifecycle management
"""

import logging
import threading
import time
from contextlib import contextmanager
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass, field
import snowflake.connector
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.errors import (
    ProgrammingError, 
    OperationalError, 
    DatabaseError,
    InterfaceError
)


@dataclass
class ConnectionConfig:
    """Configuration for Snowflake connections"""
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    autocommit: bool = True
    login_timeout: int = 30
    network_timeout: int = 300
    query_timeout: int = 3600  # 1 hour default
    client_session_keep_alive: bool = True
    client_session_keep_alive_heartbeat_frequency: int = 900  # 15 minutes
    # Proxy support
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    proxy_user: Optional[str] = None
    proxy_password: Optional[str] = None
    use_proxy: bool = False
    # SSL/TLS options for proxy environments
    insecure_mode: bool = False  # Disable SSL verification (use with caution)
    ocsp_fail_open: bool = True  # Continue if OCSP responder is unavailable
    validate_default_parameters: bool = False  # Skip parameter validation
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for snowflake.connector"""
        config = {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
            'autocommit': self.autocommit,
            'login_timeout': self.login_timeout,
            'network_timeout': self.network_timeout,
            'client_session_keep_alive': self.client_session_keep_alive,
            'client_session_keep_alive_heartbeat_frequency': self.client_session_keep_alive_heartbeat_frequency,
        }
        
        if self.role:
            config['role'] = self.role
        
        # Add proxy configuration if enabled
        if self.use_proxy and self.proxy_host:
            config['proxy_host'] = self.proxy_host
            if self.proxy_port:
                config['proxy_port'] = self.proxy_port
            if self.proxy_user:
                config['proxy_user'] = self.proxy_user
            if self.proxy_password:
                config['proxy_password'] = self.proxy_password
        
        # Add SSL/TLS options for proxy environments
        if self.insecure_mode:
            config['insecure_mode'] = True
            
        if self.ocsp_fail_open:
            config['ocsp_fail_open'] = True
            
        if not self.validate_default_parameters:
            config['validate_default_parameters'] = False
            
        return config


class SnowflakeConnectionManager:
    """
    Thread-safe Snowflake connection manager with pooling and lifecycle management.
    
    Features:
    - Connection pooling with configurable size
    - Thread-safe connection acquisition
    - Automatic retry with exponential backoff
    - Connection validation and keepalive
    - Support for async queries
    - Proper cleanup and resource management
    """
    
    def __init__(self, config: Optional[ConnectionConfig] = None, pool_size: int = 5):
        """
        Initialize connection manager.
        
        Args:
            config: Connection configuration
            pool_size: Maximum number of connections in pool
        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.pool_size = pool_size
        self._connections = []
        self._in_use = set()
        self._lock = threading.Lock()
        self._closed = False
        self._heartbeat_thread = None
        self._stop_heartbeat = threading.Event()
        
        # Start heartbeat thread if keepalive is enabled
        if config and config.client_session_keep_alive:
            self._start_heartbeat()
    
    def set_config(self, config: ConnectionConfig) -> None:
        """Set or update connection configuration"""
        with self._lock:
            # Close existing connections if config changes
            if self.config and self.config != config:
                self._close_all_connections()
            
            self.config = config
            
            # Restart heartbeat if needed
            if config.client_session_keep_alive:
                if self._heartbeat_thread and not self._heartbeat_thread.is_alive():
                    self._start_heartbeat()
    
    def _create_connection(self) -> SnowflakeConnection:
        """Create a new Snowflake connection"""
        if not self.config:
            raise ValueError("Connection configuration not set")
        
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                self.logger.debug(f"Creating new Snowflake connection (attempt {attempt + 1}/{max_retries})")
                conn = snowflake.connector.connect(**self.config.to_dict())
                
                # Test the connection
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                
                self.logger.info("Successfully created Snowflake connection")
                return conn
                
            except (OperationalError, DatabaseError) as e:
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise
    
    def _validate_connection(self, conn: SnowflakeConnection) -> bool:
        """Check if a connection is still valid"""
        try:
            # Simple query to test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except (ProgrammingError, OperationalError, InterfaceError):
            return False
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Yields:
            SnowflakeConnection: A valid connection
        """
        if self._closed:
            raise RuntimeError("Connection manager is closed")
        
        conn = None
        try:
            with self._lock:
                # Try to get an existing connection from pool
                while self._connections:
                    candidate = self._connections.pop()
                    if candidate not in self._in_use:
                        if self._validate_connection(candidate):
                            conn = candidate
                            self._in_use.add(conn)
                            break
                        else:
                            self.logger.debug("Closing invalid connection")
                            try:
                                candidate.close()
                            except:
                                pass
                
                # Create new connection if needed
                if not conn:
                    if len(self._in_use) >= self.pool_size:
                        raise RuntimeError(f"Connection pool exhausted (size: {self.pool_size})")
                    conn = self._create_connection()
                    self._in_use.add(conn)
            
            yield conn
            
        finally:
            if conn:
                with self._lock:
                    self._in_use.discard(conn)
                    if not self._closed and self._validate_connection(conn):
                        self._connections.append(conn)
                    else:
                        try:
                            conn.close()
                        except:
                            pass
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = False):
        """
        Get a cursor from a pooled connection.
        
        Args:
            dict_cursor: If True, return DictCursor
            
        Yields:
            Cursor object
        """
        with self.get_connection() as conn:
            cursor_class = DictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_class) if cursor_class else conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()
    
    def execute(self, query: str, params: Optional[tuple] = None, 
                dict_cursor: bool = False) -> list:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            dict_cursor: If True, return results as dictionaries
            
        Returns:
            Query results
        """
        with self.get_cursor(dict_cursor=dict_cursor) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_async(self, query: str, params: Optional[tuple] = None) -> str:
        """
        Execute a query asynchronously.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Query ID for checking status
        """
        with self.get_cursor() as cursor:
            if params:
                cursor.execute_async(query, params)
            else:
                cursor.execute_async(query)
            return cursor.sfqid
    
    def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """
        Get status of an async query.
        
        Args:
            query_id: Snowflake query ID
            
        Returns:
            Query status information
        """
        with self.get_connection() as conn:
            return conn.get_query_status(query_id)
    
    def get_query_results(self, query_id: str) -> list:
        """
        Get results of a completed async query.
        
        Args:
            query_id: Snowflake query ID
            
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
    
    def execute_with_retry(self, query: str, params: Optional[tuple] = None,
                          max_retries: int = 3, retry_on: Optional[tuple] = None) -> list:
        """
        Execute a query with automatic retry on failure.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            max_retries: Maximum number of retry attempts
            retry_on: Tuple of exception types to retry on
            
        Returns:
            Query results
        """
        if retry_on is None:
            retry_on = (OperationalError, DatabaseError)
        
        retry_delay = 1
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                return self.execute(query, params)
            except retry_on as e:
                last_exception = e
                self.logger.warning(f"Query failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
        
        raise last_exception
    
    def _start_heartbeat(self):
        """Start background thread for connection keepalive"""
        def heartbeat():
            while not self._stop_heartbeat.is_set():
                try:
                    with self._lock:
                        for conn in self._connections:
                            if conn not in self._in_use:
                                try:
                                    cursor = conn.cursor()
                                    cursor.execute("SELECT 1")
                                    cursor.close()
                                except:
                                    pass
                except:
                    pass
                
                # Wait for next heartbeat
                self._stop_heartbeat.wait(
                    self.config.client_session_keep_alive_heartbeat_frequency
                )
        
        self._heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        self._heartbeat_thread.start()
        self.logger.debug("Started connection keepalive heartbeat thread")
    
    def _close_all_connections(self):
        """Close all connections in the pool"""
        with self._lock:
            all_conns = self._connections + list(self._in_use)
            for conn in all_conns:
                try:
                    conn.close()
                except:
                    pass
            self._connections.clear()
            self._in_use.clear()
    
    def close(self):
        """Close all connections and cleanup resources"""
        self.logger.info("Closing connection manager")
        
        # Stop heartbeat thread
        if self._heartbeat_thread:
            self._stop_heartbeat.set()
            self._heartbeat_thread.join(timeout=5)
        
        # Close all connections
        self._close_all_connections()
        self._closed = True
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def __del__(self):
        """Cleanup on deletion"""
        if not self._closed:
            self.close()


# Convenience function for creating from config dict
def create_connection_manager(config_dict: Dict[str, Any], 
                             pool_size: int = 5) -> SnowflakeConnectionManager:
    """
    Create a connection manager from a configuration dictionary.
    
    Args:
        config_dict: Snowflake configuration dictionary
        pool_size: Connection pool size
        
    Returns:
        Configured SnowflakeConnectionManager
    """
    config = ConnectionConfig(
        account=config_dict['account'],
        user=config_dict['user'],
        password=config_dict['password'],
        warehouse=config_dict['warehouse'],
        database=config_dict['database'],
        schema=config_dict['schema'],
        role=config_dict.get('role')
    )
    
    return SnowflakeConnectionManager(config, pool_size)