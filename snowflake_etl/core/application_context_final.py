"""
Application Context for Dependency Injection

Manages shared resources across the ETL pipeline lifecycle using dependency
injection instead of singletons.
"""

import logging
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Union, TYPE_CHECKING
from contextlib import contextmanager

from snowflake_etl.utils.config_manager_v2 import ConfigManager
from snowflake_etl.utils.logging_config import setup_logging

if TYPE_CHECKING:
    from snowflake_etl.utils.snowflake_connection_v3 import SnowflakeConnectionManager
    from snowflake_etl.core.progress import ProgressTracker


class ApplicationContext:
    """
    Central dependency injection container for ETL pipeline resources.
    
    Manages the lifecycle of shared resources (connections, config, logging)
    using explicit dependency injection instead of singletons. This improves
    testability and makes dependencies explicit.
    
    Thread Safety:
        Connections are thread-local. Each thread gets its own connection
        instance to prevent sharing issues in multi-threaded operations.
    
    Error Handling:
        Non-fatal errors during resource cleanup are logged but don't raise.
        Fatal errors during initialization are raised immediately.
    
    Example:
        Basic usage with context manager (recommended)::
        
            from snowflake_etl.core.application_context import ApplicationContext
            
            # Context manager ensures cleanup
            with ApplicationContext(config_path="config.json") as ctx:
                logger = ctx.get_logger("my_module")
                conn = ctx.get_connection()
                # Resources automatically cleaned up on exit
        
        Manual management (requires explicit cleanup)::
        
            # Use for long-running processes where you control lifecycle
            ctx = ApplicationContext(
                config_path="config.json",
                quiet=True  # Suppress output for automation
            )
            try:
                result = process_data(ctx)
            finally:
                ctx.cleanup()  # Essential to release resources
    
    See Also:
        - :class:`LoadOperation`: Uses context for ETL operations
        - :class:`ConfigManager`: Handles configuration loading
    """
    
    def __init__(
        self,
        config_path: Optional[Union[str, Path]] = None,
        log_dir: Optional[Path] = None,
        log_level: str = 'INFO',
        quiet: bool = False
    ):
        """
        Initialize application context with shared resources.
        
        Args:
            config_path: Path to JSON configuration file. If None, must call
                load_config() before accessing config-dependent resources.
            log_dir: Directory for log files. Created if doesn't exist.
                Defaults to './logs'.
            log_level: One of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
                DEBUG is verbose and may impact performance; use INFO for production.
            quiet: Suppress console output (progress bars, messages). Use True
                for cron jobs and CI/CD. Logs are still written to files.
        
        Raises:
            FileNotFoundError: If config_path provided but file doesn't exist.
            ValueError: If config file missing required 'snowflake' section.
            PermissionError: If log_dir cannot be created (insufficient permissions).
            json.JSONDecodeError: If config file contains invalid JSON.
        
        Example:
            >>> # Production configuration
            >>> ctx = ApplicationContext(
            ...     config_path="config/prod.json",
            ...     log_level="INFO",      # Balance detail and performance
            ...     quiet=True             # For automated execution
            ... )
            >>> 
            >>> # Development configuration  
            >>> ctx = ApplicationContext(
            ...     config_path="config/dev.json",
            ...     log_level="DEBUG",     # Maximum detail for debugging
            ...     quiet=False            # Show progress bars
            ... )
        """
        # Initialize logging first
        self.log_dir = log_dir or Path('logs')
        self.log_level = log_level
        self.quiet = quiet
        
        # Create log directory if needed
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            raise PermissionError(f"Cannot create log directory {self.log_dir}: {e}")
        
        setup_logging(
            operation='snowflake_etl',
            log_dir=self.log_dir,
            level=log_level,
            quiet=quiet
        )
        self.logger = logging.getLogger('snowflake_etl')
        self.logger.info(f"Initializing ApplicationContext (log_level={log_level}, quiet={quiet})")
        
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        self._config = None
        self._config_path = None
        
        # Load initial config if provided
        if config_path:
            try:
                self.load_config(config_path)
            except (FileNotFoundError, ValueError) as e:
                self.logger.error(f"Failed to load config: {e}")
                raise
        
        # Thread-local storage for connections
        self._thread_local = threading.local()
        
        # Connection manager - lazy initialization
        self._connection_manager = None
        
        # Operation registry
        self._operations = {}
        
        # Progress tracker - lazy initialization based on quiet mode
        self._progress_tracker = None
        
        # Track context manager state
        self._in_context = False
    
    def load_config(self, config_path: Union[str, Path]) -> None:
        """
        Load and validate configuration from JSON file.
        
        Args:
            config_path: Path to configuration file.
        
        Raises:
            FileNotFoundError: If file doesn't exist.
            json.JSONDecodeError: If file contains invalid JSON.
            ValueError: If missing required 'snowflake' section.
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        self._config_path = config_path
        self._config = self.config_manager.load_config(str(config_path))
        
        # Validate required sections
        if "snowflake" not in self._config:
            raise ValueError("Configuration missing required 'snowflake' section")
        
        self.logger.info(f"Loaded configuration from {config_path}")
    
    @property
    def config(self) -> Dict[str, Any]:
        """
        Get current configuration.
        
        Returns:
            Configuration dictionary.
        
        Raises:
            RuntimeError: If no configuration loaded.
        """
        if self._config is None:
            raise RuntimeError("No configuration loaded. Call load_config() first.")
        return self._config
    
    @property
    def snowflake_config(self) -> Dict[str, Any]:
        """
        Get Snowflake connection parameters.
        
        Returns:
            Dict with account, user, password, warehouse, database, schema, role.
        
        Raises:
            RuntimeError: If no configuration loaded.
            KeyError: If 'snowflake' section missing.
        """
        return self.config_manager.get_snowflake_config(self.config)
    
    def get_connection(self) -> Any:
        """
        Get thread-local Snowflake connection.
        
        Creates a new connection for the current thread if one doesn't exist.
        Multiple calls in the same thread return the same cached instance.
        
        Returns:
            Active Snowflake connection.
        
        Raises:
            RuntimeError: If no configuration loaded.
            snowflake.connector.errors.DatabaseError: If connection fails.
            snowflake.connector.errors.ProgrammingError: If credentials invalid.
        """
        if not hasattr(self._thread_local, 'connection'):
            self.logger.debug(f"Creating connection for thread {threading.current_thread().name}")
            
            if self._connection_manager is None:
                from snowflake_etl.utils.snowflake_connection_v3 import SnowflakeConnectionManager
                self._connection_manager = SnowflakeConnectionManager(self.snowflake_config)
            
            self._thread_local.connection = self._connection_manager.get_connection()
            
        return self._thread_local.connection
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        Get logger for a specific module.
        
        Args:
            name: Logger name (e.g., 'etl.loader'). None returns root logger.
        
        Returns:
            Configured logger instance.
        """
        return logging.getLogger(name) if name else self.logger
    
    def get_progress_tracker(self) -> 'ProgressTracker':
        """
        Get progress tracker based on quiet mode.
        
        Returns:
            TqdmProgressTracker (normal) or NoOpProgressTracker (quiet).
        """
        if self._progress_tracker is None:
            if self.quiet:
                from snowflake_etl.core.progress import NoOpProgressTracker
                self._progress_tracker = NoOpProgressTracker()
            else:
                from snowflake_etl.core.progress import TqdmProgressTracker
                self._progress_tracker = TqdmProgressTracker()
        
        return self._progress_tracker
    
    def cleanup(self) -> None:
        """
        Release all resources.
        
        Closes connections, flushes logs, and cleans up progress trackers.
        Non-fatal errors are logged but don't raise. Always call this or
        use context manager to prevent resource leaks.
        """
        self.logger.info("Cleaning up ApplicationContext")
        
        # Close thread-local connections
        if hasattr(self._thread_local, 'connection'):
            try:
                self._thread_local.connection.close()
                self.logger.debug("Closed thread-local connection")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
        
        # Close connection manager
        if self._connection_manager:
            try:
                self._connection_manager.close_all()
            except Exception as e:
                self.logger.warning(f"Error closing connection manager: {e}")
        
        # Close progress tracker
        if self._progress_tracker:
            try:
                self._progress_tracker.close()
            except Exception:
                pass
    
    def __enter__(self) -> 'ApplicationContext':
        """Enter context manager."""
        self._in_context = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and cleanup."""
        self.cleanup()
        self._in_context = False