"""
Application Context for Dependency Injection

This module provides the central ApplicationContext class that manages
shared resources across the application lifecycle using dependency injection
pattern instead of singletons.
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
    Central context for managing application-wide resources using dependency injection.
    
    The ApplicationContext serves as a dependency injection container that manages
    the lifecycle of shared resources like database connections, configuration,
    logging, and progress tracking. It replaces the anti-pattern of singletons
    with explicit dependency injection, making the code more testable and maintainable.
    
    Attributes:
        log_dir (Path): Directory where log files are stored
        quiet (bool): Whether to suppress console output
        logger (logging.Logger): Main application logger
        config_manager (ConfigManager): Configuration management instance
        
    Example:
        Basic usage with context manager::
        
            from snowflake_etl.core.application_context import ApplicationContext
            
            with ApplicationContext(config_path="config.json") as context:
                # Get a logger for a specific module
                logger = context.get_logger("my_module")
                logger.info("Starting operation")
                
                # Get database connection
                conn = context.get_connection()
                
                # Get progress tracker
                tracker = context.get_progress_tracker()
                tracker.update(50)
    
        Manual resource management::
        
            context = ApplicationContext(
                config_path="config.json",
                log_dir=Path("/var/log/etl"),
                quiet=True
            )
            try:
                # Use context resources
                config = context.config
                conn = context.get_connection()
            finally:
                context.cleanup()
    
    Thread Safety:
        The ApplicationContext is thread-safe. Database connections are
        managed per-thread using thread-local storage to avoid connection
        sharing issues in multi-threaded environments.
    
    Note:
        Always use the context manager pattern or ensure cleanup() is called
        to properly release resources and close database connections.
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
            config_path: Path to JSON configuration file. Can be string or Path object.
                If None, configuration must be loaded later using load_config().
            log_dir: Directory for log files. Defaults to 'logs' in current directory.
                Directory will be created if it doesn't exist.
            log_level: Logging level as string. One of: 'DEBUG', 'INFO', 'WARNING', 
                'ERROR', 'CRITICAL'. Defaults to 'INFO'.
            quiet: If True, suppresses console output (progress bars, info messages).
                Log files are still written. Useful for batch processing and CI/CD.
        
        Raises:
            FileNotFoundError: If config_path is provided but file doesn't exist
            ValueError: If config_path points to an invalid configuration file
            PermissionError: If log_dir cannot be created due to permissions
        
        Example:
            >>> # Basic initialization
            >>> context = ApplicationContext(config_path="config/prod.json")
            >>> 
            >>> # With custom settings
            >>> context = ApplicationContext(
            ...     config_path="config/dev.json",
            ...     log_dir=Path("/var/log/snowflake_etl"),
            ...     log_level="DEBUG",
            ...     quiet=False
            ... )
        """
        # Initialize logging first
        self.log_dir = log_dir or Path('logs')
        self.log_level = log_level
        self.quiet = quiet
        
        # Create log directory if needed
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
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
            self.load_config(config_path)
        
        # Thread-local storage for connections
        self._thread_local = threading.local()
        
        # Connection manager will be initialized lazily
        self._connection_manager = None
        
        # Operation registry for tracking available operations
        self._operations = {}
        
        # Progress tracker - initialized based on quiet mode
        self._progress_tracker = None
        
        # Track if we're in a context manager
        self._in_context = False
        
    def load_config(self, config_path: Union[str, Path]) -> None:
        """
        Load configuration from a JSON file.
        
        This method loads and validates a configuration file, making it available
        through the `config` property. The configuration should contain Snowflake
        connection details and file processing definitions.
        
        Args:
            config_path: Path to configuration file. Can be string or Path object.
        
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
            jsonschema.ValidationError: If config doesn't match expected schema
            ValueError: If required configuration sections are missing
        
        Example:
            >>> context = ApplicationContext()
            >>> context.load_config("config/production.json")
            >>> print(context.config["snowflake"]["warehouse"])
            'PROD_WH'
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        self._config_path = config_path
        self._config = self.config_manager.load_config(str(config_path))
        self.logger.info(f"Loaded configuration from {config_path}")
        
        # Validate required sections
        if "snowflake" not in self._config:
            raise ValueError("Configuration missing required 'snowflake' section")
        if "files" not in self._config:
            self.logger.warning("Configuration missing 'files' section - file operations may fail")
    
    @property
    def config(self) -> Dict[str, Any]:
        """
        Get current configuration dictionary.
        
        Returns:
            Dict containing the full configuration loaded from JSON file.
        
        Raises:
            RuntimeError: If no configuration has been loaded yet
        
        Example:
            >>> config = context.config
            >>> warehouse = config["snowflake"]["warehouse"]
            >>> file_patterns = config["files"]
        """
        if self._config is None:
            raise RuntimeError("No configuration loaded. Call load_config() first.")
        return self._config
    
    @property
    def snowflake_config(self) -> Dict[str, Any]:
        """
        Get Snowflake-specific configuration section.
        
        Returns:
            Dict containing Snowflake connection parameters including
            account, user, password, warehouse, database, schema, and role.
        
        Raises:
            RuntimeError: If no configuration has been loaded
            KeyError: If 'snowflake' section is missing from config
        
        Example:
            >>> sf_config = context.snowflake_config
            >>> print(f"Connecting to {sf_config['account']}")
        """
        return self.config_manager.get_snowflake_config(self.config)
    
    def get_connection(self) -> Any:
        """
        Get or create a Snowflake connection for the current thread.
        
        This method returns a thread-local Snowflake connection, creating one
        if it doesn't exist. Connections are not shared between threads to
        ensure thread safety.
        
        Returns:
            snowflake.connector.connection.SnowflakeConnection: Active connection
        
        Raises:
            RuntimeError: If no configuration has been loaded
            snowflake.connector.errors.DatabaseError: If connection fails
            snowflake.connector.errors.ProgrammingError: If credentials are invalid
        
        Example:
            >>> conn = context.get_connection()
            >>> cursor = conn.cursor()
            >>> cursor.execute("SELECT CURRENT_WAREHOUSE()")
            >>> warehouse = cursor.fetchone()[0]
        
        Note:
            Connections are cached per thread. Calling this method multiple times
            in the same thread returns the same connection instance.
        """
        # Check if this thread already has a connection
        if not hasattr(self._thread_local, 'connection'):
            self.logger.debug(f"Creating new connection for thread {threading.current_thread().name}")
            
            # Lazy initialization of connection manager
            if self._connection_manager is None:
                from snowflake_etl.utils.snowflake_connection_v3 import SnowflakeConnectionManager
                self._connection_manager = SnowflakeConnectionManager(self.snowflake_config)
            
            # Create thread-local connection
            self._thread_local.connection = self._connection_manager.get_connection()
            
        return self._thread_local.connection
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        Get a logger instance for a specific module or component.
        
        Args:
            name: Logger name, typically module name. If None, returns root logger.
                Use dot notation for hierarchical loggers (e.g., 'etl.loader').
        
        Returns:
            logging.Logger: Configured logger instance
        
        Example:
            >>> # Get logger for specific module
            >>> logger = context.get_logger("snowflake_etl.operations.load")
            >>> logger.info("Starting load operation")
            >>> 
            >>> # Get root logger
            >>> root_logger = context.get_logger()
            >>> root_logger.warning("This is a warning")
        """
        if name:
            return logging.getLogger(name)
        return self.logger
    
    def get_progress_tracker(self) -> 'ProgressTracker':
        """
        Get progress tracker appropriate for current mode.
        
        Returns a progress tracker that either shows progress bars (normal mode)
        or operates silently (quiet mode). The tracker type is determined by
        the `quiet` attribute set during initialization.
        
        Returns:
            ProgressTracker: Either TqdmProgressTracker or NoOpProgressTracker
        
        Example:
            >>> tracker = context.get_progress_tracker()
            >>> tracker.set_total(100)
            >>> for i in range(100):
            ...     tracker.update(1)
            ...     # Process item
            >>> tracker.close()
        """
        if self._progress_tracker is None:
            if self.quiet:
                from snowflake_etl.core.progress import NoOpProgressTracker
                self._progress_tracker = NoOpProgressTracker()
            else:
                from snowflake_etl.core.progress import TqdmProgressTracker
                self._progress_tracker = TqdmProgressTracker()
        
        return self._progress_tracker
    
    def register_operation(self, name: str, operation_class: type) -> None:
        """
        Register an operation class for dependency injection.
        
        Args:
            name: Unique name for the operation (e.g., 'load', 'validate')
            operation_class: Class that implements the operation
        
        Raises:
            ValueError: If an operation with the same name is already registered
        
        Example:
            >>> from snowflake_etl.operations.load_operation import LoadOperation
            >>> context.register_operation('load', LoadOperation)
        """
        if name in self._operations:
            raise ValueError(f"Operation '{name}' is already registered")
        self._operations[name] = operation_class
        self.logger.debug(f"Registered operation: {name}")
    
    def get_operation(self, name: str) -> Any:
        """
        Get an instance of a registered operation.
        
        Args:
            name: Name of the operation to instantiate
        
        Returns:
            Operation instance initialized with this context
        
        Raises:
            KeyError: If the operation name is not registered
        
        Example:
            >>> load_op = context.get_operation('load')
            >>> result = load_op.execute(base_path="/data", month="2024-01")
        """
        if name not in self._operations:
            raise KeyError(f"Operation '{name}' not registered. Available: {list(self._operations.keys())}")
        
        operation_class = self._operations[name]
        return operation_class(self)
    
    def cleanup(self) -> None:
        """
        Clean up resources and close connections.
        
        This method should be called when the context is no longer needed to
        ensure all resources are properly released. It closes database connections,
        flushes logs, and cleans up progress trackers.
        
        Example:
            >>> context = ApplicationContext(config_path="config.json")
            >>> try:
            ...     # Use context
            ...     conn = context.get_connection()
            ... finally:
            ...     context.cleanup()
        
        Note:
            If using the context manager pattern, this is called automatically.
        """
        self.logger.info("Cleaning up ApplicationContext resources")
        
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
                self.logger.debug("Closed all connections in manager")
            except Exception as e:
                self.logger.warning(f"Error closing connection manager: {e}")
        
        # Close progress tracker
        if self._progress_tracker:
            try:
                self._progress_tracker.close()
            except Exception:
                pass
        
        self.logger.info("ApplicationContext cleanup complete")
    
    def __enter__(self) -> 'ApplicationContext':
        """
        Enter context manager.
        
        Returns:
            Self for use in with statement
        
        Example:
            >>> with ApplicationContext(config_path="config.json") as context:
            ...     # Use context
            ...     pass
        """
        self._in_context = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit context manager and clean up resources.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.cleanup()
        self._in_context = False
    
    def __repr__(self) -> str:
        """
        String representation of ApplicationContext.
        
        Returns:
            String showing context configuration
        """
        return (
            f"ApplicationContext("
            f"config_path={self._config_path}, "
            f"log_dir={self.log_dir}, "
            f"quiet={self.quiet})"
        )