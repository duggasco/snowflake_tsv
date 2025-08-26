"""
Application Context for Dependency Injection
Manages shared resources across the application lifecycle
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, Union

from snowflake_etl.utils.config_manager_v2 import ConfigManager
from snowflake_etl.utils.logging_config import setup_logging


class ApplicationContext:
    """
    Central context for managing application-wide resources.
    This replaces singletons with explicit dependency injection.
    
    The context is created once at application startup and passed
    to all operations that need shared resources.
    """
    
    def __init__(
        self,
        config_path: Optional[Union[str, Path]] = None,
        log_dir: Optional[Path] = None,
        log_level: str = 'INFO',
        quiet: bool = False
    ):
        """
        Initialize application context with shared resources
        
        Args:
            config_path: Path to configuration file
            log_dir: Directory for log files
            log_level: Logging level
            quiet: Suppress console output
        """
        # Initialize logging first
        self.log_dir = log_dir or Path('logs')
        self.quiet = quiet
        setup_logging(
            operation='snowflake_etl',
            log_dir=self.log_dir,
            level=log_level,
            quiet=quiet
        )
        self.logger = logging.getLogger('snowflake_etl')
        self.logger.info("Initializing application context")
        
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        self._config = None
        self._config_path = None
        
        # Load initial config if provided
        if config_path:
            self.load_config(config_path)
        
        # Connection manager will be initialized lazily
        self._connection_manager = None
        
        # Operation registry for tracking what's available
        self._operations = {}
        
        # Progress tracker - initialized based on quiet mode
        self._progress_tracker = None
        
    def load_config(self, config_path: Union[str, Path]):
        """
        Load configuration file
        
        Args:
            config_path: Path to configuration file
        """
        self._config_path = Path(config_path)
        self._config = self.config_manager.load_config(config_path)
        self.logger.info(f"Loaded configuration from {config_path}")
        
    @property
    def config(self) -> Dict[str, Any]:
        """Get current configuration"""
        if self._config is None:
            raise RuntimeError("No configuration loaded. Call load_config() first.")
        return self._config
    
    @property
    def snowflake_config(self) -> Dict[str, Any]:
        """Get Snowflake configuration"""
        return self.config_manager.get_snowflake_config(self.config)
    
    @property
    def connection_manager(self):
        """
        Get or create connection manager (lazy initialization)
        
        Returns:
            SnowflakeConnectionManager instance
        """
        if self._connection_manager is None:
            # Import here to avoid circular dependencies
            from snowflake_etl.utils.snowflake_connection_v3 import SnowflakeConnectionManager, ConnectionConfig
            
            # Create connection config from snowflake config
            conn_config = ConnectionConfig(**self.snowflake_config)
            # Use larger pool size to handle parallel operations (default was 5)
            pool_size = self.config_data.get('connection_pool_size', 10)
            self._connection_manager = SnowflakeConnectionManager(config=conn_config, pool_size=pool_size)
            self.logger.info(f"Initialized Snowflake connection pool (size: {pool_size})")
            
        return self._connection_manager
    
    def get_operation(self, operation_name: str):
        """
        Get an operation handler
        
        Args:
            operation_name: Name of the operation
            
        Returns:
            Operation handler instance
        """
        if operation_name not in self._operations:
            self._load_operation(operation_name)
        return self._operations[operation_name]
    
    def _load_operation(self, operation_name: str):
        """
        Dynamically load an operation handler
        
        Args:
            operation_name: Name of the operation to load
        """
        self.logger.info(f"Loading operation: {operation_name}")
        
        # Map operation names to modules
        operation_modules = {
            'load': 'snowflake_etl.operations.loader',
            'delete': 'snowflake_etl.operations.deleter',
            'validate': 'snowflake_etl.operations.validator',
            'report': 'snowflake_etl.operations.reporter',
            'check_duplicates': 'snowflake_etl.operations.duplicate_checker',
            'compare_files': 'snowflake_etl.operations.file_comparator'
        }
        
        if operation_name not in operation_modules:
            raise ValueError(f"Unknown operation: {operation_name}")
        
        # Dynamic import
        module_name = operation_modules[operation_name]
        try:
            module = __import__(module_name, fromlist=['Operation'])
            operation_class = getattr(module, 'Operation')
            
            # Create instance with context injection
            self._operations[operation_name] = operation_class(self)
            
        except (ImportError, AttributeError) as e:
            self.logger.error(f"Failed to load operation {operation_name}: {e}")
            raise
    
    def register_operation(self, name: str, operation_instance):
        """
        Register an operation handler
        
        Args:
            name: Operation name
            operation_instance: Operation handler instance
        """
        self._operations[name] = operation_instance
        self.logger.debug(f"Registered operation: {name}")
    
    @property
    def progress_tracker(self):
        """
        Get or create progress tracker (lazy initialization)
        
        Returns:
            ProgressTracker instance
        """
        if self._progress_tracker is None:
            if self.quiet:
                # Use no-op tracker for quiet mode
                from snowflake_etl.core.progress import NoOpProgressTracker
                self._progress_tracker = NoOpProgressTracker()
            else:
                # Try to use tqdm, fall back to logging
                try:
                    from snowflake_etl.ui.progress_bars import TqdmProgressTracker
                    self._progress_tracker = TqdmProgressTracker()
                except ImportError:
                    from snowflake_etl.core.progress import LoggingProgressTracker
                    self._progress_tracker = LoggingProgressTracker()
            
            self.logger.info(f"Using progress tracker: {self._progress_tracker.__class__.__name__}")
        
        return self._progress_tracker
    
    def set_progress_tracker(self, tracker):
        """
        Set a custom progress tracker
        
        Args:
            tracker: ProgressTracker instance
        """
        self._progress_tracker = tracker
        self.logger.info(f"Progress tracker set to: {tracker.__class__.__name__}")
    
    def cleanup(self):
        """Clean up resources on shutdown"""
        self.logger.info("Cleaning up application context")
        
        # Close connection pool if initialized
        if self._connection_manager is not None:
            self._connection_manager.close_pool()
            self.logger.info("Closed connection pool")
        
        # Clear caches
        self.config_manager.clear_cache()
        
        # Clear operations
        self._operations.clear()
        
        # Close progress tracker if initialized
        if self._progress_tracker is not None:
            self._progress_tracker.close()
            self.logger.info("Closed progress tracker")
        
        self.logger.info("Application context cleanup complete")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        self.cleanup()


class BaseOperation:
    """
    Base class for all operations
    Operations receive the application context for accessing shared resources
    """
    
    def __init__(self, context: ApplicationContext):
        """
        Initialize operation with application context
        
        Args:
            context: Application context with shared resources
        """
        self.context = context
        self.logger = logging.getLogger(f'snowflake_etl.{self.__class__.__name__}')
        self.config = context.config
        # Don't initialize connection_manager here - let operations request it when needed
        self._connection_manager = None
        self._progress_tracker = None
    
    @property
    def connection_manager(self):
        """Get connection manager lazily"""
        if self._connection_manager is None:
            self._connection_manager = self.context.connection_manager
        return self._connection_manager
    
    @property
    def progress_tracker(self):
        """Get progress tracker lazily"""
        if self._progress_tracker is None:
            self._progress_tracker = self.context.progress_tracker
        return self._progress_tracker
        
    def execute(self, **kwargs):
        """
        Execute the operation
        
        Args:
            **kwargs: Operation-specific parameters
            
        Returns:
            Operation result
        """
        raise NotImplementedError("Subclasses must implement execute()")