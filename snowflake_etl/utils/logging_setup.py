"""
Logging Setup Manager
Unified logging configuration with rotation, formatting, and performance metrics
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union


class LogManager:
    """
    Unified logging configuration manager
    
    Features:
    - Standardized logger creation
    - Log rotation support
    - Structured logging (JSON format)
    - Performance metrics logging
    - Multiple output handlers
    """
    
    _instance = None
    _loggers: Dict[str, logging.Logger] = {}
    _initialized = False
    
    # Default log format
    DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    JSON_FORMAT = {
        'timestamp': '%(asctime)s',
        'name': '%(name)s',
        'level': '%(levelname)s',
        'message': '%(message)s',
        'function': '%(funcName)s',
        'line': '%(lineno)d'
    }
    
    def __new__(cls):
        """Ensure singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize LogManager"""
        if not self._initialized:
            self.log_dir = Path('logs')
            self.log_dir.mkdir(exist_ok=True)
            self._handlers = {}
            self._initialized = True
    
    def setup_logger(
        self,
        name: str,
        level: Union[str, int] = 'INFO',
        log_file: Optional[Union[str, Path]] = None,
        console: bool = True,
        quiet: bool = False,
        json_format: bool = False,
        rotate: bool = True,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5
    ) -> logging.Logger:
        """
        Create or get a standardized logger
        
        Args:
            name: Logger name
            level: Logging level (string or int)
            log_file: Optional log file path
            console: Whether to add console handler
            quiet: Suppress console output
            json_format: Use JSON structured logging
            rotate: Enable log rotation
            max_bytes: Max size before rotation
            backup_count: Number of backup files to keep
            
        Returns:
            Configured logger
        """
        # Return existing logger if already configured
        if name in self._loggers:
            return self._loggers[name]
        
        # Create new logger
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()) if isinstance(level, str) else level)
        logger.handlers.clear()  # Clear any existing handlers
        
        # Create formatter
        if json_format:
            formatter = JsonFormatter()
        else:
            formatter = logging.Formatter(self.DEFAULT_FORMAT)
        
        # Add file handler
        if log_file:
            file_handler = self._create_file_handler(
                log_file,
                formatter,
                rotate,
                max_bytes,
                backup_count
            )
            logger.addHandler(file_handler)
        
        # Add console handler
        if console and not quiet:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(formatter)
            console_handler.setLevel(logging.INFO)
            logger.addHandler(console_handler)
        
        # Add default debug file handler
        debug_file = self.log_dir / f'{name}_debug.log'
        debug_handler = self._create_file_handler(
            debug_file,
            formatter,
            rotate,
            max_bytes,
            backup_count
        )
        debug_handler.setLevel(logging.DEBUG)
        logger.addHandler(debug_handler)
        
        # Cache logger
        self._loggers[name] = logger
        return logger
    
    def _create_file_handler(
        self,
        log_file: Union[str, Path],
        formatter: logging.Formatter,
        rotate: bool,
        max_bytes: int,
        backup_count: int
    ) -> logging.Handler:
        """
        Create file handler with optional rotation
        
        Args:
            log_file: Path to log file
            formatter: Log formatter
            rotate: Enable rotation
            max_bytes: Max size before rotation
            backup_count: Number of backups
            
        Returns:
            File handler
        """
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        if rotate:
            handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
        else:
            handler = logging.FileHandler(log_file)
        
        handler.setFormatter(formatter)
        return handler
    
    def get_logger(self, name: str) -> Optional[logging.Logger]:
        """
        Get existing logger by name
        
        Args:
            name: Logger name
            
        Returns:
            Logger if exists, None otherwise
        """
        return self._loggers.get(name)
    
    def add_performance_handler(
        self,
        logger_name: str,
        metrics_file: Optional[Union[str, Path]] = None
    ):
        """
        Add performance metrics handler to logger
        
        Args:
            logger_name: Name of logger to modify
            metrics_file: Path to metrics file
        """
        logger = self._loggers.get(logger_name)
        if not logger:
            raise ValueError(f"Logger '{logger_name}' not found")
        
        metrics_file = metrics_file or self.log_dir / f'{logger_name}_metrics.json'
        
        # Create metrics handler
        metrics_handler = logging.FileHandler(metrics_file)
        metrics_handler.setFormatter(MetricsFormatter())
        metrics_handler.setLevel(logging.INFO)
        metrics_handler.addFilter(MetricsFilter())
        
        logger.addHandler(metrics_handler)
    
    def log_performance(
        self,
        logger_name: str,
        operation: str,
        duration: float,
        **kwargs
    ):
        """
        Log performance metrics
        
        Args:
            logger_name: Logger to use
            operation: Operation name
            duration: Duration in seconds
            **kwargs: Additional metrics
        """
        logger = self.get_logger(logger_name)
        if not logger:
            logger = self.setup_logger(logger_name)
        
        metrics = {
            'operation': operation,
            'duration': duration,
            'timestamp': datetime.now().isoformat(),
            **kwargs
        }
        
        logger.info(f"METRICS: {json.dumps(metrics)}", extra={'metrics': True})
    
    def set_global_level(self, level: Union[str, int]):
        """
        Set logging level for all loggers
        
        Args:
            level: New logging level
        """
        level = getattr(logging, level.upper()) if isinstance(level, str) else level
        
        for logger in self._loggers.values():
            logger.setLevel(level)
    
    def cleanup(self):
        """Clean up all handlers and loggers"""
        for logger in self._loggers.values():
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
        self._loggers.clear()
    
    @property
    def active_loggers(self) -> List[str]:
        """Get list of active logger names"""
        return list(self._loggers.keys())


class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'name': record.name,
            'level': record.levelname,
            'message': record.getMessage(),
            'function': record.funcName,
            'line': record.lineno,
            'module': record.module,
            'process': record.process,
            'thread': record.thread
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 
                          'funcName', 'levelname', 'levelno', 'lineno', 
                          'module', 'msecs', 'message', 'pathname', 'process',
                          'processName', 'relativeCreated', 'thread', 'threadName',
                          'exc_info', 'exc_text', 'stack_info']:
                log_data[key] = value
        
        return json.dumps(log_data)


class MetricsFormatter(logging.Formatter):
    """Formatter for performance metrics"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format metrics record as JSON"""
        if hasattr(record, 'metrics') and record.metrics:
            # Extract metrics from message
            if record.msg.startswith('METRICS: '):
                return record.msg[9:]  # Remove "METRICS: " prefix
        return super().format(record)


class MetricsFilter(logging.Filter):
    """Filter to only allow metrics records"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Only allow records with metrics flag"""
        return hasattr(record, 'metrics') and record.metrics


# Convenience function for quick logger setup
def get_logger(
    name: str,
    level: str = 'INFO',
    quiet: bool = False
) -> logging.Logger:
    """
    Quick logger setup function
    
    Args:
        name: Logger name
        level: Logging level
        quiet: Suppress console output
        
    Returns:
        Configured logger
    """
    manager = LogManager()
    return manager.setup_logger(name, level=level, quiet=quiet)