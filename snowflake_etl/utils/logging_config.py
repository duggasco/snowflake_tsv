"""
Logging Configuration using dictConfig
Declarative, centralized logging setup
"""

import json
import logging
import logging.config
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


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


class PerformanceFormatter(logging.Formatter):
    """Formatter for performance metrics"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format performance record as JSON"""
        if hasattr(record, 'performance_data'):
            return json.dumps(record.performance_data)
        return super().format(record)


def get_logging_config(
    log_dir: Path,
    level: str = 'INFO',
    json_format: bool = False,
    quiet: bool = False,
    operation: str = 'etl'
) -> Dict[str, Any]:
    """
    Generate logging configuration dictionary
    
    Args:
        log_dir: Directory for log files
        level: Default logging level
        json_format: Use JSON formatting for logs
        quiet: Suppress console output
        operation: Operation name for log files (e.g., 'tsv_loader', 'drop_month')
        
    Returns:
        Logging configuration dictionary for dictConfig
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True)
    
    # Choose formatter based on json_format flag
    default_formatter = 'json' if json_format else 'default'
    
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        
        'formatters': {
            'default': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'json': {
                '()': 'snowflake_etl.utils.logging_config.JsonFormatter'
            },
            'performance': {
                '()': 'snowflake_etl.utils.logging_config.PerformanceFormatter'
            }
        },
        
        'filters': {
            'performance_only': {
                '()': 'logging.Filter',
                'name': 'performance'
            }
        },
        
        'handlers': {
            # Console handler (conditional)
            'console': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stderr',
                'formatter': default_formatter,
                'level': 'INFO'
            },
            
            # Operation-specific log file
            f'{operation}_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(log_dir / f'{operation}.log'),
                'maxBytes': 10 * 1024 * 1024,  # 10MB
                'backupCount': 5,
                'formatter': 'detailed',
                'level': 'INFO'
            },
            
            # Debug log (always created)
            'debug_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(log_dir / f'{operation}_debug.log'),
                'maxBytes': 10 * 1024 * 1024,  # 10MB
                'backupCount': 3,
                'formatter': 'detailed',
                'level': 'DEBUG'
            },
            
            # Performance metrics log
            'performance_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(log_dir / 'performance.log'),
                'maxBytes': 10 * 1024 * 1024,  # 10MB
                'backupCount': 3,
                'formatter': 'performance',
                'level': 'INFO',
                'filters': ['performance_only']
            },
            
            # Error log
            'error_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': str(log_dir / 'errors.log'),
                'maxBytes': 10 * 1024 * 1024,  # 10MB
                'backupCount': 5,
                'formatter': 'detailed',
                'level': 'ERROR'
            }
        },
        
        'loggers': {
            # Main application loggers
            'snowflake_etl': {
                'handlers': ['debug_file', f'{operation}_file', 'error_file'],
                'level': level,
                'propagate': False
            },
            
            # Performance logger
            'performance': {
                'handlers': ['performance_file'],
                'level': 'INFO',
                'propagate': False
            },
            
            # Quiet noisy libraries
            'snowflake.connector': {
                'level': 'WARNING'
            },
            
            'urllib3': {
                'level': 'WARNING'
            }
        },
        
        'root': {
            'handlers': ['debug_file', 'error_file'],
            'level': level
        }
    }
    
    # Add console handler to root if not quiet
    if not quiet:
        config['root']['handlers'].insert(0, 'console')
        config['loggers']['snowflake_etl']['handlers'].insert(0, 'console')
    
    # Add specific operation loggers if they match known operations
    # These handlers are already defined above, just create the loggers
    for op_name in ['tsv_loader', 'drop_month']:
        if op_name in config['handlers'] or f'{op_name}_file' in config['handlers']:
            config['loggers'][op_name] = {
                'handlers': ['debug_file', 'error_file'],
                'level': level,
                'propagate': False
            }
    
    return config


def setup_logging(
    operation: str = 'etl',
    log_dir: Optional[Path] = None,
    level: str = 'INFO',
    json_format: bool = False,
    quiet: bool = False
):
    """
    Initialize logging for the application
    
    Args:
        operation: Operation name for log files
        log_dir: Directory for log files (defaults to ./logs)
        level: Logging level
        json_format: Use JSON formatting
        quiet: Suppress console output
    """
    if log_dir is None:
        log_dir = Path('logs')
    
    config = get_logging_config(
        log_dir=log_dir,
        level=level,
        json_format=json_format,
        quiet=quiet,
        operation=operation
    )
    
    logging.config.dictConfig(config)
    
    # Log initialization
    logger = logging.getLogger('snowflake_etl')
    logger.info(f"Logging initialized for operation: {operation}")


def log_performance(
    operation: str,
    duration: float,
    **metrics
):
    """
    Log performance metrics
    
    Args:
        operation: Operation name
        duration: Duration in seconds
        **metrics: Additional metrics to log
    """
    logger = logging.getLogger('performance')
    
    performance_data = {
        'timestamp': datetime.now().isoformat(),
        'operation': operation,
        'duration_seconds': duration,
        **metrics
    }
    
    # Use extra to pass structured data
    logger.info(
        f"Performance: {operation} completed in {duration:.2f}s",
        extra={'performance_data': performance_data}
    )


# Convenience function for quick setup
def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name (defaults to snowflake_etl)
        
    Returns:
        Logger instance
    """
    if name is None:
        name = 'snowflake_etl'
    return logging.getLogger(name)