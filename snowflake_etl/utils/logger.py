#!/usr/bin/env python3
"""
Unified logging configuration for Snowflake ETL pipeline
Provides consistent logging across all components
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class ETLLogger:
    """
    Singleton logger manager for the ETL pipeline
    Ensures consistent logging configuration across all components
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the logger manager"""
        if not self._initialized:
            self._initialized = True
            self.log_dir = Path("logs")
            self.log_level = logging.INFO
            self.quiet_mode = False
            self.logger_name = "snowflake_etl"
            self._loggers = {}
    
    def setup(self, 
              log_dir: str = "logs",
              log_level: str = "INFO",
              quiet_mode: bool = False,
              log_file_prefix: str = "snowflake_etl") -> logging.Logger:
        """
        Set up the root logger configuration
        
        Args:
            log_dir: Directory for log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            quiet_mode: If True, suppress console output
            log_file_prefix: Prefix for log file names
            
        Returns:
            Configured root logger
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.log_level = getattr(logging, log_level.upper())
        self.quiet_mode = quiet_mode
        
        # Create root logger
        root_logger = logging.getLogger(self.logger_name)
        root_logger.setLevel(self.log_level)
        
        # Remove existing handlers to avoid duplicates
        root_logger.handlers = []
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )
        
        # File handler - always enabled with detailed formatting
        log_file = self.log_dir / f"{log_file_prefix}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Capture everything in file
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler - only if not in quiet mode
        if not quiet_mode:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(simple_formatter)
            root_logger.addHandler(console_handler)
        
        # Also create a debug log for detailed troubleshooting
        debug_file = self.log_dir / f"{log_file_prefix}_debug.log"
        debug_handler = logging.FileHandler(debug_file)
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(debug_handler)
        
        return root_logger
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance for a specific module
        
        Args:
            name: Name of the module/component
            
        Returns:
            Logger instance
        """
        if name not in self._loggers:
            # Create child logger
            logger = logging.getLogger(f"{self.logger_name}.{name}")
            self._loggers[name] = logger
        
        return self._loggers[name]
    
    def set_level(self, level: str):
        """
        Change the logging level for all loggers
        
        Args:
            level: New logging level
        """
        self.log_level = getattr(logging, level.upper())
        root_logger = logging.getLogger(self.logger_name)
        root_logger.setLevel(self.log_level)
        
        # Update console handlers
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                handler.setLevel(self.log_level)
    
    def add_operation_context(self, operation: str, context: dict):
        """
        Add context information to log messages for a specific operation
        
        Args:
            operation: Operation name
            context: Context dictionary
        """
        # Create a custom adapter that adds context
        logger = self.get_logger(operation)
        return ContextLogger(logger, context)


class ContextLogger(logging.LoggerAdapter):
    """
    Logger adapter that adds context to log messages
    """
    
    def process(self, msg, kwargs):
        """Add context to log message"""
        if self.extra:
            context_str = " ".join(f"[{k}={v}]" for k, v in self.extra.items())
            return f"{context_str} {msg}", kwargs
        return msg, kwargs


# Singleton instance
etl_logger = ETLLogger()


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return etl_logger.get_logger(name)


def setup_logging(log_dir: str = "logs",
                 log_level: str = "INFO",
                 quiet_mode: bool = False) -> logging.Logger:
    """
    Convenience function to set up logging
    
    Args:
        log_dir: Directory for log files
        log_level: Logging level
        quiet_mode: Suppress console output
        
    Returns:
        Configured root logger
    """
    return etl_logger.setup(log_dir, log_level, quiet_mode)