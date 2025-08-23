#!/usr/bin/env python3
"""
Configuration Manager for Snowflake ETL Pipeline
Handles loading and validation of configuration files
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


@dataclass
class ConfigManager:
    """
    Manages configuration loading and validation for the ETL pipeline.
    """
    config_dir: str = "config"
    _config_cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    _logger: logging.Logger = field(init=False, repr=False)
    
    def __post_init__(self):
        """Initialize logger after dataclass initialization"""
        self._logger = logging.getLogger(__name__)
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        config_path = Path(config_path)
        
        # Check cache first
        cache_key = str(config_path.absolute())
        if cache_key in self._config_cache:
            self._logger.debug(f"Using cached config for {config_path}")
            return self._config_cache[cache_key]
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        self._logger.info(f"Loading configuration from {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Validate basic structure
            self._validate_config(config)
            
            # Cache the config
            self._config_cache[cache_key] = config
            
            self._logger.debug(f"Config loaded successfully with {len(config.get('files', []))} file definitions")
            return config
            
        except json.JSONDecodeError as e:
            self._logger.error(f"Invalid JSON in config file {config_path}: {e}")
            raise
        except Exception as e:
            self._logger.error(f"Error loading config from {config_path}: {e}")
            raise
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration structure.
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Check for required top-level keys
        if 'snowflake' not in config:
            raise ValueError("Configuration missing 'snowflake' section")
        
        # Validate Snowflake configuration
        snowflake_config = config['snowflake']
        required_snowflake_keys = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        
        for key in required_snowflake_keys:
            if key not in snowflake_config:
                raise ValueError(f"Snowflake configuration missing required key: {key}")
        
        # Validate files configuration if present
        if 'files' in config:
            if not isinstance(config['files'], list):
                raise ValueError("'files' configuration must be a list")
            
            for idx, file_config in enumerate(config['files']):
                self._validate_file_config(file_config, idx)
    
    def _validate_file_config(self, file_config: Dict[str, Any], index: int) -> None:
        """
        Validate individual file configuration.
        
        Args:
            file_config: File configuration dictionary
            index: Index of file in configuration list
            
        Raises:
            ValueError: If file configuration is invalid
        """
        required_keys = ['file_pattern', 'table_name', 'expected_columns']
        
        for key in required_keys:
            if key not in file_config:
                raise ValueError(f"File configuration at index {index} missing required key: {key}")
        
        # Validate expected_columns is a list
        if not isinstance(file_config['expected_columns'], list):
            raise ValueError(f"File configuration at index {index}: 'expected_columns' must be a list")
        
        if len(file_config['expected_columns']) == 0:
            raise ValueError(f"File configuration at index {index}: 'expected_columns' cannot be empty")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the currently loaded configuration.
        
        Returns:
            Configuration dictionary or empty dict if no config loaded
        """
        if self._config_cache:
            # Return the first (and usually only) cached config
            return next(iter(self._config_cache.values()))
        return {}
    
    def get_snowflake_config(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get Snowflake-specific configuration.
        
        Args:
            config: Optional configuration dict. If not provided, uses cached config
            
        Returns:
            Snowflake configuration dictionary
        """
        if config is None:
            config = self.get_config()
        
        return config.get('snowflake', {})
    
    def get_file_configs(self, config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Get file configurations.
        
        Args:
            config: Optional configuration dict. If not provided, uses cached config
            
        Returns:
            List of file configuration dictionaries
        """
        if config is None:
            config = self.get_config()
        
        return config.get('files', [])
    
    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache.clear()
        self._logger.debug("Configuration cache cleared")
    
    def reload_config(self, config_path: str) -> Dict[str, Any]:
        """
        Force reload configuration from file, bypassing cache.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Dictionary containing configuration
        """
        # Clear this specific config from cache
        cache_key = str(Path(config_path).absolute())
        if cache_key in self._config_cache:
            del self._config_cache[cache_key]
        
        # Load fresh
        return self.load_config(config_path)