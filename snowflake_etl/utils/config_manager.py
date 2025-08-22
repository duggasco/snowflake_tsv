"""
Configuration Manager
Centralized configuration loading, validation, and caching
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import jsonschema
from jsonschema import ValidationError


class ConfigManager:
    """
    Centralized configuration management with caching and validation
    
    Features:
    - Configuration caching to reduce I/O
    - Schema validation
    - Environment variable overrides
    - Multi-config file support
    - Config inheritance
    """
    
    _cache: Dict[str, Dict[str, Any]] = {}
    _schema_cache: Dict[str, Dict[str, Any]] = {}
    
    # Default configuration schema
    DEFAULT_SCHEMA = {
        "type": "object",
        "required": ["snowflake", "files"],
        "properties": {
            "snowflake": {
                "type": "object",
                "required": ["account", "user", "password", "warehouse", "database", "schema"],
                "properties": {
                    "account": {"type": "string"},
                    "user": {"type": "string"},
                    "password": {"type": "string"},
                    "warehouse": {"type": "string"},
                    "database": {"type": "string"},
                    "schema": {"type": "string"},
                    "role": {"type": "string"}
                }
            },
            "files": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["file_pattern", "table_name"],
                    "properties": {
                        "file_pattern": {"type": "string"},
                        "table_name": {"type": "string"},
                        "date_column": {"type": "string"},
                        "duplicate_key_columns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "expected_columns": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            }
        }
    }
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Initialize ConfigManager
        
        Args:
            base_path: Base directory for configuration files
        """
        self.base_path = base_path or Path.cwd()
        self.config_dir = self.base_path / "config"
        
    def load_config(
        self,
        config_path: Union[str, Path],
        validate: bool = True,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Load and cache configuration from file
        
        Args:
            config_path: Path to configuration file
            validate: Whether to validate against schema
            use_cache: Whether to use cached version if available
            
        Returns:
            Configuration dictionary
        """
        config_path = Path(config_path).resolve()
        cache_key = str(config_path)
        
        # Check cache
        if use_cache and cache_key in self._cache:
            # Check if file has been modified since caching
            cached_time = self._cache[cache_key].get('_cached_at', 0)
            file_mtime = config_path.stat().st_mtime
            if file_mtime <= cached_time:
                return self._cache[cache_key]['config']
        
        # Load configuration
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        # Validate if requested
        if validate:
            self.validate_config(config)
        
        # Cache configuration
        self._cache[cache_key] = {
            'config': config,
            '_cached_at': datetime.now().timestamp()
        }
        
        return config
    
    def load_multiple_configs(
        self,
        config_paths: List[Union[str, Path]],
        merge: bool = True
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Load multiple configuration files
        
        Args:
            config_paths: List of configuration file paths
            merge: Whether to merge configs or return as list
            
        Returns:
            Merged configuration or list of configurations
        """
        configs = []
        for path in config_paths:
            configs.append(self.load_config(path))
        
        if merge:
            return self._merge_configs(configs)
        return configs
    
    def _merge_configs(self, configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge multiple configurations with proper precedence
        
        Args:
            configs: List of configurations to merge
            
        Returns:
            Merged configuration
        """
        merged = {}
        
        for config in configs:
            # Merge snowflake config (last one wins)
            if 'snowflake' in config:
                merged['snowflake'] = config['snowflake']
            
            # Merge files arrays
            if 'files' in config:
                if 'files' not in merged:
                    merged['files'] = []
                merged['files'].extend(config['files'])
        
        # Remove duplicate file patterns
        if 'files' in merged:
            seen_patterns = set()
            unique_files = []
            for file_config in merged['files']:
                pattern = file_config.get('file_pattern')
                if pattern not in seen_patterns:
                    seen_patterns.add(pattern)
                    unique_files.append(file_config)
            merged['files'] = unique_files
        
        return merged
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to configuration
        
        Environment variables should be prefixed with SNOWFLAKE_ETL_
        Example: SNOWFLAKE_ETL_WAREHOUSE=LARGE_WH
        
        Args:
            config: Original configuration
            
        Returns:
            Configuration with overrides applied
        """
        env_prefix = "SNOWFLAKE_ETL_"
        
        # Check for Snowflake overrides
        snowflake_overrides = {
            'ACCOUNT': 'account',
            'USER': 'user',
            'PASSWORD': 'password',
            'WAREHOUSE': 'warehouse',
            'DATABASE': 'database',
            'SCHEMA': 'schema',
            'ROLE': 'role'
        }
        
        if 'snowflake' not in config:
            config['snowflake'] = {}
        
        for env_suffix, config_key in snowflake_overrides.items():
            env_var = f"{env_prefix}{env_suffix}"
            if env_var in os.environ:
                config['snowflake'][config_key] = os.environ[env_var]
        
        return config
    
    def validate_config(
        self,
        config: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration to validate
            schema: JSON schema (uses default if not provided)
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If configuration is invalid
        """
        schema = schema or self.DEFAULT_SCHEMA
        
        try:
            jsonschema.validate(config, schema)
            return True
        except ValidationError as e:
            raise ValidationError(f"Configuration validation failed: {e.message}")
    
    def get_snowflake_config(
        self,
        config_path: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """
        Extract Snowflake connection parameters from configuration
        
        Args:
            config_path: Path to configuration file (uses cached if not provided)
            
        Returns:
            Snowflake configuration dictionary
        """
        if config_path:
            config = self.load_config(config_path)
        else:
            # Get first cached config
            if not self._cache:
                raise ValueError("No configuration loaded")
            config = next(iter(self._cache.values()))['config']
        
        return config.get('snowflake', {})
    
    def get_file_configs(
        self,
        config_path: Optional[Union[str, Path]] = None,
        table_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get file processing configurations
        
        Args:
            config_path: Path to configuration file
            table_name: Filter by specific table name
            
        Returns:
            List of file configurations
        """
        if config_path:
            config = self.load_config(config_path)
        else:
            # Get first cached config
            if not self._cache:
                raise ValueError("No configuration loaded")
            config = next(iter(self._cache.values()))['config']
        
        files = config.get('files', [])
        
        if table_name:
            files = [f for f in files if f.get('table_name') == table_name]
        
        return files
    
    def get_all_table_names(
        self,
        config_paths: Optional[List[Union[str, Path]]] = None
    ) -> List[str]:
        """
        Get all unique table names from configurations
        
        Args:
            config_paths: List of configuration files to check
            
        Returns:
            List of unique table names
        """
        if config_paths:
            configs = self.load_multiple_configs(config_paths, merge=False)
        else:
            configs = [v['config'] for v in self._cache.values()]
        
        table_names = set()
        for config in configs:
            for file_config in config.get('files', []):
                if 'table_name' in file_config:
                    table_names.add(file_config['table_name'])
        
        return sorted(list(table_names))
    
    def save_config(
        self,
        config: Dict[str, Any],
        output_path: Union[str, Path],
        validate: bool = True
    ):
        """
        Save configuration to file
        
        Args:
            config: Configuration to save
            output_path: Path to save configuration
            validate: Whether to validate before saving
        """
        if validate:
            self.validate_config(config)
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(config, f, indent=2)
    
    def clear_cache(self, config_path: Optional[Union[str, Path]] = None):
        """
        Clear configuration cache
        
        Args:
            config_path: Specific configuration to clear (clears all if not provided)
        """
        if config_path:
            cache_key = str(Path(config_path).resolve())
            self._cache.pop(cache_key, None)
        else:
            self._cache.clear()
    
    @property
    def cached_configs(self) -> List[str]:
        """Get list of cached configuration paths"""
        return list(self._cache.keys())
    
    @property
    def cache_size(self) -> int:
        """Get number of cached configurations"""
        return len(self._cache)