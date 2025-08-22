"""
Configuration Manager V2
Simplified with functools.lru_cache for efficient caching
"""

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import jsonschema
    from jsonschema import ValidationError
    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False
    ValidationError = Exception


class ConfigError(Exception):
    """Base exception for configuration errors"""
    pass


class ConfigValidationError(ConfigError):
    """Configuration validation error"""
    pass


class ConfigManager:
    """
    Configuration manager with caching and validation
    
    Features:
    - Efficient caching with lru_cache
    - Schema validation (optional)
    - Environment variable overrides
    - Multi-config file support
    - Config merging
    """
    
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
    
    def __init__(
        self,
        base_path: Optional[Path] = None,
        env_prefix: str = "SNOWFLAKE_ETL_",
        validate_on_load: bool = True
    ):
        """
        Initialize ConfigManager
        
        Args:
            base_path: Base directory for configuration files
            env_prefix: Prefix for environment variable overrides
            validate_on_load: Whether to validate configs on load
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.config_dir = self.base_path / "config"
        self.env_prefix = env_prefix
        self.validate_on_load = validate_on_load
    
    @lru_cache(maxsize=32)
    def _load_json_file(self, file_path: str) -> str:
        """
        Load and cache raw JSON file content
        Cache is based on file path and modification time
        
        Args:
            file_path: Path to JSON file (as string for hashability)
            
        Returns:
            JSON string content
        """
        path = Path(file_path)
        if not path.exists():
            raise ConfigError(f"Configuration file not found: {path}")
        
        try:
            with open(path, 'r') as f:
                return f.read()
        except IOError as e:
            raise ConfigError(f"Failed to read configuration file: {e}")
    
    def _get_file_mtime(self, file_path: Union[str, Path]) -> float:
        """Get file modification time for cache invalidation"""
        return Path(file_path).stat().st_mtime
    
    def load_config(
        self,
        config_path: Union[str, Path],
        apply_env_overrides: bool = True,
        validate: bool = None
    ) -> Dict[str, Any]:
        """
        Load configuration from file
        
        Args:
            config_path: Path to configuration file
            apply_env_overrides: Whether to apply environment variable overrides
            validate: Whether to validate (uses class default if None)
            
        Returns:
            Configuration dictionary
        """
        config_path = Path(config_path).resolve()
        
        # Use modification time in cache key for automatic invalidation
        cache_key = f"{config_path}:{self._get_file_mtime(config_path)}"
        
        try:
            # Load raw JSON (cached based on file + mtime)
            json_content = self._load_json_file(str(config_path))
            config = json.loads(json_content)
        except json.JSONDecodeError as e:
            raise ConfigError(f"Invalid JSON in configuration file: {e}")
        
        # Apply environment overrides if requested
        if apply_env_overrides:
            config = self._apply_env_overrides(config)
        
        # Validate if requested
        validate = self.validate_on_load if validate is None else validate
        if validate:
            self.validate_config(config)
        
        return config
    
    def load_all_configs(
        self,
        config_dir: Optional[Path] = None,
        pattern: str = "*.json"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Load all configuration files from a directory
        
        Args:
            config_dir: Directory to load from (uses default if None)
            pattern: File pattern to match
            
        Returns:
            Dictionary mapping config names to configurations
        """
        config_dir = config_dir or self.config_dir
        configs = {}
        
        for config_file in config_dir.glob(pattern):
            config_name = config_file.stem
            try:
                configs[config_name] = self.load_config(config_file)
            except ConfigError as e:
                # Log error but continue loading other configs
                print(f"Warning: Failed to load {config_file}: {e}")
        
        return configs
    
    def merge_configs(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple configurations
        
        Args:
            *configs: Configuration dictionaries to merge
            
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
                if pattern and pattern not in seen_patterns:
                    seen_patterns.add(pattern)
                    unique_files.append(file_config)
            merged['files'] = unique_files
        
        return merged
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides
        
        Args:
            config: Original configuration
            
        Returns:
            Configuration with overrides applied
        """
        # Snowflake connection overrides
        snowflake_mappings = {
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
        
        for env_suffix, config_key in snowflake_mappings.items():
            env_var = f"{self.env_prefix}{env_suffix}"
            if env_var in os.environ:
                config['snowflake'][config_key] = os.environ[env_var]
        
        return config
    
    def validate_config(
        self,
        config: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None
    ):
        """
        Validate configuration against schema
        
        Args:
            config: Configuration to validate
            schema: JSON schema (uses default if not provided)
            
        Raises:
            ConfigValidationError: If configuration is invalid
        """
        if not HAS_JSONSCHEMA:
            # Skip validation if jsonschema not available
            return
        
        schema = schema or self.DEFAULT_SCHEMA
        
        try:
            jsonschema.validate(config, schema)
        except ValidationError as e:
            raise ConfigValidationError(f"Configuration validation failed: {e.message}")
    
    def get_snowflake_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract Snowflake connection parameters
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Snowflake configuration
        """
        return config.get('snowflake', {})
    
    def get_file_configs(
        self,
        config: Dict[str, Any],
        table_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get file processing configurations
        
        Args:
            config: Full configuration dictionary
            table_name: Filter by specific table name
            
        Returns:
            List of file configurations
        """
        files = config.get('files', [])
        
        if table_name:
            files = [f for f in files if f.get('table_name') == table_name]
        
        return files
    
    def get_all_table_names(self, configs: Union[Dict, List[Dict]]) -> List[str]:
        """
        Get all unique table names from configurations
        
        Args:
            configs: Single config dict or list of configs
            
        Returns:
            Sorted list of unique table names
        """
        if isinstance(configs, dict):
            configs = [configs]
        
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
    
    def clear_cache(self):
        """Clear the LRU cache"""
        self._load_json_file.cache_clear()
    
    @property
    def cache_info(self):
        """Get cache statistics"""
        return self._load_json_file.cache_info()


# Convenience functions for common operations
def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """Quick config load without manager instance"""
    manager = ConfigManager()
    return manager.load_config(config_path)


def get_snowflake_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """Quick Snowflake config extraction"""
    config = load_config(config_path)
    return config.get('snowflake', {})