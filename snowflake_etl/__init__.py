"""
Snowflake ETL Pipeline Package
A comprehensive ETL solution for processing large TSV files into Snowflake

Version: 3.0.0-alpha
"""

__version__ = "3.0.0-alpha"
__author__ = "Snowflake ETL Team"

# Lazy imports to avoid import errors when connector not installed
__all__ = [
    'SnowflakeConnectionManager',
    'ConfigManager', 
    'LogManager',
    '__version__'
]

def __getattr__(name):
    """Lazy import for heavy dependencies"""
    if name == 'SnowflakeConnectionManager':
        from snowflake_etl.utils.snowflake_connection_v2 import SnowflakeConnectionManager
        return SnowflakeConnectionManager
    elif name == 'ConfigManager':
        from snowflake_etl.utils.config_manager_v2 import ConfigManager
        return ConfigManager
    elif name == 'LogManager':
        from snowflake_etl.utils.logging_config import get_logger
        return get_logger
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")