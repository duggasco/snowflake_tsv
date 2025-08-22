"""
Configuration dataclass for SnowflakeLoader.
Separated to avoid import dependencies.
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class LoaderConfig:
    """
    Configuration for SnowflakeLoader.
    Provides sensible defaults while allowing full customization.
    """
    
    # Compression settings
    chunk_size_mb: int = 10
    compression_level: int = 1
    
    # Upload settings
    parallel_uploads: int = 4
    
    # Async settings
    async_threshold_mb: int = 100
    keepalive_interval_sec: int = 240
    poll_interval_sec: int = 30
    max_wait_time_sec: int = 7200
    
    # File format settings
    file_format_options: Dict[str, Any] = field(default_factory=lambda: {
        'TYPE': 'CSV',
        'FIELD_DELIMITER': '\\t',
        'SKIP_HEADER': 0,
        'FIELD_OPTIONALLY_ENCLOSED_BY': '"',
        'ESCAPE_UNENCLOSED_FIELD': 'NONE',
        'ERROR_ON_COLUMN_COUNT_MISMATCH': False,
        'REPLACE_INVALID_CHARACTERS': True,
        'DATE_FORMAT': 'YYYY-MM-DD',
        'TIMESTAMP_FORMAT': 'YYYY-MM-DD HH24:MI:SS',
        'NULL_IF': ['', 'NULL', 'null', '\\\\N']
    })
    
    # Copy settings
    on_error: str = 'ABORT_STATEMENT'
    purge: bool = True
    size_limit: int = 5368709120
    
    # Stage settings
    stage_prefix: str = 'TSV_LOADER'
    cleanup_old_stages: bool = True