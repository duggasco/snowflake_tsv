"""
File configuration model for ETL operations
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple


@dataclass
class FileConfig:
    """
    Configuration for a single TSV file to be processed
    
    Attributes:
        file_path: Path to the TSV file
        table_name: Target Snowflake table name
        expected_columns: List of expected column names
        date_column: Name of the date column for validation
        expected_date_range: Tuple of (start_date, end_date) for validation
        duplicate_key_columns: Columns that form the composite key for duplicate detection
        file_size_bytes: Size of the file in bytes (populated during analysis)
        row_count: Number of rows in the file (populated during analysis)
    """
    file_path: str
    table_name: str
    expected_columns: List[str]
    date_column: str
    expected_date_range: Tuple[Optional[datetime], Optional[datetime]]
    duplicate_key_columns: Optional[List[str]] = None
    file_size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    
    def __post_init__(self):
        """Validate and normalize the configuration"""
        # Ensure file_path is absolute
        if not Path(self.file_path).is_absolute():
            self.file_path = str(Path(self.file_path).resolve())
        
        # Ensure duplicate_key_columns is a list if provided
        if self.duplicate_key_columns is None:
            self.duplicate_key_columns = []
        elif isinstance(self.duplicate_key_columns, str):
            self.duplicate_key_columns = [self.duplicate_key_columns]
    
    @property
    def filename(self) -> str:
        """Get just the filename without path"""
        return Path(self.file_path).name
    
    @property
    def file_size_mb(self) -> float:
        """Get file size in MB"""
        if self.file_size_bytes:
            return self.file_size_bytes / (1024 * 1024)
        return 0.0
    
    @property
    def file_size_gb(self) -> float:
        """Get file size in GB"""
        if self.file_size_bytes:
            return self.file_size_bytes / (1024 * 1024 * 1024)
        return 0.0
    
    @property
    def has_date_range(self) -> bool:
        """Check if date range is specified"""
        return self.expected_date_range is not None and any(self.expected_date_range)
    
    def validate(self) -> List[str]:
        """
        Validate the configuration
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check file exists
        if not Path(self.file_path).exists():
            errors.append(f"File does not exist: {self.file_path}")
        
        # Check required fields
        if not self.table_name:
            errors.append("Table name is required")
        
        if not self.expected_columns:
            errors.append("Expected columns list is required")
        
        if not self.date_column:
            errors.append("Date column is required")
        
        # Check date column is in expected columns
        if self.date_column and self.expected_columns:
            if self.date_column not in self.expected_columns:
                errors.append(f"Date column '{self.date_column}' not in expected columns")
        
        # Check duplicate key columns are in expected columns
        if self.duplicate_key_columns and self.expected_columns:
            for col in self.duplicate_key_columns:
                if col not in self.expected_columns:
                    errors.append(f"Duplicate key column '{col}' not in expected columns")
        
        return errors
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            'file_path': self.file_path,
            'table_name': self.table_name,
            'expected_columns': self.expected_columns,
            'date_column': self.date_column,
            'expected_date_range': [
                d.isoformat() if d else None for d in self.expected_date_range
            ] if self.expected_date_range else None,
            'duplicate_key_columns': self.duplicate_key_columns,
            'file_size_bytes': self.file_size_bytes,
            'row_count': self.row_count
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FileConfig':
        """Create from dictionary"""
        # Parse date range if provided
        date_range = data.get('expected_date_range')
        if date_range:
            date_range = tuple(
                datetime.fromisoformat(d) if d else None 
                for d in date_range
            )
        
        return cls(
            file_path=data['file_path'],
            table_name=data['table_name'],
            expected_columns=data['expected_columns'],
            date_column=data['date_column'],
            expected_date_range=date_range,
            duplicate_key_columns=data.get('duplicate_key_columns'),
            file_size_bytes=data.get('file_size_bytes'),
            row_count=data.get('row_count')
        )