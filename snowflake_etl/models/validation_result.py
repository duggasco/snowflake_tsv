"""
Validation result dataclass.
Separated to avoid import dependencies.
"""

from dataclasses import dataclass, asdict
from typing import List, Dict, Optional


@dataclass
class ValidationResult:
    """Data class for validation results."""
    valid: bool
    table_name: str
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    total_rows: int = 0
    unique_dates: int = 0
    expected_dates: int = 0
    missing_dates: List[str] = None
    gaps: List[Dict] = None
    anomalous_dates: List[Dict] = None
    duplicate_info: Optional[Dict] = None
    avg_rows_per_day: float = 0.0
    validation_time: float = 0.0
    error_message: Optional[str] = None
    failure_reasons: List[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)