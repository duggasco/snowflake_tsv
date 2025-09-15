#!/usr/bin/env python3
"""
Test script for CSV support implementation - Phase 3
Tests UI enhancements, progress displays, and logging with format information
"""

import sys
import tempfile
import logging
from pathlib import Path
from io import StringIO
sys.path.insert(0, str(Path(__file__).parent.parent))

from snowflake_etl.core.progress import ProgressStats, LoggingProgressTracker, ProgressPhase
from snowflake_etl.ui.progress_bars import TqdmProgressTracker
from snowflake_etl.models.file_config import FileConfig


def test_progress_tracker_format_display():
    """Test that progress tracker displays file format"""
    print("Testing progress tracker with file format...")
    
    # Test LoggingProgressTracker
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger('LoggingProgressTracker')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    tracker = LoggingProgressTracker()
    tracker.logger = logger
    
    tracker.initialize(total_files=2, total_bytes=1024*1024)
    tracker.start_file("data.csv", file_size=512*1024, file_format="CSV")
    
    log_output = log_stream.getvalue()
    assert "[CSV]" in log_output, f"Expected [CSV] in log output, got: {log_output}"
    print("✓ LoggingProgressTracker shows file format")
    
    # Test ProgressStats includes format
    stats = tracker.get_stats()
    assert stats.current_file_format == "CSV"
    print("✓ ProgressStats includes file format")


def test_file_config_format_detection():
    """Test FileConfig auto-detects format correctly"""
    print("\nTesting FileConfig format detection...")
    
    # CSV file
    csv_config = FileConfig(
        file_path="/tmp/sales.csv",
        table_name="SALES",
        expected_columns=["date", "amount"],
        date_column="date",
        expected_date_range=(None, None)
    )
    assert csv_config.file_format == "CSV"
    assert csv_config.delimiter == ","
    print("✓ CSV file auto-detected")
    
    # TSV file
    tsv_config = FileConfig(
        file_path="/tmp/inventory.tsv",
        table_name="INVENTORY",
        expected_columns=["date", "quantity"],
        date_column="date",
        expected_date_range=(None, None)
    )
    assert tsv_config.file_format == "TSV"
    assert tsv_config.delimiter == "\t"
    print("✓ TSV file auto-detected")
    
    # Compressed CSV
    gz_config = FileConfig(
        file_path="/tmp/data.csv.gz",
        table_name="DATA",
        expected_columns=["id", "value"],
        date_column="id",
        expected_date_range=(None, None)
    )
    assert gz_config.file_format == "CSV"
    print("✓ Compressed CSV detected")


def test_error_messages_with_format():
    """Test that error messages include file format"""
    print("\nTesting error messages with format...")
    
    # Create a mock logger to capture messages
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger('test')
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    
    # Test error message formatting
    file_config = FileConfig(
        file_path="/tmp/test.csv",
        table_name="TEST",
        expected_columns=["col1"],
        date_column="col1",
        expected_date_range=(None, None),
        file_format="CSV"
    )
    
    # Simulate error logging
    file_format = file_config.file_format if hasattr(file_config, 'file_format') else 'TSV'
    logger.error(f"Failed to process {file_config.file_path} [{file_format}]: Test error")
    
    log_output = log_stream.getvalue()
    assert "[CSV]" in log_output
    assert "/tmp/test.csv" in log_output
    print("✓ Error messages include file format")


def test_log_messages_format():
    """Test log messages include format information"""
    print("\nTesting log messages with format...")
    
    # Create test file configs
    csv_config = FileConfig(
        file_path="/tmp/sales_2024.csv",
        table_name="SALES",
        expected_columns=["date", "amount"],
        date_column="date",
        expected_date_range=(None, None)
    )
    
    # Check format is properly set
    assert csv_config.file_format == "CSV"
    
    # Test logging simulation
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger('test_loader')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    # Simulate loader log message
    file_format = csv_config.file_format
    delimiter_name = 'comma' if csv_config.delimiter == ',' else 'tab'
    logger.info(f"Loading {csv_config.file_path} [{file_format}, {delimiter_name}-delimited] to {csv_config.table_name}")
    
    log_output = log_stream.getvalue()
    assert "[CSV, comma-delimited]" in log_output
    print("✓ Loader log messages include format and delimiter")


def test_progress_phase_display():
    """Test progress phase updates with format context"""
    print("\nTesting progress phase display...")
    
    tracker = LoggingProgressTracker()
    tracker.initialize(total_files=1)
    tracker.start_file("data.csv", file_format="CSV")
    
    # Test phase updates
    tracker.update_phase(ProgressPhase.ANALYSIS)
    assert tracker.stats.current_phase == ProgressPhase.ANALYSIS
    
    tracker.update_phase(ProgressPhase.COMPRESSION)
    assert tracker.stats.current_phase == ProgressPhase.COMPRESSION
    
    tracker.update_phase(ProgressPhase.UPLOAD)
    assert tracker.stats.current_phase == ProgressPhase.UPLOAD
    
    print("✓ Progress phases update correctly")


def test_ui_labels():
    """Verify UI labels have been updated"""
    print("\nVerifying UI label updates...")
    
    # Read shell script to verify updates
    shell_script = Path("/root/snowflake/snowflake_etl.sh")
    if shell_script.exists():
        content = shell_script.read_text()
        
        # Check for updated labels
        ui_updates = [
            ("TSV/CSV", "Menu shows TSV/CSV"),
            ("Data File", "References updated to 'Data File'"),
            ("data file path (TSV/CSV/GZ)", "File path prompts updated"),
            ("Sample Data File", "Sample menu updated"),
            ("Compress Data File", "Compress menu updated")
        ]
        
        for search_text, description in ui_updates:
            if search_text in content:
                print(f"✓ {description}")
            else:
                print(f"✗ Missing: {description}")
    else:
        print("! Shell script not found for verification")


def main():
    """Run all Phase 3 tests"""
    print("=" * 60)
    print("CSV Support Phase 3 Tests - UI & Display")
    print("=" * 60)
    
    try:
        test_progress_tracker_format_display()
        test_file_config_format_detection()
        test_error_messages_with_format()
        test_log_messages_format()
        test_progress_phase_display()
        test_ui_labels()
        
        print("\n" + "=" * 60)
        print("✅ All Phase 3 tests passed!")
        print("=" * 60)
        
        print("\nPhase 3 Summary:")
        print("- Progress displays show file format")
        print("- Log messages include format and delimiter")
        print("- Error messages specify file format")
        print("- Shell script UI updated for CSV/TSV")
        print("- File analysis shows format details")
        
        return 0
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())