#!/usr/bin/env python3
"""
Test the new progress tracking system
Demonstrates the clean abstraction and dependency injection
"""

import sys
import time
import tempfile
import json
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))

from snowflake_etl.core.application_context import ApplicationContext, BaseOperation
from snowflake_etl.core.progress import ProgressPhase, NoOpProgressTracker, LoggingProgressTracker
from snowflake_etl.ui.progress_bars import TqdmProgressTracker


class MockETLOperation(BaseOperation):
    """Mock ETL operation to demonstrate progress tracking"""
    
    def execute(self, num_files: int = 3):
        """Simulate ETL processing with progress tracking"""
        # Initialize progress tracker
        self.progress_tracker.initialize(
            total_files=num_files,
            total_bytes=num_files * 1024 * 1024,  # 1MB per file
            total_rows=num_files * 1000
        )
        
        # Process each file
        for i in range(num_files):
            filename = f"test_file_{i}.tsv"
            
            # Start file
            self.progress_tracker.start_file(filename, file_size=1024*1024, row_count=1000)
            
            # Simulate phases
            phases = [
                (ProgressPhase.ANALYSIS, 0.1),
                (ProgressPhase.QUALITY_CHECK, 0.3),
                (ProgressPhase.COMPRESSION, 0.2),
                (ProgressPhase.UPLOAD, 0.2),
                (ProgressPhase.COPY, 0.3),
                (ProgressPhase.VALIDATION, 0.1),
            ]
            
            for phase, duration in phases:
                self.progress_tracker.update_phase(phase)
                
                # Simulate work
                time.sleep(duration)
                
                # Update progress
                if phase == ProgressPhase.QUALITY_CHECK:
                    # Simulate row processing
                    for _ in range(10):
                        self.progress_tracker.update_progress(rows_processed=100)
                        time.sleep(0.01)
                elif phase == ProgressPhase.UPLOAD:
                    # Simulate byte processing
                    for _ in range(10):
                        self.progress_tracker.update_progress(bytes_processed=102400)  # 100KB
                        time.sleep(0.01)
            
            # Complete file
            self.progress_tracker.complete_file(success=True)
        
        # Get final stats
        stats = self.progress_tracker.get_stats()
        return {
            'files_processed': stats.processed_files,
            'bytes_processed': stats.processed_bytes,
            'rows_processed': stats.processed_rows,
            'elapsed_time': stats.elapsed_time
        }


def test_progress_trackers():
    """Test different progress tracker implementations"""
    print("\n" + "="*60)
    print("Testing Progress Tracking System")
    print("="*60)
    
    # Create test config
    test_config = {
        "snowflake": {
            "account": "test",
            "user": "test",
            "password": "test",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA"
        },
        "files": []
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(test_config, f)
        config_path = Path(f.name)
    
    try:
        # Test 1: NoOp tracker (quiet mode)
        print("\n1. Testing NoOpProgressTracker (quiet mode)...")
        with ApplicationContext(config_path=config_path, quiet=True) as context:
            # Force NoOp tracker
            context.set_progress_tracker(NoOpProgressTracker())
            
            operation = MockETLOperation(context)
            result = operation.execute(num_files=2)
            
            print(f"   Processed {result['files_processed']} files in {result['elapsed_time']:.2f}s")
            print("   [PASS] NoOp tracker works (no visual output)")
        
        # Test 2: Logging tracker
        print("\n2. Testing LoggingProgressTracker...")
        with ApplicationContext(config_path=config_path, quiet=False) as context:
            # Force logging tracker
            context.set_progress_tracker(LoggingProgressTracker(log_interval=1))
            
            operation = MockETLOperation(context)
            print("   (Progress will be logged to console)")
            result = operation.execute(num_files=2)
            
            print(f"   Processed {result['files_processed']} files")
            print("   [PASS] Logging tracker works")
        
        # Test 3: Tqdm tracker (if available)
        print("\n3. Testing TqdmProgressTracker...")
        try:
            import tqdm
            with ApplicationContext(config_path=config_path, quiet=False) as context:
                # Force tqdm tracker
                context.set_progress_tracker(TqdmProgressTracker())
                
                operation = MockETLOperation(context)
                print("   (Visual progress bars should appear)")
                result = operation.execute(num_files=3)
                
                print(f"\n   Processed {result['files_processed']} files")
                print("   [PASS] Tqdm tracker works")
        except ImportError:
            print("   [SKIP] tqdm not installed")
        
        # Test 4: Progress tracker injection
        print("\n4. Testing progress tracker injection...")
        with ApplicationContext(config_path=config_path, quiet=False) as context:
            # Create two operations that share the same progress tracker
            op1 = MockETLOperation(context)
            op2 = MockETLOperation(context)
            
            # Both should have the same tracker instance
            assert op1.progress_tracker is op2.progress_tracker
            print("   [PASS] Operations share the same progress tracker")
        
        print("\n[SUCCESS] All progress tracking tests passed!")
        
    finally:
        config_path.unlink()


def compare_old_vs_new():
    """Compare old vs new progress tracking approach"""
    print("\n" + "="*60)
    print("Progress Tracking: Old vs New")
    print("="*60)
    
    print("\n### OLD APPROACH (tsv_loader.py ProgressTracker):")
    print("- Tightly coupled to tqdm")
    print("- Complex position calculation for bash parallelism")
    print("- Environment variable dependencies (TSV_JOB_POSITION)")
    print("- Mixed concerns (UI and business logic)")
    print("- Hard to test (direct tqdm usage)")
    print("- 291 lines of complex code")
    
    print("\n### NEW APPROACH (Dependency Injection):")
    print("- Abstract ProgressTracker interface")
    print("- Multiple implementations (NoOp, Logging, Tqdm)")
    print("- No environment variable dependencies")
    print("- Clean separation of concerns")
    print("- Easy to test (can inject mock)")
    print("- ~150 lines per implementation, cleaner code")
    
    print("\n### BENEFITS:")
    print("✓ Testability: Can use NoOpProgressTracker in tests")
    print("✓ Flexibility: Easy to add new tracker types")
    print("✓ Simplicity: No bash parallelism complexity")
    print("✓ Maintainability: Each tracker is self-contained")
    print("✓ Performance: Same tracker shared across operations")


def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("# PROGRESS TRACKING SYSTEM TEST SUITE")
    print("#"*60)
    
    try:
        test_progress_trackers()
        compare_old_vs_new()
        
        print("\n" + "#"*60)
        print("# ALL TESTS PASSED!")
        print("#"*60)
        print("\nThe new progress tracking system is much cleaner!")
        print("Next step: Continue extracting classes from tsv_loader.py")
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())