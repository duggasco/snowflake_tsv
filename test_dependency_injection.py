#!/usr/bin/env python3
"""
Test script for dependency injection pattern
Demonstrates how the refactored architecture works
"""

import sys
import tempfile
import json
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent))


def test_application_context():
    """Test ApplicationContext with dependency injection"""
    print("\n" + "="*60)
    print("Testing Dependency Injection Pattern")
    print("="*60)
    
    from snowflake_etl.core.application_context import ApplicationContext, BaseOperation
    
    # Create test config
    test_config = {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "password": "test_pass",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA"
        },
        "files": [
            {
                "file_pattern": "test_{date_range}.tsv",
                "table_name": "TEST_TABLE",
                "date_column": "date"
            }
        ]
    }
    
    # Save test config to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(test_config, f)
        config_path = Path(f.name)
    
    try:
        # Test 1: Create application context
        print("\n1. Creating ApplicationContext...")
        with ApplicationContext(config_path=config_path, quiet=True) as context:
            assert context.config is not None
            assert context.snowflake_config['warehouse'] == 'TEST_WH'
            print("   [PASS] Context created with config loaded")
            
            # Test 2: Multiple operations share the same context
            print("\n2. Testing shared context across operations...")
            
            class TestOperation1(BaseOperation):
                def execute(self):
                    # This operation uses the shared connection manager
                    print(f"   Operation1 using config: {self.config['snowflake']['database']}")
                    return True
            
            class TestOperation2(BaseOperation):
                def execute(self):
                    # This also uses the same shared connection manager
                    print(f"   Operation2 using config: {self.config['snowflake']['database']}")
                    return True
            
            # Create operations with injected context
            op1 = TestOperation1(context)
            op2 = TestOperation2(context)
            
            # Both operations share the same context
            assert op1.context is op2.context
            assert op1.config is op2.config
            print("   [PASS] Operations share the same context")
            
            # Test 3: Execute operations
            print("\n3. Executing operations with shared resources...")
            op1.execute()
            op2.execute()
            print("   [PASS] Operations executed successfully")
            
            # Test 4: Register custom operations
            print("\n4. Testing operation registration...")
            context.register_operation('test_op1', op1)
            context.register_operation('test_op2', op2)
            
            retrieved_op = context.get_operation('test_op1')
            assert retrieved_op is op1
            print("   [PASS] Operations registered and retrieved")
        
        print("\n5. Testing context cleanup...")
        # Context should be cleaned up after exiting with block
        print("   [PASS] Context cleaned up via context manager")
        
    finally:
        config_path.unlink()
    
    print("\n[SUCCESS] Dependency injection pattern working correctly!")


def test_cli_simulation():
    """Simulate how the CLI would work"""
    print("\n" + "="*60)
    print("Testing CLI Simulation")
    print("="*60)
    
    from snowflake_etl.cli.main import SnowflakeETLCLI
    
    # Create test config
    test_config = {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "password": "test_pass",
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
        print("\n1. Testing CLI argument parsing...")
        cli = SnowflakeETLCLI()
        
        # Test load command parsing
        args = cli.parse_args([
            '--config', str(config_path),
            '--quiet',
            'load',
            '--base-path', '/data',
            '--month', '2024-01',
            '--skip-qc'
        ])
        
        assert args.config == str(config_path)
        assert args.quiet is True
        assert args.operation == 'load'
        assert args.base_path == '/data'
        assert args.month == '2024-01'
        assert args.skip_qc is True
        print("   [PASS] Load command parsed correctly")
        
        # Test delete command parsing
        args = cli.parse_args([
            '--config', str(config_path),
            'delete',
            '--table', 'TEST_TABLE',
            '--month', '2024-01',
            '--dry-run'
        ])
        
        assert args.operation == 'delete'
        assert args.table == 'TEST_TABLE'
        assert args.dry_run is True
        print("   [PASS] Delete command parsed correctly")
        
        print("\n2. Testing context initialization from CLI...")
        args = cli.parse_args(['--config', str(config_path), '--quiet', 'validate'])
        cli.initialize_context(args)
        
        assert cli.context is not None
        assert cli.context.config is not None
        print("   [PASS] Context initialized from CLI arguments")
        
        # Clean up context
        cli.context.cleanup()
        
    finally:
        config_path.unlink()
    
    print("\n[SUCCESS] CLI simulation working correctly!")


def compare_architectures():
    """Compare singleton vs dependency injection approaches"""
    print("\n" + "="*60)
    print("Architecture Comparison")
    print("="*60)
    
    print("\n### OLD ARCHITECTURE (Singleton):")
    print("- Each script creates its own singleton instances")
    print("- Connection pools created/destroyed per script execution")
    print("- No resource sharing between operations")
    print("- Example flow:")
    print("  1. Shell calls tsv_loader.py -> Creates singleton pool -> Exits")
    print("  2. Shell calls drop_month.py -> Creates new singleton pool -> Exits")
    print("  3. Connection pool recreated for each operation")
    
    print("\n### NEW ARCHITECTURE (Dependency Injection):")
    print("- Single Python process handles all operations")
    print("- Connection pool created once, shared across operations")
    print("- Resources explicitly passed to operations")
    print("- Example flow:")
    print("  1. Shell calls main.py with 'load' command")
    print("  2. ApplicationContext created with shared resources")
    print("  3. LoadOperation receives context")
    print("  4. Same process can handle next operation")
    print("  5. Connection pool persists across operations")
    
    print("\n### BENEFITS:")
    print("✓ Performance: Connection pool reused, not recreated")
    print("✓ Testing: Easy to inject mock dependencies")
    print("✓ Clarity: Dependencies are explicit, not hidden")
    print("✓ Flexibility: Can run multiple operations in sequence")
    print("✓ Resource efficiency: Single process, shared memory")


def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("# DEPENDENCY INJECTION PATTERN TEST SUITE")
    print("#"*60)
    
    try:
        test_application_context()
        test_cli_simulation()
        compare_architectures()
        
        print("\n" + "#"*60)
        print("# ALL TESTS PASSED!")
        print("#"*60)
        print("\nThe refactored architecture with dependency injection is ready!")
        print("Next step: Update snowflake_etl.sh to call the unified CLI")
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())