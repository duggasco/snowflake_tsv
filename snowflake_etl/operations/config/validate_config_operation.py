#!/usr/bin/env python3
"""
Validate configuration files for Snowflake ETL pipeline
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple


class ValidateConfigOperation:
    """
    Operation to validate configuration files
    """
    
    def __init__(self, context=None):
        """
        Initialize validate config operation
        
        Args:
            context: Optional ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
        self.errors = []
        self.warnings = []
    
    def execute(self, config_file: str, check_connection: bool = False) -> bool:
        """
        Validate a configuration file
        
        Args:
            config_file: Path to configuration file
            check_connection: Whether to test Snowflake connection
            
        Returns:
            True if valid, False otherwise
        """
        self.errors = []
        self.warnings = []
        
        config_path = Path(config_file)
        
        # Check file exists
        if not config_path.exists():
            self.errors.append(f"Configuration file not found: {config_file}")
            self._print_results()
            return False
        
        print(f"\nValidating configuration: {config_file}")
        print("="*60)
        
        try:
            # Load and parse JSON
            with open(config_path, 'r') as f:
                config = json.load(f)
            print("✓ Valid JSON format")
            
            # Validate structure
            self._validate_structure(config)
            
            # Validate Snowflake config
            self._validate_snowflake_config(config.get('snowflake', {}))
            
            # Validate file configurations
            self._validate_file_configs(config.get('files', []))
            
            # Test Snowflake connection if requested
            if check_connection and self.context:
                self._test_connection(config.get('snowflake', {}))
            
            # Print results
            self._print_results()
            
            return len(self.errors) == 0
            
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON: {e}")
            self._print_results()
            return False
        except Exception as e:
            self.errors.append(f"Validation error: {e}")
            self._print_results()
            return False
    
    def _validate_structure(self, config: Dict[str, Any]):
        """
        Validate basic configuration structure
        
        Args:
            config: Configuration dictionary
        """
        # Check required top-level keys
        if 'snowflake' not in config:
            self.errors.append("Missing 'snowflake' section")
        else:
            print("✓ Has 'snowflake' section")
        
        if 'files' not in config:
            self.warnings.append("Missing 'files' section - no files configured")
        elif not config['files']:
            self.warnings.append("'files' section is empty")
        else:
            print(f"✓ Has {len(config['files'])} file configuration(s)")
    
    def _validate_snowflake_config(self, sf_config: Dict[str, Any]):
        """
        Validate Snowflake configuration
        
        Args:
            sf_config: Snowflake configuration dictionary
        """
        required_fields = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        
        print("\nSnowflake Configuration:")
        for field in required_fields:
            if field not in sf_config:
                self.errors.append(f"Missing Snowflake field: {field}")
            elif not sf_config[field]:
                self.errors.append(f"Empty Snowflake field: {field}")
            else:
                # Check for environment variable placeholders
                value = sf_config[field]
                if value.startswith('${') and value.endswith('}'):
                    self.warnings.append(f"Snowflake {field} uses environment variable: {value}")
                else:
                    # Mask sensitive fields
                    if field == 'password':
                        print(f"  {field}: ***")
                    else:
                        print(f"  {field}: {value}")
        
        # Optional fields
        if 'role' in sf_config and sf_config['role']:
            print(f"  role: {sf_config['role']}")
    
    def _validate_file_configs(self, file_configs: List[Dict[str, Any]]):
        """
        Validate file configurations
        
        Args:
            file_configs: List of file configuration dictionaries
        """
        if not file_configs:
            return
        
        print("\nFile Configurations:")
        
        for i, file_config in enumerate(file_configs, 1):
            print(f"\n  File Config #{i}:")
            
            # Check required fields
            required = ['file_pattern', 'table_name', 'expected_columns']
            for field in required:
                if field not in file_config:
                    self.errors.append(f"File config #{i} missing field: {field}")
                elif field == 'expected_columns':
                    if not isinstance(file_config[field], list):
                        self.errors.append(f"File config #{i}: expected_columns must be a list")
                    elif not file_config[field]:
                        self.errors.append(f"File config #{i}: expected_columns is empty")
                    else:
                        print(f"    {field}: {len(file_config[field])} columns")
                else:
                    print(f"    {field}: {file_config[field]}")
            
            # Check optional fields
            if 'date_column' in file_config:
                date_col = file_config['date_column']
                print(f"    date_column: {date_col}")
                
                # Check if date column is in expected columns
                if 'expected_columns' in file_config:
                    if date_col not in file_config['expected_columns']:
                        self.warnings.append(
                            f"File config #{i}: date_column '{date_col}' not in expected_columns"
                        )
            
            if 'duplicate_key_columns' in file_config:
                dup_cols = file_config['duplicate_key_columns']
                if isinstance(dup_cols, list):
                    print(f"    duplicate_key_columns: {', '.join(dup_cols)}")
                    
                    # Check if all duplicate key columns are in expected columns
                    if 'expected_columns' in file_config:
                        for col in dup_cols:
                            if col not in file_config['expected_columns']:
                                self.warnings.append(
                                    f"File config #{i}: duplicate key column '{col}' not in expected_columns"
                                )
                else:
                    self.errors.append(f"File config #{i}: duplicate_key_columns must be a list")
            
            # Validate file pattern
            pattern = file_config.get('file_pattern', '')
            if '{date_range}' in pattern:
                print("    Pattern type: date_range (YYYYMMDD-YYYYMMDD)")
            elif '{month}' in pattern:
                print("    Pattern type: month (YYYY-MM)")
            else:
                self.warnings.append(f"File config #{i}: No date placeholder in pattern")
    
    def _test_connection(self, sf_config: Dict[str, Any]):
        """
        Test Snowflake connection
        
        Args:
            sf_config: Snowflake configuration dictionary
        """
        print("\nTesting Snowflake connection...")
        
        try:
            # Skip if using environment variables
            for value in sf_config.values():
                if isinstance(value, str) and value.startswith('${'):
                    print("  ⚠ Skipping - configuration uses environment variables")
                    return
            
            # Test connection
            conn_manager = self.context.get_connection_manager()
            with conn_manager.get_cursor() as cursor:
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                print(f"  ✓ Connected successfully")
                print(f"  Snowflake version: {version}")
                
                # Test database and schema
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                db, schema = cursor.fetchone()
                print(f"  Current context: {db}.{schema}")
                
        except Exception as e:
            self.errors.append(f"Connection test failed: {e}")
    
    def _print_results(self):
        """Print validation results"""
        print("\n" + "="*60)
        print("VALIDATION RESULTS")
        print("="*60)
        
        if self.errors:
            print(f"\n❌ {len(self.errors)} Error(s):")
            for error in self.errors:
                print(f"   - {error}")
        
        if self.warnings:
            print(f"\n⚠ {len(self.warnings)} Warning(s):")
            for warning in self.warnings:
                print(f"   - {warning}")
        
        if not self.errors and not self.warnings:
            print("\n✓ Configuration is valid!")
        elif not self.errors:
            print("\n✓ Configuration is valid (with warnings)")
        else:
            print("\n❌ Configuration is invalid!")