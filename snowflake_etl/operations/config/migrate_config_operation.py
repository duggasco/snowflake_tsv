#!/usr/bin/env python3
"""
Migrate configuration files between versions
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class MigrateConfigOperation:
    """
    Operation to migrate configuration files between versions
    """
    
    def __init__(self, context=None):
        """
        Initialize migrate config operation
        
        Args:
            context: Optional ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, config_file: str, 
                target_version: str = "3.0",
                backup: bool = True) -> bool:
        """
        Migrate a configuration file to a new version
        
        Args:
            config_file: Path to configuration file
            target_version: Target version to migrate to
            backup: Whether to create a backup
            
        Returns:
            True if successful
        """
        config_path = Path(config_file)
        
        if not config_path.exists():
            print(f"Error: Configuration file not found: {config_file}")
            return False
        
        print(f"\nMigrating configuration: {config_file}")
        print(f"Target version: {target_version}")
        print("="*60)
        
        try:
            # Load existing config
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Detect current version
            current_version = self._detect_version(config)
            print(f"Current version: {current_version}")
            
            if current_version == target_version:
                print("[VALID] Configuration is already at target version")
                return True
            
            # Create backup if requested
            if backup:
                backup_path = config_path.with_suffix(
                    f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                with open(backup_path, 'w') as f:
                    json.dump(config, f, indent=2)
                print(f"[VALID] Backup created: {backup_path}")
            
            # Perform migration
            if target_version == "3.0":
                config = self._migrate_to_v3(config)
            else:
                print(f"Error: Unknown target version: {target_version}")
                return False
            
            # Save migrated config
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            print(f"[VALID] Configuration migrated to version {target_version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            print(f"Error: Migration failed: {e}")
            return False
    
    def _detect_version(self, config: Dict[str, Any]) -> str:
        """
        Detect configuration version
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Version string
        """
        # Check for version field
        if 'version' in config:
            return config['version']
        
        # Check for v3.0 indicators
        if 'files' in config and config.get('files'):
            first_file = config['files'][0]
            if 'duplicate_key_columns' in first_file:
                return "3.0"
            if 'expected_date_range' in first_file:
                return "2.0"
        
        # Legacy format
        return "1.0"
    
    def _migrate_to_v3(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate configuration to version 3.0
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Migrated configuration
        """
        print("\nMigrating to v3.0 format...")
        
        # Add version field
        config['version'] = "3.0"
        
        # Migrate file configurations
        if 'files' in config:
            for file_config in config['files']:
                # Add duplicate_key_columns if not present
                if 'duplicate_key_columns' not in file_config:
                    # Auto-detect based on common patterns
                    duplicate_keys = []
                    
                    # Add date column
                    if 'date_column' in file_config:
                        duplicate_keys.append(file_config['date_column'])
                    elif 'expected_columns' in file_config:
                        for col in ['recordDate', 'RECORDDATE', 'RECORDDATEID']:
                            if col in file_config['expected_columns']:
                                duplicate_keys.append(col)
                                break
                    
                    # Add ID columns
                    if 'expected_columns' in file_config:
                        for col in file_config['expected_columns']:
                            col_upper = col.upper()
                            if 'ASSETID' in col_upper or 'FUNDID' in col_upper:
                                duplicate_keys.append(col)
                    
                    if duplicate_keys:
                        file_config['duplicate_key_columns'] = duplicate_keys[:3]
                        print(f"  Added duplicate_key_columns: {duplicate_keys[:3]}")
                
                # Remove deprecated fields
                deprecated = ['expected_date_range', 'chunk_size']
                for field in deprecated:
                    if field in file_config:
                        del file_config[field]
                        print(f"  Removed deprecated field: {field}")
        
        # Ensure Snowflake config has all required fields
        if 'snowflake' in config:
            sf_config = config['snowflake']
            defaults = {
                'autocommit': True,
                'client_session_keep_alive': True
            }
            for key, value in defaults.items():
                if key not in sf_config:
                    sf_config[key] = value
                    print(f"  Added Snowflake setting: {key}={value}")
        
        return config