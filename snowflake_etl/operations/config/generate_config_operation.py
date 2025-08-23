#!/usr/bin/env python3
"""
Generate configuration files for Snowflake ETL pipeline
"""

import json
import logging
import re
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


class GenerateConfigOperation:
    """
    Operation to generate configuration files from TSV files
    """
    
    def __init__(self, context=None):
        """
        Initialize generate config operation
        
        Args:
            context: Optional ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, 
                files: List[str],
                output_file: Optional[str] = None,
                table_name: Optional[str] = None,
                column_headers: Optional[str] = None,
                base_path: str = ".",
                date_column: str = "RECORDDATEID",
                merge_with: Optional[str] = None,
                interactive: bool = False,
                dry_run: bool = False) -> Dict[str, Any]:
        """
        Generate configuration from TSV files
        
        Args:
            files: List of TSV files to analyze
            output_file: Output configuration file path
            table_name: Snowflake table name to query for columns
            column_headers: Comma-separated column headers for headerless files
            base_path: Base path for file patterns
            date_column: Name of date column
            merge_with: Existing config file to merge with
            interactive: Interactive mode for credentials
            dry_run: Show what would be generated without creating files
            
        Returns:
            Generated configuration dictionary
        """
        self.logger.info(f"Generating config for {len(files)} files")
        
        # Start with base config or merge config
        if merge_with and Path(merge_with).exists():
            with open(merge_with, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Merging with existing config: {merge_with}")
        else:
            config = self._create_base_config(interactive)
        
        # Process each file
        file_configs = []
        for file_path in files:
            file_config = self._analyze_file(
                file_path,
                table_name,
                column_headers,
                base_path,
                date_column
            )
            if file_config:
                file_configs.append(file_config)
        
        # Merge file configs (deduplicate by pattern)
        unique_configs = self._deduplicate_configs(file_configs)
        config['files'] = unique_configs
        
        # Output results
        if dry_run:
            print("\n=== Generated Configuration (DRY RUN) ===")
            print(json.dumps(config, indent=2))
            return config
        
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2)
            print(f"✓ Configuration saved to: {output_file}")
        else:
            print(json.dumps(config, indent=2))
        
        return config
    
    def _create_base_config(self, interactive: bool = False) -> Dict[str, Any]:
        """
        Create base configuration with Snowflake credentials
        
        Args:
            interactive: Whether to prompt for credentials
            
        Returns:
            Base configuration dictionary
        """
        if interactive:
            print("\n=== Snowflake Configuration ===")
            account = input("Account: ")
            user = input("User: ")
            password = input("Password: ")
            warehouse = input("Warehouse: ")
            database = input("Database: ")
            schema = input("Schema: ")
            role = input("Role (optional): ")
            
            snowflake_config = {
                "account": account,
                "user": user,
                "password": password,
                "warehouse": warehouse,
                "database": database,
                "schema": schema
            }
            if role:
                snowflake_config["role"] = role
        else:
            # Use defaults or environment variables
            snowflake_config = {
                "account": "${SNOWFLAKE_ACCOUNT}",
                "user": "${SNOWFLAKE_USER}",
                "password": "${SNOWFLAKE_PASSWORD}",
                "warehouse": "${SNOWFLAKE_WAREHOUSE}",
                "database": "${SNOWFLAKE_DATABASE}",
                "schema": "${SNOWFLAKE_SCHEMA}",
                "role": "${SNOWFLAKE_ROLE}"
            }
        
        return {
            "snowflake": snowflake_config,
            "files": []
        }
    
    def _analyze_file(self,
                     file_path: str,
                     table_name: Optional[str],
                     column_headers: Optional[str],
                     base_path: str,
                     date_column: str) -> Optional[Dict[str, Any]]:
        """
        Analyze a TSV file and generate its configuration
        
        Args:
            file_path: Path to TSV file
            table_name: Snowflake table name
            column_headers: Manual column headers
            base_path: Base path for patterns
            date_column: Date column name
            
        Returns:
            File configuration dictionary
        """
        file_path = Path(file_path)
        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            return None
        
        # Detect file pattern
        pattern = self._detect_pattern(file_path.name)
        if not pattern:
            self.logger.warning(f"Could not detect pattern for: {file_path.name}")
            pattern = file_path.name
        
        # Get columns
        if column_headers:
            columns = [col.strip() for col in column_headers.split(',')]
        elif table_name:
            columns = self._get_columns_from_table(table_name)
        else:
            columns = self._detect_columns_from_file(file_path)
        
        # Extract table name from file if not provided
        if not table_name:
            table_name = self._extract_table_name(file_path.name)
        
        config = {
            "file_pattern": pattern,
            "table_name": table_name.upper() if table_name else "UNKNOWN_TABLE",
            "expected_columns": columns,
            "date_column": date_column
        }
        
        # Add duplicate key columns if we can detect them
        if columns:
            duplicate_keys = self._detect_duplicate_keys(columns)
            if duplicate_keys:
                config["duplicate_key_columns"] = duplicate_keys
        
        return config
    
    def _detect_pattern(self, filename: str) -> Optional[str]:
        """
        Detect date pattern in filename
        
        Args:
            filename: File name to analyze
            
        Returns:
            Pattern with placeholders or None
        """
        # Remove .tsv extension
        base_name = filename.replace('.tsv', '')
        
        # Check for date range pattern (YYYYMMDD-YYYYMMDD)
        date_range_pattern = r'(\d{8})-(\d{8})'
        if re.search(date_range_pattern, base_name):
            pattern = re.sub(date_range_pattern, '{date_range}', base_name) + '.tsv'
            return pattern
        
        # Check for month pattern (YYYY-MM)
        month_pattern = r'\d{4}-\d{2}'
        if re.search(month_pattern, base_name):
            pattern = re.sub(month_pattern, '{month}', base_name) + '.tsv'
            return pattern
        
        # Check for YYYYMM pattern
        yyyymm_pattern = r'\d{6}'
        if re.search(yyyymm_pattern, base_name):
            pattern = re.sub(yyyymm_pattern, '{month}', base_name) + '.tsv'
            return pattern
        
        return None
    
    def _extract_table_name(self, filename: str) -> str:
        """
        Extract table name from filename
        
        Args:
            filename: File name
            
        Returns:
            Extracted table name
        """
        # Remove extension and date patterns
        base_name = filename.replace('.tsv', '')
        base_name = re.sub(r'_?\d{8}-\d{8}', '', base_name)
        base_name = re.sub(r'_?\d{4}-\d{2}', '', base_name)
        base_name = re.sub(r'_?\d{6}', '', base_name)
        
        # Remove common prefixes
        base_name = re.sub(r'^(fact|dim|test_|custom_)', '', base_name, flags=re.IGNORECASE)
        
        return base_name.upper()
    
    def _detect_columns_from_file(self, file_path: Path) -> List[str]:
        """
        Detect columns from TSV file
        
        Args:
            file_path: Path to TSV file
            
        Returns:
            List of column names
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read first line
                first_line = f.readline().strip()
                
                # Try to detect delimiter
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(first_line).delimiter
                
                # Parse columns
                reader = csv.reader([first_line], delimiter=delimiter)
                columns = next(reader)
                
                # Check if these look like headers or data
                if all(self._looks_like_header(col) for col in columns[:5]):
                    return columns
                else:
                    # No headers, generate generic column names
                    return [f"COLUMN_{i+1}" for i in range(len(columns))]
                    
        except Exception as e:
            self.logger.error(f"Error detecting columns from file: {e}")
            return []
    
    def _looks_like_header(self, value: str) -> bool:
        """
        Check if a value looks like a column header
        
        Args:
            value: Value to check
            
        Returns:
            True if it looks like a header
        """
        # Headers typically don't start with numbers
        if value and value[0].isdigit():
            return False
        
        # Headers often contain underscores or are alphabetic
        if '_' in value or value.replace('_', '').isalpha():
            return True
        
        # Check for date-like values (likely data, not header)
        if re.match(r'^\d{4}-\d{2}-\d{2}', value):
            return False
        
        return True
    
    def _get_columns_from_table(self, table_name: str) -> List[str]:
        """
        Get columns from Snowflake table
        
        Args:
            table_name: Table name
            
        Returns:
            List of column names
        """
        if not self.context:
            self.logger.warning("No context available for Snowflake query")
            return []
        
        try:
            conn_manager = self.context.get_connection_manager()
            with conn_manager.get_cursor() as cursor:
                query = f"""
                SELECT column_name 
                FROM information_schema.columns
                WHERE UPPER(table_name) = UPPER('{table_name}')
                ORDER BY ordinal_position
                """
                cursor.execute(query)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting columns from table: {e}")
            return []
    
    def _detect_duplicate_keys(self, columns: List[str]) -> List[str]:
        """
        Detect likely duplicate key columns
        
        Args:
            columns: List of column names
            
        Returns:
            List of duplicate key column names
        """
        duplicate_keys = []
        
        # Common date columns
        date_columns = ['RECORDDATE', 'RECORDDATEID', 'DATE', 'TRADE_DATE']
        for col in columns:
            if col.upper() in date_columns:
                duplicate_keys.append(col)
                break
        
        # Common ID columns
        id_columns = ['ASSETID', 'ASSET_ID', 'FUNDID', 'FUND_ID', 'SECURITY_ID']
        for col in columns:
            if col.upper() in id_columns:
                duplicate_keys.append(col)
        
        return duplicate_keys[:3]  # Limit to 3 keys max
    
    def _deduplicate_configs(self, configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate file configurations by pattern
        
        Args:
            configs: List of file configurations
            
        Returns:
            Deduplicated list
        """
        seen_patterns = set()
        unique = []
        
        for config in configs:
            pattern = config.get('file_pattern')
            if pattern not in seen_patterns:
                seen_patterns.add(pattern)
                unique.append(config)
        
        return unique