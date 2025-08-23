#!/usr/bin/env python3
"""
Diagnose Snowflake COPY errors and provide troubleshooting guidance
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any


class DiagnoseErrorOperation:
    """
    Operation to diagnose Snowflake COPY errors
    """
    
    def __init__(self, context):
        """
        Initialize diagnose error operation
        
        Args:
            context: ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, table_name: Optional[str] = None, 
                hours_back: int = 24) -> bool:
        """
        Analyze recent COPY errors and provide recommendations
        
        Args:
            table_name: Optional table name to filter errors
            hours_back: How many hours back to search for errors
            
        Returns:
            True if analysis completed successfully
        """
        self.logger.info(f"Diagnosing COPY errors for table: {table_name or 'ALL'}")
        
        print("\n" + "="*60)
        print("SNOWFLAKE COPY ERROR DIAGNOSTIC")
        print("="*60)
        
        conn_manager = self.context.get_connection_manager()
        
        try:
            with conn_manager.get_cursor() as cursor:
                # Check recent failed queries
                errors = self._check_recent_errors(cursor, hours_back)
                if errors:
                    self._display_errors(errors)
                else:
                    print(f"\nNo COPY errors found in the last {hours_back} hours")
                
                # Check stage files if table specified
                if table_name:
                    self._check_stage_files(cursor, table_name)
                
                # Provide recommendations
                self._provide_recommendations(errors)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error during diagnosis: {e}")
            print(f"\nError during diagnosis: {e}")
            return False
    
    def _check_recent_errors(self, cursor, hours_back: int) -> List[Dict[str, Any]]:
        """
        Check for recent COPY operation errors
        
        Args:
            cursor: Snowflake cursor
            hours_back: Hours to look back
            
        Returns:
            List of error records
        """
        print(f"\n1. Checking recent failed COPY operations (last {hours_back} hours)...")
        
        query = f"""
        SELECT 
            QUERY_ID,
            QUERY_TEXT,
            ERROR_CODE,
            ERROR_MESSAGE,
            START_TIME,
            END_TIME,
            TOTAL_ELAPSED_TIME / 1000 as ELAPSED_SECONDS,
            DATABASE_NAME,
            SCHEMA_NAME,
            WAREHOUSE_NAME,
            WAREHOUSE_SIZE,
            BYTES_SCANNED / (1024*1024*1024) as GB_SCANNED
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START => DATEADD('hour', -{hours_back}, CURRENT_TIMESTAMP()),
            END_TIME_RANGE_END => CURRENT_TIMESTAMP()
        ))
        WHERE (ERROR_CODE IN ('000603', '300005')
           OR ERROR_MESSAGE LIKE '%incident%'
           OR (QUERY_TYPE = 'COPY' AND ERROR_CODE IS NOT NULL))
        ORDER BY START_TIME DESC
        LIMIT 10
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        errors = []
        for row in results:
            errors.append({
                'query_id': row[0],
                'query_text': row[1],
                'error_code': row[2],
                'error_message': row[3],
                'start_time': row[4],
                'end_time': row[5],
                'elapsed_seconds': row[6],
                'database': row[7],
                'schema': row[8],
                'warehouse': row[9],
                'warehouse_size': row[10],
                'gb_scanned': row[11]
            })
        
        return errors
    
    def _display_errors(self, errors: List[Dict[str, Any]]):
        """
        Display error information
        
        Args:
            errors: List of error records
        """
        print(f"\nFound {len(errors)} recent errors:")
        
        for i, error in enumerate(errors, 1):
            print(f"\n  Error #{i}:")
            print(f"    Query ID: {error['query_id']}")
            print(f"    Error Code: {error['error_code']}")
            print(f"    Error Message: {error['error_message'][:200]}")
            if len(error['error_message']) > 200:
                print(f"                  ...{error['error_message'][200:400]}")
            print(f"    Time: {error['start_time']} ({error['elapsed_seconds']:.1f} seconds)")
            print(f"    Warehouse: {error['warehouse']} (Size: {error['warehouse_size']})")
            if error['gb_scanned']:
                print(f"    Data Scanned: {error['gb_scanned']:.2f} GB")
    
    def _check_stage_files(self, cursor, table_name: str):
        """
        Check for problematic stage files
        
        Args:
            cursor: Snowflake cursor
            table_name: Table name to check
        """
        print(f"\n2. Checking stage files for {table_name}...")
        
        # Extract base name for pattern matching
        base_name = table_name.replace('TEST_CUSTOM_', '').replace('TEST_', '')
        pattern = f".*{base_name}.*" if base_name else ".*"
        
        try:
            query = f"LIST @~/ PATTERN='{pattern}'"
            cursor.execute(query)
            files = cursor.fetchall()
            
            if files:
                print(f"  Found {len(files)} related files in stage:")
                for file in files[:5]:  # Show first 5
                    file_name = file[0]
                    file_size = file[1] / (1024*1024*1024)  # Convert to GB
                    print(f"    - {file_name} ({file_size:.2f} GB)")
                if len(files) > 5:
                    print(f"    ... and {len(files) - 5} more files")
            else:
                print("  No related files found in stage")
        except Exception as e:
            print(f"  Could not list stage files: {e}")
    
    def _provide_recommendations(self, errors: List[Dict[str, Any]]):
        """
        Provide recommendations based on errors found
        
        Args:
            errors: List of error records
        """
        print("\n" + "="*60)
        print("RECOMMENDATIONS")
        print("="*60)
        
        if not errors:
            print("\n✓ No recent errors detected")
            return
        
        # Analyze error patterns
        has_incident = any('incident' in str(e.get('error_message', '')).lower() 
                          for e in errors)
        has_timeout = any('timeout' in str(e.get('error_message', '')).lower() 
                         for e in errors)
        has_memory = any('memory' in str(e.get('error_message', '')).lower() 
                        for e in errors)
        
        if has_incident:
            print("\n⚠ Internal incidents detected:")
            print("  1. These are usually temporary Snowflake infrastructure issues")
            print("  2. Retry the operation after a few minutes")
            print("  3. If persistent, contact Snowflake support with the incident ID")
        
        if has_timeout:
            print("\n⚠ Timeout errors detected:")
            print("  1. Consider using a larger warehouse (MEDIUM or LARGE)")
            print("  2. Break large files into smaller chunks")
            print("  3. Use COPY with smaller batch sizes")
        
        if has_memory:
            print("\n⚠ Memory errors detected:")
            print("  1. File might be too large for current warehouse")
            print("  2. Upgrade to a larger warehouse size")
            print("  3. Check for data quality issues causing parsing problems")
        
        # General recommendations
        print("\nGeneral troubleshooting steps:")
        print("  1. Check file format and encoding (should be UTF-8)")
        print("  2. Verify column count matches table definition")
        print("  3. Look for special characters or malformed rows")
        print("  4. Use VALIDATION_MODE to test before loading")
        print("  5. Consider using ON_ERROR='CONTINUE' to skip bad rows")