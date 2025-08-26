#!/usr/bin/env python3
"""
Check Snowflake table existence and get column information
"""

import logging
from typing import Optional, List, Tuple
from dataclasses import dataclass


@dataclass
class TableInfo:
    """Information about a Snowflake table"""
    database: str
    schema: str
    table_name: str
    column_count: int
    columns: List[Tuple[str, str, int]]  # (name, type, position)


class CheckTableOperation:
    """
    Operation to check Snowflake table existence and retrieve column information
    """
    
    def __init__(self, context):
        """
        Initialize check table operation
        
        Args:
            context: ApplicationContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
    
    def execute(self, table_name: str) -> bool:
        """
        Check if table exists and display information
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table exists, False otherwise
        """
        self.logger.info(f"Checking table: {table_name}")
        
        # Get connection
        conn_manager = self.context.get_connection_manager()
        
        try:
            with conn_manager.get_cursor() as cursor:
                # Get current database/schema context
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
                current_db, current_schema = cursor.fetchone()
                
                self.logger.info(f"Current context: {current_db}.{current_schema}")
                print(f"Checking table '{table_name}' in {current_db}.{current_schema}")
                
                # Check if table exists in current schema
                table_info = self._get_table_info(
                    cursor, table_name, current_db, current_schema
                )
                
                if table_info:
                    self._display_table_info(table_info)
                    return True
                else:
                    # Search all accessible schemas
                    self.logger.info(f"Table not found in current schema, searching all schemas")
                    all_tables = self._search_all_schemas(cursor, table_name)
                    
                    if all_tables:
                        print(f"\nTable '{table_name}' found in:")
                        for db, schema, col_count in all_tables:
                            print(f"  - {db}.{schema} ({col_count} columns)")
                        return True
                    else:
                        print(f"\nTable '{table_name}' not found in any accessible schema")
                        self._list_available_tables(cursor, current_schema)
                        return False
                        
        except Exception as e:
            self.logger.error(f"Error checking table: {e}")
            print(f"Error: {e}")
            return False
    
    def _get_table_info(self, cursor, table_name: str, 
                       database: str, schema: str) -> Optional[TableInfo]:
        """
        Get detailed information about a table
        
        Args:
            cursor: Snowflake cursor
            table_name: Table name
            database: Database name
            schema: Schema name
            
        Returns:
            TableInfo object or None if table doesn't exist
        """
        # Check if table exists and get column count
        query = f"""
        SELECT COUNT(*) as col_count
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER('{table_name}')
          AND UPPER(table_schema) = UPPER('{schema}')
          AND UPPER(table_catalog) = UPPER('{database}')
        """
        cursor.execute(query)
        count = cursor.fetchone()[0]
        
        if count == 0:
            return None
        
        # Get column details
        query = f"""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER('{table_name}')
          AND UPPER(table_schema) = UPPER('{schema}')
          AND UPPER(table_catalog) = UPPER('{database}')
        ORDER BY ordinal_position
        """
        cursor.execute(query)
        columns = cursor.fetchall()
        
        return TableInfo(
            database=database,
            schema=schema,
            table_name=table_name,
            column_count=count,
            columns=columns
        )
    
    def _display_table_info(self, table_info: TableInfo):
        """
        Display table information
        
        Args:
            table_info: TableInfo object
        """
        print(f"\n[VALID] Table found: {table_info.database}.{table_info.schema}.{table_info.table_name}")
        print(f"  Total columns: {table_info.column_count}")
        
        # Display first 20 columns
        print("\n  Column details:")
        for i, (col_name, data_type, pos) in enumerate(table_info.columns[:20]):
            print(f"    {pos:3d}. {col_name:30s} {data_type}")
        
        if len(table_info.columns) > 20:
            print(f"    ... and {len(table_info.columns) - 20} more columns")
    
    def _search_all_schemas(self, cursor, table_name: str) -> List[Tuple[str, str, int]]:
        """
        Search for table in all accessible schemas
        
        Args:
            cursor: Snowflake cursor
            table_name: Table name to search
            
        Returns:
            List of (database, schema, column_count) tuples
        """
        query = f"""
        SELECT table_catalog, table_schema, COUNT(*) as col_count
        FROM information_schema.columns
        WHERE UPPER(table_name) = UPPER('{table_name}')
        GROUP BY table_catalog, table_schema
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    def _list_available_tables(self, cursor, schema: str, limit: int = 20):
        """
        List available tables in the current schema
        
        Args:
            cursor: Snowflake cursor
            schema: Schema name
            limit: Maximum number of tables to show
        """
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        LIMIT {limit}
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor]
        
        if tables:
            print(f"\nAvailable tables in {schema} (showing first {limit}):")
            for table in tables:
                print(f"  - {table}")
        else:
            print(f"\nNo tables found in schema {schema}")