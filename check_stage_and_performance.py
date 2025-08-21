#!/usr/bin/env python3
"""
Diagnostic script to check Snowflake stage contents and investigate COPY performance issues
"""

import snowflake.connector
import json
import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path):
    """Load configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

def check_stage_contents(cursor, stage_pattern="@~/tsv_stage/"):
    """List all files in the stage"""
    logger.info(f"Checking contents of stage: {stage_pattern}")
    
    try:
        # List all files in the stage
        list_query = f"LIST {stage_pattern}"
        cursor.execute(list_query)
        files = cursor.fetchall()
        
        if not files:
            logger.info("Stage is empty")
            return []
        
        logger.info(f"Found {len(files)} files in stage:")
        
        file_info = []
        total_size = 0
        for file in files:
            # File columns: name, size, md5, last_modified
            name = file[0] if len(file) > 0 else "unknown"
            size = file[1] if len(file) > 1 else 0
            last_modified = file[3] if len(file) > 3 else "unknown"
            
            size_mb = size / (1024 * 1024)
            total_size += size
            
            logger.info(f"  - {name}: {size_mb:.2f} MB, Last modified: {last_modified}")
            file_info.append({
                'name': name,
                'size_mb': size_mb,
                'last_modified': str(last_modified)
            })
        
        total_size_mb = total_size / (1024 * 1024)
        logger.info(f"Total stage size: {total_size_mb:.2f} MB")
        
        return file_info
        
    except Exception as e:
        logger.error(f"Failed to list stage contents: {e}")
        return []

def check_table_specific_stage(cursor, table_name):
    """Check stage for a specific table"""
    stage_pattern = f"@~/tsv_stage/{table_name}/"
    logger.info(f"\nChecking stage for table: {table_name}")
    return check_stage_contents(cursor, stage_pattern)

def remove_old_stage_files(cursor, stage_pattern, dry_run=True):
    """Remove old files from stage"""
    logger.info(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning up stage: {stage_pattern}")
    
    try:
        if dry_run:
            logger.info("Would execute: REMOVE " + stage_pattern)
        else:
            cursor.execute(f"REMOVE {stage_pattern}")
            logger.info(f"Successfully removed files from {stage_pattern}")
    except Exception as e:
        logger.error(f"Failed to remove stage files: {e}")

def check_query_history(cursor, hours=24):
    """Check recent COPY INTO query performance"""
    logger.info(f"\nChecking COPY query history for last {hours} hours...")
    
    query = f"""
    SELECT 
        QUERY_ID,
        QUERY_TEXT,
        DATABASE_NAME,
        SCHEMA_NAME,
        QUERY_TYPE,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME / 1000 as ELAPSED_SECONDS,
        EXECUTION_TIME / 1000 as EXEC_SECONDS,
        QUEUED_PROVISIONING_TIME / 1000 as QUEUE_SECONDS,
        COMPILATION_TIME / 1000 as COMPILE_SECONDS,
        ROWS_PRODUCED,
        BYTES_SCANNED / (1024*1024) as MB_SCANNED
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START => DATEADD('hour', -{hours}, CURRENT_TIMESTAMP()),
        END_TIME_RANGE_END => CURRENT_TIMESTAMP()
    ))
    WHERE QUERY_TYPE = 'COPY'
        AND QUERY_TEXT LIKE '%COPY INTO%'
    ORDER BY START_TIME DESC
    LIMIT 10
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            logger.info("No COPY queries found in recent history")
            return
        
        logger.info(f"Found {len(results)} recent COPY queries:")
        for row in results:
            query_id = row[0]
            query_text = row[1][:100] + "..." if len(row[1]) > 100 else row[1]
            elapsed = row[7]
            exec_time = row[8]
            queue_time = row[9]
            compile_time = row[10]
            rows = row[11]
            mb_scanned = row[12]
            
            logger.info(f"\n  Query ID: {query_id}")
            logger.info(f"  Query: {query_text}")
            logger.info(f"  Total time: {elapsed:.1f}s (Queue: {queue_time:.1f}s, Compile: {compile_time:.1f}s, Exec: {exec_time:.1f}s)")
            logger.info(f"  Rows: {rows:,}, Data scanned: {mb_scanned:.1f} MB")
            
            if elapsed > 300:  # More than 5 minutes
                logger.warning(f"  ⚠️  This query took {elapsed/60:.1f} minutes!")
                
    except Exception as e:
        logger.error(f"Failed to query history: {e}")

def check_warehouse_size(cursor):
    """Check current warehouse size and configuration"""
    logger.info("\nChecking warehouse configuration...")
    
    try:
        cursor.execute("SHOW WAREHOUSES")
        warehouses = cursor.fetchall()
        
        cursor.execute("SELECT CURRENT_WAREHOUSE()")
        current_wh = cursor.fetchone()[0]
        
        for wh in warehouses:
            if wh[0] == current_wh:
                logger.info(f"Current warehouse: {wh[0]}")
                logger.info(f"  Size: {wh[2]}")
                logger.info(f"  State: {wh[3]}")
                logger.info(f"  Type: {wh[4]}")
                
                if wh[2] in ['X-Small', 'Small']:
                    logger.warning("  ⚠️  Small warehouse size may cause slow COPY performance for large files")
                    logger.info("  Consider using a larger warehouse (Medium or Large) for 700MB+ files")
                
    except Exception as e:
        logger.error(f"Failed to check warehouse: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python check_stage_and_performance.py <config.json> [table_name]")
        sys.exit(1)
    
    config_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Load configuration
    config = load_config(config_path)
    
    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**config['snowflake'])
        cursor = conn.cursor()
        logger.info("Connected to Snowflake")
        
        # Set session parameters for better diagnostics
        cursor.execute("ALTER SESSION SET ABORT_DETACHED_QUERY = FALSE")
        
        # 1. Check warehouse configuration
        check_warehouse_size(cursor)
        
        # 2. List all stage contents
        all_files = check_stage_contents(cursor)
        
        # 3. Check specific table stage if provided
        if table_name:
            table_files = check_table_specific_stage(cursor, table_name)
            
            # Offer to clean up old files
            if table_files:
                response = input(f"\nFound {len(table_files)} files for {table_name}. Clean up old files? (y/n): ")
                if response.lower() == 'y':
                    stage_pattern = f"@~/tsv_stage/{table_name}/"
                    remove_old_stage_files(cursor, stage_pattern, dry_run=False)
        
        # 4. Check query history for performance issues
        check_query_history(cursor)
        
        # 5. Provide recommendations
        logger.info("\n" + "="*60)
        logger.info("PERFORMANCE RECOMMENDATIONS:")
        logger.info("="*60)
        
        if all_files:
            logger.info("1. Clean up old stage files to improve performance:")
            logger.info("   REMOVE @~/tsv_stage/;")
        
        logger.info("2. For 700MB+ files, use a MEDIUM or LARGE warehouse:")
        logger.info("   ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'MEDIUM';")
        
        logger.info("3. Consider file format optimizations:")
        logger.info("   - Use COMPRESSION = GZIP for better network transfer")
        logger.info("   - Split very large files into smaller chunks (100-200MB each)")
        
        logger.info("4. Monitor with query profile:")
        logger.info("   SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())")
        logger.info("   WHERE QUERY_ID = '<your_query_id>';")
        
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()