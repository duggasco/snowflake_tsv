#!/bin/bash

# Recovery script for failed COPY operations
# Handles Snowflake internal errors and provides recovery options
#
# ============================================================================
# DEPRECATION WARNING
# ============================================================================
# This script is DEPRECATED as of v3.0.0
# Recovery functionality is now integrated into the main CLI.
# Please use the interactive wrapper:
#   ./snowflake_etl.sh
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}============================================================${NC}"
echo -e "${YELLOW}DEPRECATION WARNING${NC}"
echo -e "${YELLOW}This script is deprecated as of v3.0.0${NC}"
echo -e "${YELLOW}Recovery features are now in: ./snowflake_etl.sh${NC}"
echo -e "${YELLOW}============================================================${NC}"
echo ""

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}SNOWFLAKE COPY ERROR RECOVERY TOOL${NC}"
echo -e "${BLUE}============================================================${NC}"

# Default values
CONFIG=""
TABLE=""
MONTH=""
ACTION="diagnose"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG="$2"
            shift 2
            ;;
        --table)
            TABLE="$2"
            shift 2
            ;;
        --month)
            MONTH="$2"
            shift 2
            ;;
        --action)
            ACTION="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --config <config.json> --table <table_name> --month <YYYY-MM> --action <diagnose|cleanup|retry|split>"
            echo ""
            echo "Actions:"
            echo "  diagnose - Run diagnostic checks (default)"
            echo "  cleanup  - Remove partial data and stage files"
            echo "  retry    - Retry with more robust settings"
            echo "  split    - Split file and load in chunks"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$CONFIG" ]; then
    echo -e "${RED}Error: --config is required${NC}"
    exit 1
fi

case "$ACTION" in
    diagnose)
        echo -e "\n${YELLOW}Running diagnostics...${NC}"
        python diagnose_copy_error.py "$CONFIG"
        
        echo -e "\n${YELLOW}Checking for partial loads...${NC}"
        if [ ! -z "$TABLE" ] && [ ! -z "$MONTH" ]; then
            echo "Checking $TABLE for month $MONTH..."
            # Extract year and month
            YEAR=$(echo $MONTH | cut -d'-' -f1)
            MON=$(echo $MONTH | cut -d'-' -f2)
            
            python -c "
import snowflake.connector
import json

config = json.load(open('$CONFIG'))
conn = snowflake.connector.connect(**config['snowflake'])
cursor = conn.cursor()

# First check the data type of RECORDDATE
check_type = '''
SELECT DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = '$TABLE' 
  AND COLUMN_NAME = 'RECORDDATE'
'''
cursor.execute(check_type)
data_type = cursor.fetchone()[0] if cursor.rowcount else 'UNKNOWN'
print(f'RECORDDATE column type: {data_type}')

# Use appropriate query based on data type
if 'VARCHAR' in data_type or 'CHAR' in data_type or 'TEXT' in data_type or 'STRING' in data_type:
    # For VARCHAR date columns
    query = '''
    SELECT COUNT(*) as rows,
           MIN(RECORDDATE) as min_date,
           MAX(RECORDDATE) as max_date
    FROM $TABLE
    WHERE RECORDDATE LIKE '$YEAR-$MON-%'
       OR RECORDDATE LIKE '$YEAR$MON%'
    '''
else:
    # For DATE columns
    query = '''
    SELECT COUNT(*) as rows,
           MIN(RECORDDATE) as min_date,
           MAX(RECORDDATE) as max_date
    FROM $TABLE
    WHERE RECORDDATE >= '$YEAR-$MON-01'
      AND RECORDDATE < DATEADD(MONTH, 1, '$YEAR-$MON-01')
    '''

cursor.execute(query)
result = cursor.fetchone()
if result[0] > 0:
    print(f'Found {result[0]:,} rows for $MONTH')
    print(f'Date range: {result[1]} to {result[2]}')
else:
    print('No data found for $MONTH')

cursor.close()
conn.close()
"
        fi
        ;;
        
    cleanup)
        echo -e "\n${YELLOW}Cleaning up partial data and stage files...${NC}"
        
        if [ -z "$TABLE" ] || [ -z "$MONTH" ]; then
            echo -e "${RED}Error: --table and --month required for cleanup${NC}"
            exit 1
        fi
        
        # Extract year and month
        YEAR=$(echo $MONTH | cut -d'-' -f1)
        MON=$(echo $MONTH | cut -d'-' -f2)
        
        echo -e "${YELLOW}This will delete data for $MONTH from $TABLE${NC}"
        read -p "Are you sure? (yes/no): " CONFIRM
        
        if [ "$CONFIRM" == "yes" ]; then
            python -c "
import snowflake.connector
import json

config = json.load(open('$CONFIG'))
conn = snowflake.connector.connect(**config['snowflake'])
cursor = conn.cursor()

# First check the data type of RECORDDATE
check_type = '''
SELECT DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = '$TABLE' 
  AND COLUMN_NAME = 'RECORDDATE'
'''
cursor.execute(check_type)
data_type = cursor.fetchone()[0] if cursor.rowcount else 'UNKNOWN'
print(f'RECORDDATE column type: {data_type}')

# Use appropriate DELETE query based on data type
if 'VARCHAR' in data_type or 'CHAR' in data_type or 'TEXT' in data_type or 'STRING' in data_type:
    # For VARCHAR date columns - handle multiple formats
    delete_query = '''
    DELETE FROM $TABLE
    WHERE RECORDDATE LIKE '$YEAR-$MON-%'     -- Format: 2023-08-01
       OR RECORDDATE LIKE '$YEAR$MON%'       -- Format: 20230801
       OR RECORDDATE LIKE '%-$MON-$YEAR'     -- Format: 01-08-2023
       OR RECORDDATE LIKE '$MON/%/$YEAR'     -- Format: 08/01/2023
    '''
else:
    # For DATE columns
    delete_query = '''
    DELETE FROM $TABLE
    WHERE RECORDDATE >= '$YEAR-$MON-01'
      AND RECORDDATE < DATEADD(MONTH, 1, '$YEAR-$MON-01')
    '''

print('Deleting partial data...')
result = cursor.execute(delete_query)
deleted = result.rowcount if result else 0
print(f'Deleted {deleted:,} rows')

# Clean up stage
stage_query = '''
REMOVE @~/tsv_stage/$TABLE/
'''

print('Cleaning up stage...')
try:
    cursor.execute(stage_query)
    print('Stage cleaned')
except Exception as e:
    print(f'Stage cleanup note: {e}')

cursor.close()
conn.close()
print('Cleanup complete')
"
        else
            echo "Cleanup cancelled"
        fi
        ;;
        
    retry)
        echo -e "\n${YELLOW}Retrying with more robust settings...${NC}"
        
        if [ -z "$MONTH" ]; then
            echo -e "${RED}Error: --month required for retry${NC}"
            exit 1
        fi
        
        # First ensure we're using a large enough warehouse
        echo -e "${BLUE}Setting warehouse to LARGE...${NC}"
        python -c "
import snowflake.connector
import json

config = json.load(open('$CONFIG'))
conn = snowflake.connector.connect(**config['snowflake'])
cursor = conn.cursor()

cursor.execute('ALTER SESSION SET USE_WAREHOUSE = \\'{}\\';'.format(config['snowflake']['warehouse']))
cursor.execute('ALTER WAREHOUSE {} SET WAREHOUSE_SIZE = \\'LARGE\\';'.format(config['snowflake']['warehouse']))
print('Warehouse set to LARGE')

cursor.close()
conn.close()
"
        
        # Run with special flags
        echo -e "${BLUE}Retrying load with robust settings...${NC}"
        python tsv_loader.py \
            --config "$CONFIG" \
            --month "$MONTH" \
            --skip-qc \
            --validate-in-snowflake
        ;;
        
    split)
        echo -e "\n${YELLOW}Splitting file into smaller chunks...${NC}"
        
        if [ -z "$MONTH" ]; then
            echo -e "${RED}Error: --month required for split${NC}"
            exit 1
        fi
        
        # Find the file
        BASE_PATH=$(python -c "
import json
config = json.load(open('$CONFIG'))
# Try to find base path from previous runs
import os
for root, dirs, files in os.walk('.'):
    for d in dirs:
        if '$MONTH'.replace('-', '') in d:
            print(os.path.dirname(os.path.join(root, d)))
            break
")
        
        if [ -z "$BASE_PATH" ]; then
            BASE_PATH="./data"
        fi
        
        echo "Looking for files in $BASE_PATH..."
        
        # Find TSV file for the month
        TSV_FILE=$(find "$BASE_PATH" -name "*${MONTH//-/}*.tsv" -o -name "*${MONTH}*.tsv" | head -1)
        
        if [ -z "$TSV_FILE" ]; then
            echo -e "${RED}Could not find TSV file for month $MONTH${NC}"
            exit 1
        fi
        
        echo "Found file: $TSV_FILE"
        
        # Split the file
        echo "Splitting into 1GB chunks..."
        split -b 1G "$TSV_FILE" "${TSV_FILE}.part_"
        
        echo "Created chunks:"
        ls -lh "${TSV_FILE}.part_"*
        
        echo -e "\n${GREEN}Now load each chunk separately:${NC}"
        for CHUNK in "${TSV_FILE}.part_"*; do
            echo "  ./run_loader.sh --direct-file $CHUNK --skip-qc"
        done
        ;;
        
    *)
        echo -e "${RED}Unknown action: $ACTION${NC}"
        exit 1
        ;;
esac

echo -e "\n${BLUE}============================================================${NC}"
echo -e "${GREEN}Recovery process complete${NC}"
echo -e "${BLUE}============================================================${NC}"

# Provide incident information
echo -e "\n${YELLOW}If the error persists, contact Snowflake Support with:${NC}"
echo "  - Incident Number: 5452401"
echo "  - Error Code: 000603"
echo "  - Internal Error: 300005:1759998092"
echo "  - Query ID from logs"