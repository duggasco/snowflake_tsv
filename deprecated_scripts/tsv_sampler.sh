#!/bin/bash

# TSV File Sampler for Snowflake Config Generation
# This script samples TSV files to help define the configuration JSON
# Usage: ./tsv_sampler.sh <tsv_file> [sample_rows]

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default sample size
SAMPLE_ROWS=${2:-1000}

# Check if file argument provided
if [ $# -lt 1 ]; then
    echo -e "${RED}Error: TSV file path required${NC}"
    echo "Usage: $0 <tsv_file> [sample_rows]"
    echo "Example: $0 data/file_2024-01.tsv 1000"
    exit 1
fi

TSV_FILE="$1"

# Check if file exists
if [ ! -f "$TSV_FILE" ]; then
    echo -e "${RED}Error: File not found: $TSV_FILE${NC}"
    exit 1
fi

# Get file info
FILE_NAME=$(basename "$TSV_FILE")
FILE_SIZE=$(du -h "$TSV_FILE" | cut -f1)
TOTAL_LINES=$(wc -l < "$TSV_FILE")

echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}TSV FILE ANALYZER${NC}"
echo -e "${CYAN}========================================${NC}"
echo -e "${GREEN}File:${NC} $FILE_NAME"
echo -e "${GREEN}Size:${NC} $FILE_SIZE"
echo -e "${GREEN}Total Lines:${NC} $(printf "%'d" $TOTAL_LINES)"
echo -e "${GREEN}Sample Size:${NC} $(printf "%'d" $SAMPLE_ROWS) rows"
echo ""

# Create temp file for sample
TEMP_SAMPLE=$(mktemp /tmp/tsv_sample.XXXXXX)
trap "rm -f $TEMP_SAMPLE" EXIT

# Extract sample
echo -e "${YELLOW}Extracting sample...${NC}"
head -n $SAMPLE_ROWS "$TSV_FILE" > "$TEMP_SAMPLE"

# Count columns (using first line)
NUM_COLS=$(head -1 "$TEMP_SAMPLE" | awk -F'\t' '{print NF}')
echo -e "${GREEN}Number of columns:${NC} $NUM_COLS"
echo ""

# Analyze each column
echo -e "${CYAN}----------------------------------------${NC}"
echo -e "${CYAN}COLUMN ANALYSIS${NC}"
echo -e "${CYAN}----------------------------------------${NC}"

# Arrays to store column info
declare -a COL_TYPES
declare -a COL_SAMPLES
declare -a COL_NULLS
declare -a COL_UNIQUES
declare -a DATE_CANDIDATES

# Function to check if value looks like a date
is_date_value() {
    local value="$1"
    # Check common date patterns
    if [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        return 0  # YYYY-MM-DD
    elif [[ "$value" =~ ^[0-9]{8}$ ]] && [ ${#value} -eq 8 ]; then
        # YYYYMMDD - check if valid date range
        local year=${value:0:4}
        local month=${value:4:2}
        local day=${value:6:2}
        if [ "$year" -ge 1900 ] && [ "$year" -le 2100 ] && \
           [ "$month" -ge 1 ] && [ "$month" -le 12 ] && \
           [ "$day" -ge 1 ] && [ "$day" -le 31 ]; then
            return 0
        fi
    elif [[ "$value" =~ ^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$ ]]; then
        return 0  # MM/DD/YYYY
    fi
    return 1
}

# Function to infer column type
infer_type() {
    local col_num=$1
    local all_numeric=true
    local all_integer=true
    local all_dates=true
    local has_values=false
    local sample_value=""
    
    # Sample up to 100 non-null values
    local count=0
    while IFS= read -r value; do
        # Skip empty values
        if [ -z "$value" ] || [ "$value" = "NULL" ] || [ "$value" = "null" ] || [ "$value" = "\\N" ]; then
            continue
        fi
        
        has_values=true
        if [ -z "$sample_value" ]; then
            sample_value="$value"
        fi
        
        # Check if numeric
        if ! [[ "$value" =~ ^-?[0-9]+\.?[0-9]*$ ]]; then
            all_numeric=false
        fi
        
        # Check if integer
        if ! [[ "$value" =~ ^-?[0-9]+$ ]]; then
            all_integer=false
        fi
        
        # Check if date
        if ! is_date_value "$value"; then
            all_dates=false
        fi
        
        count=$((count + 1))
        if [ $count -ge 100 ]; then
            break
        fi
    done < <(tail -n +1 "$TEMP_SAMPLE" | cut -f$col_num)
    
    # Determine type
    if [ "$has_values" = false ]; then
        echo "UNKNOWN"
    elif [ "$all_dates" = true ]; then
        echo "DATE"
    elif [ "$all_integer" = true ]; then
        echo "INTEGER"
    elif [ "$all_numeric" = true ]; then
        echo "FLOAT"
    else
        echo "VARCHAR"
    fi
}

# Analyze each column
for (( i=1; i<=$NUM_COLS; i++ )); do
    echo -e "${BLUE}Column $i:${NC}"
    
    # Get sample values
    SAMPLE_VALUES=$(cut -f$i "$TEMP_SAMPLE" | head -5 | tr '\n' '|' | sed 's/|$//')
    echo "  Sample values: $SAMPLE_VALUES"
    
    # Count nulls
    NULL_COUNT=$(cut -f$i "$TEMP_SAMPLE" | grep -c -E '^$|^NULL$|^null$|^\\N$' || true)
    NULL_PCT=$(awk "BEGIN {printf \"%.1f\", $NULL_COUNT * 100 / $SAMPLE_ROWS}")
    echo "  Null count: $NULL_COUNT ($NULL_PCT%)"
    
    # Count unique values
    UNIQUE_COUNT=$(cut -f$i "$TEMP_SAMPLE" | sort -u | wc -l)
    echo "  Unique values: $UNIQUE_COUNT"
    
    # Infer type
    COL_TYPE=$(infer_type $i)
    echo "  Inferred type: $COL_TYPE"
    
    # Check if likely a date column
    if [ "$COL_TYPE" = "DATE" ]; then
        DATE_CANDIDATES+=($i)
        echo -e "  ${YELLOW}[NOTE] Likely date column${NC}"
    fi
    
    # Store column info
    COL_TYPES[$i]="$COL_TYPE"
    COL_SAMPLES[$i]="$SAMPLE_VALUES"
    COL_NULLS[$i]="$NULL_COUNT"
    COL_UNIQUES[$i]="$UNIQUE_COUNT"
    
    echo ""
done

# Extract date pattern from filename
echo -e "${CYAN}----------------------------------------${NC}"
echo -e "${CYAN}FILE PATTERN ANALYSIS${NC}"
echo -e "${CYAN}----------------------------------------${NC}"

FILE_PATTERN=""
DATE_TYPE=""

# Check for date range pattern (YYYYMMDD-YYYYMMDD)
if [[ "$FILE_NAME" =~ ([0-9]{8})-([0-9]{8}) ]]; then
    START_DATE="${BASH_REMATCH[1]}"
    END_DATE="${BASH_REMATCH[2]}"
    FILE_PATTERN="${FILE_NAME/$START_DATE-$END_DATE/{date_range}}"
    DATE_TYPE="date_range"
    echo -e "${GREEN}Date range detected:${NC} $START_DATE to $END_DATE"
    echo -e "${GREEN}Pattern:${NC} $FILE_PATTERN"
# Check for month pattern (YYYY-MM)
elif [[ "$FILE_NAME" =~ ([0-9]{4})-([0-9]{2}) ]]; then
    YEAR="${BASH_REMATCH[1]}"
    MONTH="${BASH_REMATCH[2]}"
    FILE_PATTERN="${FILE_NAME/$YEAR-$MONTH/{month}}"
    DATE_TYPE="month"
    echo -e "${GREEN}Month pattern detected:${NC} $YEAR-$MONTH"
    echo -e "${GREEN}Pattern:${NC} $FILE_PATTERN"
else
    echo -e "${YELLOW}No date pattern detected in filename${NC}"
    FILE_PATTERN="$FILE_NAME"
fi

# Generate table name suggestion
TABLE_NAME=$(echo "$FILE_NAME" | sed -E 's/[0-9]{8}-[0-9]{8}|[0-9]{4}-[0-9]{2}//g' | \
             sed 's/\.tsv$//' | sed 's/^_//;s/_$//' | tr '[:lower:]' '[:upper:]')
if [ -z "$TABLE_NAME" ]; then
    TABLE_NAME="TABLE_NAME"
fi

echo ""
echo -e "${CYAN}----------------------------------------${NC}"
echo -e "${CYAN}SUGGESTED CONFIGURATION${NC}"
echo -e "${CYAN}----------------------------------------${NC}"

# Generate column names
COLUMN_NAMES=""
for (( i=1; i<=$NUM_COLS; i++ )); do
    if [ $i -eq 1 ]; then
        COLUMN_NAMES="\"column_$i\""
    else
        COLUMN_NAMES="$COLUMN_NAMES, \"column_$i\""
    fi
done

# Determine date column
DATE_COLUMN="column_1"
if [ ${#DATE_CANDIDATES[@]} -gt 0 ]; then
    DATE_COLUMN="column_${DATE_CANDIDATES[0]}"
    echo -e "${GREEN}Suggested date column:${NC} $DATE_COLUMN (column ${DATE_CANDIDATES[0]})"
else
    echo -e "${YELLOW}No obvious date column found. Using column_1 as default.${NC}"
fi

# Generate JSON config snippet
cat << EOF

{
  "file_pattern": "$FILE_PATTERN",
  "table_name": "$TABLE_NAME",
  "expected_columns": [$COLUMN_NAMES],
  "date_column": "$DATE_COLUMN"
}

EOF

echo -e "${CYAN}----------------------------------------${NC}"
echo -e "${CYAN}COLUMN TYPE MAPPING${NC}"
echo -e "${CYAN}----------------------------------------${NC}"
echo ""
echo "Suggested Snowflake DDL:"
echo ""
echo "CREATE TABLE IF NOT EXISTS $TABLE_NAME ("
for (( i=1; i<=$NUM_COLS; i++ )); do
    COL_TYPE_SQL="VARCHAR"
    case "${COL_TYPES[$i]}" in
        "INTEGER")
            COL_TYPE_SQL="NUMBER(38,0)"
            ;;
        "FLOAT")
            COL_TYPE_SQL="FLOAT"
            ;;
        "DATE")
            COL_TYPE_SQL="DATE"
            ;;
        *)
            COL_TYPE_SQL="VARCHAR"
            ;;
    esac
    
    if [ $i -eq $NUM_COLS ]; then
        echo "    column_$i $COL_TYPE_SQL"
    else
        echo "    column_$i $COL_TYPE_SQL,"
    fi
done
echo ");"
echo ""

# Summary
echo -e "${CYAN}========================================${NC}"
echo -e "${GREEN}[DONE] Analysis complete!${NC}"
echo ""
echo "Next steps:"
echo "1. Review the suggested configuration above"
echo "2. Update column names to match your business logic"
echo "3. Verify the date column is correct"
echo "4. Add this to your config.json file"
echo ""

# If date columns found, show them
if [ ${#DATE_CANDIDATES[@]} -gt 0 ]; then
    echo -e "${YELLOW}Note: Found ${#DATE_CANDIDATES[@]} potential date column(s):${NC}"
    for col in "${DATE_CANDIDATES[@]}"; do
        echo "  - Column $col"
    done
    echo ""
fi