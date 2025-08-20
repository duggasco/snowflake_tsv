#!/bin/bash

set -euo pipefail

VERSION="1.0.0"
SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Default values
OUTPUT_FILE=""
TABLE_NAME=""
COLUMN_HEADERS=""
INTERACTIVE=false
MERGE_CONFIG=""
DRY_RUN=false
SNOWFLAKE_CREDS=""
BASE_PATH="."
DATE_COLUMN="RECORDDATEID"
VERBOSE=false
GENERATE_CREDS_ONLY=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Function to print usage
usage() {
    cat << EOF
Usage: $SCRIPT_NAME [OPTIONS] FILE(s)

Generate config.json for Snowflake TSV Loader

OPTIONS:
    -t, --table TABLE_NAME       Snowflake table name to query for column info
    -o, --output FILE           Output config file (default: stdout)
    -h, --headers "col1,col2"   Comma-separated column headers for headerless files
    -i, --interactive           Interactive mode for Snowflake credentials
    -m, --merge CONFIG          Merge with existing config file
    -c, --creds CONFIG          Use Snowflake credentials from existing config
    -b, --base-path PATH        Base path for file patterns (default: .)
    -d, --date-column NAME      Date column name (default: RECORDDATEID)
    --generate-creds            Generate only Snowflake credentials config
    --dry-run                   Show what would be generated without creating files
    -v, --verbose               Verbose output
    --help                      Show this help message
    --version                   Show version

EXAMPLES:
    # Generate Snowflake credentials only
    $SCRIPT_NAME --generate-creds -o config/snowflake_creds.json

    # Analyze TSV file and generate config
    $SCRIPT_NAME data/file_20240101-20240131.tsv

    # Use Snowflake table to get column names
    $SCRIPT_NAME -t FACTLENDINGBENCHMARK data/*.tsv

    # Interactive mode with output file
    $SCRIPT_NAME -i -o config/my_config.json data/*.tsv

    # Use existing config for credentials
    $SCRIPT_NAME -c config/existing.json -t MY_TABLE data/new_file.tsv

    # Merge with existing config
    $SCRIPT_NAME -m config/base.json data/new_files/*.tsv

EOF
}

# Function to detect file pattern
detect_pattern() {
    local filename="$1"
    local base_name="$(basename "$filename")"
    
    # Remove .tsv extension
    base_name="${base_name%.tsv}"
    
    # Check for date range pattern (YYYYMMDD-YYYYMMDD)
    if [[ "$base_name" =~ ([0-9]{8})-([0-9]{8}) ]]; then
        # Replace the date range with placeholder
        local pattern="${base_name/${BASH_REMATCH[0]}/{date_range}}.tsv"
        echo "$pattern"
        return 0
    fi
    
    # Check for month pattern (YYYY-MM)
    if [[ "$base_name" =~ [0-9]{4}-[0-9]{2} ]]; then
        # Replace the month with placeholder
        local pattern="${base_name/${BASH_REMATCH[0]}/{month}}.tsv"
        echo "$pattern"
        return 0
    fi
    
    # No pattern detected, return original filename
    echo "$(basename "$filename")"
}

# Function to extract table name from file
extract_table_name() {
    local filename="$1"
    local base_name="$(basename "$filename" .tsv)"
    
    # Remove date range patterns (YYYYMMDD-YYYYMMDD)
    base_name=$(echo "$base_name" | sed -E 's/_?[0-9]{8}-[0-9]{8}//g')
    
    # Remove month patterns (YYYY-MM)
    base_name=$(echo "$base_name" | sed -E 's/_?[0-9]{4}-[0-9]{2}//g')
    
    # Convert to uppercase and replace non-alphanumeric with underscore
    echo "$base_name" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]/_/g'
}

# Function to analyze TSV file
analyze_tsv() {
    local file="$1"
    local column_count=0
    local has_header=false
    local sample_lines=()
    
    if [[ ! -f "$file" ]]; then
        print_color "$RED" "Error: File not found: $file"
        return 1
    fi
    
    # Get column count from first line
    if [[ -f "$file" ]]; then
        column_count=$(head -1 "$file" | awk -F'\t' '{print NF}')
    fi
    
    # Check if first line looks like headers (contains non-numeric values)
    local first_line=$(head -1 "$file")
    if echo "$first_line" | grep -qE '[a-zA-Z]'; then
        has_header=true
    fi
    
    # Get sample data (first 5 lines)
    mapfile -t sample_lines < <(head -5 "$file")
    
    echo "$column_count|$has_header"
}

# Function to query Snowflake for column information
query_snowflake_columns() {
    local table_name="$1"
    local creds_file="$2"
    
    if [[ -z "$table_name" ]]; then
        return 1
    fi
    
    # Create Python script to query Snowflake
    local python_script=$(mktemp /tmp/query_columns_XXXXXX.py)
    
    cat > "$python_script" << 'EOPYTHON'
import sys
import json
import snowflake.connector

def query_columns(config_file, table_name):
    try:
        # Load config
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        sf_config = config.get('snowflake', {})
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=sf_config.get('account'),
            user=sf_config.get('user'),
            password=sf_config.get('password'),
            warehouse=sf_config.get('warehouse'),
            database=sf_config.get('database'),
            schema=sf_config.get('schema'),
            role=sf_config.get('role')
        )
        
        cursor = conn.cursor()
        
        # Query column information - include database and schema for precision
        database = sf_config.get('database')
        schema = sf_config.get('schema')
        
        # First try with full qualification
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
        
        # If no results, try without database/schema filters (backwards compatibility)
        if not columns:
            query = f"""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE UPPER(table_name) = UPPER('{table_name}')
            ORDER BY ordinal_position
            """
            cursor.execute(query)
            columns = cursor.fetchall()
        
        if columns:
            column_names = [col[0] for col in columns]
            print(','.join(column_names))
        else:
            print(f"ERROR: Table '{table_name}' not found in {database}.{schema} or no columns")
            sys.exit(1)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py config_file table_name")
        sys.exit(1)
    
    query_columns(sys.argv[1], sys.argv[2])
EOPYTHON
    
    # Execute Python script using venv if available
    if [[ "$VERBOSE" == true ]]; then
        print_color "$BLUE" "Querying Snowflake table: $table_name"
    fi
    
    if [[ -d "$SCRIPT_DIR/test_venv" ]]; then
        local result=$("$SCRIPT_DIR/test_venv/bin/python3" "$python_script" "$creds_file" "$table_name" 2>&1)
    else
        local result=$(python3 "$python_script" "$creds_file" "$table_name" 2>&1)
    fi
    rm -f "$python_script"
    
    if [[ "$result" == ERROR:* ]]; then
        print_color "$YELLOW" "Warning: ${result#ERROR: }"
        if [[ "$VERBOSE" == true ]]; then
            print_color "$YELLOW" "Tip: Check that table exists in the specified database/schema"
            print_color "$YELLOW" "     Or use -h flag to manually specify column names"
        fi
        return 1
    fi
    
    if [[ "$VERBOSE" == true ]] && [[ -n "$result" ]]; then
        local col_count=$(echo "$result" | tr ',' '\n' | wc -l)
        print_color "$GREEN" "Found $col_count columns from Snowflake"
    fi
    
    echo "$result"
}

# Function to generate config JSON
generate_config() {
    local files=("$@")
    local config_json=""
    local files_array="[]"
    
    # Start building config
    config_json='{'
    
    # Add Snowflake section
    if [[ -n "$SNOWFLAKE_CREDS" ]] && [[ -f "$SNOWFLAKE_CREDS" ]]; then
        # Extract Snowflake section from existing config
        local sf_section=$(python3 -c "
import json
with open('$SNOWFLAKE_CREDS', 'r') as f:
    config = json.load(f)
    print(json.dumps(config.get('snowflake', {}), indent=2))
")
        config_json="${config_json}
  \"snowflake\": $sf_section,"
    else
        # Empty Snowflake section
        config_json="${config_json}
  \"snowflake\": {
    \"account\": \"\",
    \"user\": \"\",
    \"password\": \"\",
    \"warehouse\": \"\",
    \"database\": \"\",
    \"schema\": \"\",
    \"role\": \"\"
  },"
    fi
    
    # Process files
    config_json="${config_json}
  \"files\": ["
    
    local first=true
    declare -A seen_patterns=()
    
    for file in "${files[@]}"; do
        if [[ ! -f "$file" ]]; then
            print_color "$YELLOW" "Warning: Skipping non-existent file: $file"
            continue
        fi
        
        # Detect pattern
        local pattern=$(detect_pattern "$file")
        
        # Skip if pattern already processed
        if [[ -n "${seen_patterns[$pattern]:-}" ]]; then
            continue
        fi
        seen_patterns[$pattern]=1
        
        # Get table name
        local table="${TABLE_NAME:-$(extract_table_name "$file")}"
        
        # Get columns
        local columns="[]"
        if [[ -n "$COLUMN_HEADERS" ]]; then
            # Use provided headers
            IFS=',' read -ra cols <<< "$COLUMN_HEADERS"
            columns="["
            for i in "${!cols[@]}"; do
                [[ $i -gt 0 ]] && columns="${columns},"
                columns="${columns}\"${cols[$i]}\""
            done
            columns="${columns}]"
        elif [[ -n "$TABLE_NAME" ]] && [[ -n "$SNOWFLAKE_CREDS" ]]; then
            # Query Snowflake for columns
            local sf_columns=$(query_snowflake_columns "$TABLE_NAME" "$SNOWFLAKE_CREDS")
            if [[ -n "$sf_columns" ]] && [[ "$sf_columns" != ERROR:* ]] && [[ "$sf_columns" != *"Traceback"* ]]; then
                IFS=',' read -ra cols <<< "$sf_columns"
                columns="["
                for i in "${!cols[@]}"; do
                    [[ $i -gt 0 ]] && columns="${columns},"
                    # Escape column names properly
                    local col_name="${cols[$i]}"
                    col_name="${col_name//\\/\\\\}"  # Escape backslashes
                    col_name="${col_name//\"/\\\"}"  # Escape quotes
                    columns="${columns}\"${col_name}\""
                done
                columns="${columns}]"
            fi
        else
            # Analyze file for column count
            local analysis=$(analyze_tsv "$file")
            local col_count=$(echo "$analysis" | cut -d'|' -f1)
            
            # Generate generic column names
            columns="["
            for ((i=1; i<=col_count; i++)); do
                [[ $i -gt 1 ]] && columns="${columns},"
                columns="${columns}\"column${i}\""
            done
            columns="${columns}]"
        fi
        
        # Add file entry
        [[ "$first" == false ]] && config_json="${config_json},"
        config_json="${config_json}
    {
      \"file_pattern\": \"$pattern\",
      \"table_name\": \"$table\",
      \"expected_columns\": $columns,
      \"date_column\": \"$DATE_COLUMN\"
    }"
        first=false
    done
    
    config_json="${config_json}
  ]
}"
    
    echo "$config_json"
}

# Function for interactive mode
interactive_mode() {
    print_color "$BLUE" "=== Interactive Snowflake Configuration ==="
    
    read -p "Snowflake Account: " sf_account
    read -p "Snowflake User: " sf_user
    read -s -p "Snowflake Password: " sf_password
    echo
    read -p "Snowflake Warehouse: " sf_warehouse
    read -p "Snowflake Database: " sf_database
    read -p "Snowflake Schema: " sf_schema
    read -p "Snowflake Role: " sf_role
    
    # Create temp config with credentials
    local temp_config=$(mktemp /tmp/sf_config_XXXXXX.json)
    cat > "$temp_config" << EOF
{
  "snowflake": {
    "account": "$sf_account",
    "user": "$sf_user",
    "password": "$sf_password",
    "warehouse": "$sf_warehouse",
    "database": "$sf_database",
    "schema": "$sf_schema",
    "role": "$sf_role"
  }
}
EOF
    
    SNOWFLAKE_CREDS="$temp_config"
}

# Function to generate credentials config only
generate_credentials_only() {
    print_color "$BLUE" "=== Generate Snowflake Credentials Config ==="
    print_color "$YELLOW" "Enter your Snowflake connection details:"
    echo
    
    read -p "Snowflake Account (e.g., mycompany.us-east-1): " sf_account
    read -p "Snowflake User: " sf_user
    read -s -p "Snowflake Password: " sf_password
    echo
    read -p "Snowflake Warehouse (e.g., COMPUTE_WH): " sf_warehouse
    read -p "Snowflake Database: " sf_database
    read -p "Snowflake Schema (e.g., PUBLIC): " sf_schema
    read -p "Snowflake Role (optional, press Enter to skip): " sf_role
    
    # Build the credentials JSON
    local creds_json='{
  "snowflake": {
    "account": "'$sf_account'",
    "user": "'$sf_user'",
    "password": "'$sf_password'",
    "warehouse": "'$sf_warehouse'",
    "database": "'$sf_database'",
    "schema": "'$sf_schema'"'
    
    # Add role if provided
    if [[ -n "$sf_role" ]]; then
        creds_json="${creds_json},"$'\n    "role": "'$sf_role'"'
    fi
    
    creds_json="${creds_json}"$'\n  }\n}'
    
    echo "$creds_json"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--table)
            TABLE_NAME="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -h|--headers)
            COLUMN_HEADERS="$2"
            shift 2
            ;;
        -i|--interactive)
            INTERACTIVE=true
            shift
            ;;
        -m|--merge)
            MERGE_CONFIG="$2"
            shift 2
            ;;
        -c|--creds)
            SNOWFLAKE_CREDS="$2"
            shift 2
            ;;
        -b|--base-path)
            BASE_PATH="$2"
            shift 2
            ;;
        -d|--date-column)
            DATE_COLUMN="$2"
            shift 2
            ;;
        --generate-creds)
            GENERATE_CREDS_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        --version)
            echo "$SCRIPT_NAME version $VERSION"
            exit 0
            ;;
        -*)
            print_color "$RED" "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

# Handle generate-creds-only mode
if [[ "$GENERATE_CREDS_ONLY" == true ]]; then
    creds_json=$(generate_credentials_only)
    
    if [[ "$DRY_RUN" == true ]]; then
        print_color "$YELLOW" "=== DRY RUN - Generated Credentials Config ==="
        echo "$creds_json" | python3 -m json.tool
    elif [[ -n "$OUTPUT_FILE" ]]; then
        echo "$creds_json" | python3 -m json.tool > "$OUTPUT_FILE"
        print_color "$GREEN" "Credentials config saved to: $OUTPUT_FILE"
    else
        echo "$creds_json" | python3 -m json.tool
    fi
    
    print_color "$GREEN" "Done!"
    exit 0
fi

# Check for files (only if not generating creds)
if [[ $# -eq 0 ]]; then
    print_color "$RED" "Error: No files specified"
    usage
    exit 1
fi

# Interactive mode
if [[ "$INTERACTIVE" == true ]]; then
    interactive_mode
fi

# Generate config
print_color "$GREEN" "Analyzing files..."
config_json=$(generate_config "$@")

# Merge with existing config if specified
if [[ -n "$MERGE_CONFIG" ]] && [[ -f "$MERGE_CONFIG" ]]; then
    print_color "$BLUE" "Merging with existing config..."
    # TODO: Implement merge logic using Python
fi

# Output config
if [[ "$DRY_RUN" == true ]]; then
    print_color "$YELLOW" "=== DRY RUN - Generated Config ==="
    echo "$config_json" | python3 -m json.tool
elif [[ -n "$OUTPUT_FILE" ]]; then
    echo "$config_json" | python3 -m json.tool > "$OUTPUT_FILE"
    print_color "$GREEN" "Config saved to: $OUTPUT_FILE"
else
    echo "$config_json" | python3 -m json.tool
fi

# Cleanup temp files
if [[ -n "$SNOWFLAKE_CREDS" ]] && [[ "$SNOWFLAKE_CREDS" == /tmp/sf_config_* ]]; then
    rm -f "$SNOWFLAKE_CREDS"
fi

print_color "$GREEN" "Done!"