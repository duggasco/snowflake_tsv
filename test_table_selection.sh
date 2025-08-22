#!/bin/bash

# Test script for table selection functionality

set -euo pipefail

# Source the necessary functions from snowflake_etl.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config/factLendingBenchmark_config.json"

# Extract the get_tables_from_config function
get_tables_from_config() {
    local config_file="${1:-$CONFIG_FILE}"
    
    if [[ ! -f "$config_file" ]]; then
        echo ""
        return 1
    fi
    
    # Extract table names from the JSON config
    python3 -c "
import json
import sys

try:
    with open('$config_file', 'r') as f:
        config = json.load(f)
    
    tables = []
    if 'files' in config:
        for file_config in config['files']:
            if 'table_name' in file_config:
                tables.append(file_config['table_name'])
    
    # Return unique tables
    print(' '.join(sorted(set(tables))))
except Exception as e:
    sys.exit(1)
" 2>/dev/null
}

echo "Testing table extraction from config files..."
echo "============================================="
echo

# Test with factLendingBenchmark_config.json
echo "Config: factLendingBenchmark_config.json"
tables=$(get_tables_from_config "$CONFIG_FILE")
if [[ -n "$tables" ]]; then
    echo "Found tables: $tables"
else
    echo "No tables found or error occurred"
fi
echo

# Test with generated_config.json
CONFIG_FILE="${SCRIPT_DIR}/config/generated_config.json"
echo "Config: generated_config.json"
tables=$(get_tables_from_config "$CONFIG_FILE")
if [[ -n "$tables" ]]; then
    echo "Found tables: $tables"
else
    echo "No tables found or error occurred"
fi
echo

# Test with test configs
for config in "${SCRIPT_DIR}/config/test_config_"*.json; do
    if [[ -f "$config" ]]; then
        echo "Config: $(basename "$config")"
        tables=$(get_tables_from_config "$config")
        if [[ -n "$tables" ]]; then
            echo "Found tables: $tables"
        else
            echo "No tables found or error occurred"
        fi
        echo
    fi
done

echo "Test completed!"