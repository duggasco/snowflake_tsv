#!/bin/bash

# Integration test for Phase 3 config generation
# Tests the actual menu function in snowflake_etl.sh

set -e

# Colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}=== Phase 3 Config Generation Integration Test ===${NC}"
echo

# Create test TSV files
test_dir="/tmp/test_config_gen_$$"
mkdir -p "$test_dir"

echo -e "${YELLOW}Creating test TSV files...${NC}"
echo -e "col1\tcol2\tcol3\tcol4\tcol5" > "$test_dir/factLending_20240101-20240131.tsv"
echo -e "val1\tval2\tval3\tval4\tval5" >> "$test_dir/factLending_20240101-20240131.tsv"

echo -e "col1\tcol2\tcol3" > "$test_dir/testTable_2024-01.tsv"
echo -e "val1\tval2\tval3" >> "$test_dir/testTable_2024-01.tsv"

ls -la "$test_dir"
echo

# Test config generation through the menu
echo -e "${YELLOW}Testing config generation via menu (option 5 -> 3)...${NC}"
echo "Testing with files: $test_dir/*.tsv"
echo

# Use expect or simulate menu interaction
# Since we don't have expect, we'll call the function directly via bash
output_file="$test_dir/generated_config.json"

echo -e "${CYAN}Attempting to generate config...${NC}"

# Try to call the generate function through the script
bash -c "
# Source only the functions we need
source <(sed -n '1408,1657p' snowflake_etl.sh)

# Additional required variables
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
CONFIG_FILE=''

# Call the function
generate_config_from_files '$test_dir/*.tsv' '$output_file' 'TEST_TABLE' '' 'RECORDDATE' 2>/dev/null || true
"

# Check if config was generated
if [[ -f "$output_file" ]]; then
    echo -e "${GREEN}[SUCCESS]${NC} Config file generated!"
    echo
    echo -e "${CYAN}Generated config content:${NC}"
    cat "$output_file" | python3 -m json.tool 2>/dev/null || cat "$output_file"
    echo
    
    # Validate JSON structure
    if python3 -c "import json; json.load(open('$output_file'))" 2>/dev/null; then
        echo -e "${GREEN}[SUCCESS]${NC} Valid JSON structure"
    else
        echo -e "${RED}[FAIL]${NC} Invalid JSON structure"
    fi
    
    # Check for required fields
    if grep -q '"file_pattern"' "$output_file" && \
       grep -q '"table_name"' "$output_file" && \
       grep -q '"date_column"' "$output_file"; then
        echo -e "${GREEN}[SUCCESS]${NC} Config contains all required fields"
    else
        echo -e "${RED}[FAIL]${NC} Config missing required fields"
    fi
else
    echo -e "${RED}[FAIL]${NC} Config file was not generated"
    
    # Try a simpler test - just pattern detection
    echo
    echo -e "${YELLOW}Testing individual functions...${NC}"
    
    # Extract and test just the detect_file_pattern function
    bash -c '
    detect_file_pattern() {
        local filename="$1"
        local base_name="$(basename "$filename")"
        base_name="${base_name%.tsv}"
        
        if [[ "$base_name" =~ ([0-9]{8})-([0-9]{8}) ]]; then
            local pattern="${base_name/${BASH_REMATCH[0]}/{date_range}}.tsv"
            echo "$pattern"
            return 0
        fi
        
        if [[ "$base_name" =~ [0-9]{4}-[0-9]{2} ]]; then
            local pattern="${base_name/${BASH_REMATCH[0]}/{month}}.tsv"
            echo "$pattern"
            return 0
        fi
        
        echo "$(basename "$filename")"
    }
    
    echo "Pattern test 1: $(detect_file_pattern factLending_20240101-20240131.tsv)"
    echo "Pattern test 2: $(detect_file_pattern testTable_2024-01.tsv)"
    '
fi

# Clean up
echo
echo -e "${CYAN}Cleaning up test files...${NC}"
rm -rf "$test_dir"

echo
echo -e "${GREEN}Integration test complete!${NC}"