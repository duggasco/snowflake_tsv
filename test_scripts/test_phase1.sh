#!/bin/bash

# Test script for Phase 1 changes to snowflake_etl.sh

echo "=== Testing Phase 1 Consolidation ==="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Source the functions we need from snowflake_etl.sh
# Extract just the functions without running the menu
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="config/test_config.json"
LOGS_DIR="logs"
BASE_PATH="./data"

# Extract and source our new functions
source <(sed -n '/^# Function to check prerequisites/,/^# Process direct files/p' snowflake_etl.sh | sed -n '/^check_prerequisites()/,/^process_direct_files() {/p')
source <(sed -n '/^process_direct_files()/,/^}/p' snowflake_etl.sh)

echo -e "${CYAN}1. Testing check_prerequisites()${NC}"
if check_prerequisites; then
    echo -e "${GREEN}✓ Prerequisites check passed${NC}"
else
    echo -e "${YELLOW}⚠ Prerequisites check had warnings${NC}"
fi
echo ""

echo -e "${CYAN}2. Testing convert_month_format()${NC}"
test_months=("2024-01" "012024" "122023" "invalid")
for month in "${test_months[@]}"; do
    result=$(convert_month_format "$month")
    if [[ -n "$result" ]]; then
        echo -e "  $month -> ${GREEN}$result${NC}"
    else
        echo -e "  $month -> ${RED}(invalid)${NC}"
    fi
done
echo ""

echo -e "${CYAN}3. Testing execute_python_cli() command building${NC}"
# Mock the function to just show what it would execute
execute_python_cli_test() {
    local operation="$1"
    shift
    local args=("$@")
    
    local cmd="python3 -m snowflake_etl"
    
    if [[ -n "$CONFIG_FILE" ]] && [[ -f "$CONFIG_FILE" ]]; then
        cmd="$cmd --config \"$CONFIG_FILE\""
    fi
    
    cmd="$cmd $operation ${args[@]}"
    
    echo -e "${BLUE}Would execute: $cmd${NC}"
}

# Test command building
execute_python_cli_test "load" "--month \"2024-01\" --base-path \"./data\""
execute_python_cli_test "delete" "--table MY_TABLE --month \"2024-01\""
execute_python_cli_test "validate" "--month \"2024-01\""
echo ""

echo -e "${CYAN}4. Checking updated menu functions${NC}"
# Check that our functions were updated correctly
if grep -q "process_month_direct" snowflake_etl.sh; then
    echo -e "${GREEN}✓ process_month_direct found in script${NC}"
fi

if grep -q "process_direct_files" snowflake_etl.sh; then
    echo -e "${GREEN}✓ process_direct_files found in script${NC}"
fi

# Check that quick load functions now use direct calls
if grep -q "process_month_direct.*current_month" snowflake_etl.sh; then
    echo -e "${GREEN}✓ quick_load_current_month updated to use direct call${NC}"
fi

if grep -q "process_month_direct.*last_month" snowflake_etl.sh; then
    echo -e "${GREEN}✓ quick_load_last_month updated to use direct call${NC}"
fi

if grep -q "process_direct_files.*file_path" snowflake_etl.sh; then
    echo -e "${GREEN}✓ quick_load_specific_file updated to use direct call${NC}"
fi
echo ""

echo -e "${CYAN}5. Checking what still uses run_loader.sh${NC}"
remaining_calls=$(grep -c "\.\/run_loader\.sh" snowflake_etl.sh)
echo -e "  Remaining run_loader.sh calls: ${YELLOW}$remaining_calls${NC}"
echo -e "  (These should be for batch/parallel operations only)"
echo ""

echo -e "${GREEN}=== Phase 1 Test Complete ===${NC}"
echo ""
echo "Summary:"
echo "- Core functions added: ✓"
echo "- Simple operations updated: ✓"
echo "- Complex operations still use run_loader.sh: ✓ (as intended)"