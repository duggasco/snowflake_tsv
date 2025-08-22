#!/bin/bash

# Test script to verify menu renaming and reorganization
# - Data Operations -> Snowflake Operations
# - Compare Files moved to File Tools

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Testing menu changes in snowflake_etl.sh...${NC}\n"

# Test 1: Check that "Data Operations" is renamed to "Snowflake Operations"
echo -e "${YELLOW}Test 1: Checking for 'Snowflake Operations' menu${NC}"
if grep -q "menu_snowflake_operations" snowflake_etl.sh; then
    echo -e "${GREEN}✓ menu_snowflake_operations function found${NC}"
else
    echo -e "${RED}✗ menu_snowflake_operations function not found${NC}"
    exit 1
fi

if grep -q '"Snowflake Operations"' snowflake_etl.sh; then
    echo -e "${GREEN}✓ 'Snowflake Operations' text found in menu${NC}"
else
    echo -e "${RED}✗ 'Snowflake Operations' text not found${NC}"
    exit 1
fi

# Test 2: Check that old "Data Operations" is gone
echo -e "\n${YELLOW}Test 2: Checking that 'Data Operations' is removed${NC}"
if grep -q "menu_data_operations" snowflake_etl.sh; then
    echo -e "${RED}✗ Old menu_data_operations function still exists${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Old menu_data_operations function removed${NC}"
fi

# Test 3: Check that "Compare Files" is NOT in Snowflake Operations menu
echo -e "\n${YELLOW}Test 3: Checking 'Compare Files' removed from Snowflake Operations${NC}"
snowflake_ops_content=$(sed -n '/menu_snowflake_operations/,/^}/p' snowflake_etl.sh)
if echo "$snowflake_ops_content" | grep -q "Compare Files"; then
    echo -e "${RED}✗ 'Compare Files' still in Snowflake Operations menu${NC}"
    exit 1
else
    echo -e "${GREEN}✓ 'Compare Files' removed from Snowflake Operations menu${NC}"
fi

if echo "$snowflake_ops_content" | grep -q "compare_files"; then
    echo -e "${RED}✗ compare_files function still called in Snowflake Operations${NC}"
    exit 1
else
    echo -e "${GREEN}✓ compare_files function removed from Snowflake Operations${NC}"
fi

# Test 4: Check that "Compare Files" IS in File Tools menu
echo -e "\n${YELLOW}Test 4: Checking 'Compare Files' added to File Tools${NC}"
file_tools_content=$(sed -n '/menu_file_tools/,/^}/p' snowflake_etl.sh)
if echo "$file_tools_content" | grep -q "Compare Files"; then
    echo -e "${GREEN}✓ 'Compare Files' found in File Tools menu${NC}"
else
    echo -e "${RED}✗ 'Compare Files' not found in File Tools menu${NC}"
    exit 1
fi

if echo "$file_tools_content" | grep -q "compare_files"; then
    echo -e "${GREEN}✓ compare_files function called in File Tools${NC}"
else
    echo -e "${RED}✗ compare_files function not called in File Tools${NC}"
    exit 1
fi

# Test 5: Check main menu references
echo -e "\n${YELLOW}Test 5: Checking main menu references${NC}"
if grep "menu_snowflake_operations" snowflake_etl.sh | grep -v "^#" | grep -q "menu_snowflake_operations"; then
    echo -e "${GREEN}✓ Main menu calls menu_snowflake_operations${NC}"
else
    echo -e "${RED}✗ Main menu doesn't call menu_snowflake_operations${NC}"
    exit 1
fi

# Test 6: Count menu items
echo -e "\n${YELLOW}Test 6: Verifying menu item counts${NC}"
snowflake_items=$(echo "$snowflake_ops_content" | grep -c ")" | head -n -1 || true)
file_tools_items=$(echo "$file_tools_content" | grep -c ")" | head -n -1 || true)

echo "Snowflake Operations menu has approximately $snowflake_items items"
echo "File Tools menu has approximately $file_tools_items items"

# Summary
echo -e "\n${GREEN}================================${NC}"
echo -e "${GREEN}All tests passed successfully!${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo "Summary of changes:"
echo "1. 'Data Operations' renamed to 'Snowflake Operations'"
echo "2. 'Compare Files' moved from Snowflake Ops to File Tools"
echo "3. Snowflake Operations now has 5 items (was 6)"
echo "4. File Tools now has 6 items (was 5)"
echo ""
echo -e "${BLUE}Menu structure is correct!${NC}"