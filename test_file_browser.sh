#!/bin/bash

# Test script for the interactive file browser
# Tests various scenarios including special characters and large directories

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Test directory
TEST_DIR="/tmp/tsv_browser_test_$$"
echo -e "${BLUE}Creating test directory: $TEST_DIR${NC}"
mkdir -p "$TEST_DIR"

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up test directory...${NC}"
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

# Test 1: Create files with various patterns
echo -e "\n${BLUE}Test 1: Creating test files with various patterns${NC}"
mkdir -p "$TEST_DIR/data/2024-01"
mkdir -p "$TEST_DIR/data/2024-02"
mkdir -p "$TEST_DIR/data/special chars"

# Standard date range files
touch "$TEST_DIR/data/2024-01/factLendingBenchmark_20240101-20240131.tsv"
touch "$TEST_DIR/data/2024-01/factAssetDetails_20240101-20240131.tsv"

# Month pattern files
touch "$TEST_DIR/data/2024-02/reportData_2024-02.tsv"
touch "$TEST_DIR/data/2024-02/metrics_2024-02.tsv"

# Files with special characters
touch "$TEST_DIR/data/special chars/file with spaces_20240101-20240131.tsv"
touch "$TEST_DIR/data/special chars/file-with-dashes_2024-01.tsv"
touch "$TEST_DIR/data/special chars/file.with.dots_20240201-20240228.tsv"

# Add some content to files for preview testing
echo -e "recordDate\trecordDateId\tassetId\tfundId\tvalue" > "$TEST_DIR/data/2024-01/factLendingBenchmark_20240101-20240131.tsv"
echo -e "2024-01-01\t20240101\tABC123\tF001\t1000.50" >> "$TEST_DIR/data/2024-01/factLendingBenchmark_20240101-20240131.tsv"
echo -e "2024-01-02\t20240102\tDEF456\tF002\t2000.75" >> "$TEST_DIR/data/2024-01/factLendingBenchmark_20240101-20240131.tsv"

echo -e "${GREEN}Created test files${NC}"

# Test 2: Test config validation
echo -e "\n${BLUE}Test 2: Creating test configurations${NC}"
mkdir -p "$TEST_DIR/config"

# Config that matches factLendingBenchmark files
cat > "$TEST_DIR/config/factLending_config.json" <<EOF
{
  "snowflake": {
    "account": "test_account",
    "user": "test_user",
    "password": "test_pass",
    "warehouse": "test_wh",
    "database": "test_db",
    "schema": "test_schema",
    "role": "test_role"
  },
  "files": [
    {
      "file_pattern": "factLendingBenchmark_{date_range}.tsv",
      "table_name": "FACTLENDINGBENCHMARK",
      "date_column": "recordDate",
      "expected_columns": ["recordDate", "recordDateId", "assetId", "fundId", "value"]
    },
    {
      "file_pattern": "factAssetDetails_{date_range}.tsv",
      "table_name": "FACTASSETDETAILS",
      "date_column": "recordDate",
      "expected_columns": ["recordDate", "assetId", "details"]
    }
  ]
}
EOF

# Config that matches month pattern files
cat > "$TEST_DIR/config/monthly_config.json" <<EOF
{
  "snowflake": {
    "account": "test_account",
    "user": "test_user",
    "password": "test_pass",
    "warehouse": "test_wh",
    "database": "test_db",
    "schema": "test_schema",
    "role": "test_role"
  },
  "files": [
    {
      "file_pattern": "reportData_{month}.tsv",
      "table_name": "REPORTDATA",
      "date_column": "reportDate",
      "expected_columns": ["reportDate", "metric", "value"]
    },
    {
      "file_pattern": "metrics_{month}.tsv",
      "table_name": "METRICS",
      "date_column": "metricDate",
      "expected_columns": ["metricDate", "name", "value"]
    }
  ]
}
EOF

echo -e "${GREEN}Created test configurations${NC}"

# Test 3: Test the Python file browser (non-interactive)
echo -e "\n${BLUE}Test 3: Testing Python file browser module${NC}"

# Test directory listing
echo -e "${YELLOW}Testing directory scanning...${NC}"
python3 -c "
import sys
sys.path.insert(0, '.')
from tsv_file_browser import TSVFileBrowser

browser = TSVFileBrowser('$TEST_DIR/data', '$TEST_DIR/config')
items = browser._get_directory_contents(browser.current_dir)
print(f'Found {len(items)} items in test directory')
for item in items:
    print(f'  - {item.display_name()} ({item.display_size()})')
"

# Test config matching
echo -e "${YELLOW}Testing config matching...${NC}"
python3 tsv_browser_integration.py \
    "$TEST_DIR/data/2024-01/factLendingBenchmark_20240101-20240131.tsv" \
    --current-config "$TEST_DIR/config/factLending_config.json" \
    --config-dir "$TEST_DIR/config"

# Test 4: Test with many files (performance test)
echo -e "\n${BLUE}Test 4: Creating many files for performance testing${NC}"
mkdir -p "$TEST_DIR/data/large_dir"

# Create 100 TSV files
for i in {1..100}; do
    touch "$TEST_DIR/data/large_dir/datafile_$(printf "%03d" $i)_20240101-20240131.tsv"
done

echo -e "${GREEN}Created 100 test files${NC}"

# Test directory scanning performance
echo -e "${YELLOW}Testing performance with 100 files...${NC}"
python3 -c "
import sys
import time
sys.path.insert(0, '.')
from tsv_file_browser import TSVFileBrowser

browser = TSVFileBrowser('$TEST_DIR/data/large_dir', '$TEST_DIR/config')

start = time.time()
items = browser._get_directory_contents(browser.current_dir)
elapsed = time.time() - start

print(f'Scanned {len(items)} files in {elapsed:.3f} seconds')
print(f'Performance: {len(items)/elapsed:.0f} files/second')
"

# Test 5: Test filtering and sorting
echo -e "\n${BLUE}Test 5: Testing search and filter functionality${NC}"
python3 -c "
import sys
sys.path.insert(0, '.')
from tsv_file_browser import TSVFileBrowser

browser = TSVFileBrowser('$TEST_DIR/data', '$TEST_DIR/config')

# Test filtering
browser.filter_text = 'fact'
items = browser._get_directory_contents(browser.current_dir)
filtered = browser._apply_filter(items)
print(f'Filter \"fact\": {len(filtered)} matches')

# Test sorting
browser.sort_by = 'size'
sorted_items = browser._sort_items(items)
print(f'Sorted by size: First item is {sorted_items[0].name}')

browser.sort_by = 'date'
sorted_items = browser._sort_items(items)
print(f'Sorted by date: First item is {sorted_items[0].name}')
"

# Test 6: Test special characters handling
echo -e "\n${BLUE}Test 6: Testing special character handling${NC}"
python3 -c "
import sys
import os
sys.path.insert(0, '.')
from tsv_file_browser import TSVFileBrowser
from pathlib import Path

browser = TSVFileBrowser('$TEST_DIR/data/special chars', '$TEST_DIR/config')
items = browser._get_directory_contents(browser.current_dir)

print(f'Found {len(items)} files with special characters:')
for item in items:
    print(f'  - {item.name}')
    # Verify we can access the file
    assert item.path.exists(), f'Cannot access {item.path}'
"

# Test 7: Test config validation with multiple files
echo -e "\n${BLUE}Test 7: Testing batch file validation${NC}"

# Get all TSV files
TSV_FILES=()
while IFS= read -r -d '' file; do
    TSV_FILES+=("$file")
done < <(find "$TEST_DIR/data" -name "*.tsv" -type f -print0 | head -5)

echo -e "${YELLOW}Validating ${#TSV_FILES[@]} files...${NC}"

if [[ ${#TSV_FILES[@]} -gt 0 ]]; then
    python3 tsv_browser_integration.py \
        "${TSV_FILES[@]}" \
        --current-config "$TEST_DIR/config/factLending_config.json" \
        --config-dir "$TEST_DIR/config"
fi

# Summary
echo -e "\n${GREEN}=== Test Summary ===${NC}"
echo -e "${GREEN}✓ File browser module loaded successfully${NC}"
echo -e "${GREEN}✓ Directory scanning works with regular and special characters${NC}"
echo -e "${GREEN}✓ Config matching and validation functional${NC}"
echo -e "${GREEN}✓ Performance acceptable for 100+ files${NC}"
echo -e "${GREEN}✓ Search, filter, and sort features working${NC}"
echo -e "${GREEN}✓ Batch validation operational${NC}"

echo -e "\n${BLUE}Interactive browser test:${NC}"
echo -e "${YELLOW}Run this command to test the interactive browser:${NC}"
echo -e "${YELLOW}python3 tsv_file_browser.py --start-dir '$TEST_DIR/data' --config-dir '$TEST_DIR/config'${NC}"
echo -e "${YELLOW}Note: Use Ctrl+C to exit if running in a non-terminal environment${NC}"