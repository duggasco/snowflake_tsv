#!/bin/bash

# Test the interactive browser fix for parent directory duplication
# This creates a test environment and provides instructions

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Create test directory
TEST_DIR="/tmp/browser_test_$$"
mkdir -p "$TEST_DIR/subdir1/subdir2"
mkdir -p "$TEST_DIR/subdir3"

# Create some test TSV files
echo "Creating test files..."
touch "$TEST_DIR/file1_20240101-20240131.tsv"
touch "$TEST_DIR/file2_2024-01.tsv"
touch "$TEST_DIR/subdir1/data_20240201-20240228.tsv"
touch "$TEST_DIR/subdir1/subdir2/nested_2024-02.tsv"

echo -e "${GREEN}Test environment created at: $TEST_DIR${NC}"
echo ""
echo -e "${BLUE}=== Testing Instructions ===${NC}"
echo -e "${YELLOW}1. The browser will open in the test directory${NC}"
echo -e "${YELLOW}2. Press UP/DOWN arrow keys multiple times${NC}"
echo -e "${YELLOW}3. Verify that only ONE '..' entry appears at the top${NC}"
echo -e "${YELLOW}4. Navigate into 'subdir1' and test arrow keys again${NC}"
echo -e "${YELLOW}5. Press 'q' to quit when done${NC}"
echo ""
echo -e "${BLUE}Starting browser in 3 seconds...${NC}"
sleep 3

# Run the browser
python3 tsv_file_browser.py --start-dir "$TEST_DIR"

# Cleanup
echo -e "${GREEN}Cleaning up test directory...${NC}"
rm -rf "$TEST_DIR"

echo -e "${GREEN}Test complete!${NC}"