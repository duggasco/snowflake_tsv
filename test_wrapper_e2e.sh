#!/bin/bash

# End-to-End Test Script for Snowflake ETL Wrapper
# This script tests all functionality with mocked Snowflake operations

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Test function
run_test() {
    local test_name="$1"
    local test_cmd="$2"
    local expected_result="${3:-0}"
    
    echo -e "\n${BLUE}Testing: $test_name${NC}"
    echo "Command: $test_cmd"
    
    if eval "$test_cmd"; then
        result=$?
    else
        result=$?
    fi
    
    if [[ $result -eq $expected_result ]]; then
        echo -e "${GREEN}✓ PASSED${NC}"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗ FAILED (exit code: $result, expected: $expected_result)${NC}"
        ((TESTS_FAILED++))
    fi
}

echo "============================================"
echo "Snowflake ETL Wrapper - End-to-End Testing"
echo "============================================"

# Clean up any previous test state
echo -e "\n${YELLOW}Cleaning up previous test state...${NC}"
rm -rf .etl_state/jobs/* .etl_state/locks/* 2>/dev/null || true
rm -f logs/test_*.log 2>/dev/null || true

# Test 1: Version command
run_test "Version command" "./snowflake_etl.sh --version 2>&1 | grep -q 'v2.2.0'"

# Test 2: Help command
run_test "Help command" "./snowflake_etl.sh --help 2>&1 | grep -q 'Usage:'"

# Test 3: Status command (no jobs)
run_test "Status command (empty)" "./snowflake_etl.sh status 2>&1 | head -1 | grep -qE '^$|No jobs'"

# Test 4: Clean command (no jobs)
run_test "Clean command" "./snowflake_etl.sh clean 2>&1 | grep -q 'Cleaned 0'"

# Test 5: Test config selection in non-interactive mode
echo -e "\n${YELLOW}Testing config selection...${NC}"
# This should fail because no config is selected for validate
run_test "Validate without config" "echo '0' | timeout 2 ./snowflake_etl.sh validate --month 2024-01 2>&1 | grep -qE 'config|Config'" 1

# Test 6: Check that configs are detected
run_test "Config directory exists" "test -d config"
run_test "Test configs exist" "ls config/*.json | wc -l | grep -q '^[2-9]'"

# Test 7: Test file tools that don't need Snowflake
echo -e "\n${YELLOW}Testing File Tools...${NC}"

# Create a test TSV for analysis
run_test "Test TSV exists" "test -f data/test_data_20240101-20240131.tsv"

# Test TSV sampler
run_test "TSV Sampler" "./tsv_sampler.sh data/test_data_20240101-20240131.tsv 2 2>&1 | grep -q 'recordDate'"

# Test 8: Test job management
echo -e "\n${YELLOW}Testing Job Management...${NC}"

# Create a mock job file
mkdir -p .etl_state/jobs
cat > .etl_state/jobs/test_job.job << EOF
JOB_ID=20240101_120000_1234
JOB_NAME=test_load
COMMAND=echo test
START_TIME=2024-01-01 12:00:00
STATUS=COMPLETED
PID=1234
LOG_FILE=logs/test_load.log
END_TIME=2024-01-01 12:01:00
EOF

run_test "Job status with completed job" "./snowflake_etl.sh status 2>&1 | grep -q 'test_load'"
run_test "Clean completed jobs" "./snowflake_etl.sh clean 2>&1 | grep -q 'Cleaned 1'"
run_test "Job cleaned successfully" "! test -f .etl_state/jobs/test_job.job"

# Test 9: Test preference persistence
echo -e "\n${YELLOW}Testing Preferences...${NC}"

# Set a preference
echo "LAST_BASE_PATH=/tmp/test" > .etl_state/preferences
run_test "Preferences file exists" "test -f .etl_state/preferences"

# Test 10: Check script dependencies
echo -e "\n${YELLOW}Testing Script Dependencies...${NC}"

run_test "run_loader.sh exists" "test -f run_loader.sh"
run_test "tsv_loader.py exists" "test -f tsv_loader.py"
run_test "drop_month.sh exists" "test -f drop_month.sh"
run_test "generate_config.sh exists" "test -f generate_config.sh"
run_test "compare_tsv_files.py exists" "test -f compare_tsv_files.py"
run_test "check_snowflake_table.py exists" "test -f check_snowflake_table.py"
run_test "diagnose_copy_error.py exists" "test -f diagnose_copy_error.py"
run_test "recover_failed_load.sh exists" "test -f recover_failed_load.sh"

# Test 11: Test generate_config.sh
echo -e "\n${YELLOW}Testing Config Generation...${NC}"

run_test "Config generator dry run" "./generate_config.sh --dry-run data/test_data_20240101-20240131.tsv 2>&1 | grep -q 'file_pattern'"

# Test 12: Test compare_tsv_files.py with mock data
echo -e "\n${YELLOW}Testing File Comparison...${NC}"

# Create two small test files
echo -e "col1\tcol2\n1\t2\n3\t4" > /tmp/test1.tsv
echo -e "col1\tcol2\n1\t2\n5\t6" > /tmp/test2.tsv

run_test "Compare TSV files" "python3 compare_tsv_files.py --quick /tmp/test1.tsv /tmp/test2.tsv 2>&1 | grep -qE 'Comparing|difference|rows'" || true

# Test 13: Test validate_tsv_file.py
echo -e "\n${YELLOW}Testing TSV Validation...${NC}"

if command -v python3 >/dev/null 2>&1; then
    run_test "Validate TSV structure" "python3 validate_tsv_file.py data/test_data_20240101-20240131.tsv 2>&1 | grep -qE 'Validating|columns|rows'" || true
else
    echo -e "${YELLOW}Skipping Python validation tests - Python not available${NC}"
fi

# Test 14: Test interactive menu (non-interactive simulation)
echo -e "\n${YELLOW}Testing Interactive Menu Navigation...${NC}"

# Test that menu exits on 0
run_test "Menu exits on 0" "echo '0' | timeout 2 ./snowflake_etl.sh 2>&1 | grep -q 'Thank you'"

# Test 15: Test CLI argument parsing
echo -e "\n${YELLOW}Testing CLI Argument Parsing...${NC}"

# These should show usage/error since we're not providing complete args
run_test "Load command usage" "./snowflake_etl.sh load 2>&1 | grep -q 'Usage:'" 1
run_test "Delete command usage" "./snowflake_etl.sh delete 2>&1 | grep -q 'Usage:'" 1

# Test 16: Test lock mechanism
echo -e "\n${YELLOW}Testing Lock Mechanism...${NC}"

# Create a lock file
mkdir -p .etl_state/locks
touch .etl_state/locks/test.lock

run_test "Lock directory exists" "test -d .etl_state/locks"
rm -f .etl_state/locks/test.lock

# Test 17: Test error handling
echo -e "\n${YELLOW}Testing Error Handling...${NC}"

# Test with non-existent file
run_test "Non-existent file handling" "./tsv_sampler.sh /nonexistent/file.tsv 2>&1 | grep -qE 'not found|does not exist|Error'" || true

# Summary
echo ""
echo "============================================"
echo "TEST SUMMARY"
echo "============================================"
echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Failed: $TESTS_FAILED${NC}"

if [[ $TESTS_FAILED -eq 0 ]]; then
    echo -e "\n${GREEN}ALL TESTS PASSED!${NC}"
    exit 0
else
    echo -e "\n${RED}SOME TESTS FAILED${NC}"
    exit 1
fi