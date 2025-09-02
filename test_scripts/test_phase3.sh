#!/bin/bash

# Test script for Phase 3 config generation functions
# Created: 2025-09-02

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

echo -e "${CYAN}=== Testing Phase 3 Config Generation Functions ===${NC}"
echo

# Test function wrapper
run_test() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    
    TESTS_RUN=$((TESTS_RUN + 1))
    
    if [[ "$actual" == "$expected" ]]; then
        echo -e "${GREEN}[PASS]${NC} $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}[FAIL]${NC} $test_name"
        echo -e "       Expected: '$expected'"
        echo -e "       Got:      '$actual'"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Test 1: detect_file_pattern function
echo -e "${YELLOW}Testing detect_file_pattern function...${NC}"

# Create a test script that calls the function from snowflake_etl.sh
test_detect_pattern() {
    local filename="$1"
    bash -c "
        source ./snowflake_etl.sh >/dev/null 2>&1 <<EOF
0
EOF
        detect_file_pattern '$filename'
    " 2>/dev/null
}

result=$(test_detect_pattern "factLendingBenchmark_20240101-20240131.tsv")
run_test "Date range pattern detection" "factLendingBenchmark_{date_range}.tsv" "$result"

result=$(test_detect_pattern "data/myTable_20240101-20240131.tsv")
run_test "Date range pattern with path" "myTable_{date_range}.tsv" "$result"

result=$(test_detect_pattern "factLending_2024-01.tsv")
run_test "Month pattern detection" "factLending_{month}.tsv" "$result"

result=$(test_detect_pattern "static_data.tsv")
run_test "No pattern detection" "static_data.tsv" "$result"

echo

# Test 2: extract_table_name function
echo -e "${YELLOW}Testing extract_table_name function...${NC}"

test_extract_table() {
    local filename="$1"
    bash -c "
        source ./snowflake_etl.sh >/dev/null 2>&1 <<EOF
0
EOF
        extract_table_name '$filename'
    " 2>/dev/null
}

result=$(test_extract_table "factLendingBenchmark_20240101-20240131.tsv")
run_test "Extract table name from date range file" "FACTLENDINGBENCHMARK" "$result"

result=$(test_extract_table "myTable_2024-01.tsv")
run_test "Extract table name from month file" "MYTABLE" "$result"

result=$(test_extract_table "data/fact-lending_20240101-20240131.tsv")
run_test "Extract table name with special chars" "FACT_LENDING" "$result"

echo

# Test 3: analyze_tsv_file function
echo -e "${YELLOW}Testing analyze_tsv_file function...${NC}"

# Create a test TSV file
test_tsv="/tmp/test_file.tsv"
echo -e "col1\tcol2\tcol3\tcol4\tcol5" > "$test_tsv"
echo -e "val1\tval2\tval3\tval4\tval5" >> "$test_tsv"

test_analyze_file() {
    local file="$1"
    bash -c "
        source ./snowflake_etl.sh >/dev/null 2>&1 <<EOF
0
EOF
        analyze_tsv_file '$file'
    " 2>/dev/null
}

result=$(test_analyze_file "$test_tsv")
run_test "Analyze TSV file column count" "5" "$result"

# Test with non-existent file
result=$(test_analyze_file "/tmp/nonexistent.tsv" 2>/dev/null || echo "error")
if [[ -z "$result" ]]; then
    result="error"
fi
run_test "Analyze non-existent file returns error" "error" "$result"

# Clean up test file
rm -f "$test_tsv"

echo

# Test 4: Test various filename patterns
echo -e "${YELLOW}Testing various filename patterns...${NC}"

test_files=(
    "FACTLENDING_20220901-20220930.tsv"
    "table_2024-01.tsv"
    "my-data_20240115-20240215.tsv"
    "static_reference_data.tsv"
)

expected_patterns=(
    "FACTLENDING_{date_range}.tsv"
    "table_{month}.tsv"
    "my-data_{date_range}.tsv"
    "static_reference_data.tsv"
)

expected_tables=(
    "FACTLENDING"
    "TABLE"
    "MY_DATA"
    "STATIC_REFERENCE_DATA"
)

for i in "${!test_files[@]}"; do
    file="${test_files[$i]}"
    expected_pattern="${expected_patterns[$i]}"
    expected_table="${expected_tables[$i]}"
    
    result=$(test_detect_pattern "$file")
    run_test "Pattern: $file" "$expected_pattern" "$result"
    
    result=$(test_extract_table "$file")
    run_test "Table: $file" "$expected_table" "$result"
done

echo

# Test 5: Simple integration test
echo -e "${YELLOW}Testing basic integration...${NC}"

# Create test TSV files
test_dir="/tmp/test_tsv_$$"
mkdir -p "$test_dir"

echo -e "col1\tcol2\tcol3" > "$test_dir/testTable_20240101-20240131.tsv"
echo -e "val1\tval2\tval3" >> "$test_dir/testTable_20240101-20240131.tsv"

# Test that we can analyze the file
result=$(test_analyze_file "$test_dir/testTable_20240101-20240131.tsv")
run_test "Analyze created test file" "3" "$result"

# Test pattern detection on our test file
result=$(test_detect_pattern "$test_dir/testTable_20240101-20240131.tsv")
run_test "Pattern detection on test file" "testTable_{date_range}.tsv" "$result"

# Test table extraction on our test file
result=$(test_extract_table "$test_dir/testTable_20240101-20240131.tsv")
run_test "Table extraction on test file" "TESTTABLE" "$result"

# Clean up
rm -rf "$test_dir"

echo
echo -e "${CYAN}=== Test Summary ===${NC}"
echo -e "Tests Run:    $TESTS_RUN"
echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "Tests Failed: ${RED}$TESTS_FAILED${NC}"
echo

if [[ $TESTS_FAILED -eq 0 ]]; then
    echo -e "${GREEN}All Phase 3 functions are working correctly!${NC}"
    echo -e "${CYAN}The functions are ready for production use.${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Review the functions for issues.${NC}"
    exit 1
fi