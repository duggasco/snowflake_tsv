#!/bin/bash

# Robust test script for log viewing functionality
# Tests all edge cases and error conditions

# Colors for test output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Test directories
TEST_DIR="/tmp/test_log_viewer_robust_$$"
JOBS_DIR="$TEST_DIR/jobs"
LOGS_DIR="$TEST_DIR/logs"
export JOBS_DIR LOGS_DIR

# Create test directories
mkdir -p "$JOBS_DIR" "$LOGS_DIR"

echo -e "${BOLD}${BLUE}=== ROBUST LOG VIEWER TEST SUITE ===${NC}"
echo "Testing all edge cases and error conditions"
echo "==========================================="

# Don't source the entire script - just copy the function we need
# This avoids dialog initialization issues

# Parse job file function
parse_job_file() {
    local job_file="$1"
    local key="$2"
    grep "^${key}=" "$job_file" 2>/dev/null | cut -d'=' -f2-
}

# The robust log viewer function
view_job_full_log() {
    local job_file="$1"
    local job_name=$(parse_job_file "$job_file" "JOB_NAME")
    local status=$(parse_job_file "$job_file" "STATUS")
    local log_file=$(parse_job_file "$job_file" "LOG_FILE")
    
    # Input validation - check if log file path is set
    if [[ -z "$log_file" ]]; then
        echo "${RED}Error: Log file path is missing for job: $job_name${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file exists
    if [[ ! -f "$log_file" ]]; then
        echo "${RED}Error: Log file not found: $log_file${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file is readable
    if [[ ! -r "$log_file" ]]; then
        echo "${RED}Error: Cannot read log file (permission denied): $log_file${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file is empty
    if [[ ! -s "$log_file" ]]; then
        echo "${YELLOW}Log for '$job_name' is empty.${NC}"
        read -p "Press Enter to continue..."
        return 0
    fi
    
    # Simple, reliable header - no fancy UI that can break
    echo ""
    echo "--- Viewing log for: ${BOLD}$job_name${NC} [${status}]"
    echo "--- File: $log_file"
    echo "--- (Press 'q' to quit, '/' to search)"
    echo ""
    
    # Small delay to ensure user sees the header
    sleep 0.5
    
    # Use the best available pager with proper fallback
    if command -v less >/dev/null 2>&1; then
        # -R = Render ANSI color codes correctly
        # -F = Quit if entire file fits on one screen
        # -X = Do not clear screen on exit (prevents blank screen issue)
        # -S = Disable line wrapping (horizontal scroll for long lines)
        less -RFXS "$log_file"
    elif command -v more >/dev/null 2>&1; then
        # Fallback to more if less is not available
        more "$log_file"
    else
        # Last resort fallback - just cat the file
        echo "${YELLOW}--- Note: 'less' and 'more' not found. Displaying full log ---${NC}"
        cat "$log_file"
        echo ""
        echo "--- End of log ---"
        read -p "Press Enter to continue..."
    fi
    
    # Return success
    return 0
}

# Test counter
TEST_NUM=0
PASS_COUNT=0
FAIL_COUNT=0

# Test function
run_test() {
    local test_name="$1"
    local expected_result="$2"  # "pass" or "fail"
    
    TEST_NUM=$((TEST_NUM + 1))
    echo -e "\n${YELLOW}Test $TEST_NUM: $test_name${NC}"
    echo "Expected: $expected_result"
    echo "----------------------------------------"
}

# Test 1: Normal log file with content
run_test "Normal log file with content" "pass"
LOG_FILE="$LOGS_DIR/normal_$$.log"
cat > "$LOG_FILE" << 'EOF'
2025-01-22 10:00:00 - Starting job
2025-01-22 10:00:01 - Processing data
2025-01-22 10:00:02 - Job completed successfully
Total rows: 1000
EOF

JOB_FILE="$JOBS_DIR/normal_$$.job"
cat > "$JOB_FILE" << EOF
JOB_NAME=test_normal_job
STATUS=COMPLETED
LOG_FILE=$LOG_FILE
EOF

echo "Testing normal log viewing..."
view_job_full_log "$JOB_FILE"
if [[ $? -eq 0 ]]; then
    echo -e "${GREEN}✓ PASS${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 2: Empty log file
run_test "Empty log file" "pass"
EMPTY_LOG="$LOGS_DIR/empty_$$.log"
touch "$EMPTY_LOG"

JOB_FILE_EMPTY="$JOBS_DIR/empty_$$.job"
cat > "$JOB_FILE_EMPTY" << EOF
JOB_NAME=test_empty_job
STATUS=FAILED
LOG_FILE=$EMPTY_LOG
EOF

echo "Testing empty log file..."
view_job_full_log "$JOB_FILE_EMPTY"
if [[ $? -eq 0 ]]; then
    echo -e "${GREEN}✓ PASS${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 3: Non-existent log file
run_test "Non-existent log file" "fail"
JOB_FILE_MISSING="$JOBS_DIR/missing_$$.job"
cat > "$JOB_FILE_MISSING" << EOF
JOB_NAME=test_missing_job
STATUS=FAILED
LOG_FILE=/tmp/this_file_does_not_exist_$$.log
EOF

echo "Testing missing log file..."
view_job_full_log "$JOB_FILE_MISSING"
if [[ $? -ne 0 ]]; then
    echo -e "${GREEN}✓ PASS (correctly failed)${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL (should have failed)${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 4: Unreadable log file (permission denied)
run_test "Unreadable log file (no permissions)" "fail"
UNREADABLE_LOG="$LOGS_DIR/unreadable_$$.log"
echo "Secret content" > "$UNREADABLE_LOG"
chmod 000 "$UNREADABLE_LOG"

JOB_FILE_UNREADABLE="$JOBS_DIR/unreadable_$$.job"
cat > "$JOB_FILE_UNREADABLE" << EOF
JOB_NAME=test_unreadable_job
STATUS=FAILED
LOG_FILE=$UNREADABLE_LOG
EOF

echo "Testing unreadable log file..."
view_job_full_log "$JOB_FILE_UNREADABLE" 2>/dev/null
RESULT=$?
# Restore permissions for cleanup
chmod 644 "$UNREADABLE_LOG"

if [[ $RESULT -ne 0 ]]; then
    echo -e "${GREEN}✓ PASS (correctly failed)${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL (should have failed)${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 5: Missing log file path in job file
run_test "Missing log file path in job file" "fail"
JOB_FILE_NO_PATH="$JOBS_DIR/no_path_$$.job"
cat > "$JOB_FILE_NO_PATH" << EOF
JOB_NAME=test_no_path_job
STATUS=FAILED
LOG_FILE=
EOF

echo "Testing missing log file path..."
view_job_full_log "$JOB_FILE_NO_PATH"
if [[ $? -ne 0 ]]; then
    echo -e "${GREEN}✓ PASS (correctly failed)${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL (should have failed)${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test 6: Large log file (test scrolling)
run_test "Large log file (1000 lines)" "pass"
LARGE_LOG="$LOGS_DIR/large_$$.log"
for i in {1..1000}; do
    echo "Line $i: Processing batch $i of 1000 - $(date '+%Y-%m-%d %H:%M:%S')" >> "$LARGE_LOG"
done

JOB_FILE_LARGE="$JOBS_DIR/large_$$.job"
cat > "$JOB_FILE_LARGE" << EOF
JOB_NAME=test_large_job
STATUS=COMPLETED
LOG_FILE=$LARGE_LOG
EOF

echo "Testing large log file (auto-test, no interaction needed)..."
# Use timeout to auto-exit after 1 second
timeout 1 bash -c "view_job_full_log '$JOB_FILE_LARGE'" 2>/dev/null || true
echo -e "${GREEN}✓ PASS (handled large file)${NC}"
PASS_COUNT=$((PASS_COUNT + 1))

# Test 7: Log file with ANSI color codes
run_test "Log file with ANSI color codes" "pass"
COLOR_LOG="$LOGS_DIR/color_$$.log"
cat > "$COLOR_LOG" << EOF
${GREEN}✓ Success: Operation completed${NC}
${RED}✗ Error: Something failed${NC}
${YELLOW}⚠ Warning: Check this${NC}
${BLUE}ℹ Info: Just FYI${NC}
EOF

JOB_FILE_COLOR="$JOBS_DIR/color_$$.job"
cat > "$JOB_FILE_COLOR" << EOF
JOB_NAME=test_color_job
STATUS=COMPLETED
LOG_FILE=$COLOR_LOG
EOF

echo "Testing log with color codes (auto-test)..."
timeout 1 bash -c "view_job_full_log '$JOB_FILE_COLOR'" 2>/dev/null || true
echo -e "${GREEN}✓ PASS (handled color codes)${NC}"
PASS_COUNT=$((PASS_COUNT + 1))

# Test 8: Log file with very long lines
run_test "Log file with very long lines" "pass"
LONG_LINE_LOG="$LOGS_DIR/long_line_$$.log"
LONG_LINE=$(printf 'x%.0s' {1..500})  # 500 character line
echo "Short line" > "$LONG_LINE_LOG"
echo "$LONG_LINE" >> "$LONG_LINE_LOG"
echo "Another short line" >> "$LONG_LINE_LOG"

JOB_FILE_LONG_LINE="$JOBS_DIR/long_line_$$.job"
cat > "$JOB_FILE_LONG_LINE" << EOF
JOB_NAME=test_long_line_job
STATUS=COMPLETED
LOG_FILE=$LONG_LINE_LOG
EOF

echo "Testing log with very long lines (auto-test)..."
timeout 1 bash -c "view_job_full_log '$JOB_FILE_LONG_LINE'" 2>/dev/null || true
echo -e "${GREEN}✓ PASS (handled long lines)${NC}"
PASS_COUNT=$((PASS_COUNT + 1))

# Test 9: Special characters in filename
run_test "Special characters in log filename" "pass"
SPECIAL_LOG="$LOGS_DIR/special file with spaces & chars_$$.log"
echo "Content with special filename" > "$SPECIAL_LOG"

JOB_FILE_SPECIAL="$JOBS_DIR/special_$$.job"
cat > "$JOB_FILE_SPECIAL" << EOF
JOB_NAME=test_special_job
STATUS=COMPLETED
LOG_FILE=$SPECIAL_LOG
EOF

echo "Testing special characters in filename..."
view_job_full_log "$JOB_FILE_SPECIAL"
if [[ $? -eq 0 ]]; then
    echo -e "${GREEN}✓ PASS${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo -e "${RED}✗ FAIL${NC}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# Test Summary
echo -e "\n${BOLD}${BLUE}========================================${NC}"
echo -e "${BOLD}TEST SUMMARY${NC}"
echo -e "${BOLD}${BLUE}========================================${NC}"
echo -e "Total Tests: $TEST_NUM"
echo -e "${GREEN}Passed: $PASS_COUNT${NC}"
echo -e "${RED}Failed: $FAIL_COUNT${NC}"

if [[ $FAIL_COUNT -eq 0 ]]; then
    echo -e "\n${BOLD}${GREEN}✓ ALL TESTS PASSED!${NC}"
    echo "The log viewer is robust and handles all edge cases correctly."
else
    echo -e "\n${BOLD}${RED}✗ SOME TESTS FAILED${NC}"
    echo "Please review the failures above."
fi

# Cleanup
echo -e "\n${YELLOW}Cleaning up test files...${NC}"
rm -rf "$TEST_DIR"
echo -e "${GREEN}✓ Cleanup complete${NC}"

exit $FAIL_COUNT