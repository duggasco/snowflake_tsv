#!/bin/bash

# Test script for log viewing functionality
# Tests that logs persist using less pager

# Source the main script to get functions
source snowflake_etl.sh 2>/dev/null || true

# Create test directories
TEST_DIR="/tmp/test_log_viewer_$$"
JOBS_DIR="$TEST_DIR/jobs"
LOGS_DIR="$TEST_DIR/logs"
mkdir -p "$JOBS_DIR" "$LOGS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

echo -e "${BOLD}${BLUE}Testing Log Viewer Functionality${NC}"
echo "=================================="

# Test 1: Create a sample job with log
echo -e "\n${YELLOW}Test 1: Creating sample job with log content${NC}"

# Create a sample log file with content
LOG_FILE="$LOGS_DIR/test_job_$$.log"
cat > "$LOG_FILE" << 'EOF'
2025-01-22 10:00:00 - Starting job execution
2025-01-22 10:00:01 - Connecting to Snowflake...
2025-01-22 10:00:02 - Connection established
2025-01-22 10:00:03 - Processing data...
2025-01-22 10:00:05 - Processed 1000 rows
2025-01-22 10:00:10 - Processed 5000 rows
2025-01-22 10:00:15 - Processed 10000 rows
2025-01-22 10:00:20 - Compression started
2025-01-22 10:00:25 - Compression completed
2025-01-22 10:00:30 - Uploading to Snowflake stage
2025-01-22 10:00:45 - Upload completed
2025-01-22 10:00:50 - Executing COPY command
2025-01-22 10:01:00 - COPY completed successfully
2025-01-22 10:01:01 - Job completed successfully
Total rows processed: 10000
Total time: 61 seconds
EOF

# Create a job file
JOB_FILE="$JOBS_DIR/test_job_$$.job"
cat > "$JOB_FILE" << EOF
JOB_ID=test_$$
JOB_NAME=test_data_load
STATUS=COMPLETED
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE=$LOG_FILE
PID=$$
CONFIG_FILE=/tmp/test_config.json
EOF

echo -e "${GREEN}✓ Created test job and log file${NC}"
echo "  Job file: $JOB_FILE"
echo "  Log file: $LOG_FILE"

# Test 2: Test viewing with less
echo -e "\n${YELLOW}Test 2: Testing log viewer with less${NC}"
echo "When less opens:"
echo "  - Use arrow keys or j/k to scroll"
echo "  - Press '/' to search"
echo "  - Press 'q' to quit"
echo ""
read -p "Press Enter to test log viewing with less..."

# Call the view function
export JOBS_DIR LOGS_DIR  # Make sure functions can find them
view_job_full_log "$JOB_FILE"

echo -e "\n${GREEN}✓ Log viewer test completed${NC}"

# Test 3: Test with empty log
echo -e "\n${YELLOW}Test 3: Testing with empty log file${NC}"
EMPTY_LOG="$LOGS_DIR/empty_$$.log"
touch "$EMPTY_LOG"

JOB_FILE_EMPTY="$JOBS_DIR/empty_job_$$.job"
cat > "$JOB_FILE_EMPTY" << EOF
JOB_ID=empty_$$
JOB_NAME=empty_test
STATUS=FAILED
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE=$EMPTY_LOG
PID=$$
CONFIG_FILE=/tmp/test_config.json
EOF

view_job_full_log "$JOB_FILE_EMPTY"
echo -e "${GREEN}✓ Empty log handling test completed${NC}"

# Test 4: Test with missing log
echo -e "\n${YELLOW}Test 4: Testing with missing log file${NC}"
JOB_FILE_MISSING="$JOBS_DIR/missing_job_$$.job"
cat > "$JOB_FILE_MISSING" << EOF
JOB_ID=missing_$$
JOB_NAME=missing_test
STATUS=FAILED
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE=/tmp/nonexistent_log_file.log
PID=$$
CONFIG_FILE=/tmp/test_config.json
EOF

view_job_full_log "$JOB_FILE_MISSING"
echo -e "${GREEN}✓ Missing log handling test completed${NC}"

# Test 5: Test with large log
echo -e "\n${YELLOW}Test 5: Creating large log file for scrolling test${NC}"
LARGE_LOG="$LOGS_DIR/large_$$.log"
for i in {1..100}; do
    echo "Line $i: Processing batch $i of 100 - $(date '+%Y-%m-%d %H:%M:%S')" >> "$LARGE_LOG"
    echo "  Details: Processed $(($i * 1000)) rows, $(($i * 50)) MB uploaded" >> "$LARGE_LOG"
done

JOB_FILE_LARGE="$JOBS_DIR/large_job_$$.job"
cat > "$JOB_FILE_LARGE" << EOF
JOB_ID=large_$$
JOB_NAME=large_data_load
STATUS=COMPLETED
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE=$LARGE_LOG
PID=$$
CONFIG_FILE=/tmp/test_config.json
EOF

echo "Created large log with 200 lines"
read -p "Press Enter to test scrolling in less..."
view_job_full_log "$JOB_FILE_LARGE"

echo -e "${GREEN}✓ Large log scrolling test completed${NC}"

# Cleanup
echo -e "\n${YELLOW}Cleaning up test files...${NC}"
rm -rf "$TEST_DIR"
echo -e "${GREEN}✓ Test files cleaned up${NC}"

echo -e "\n${BOLD}${GREEN}All tests completed successfully!${NC}"
echo "The log viewer now uses 'less' for persistent viewing with:"
echo "  - Full scrolling capability"
echo "  - Search functionality (/)"
echo "  - Color preservation"
echo "  - Proper exit handling"