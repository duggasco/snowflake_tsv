#!/bin/bash

#############################################################################
# Basic CLI Test Suite - Works without Snowflake connectivity
# 
# Tests basic functionality that doesn't require database access
#############################################################################

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Test configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/test_runs"
TEST_DATA_DIR="${TEST_DIR}/test_data"
TEST_LOG="${TEST_DIR}/basic_test_$(date +%Y%m%d_%H%M%S).log"
CONFIG_FILE="${1:-${SCRIPT_DIR}/config/factLendingBenchmark_config.json}"

# Setup Python environment
if [ -f "${SCRIPT_DIR}/etl_venv/bin/activate" ]; then
    source "${SCRIPT_DIR}/etl_venv/bin/activate"
    PYTHON_CMD="${SCRIPT_DIR}/etl_venv/bin/python3"
else
    PYTHON_CMD="python3"
fi

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Create test directories
mkdir -p "${TEST_DIR}"
mkdir -p "${TEST_DATA_DIR}"

#############################################################################
# Helper Functions
#############################################################################

log() {
    echo -e "$1" | tee -a "${TEST_LOG}"
}

run_test() {
    local test_name="$1"
    local command="$2"
    
    ((TESTS_RUN++))
    log "\n${CYAN}[TEST $TESTS_RUN] ${test_name}${NC}"
    
    if eval "$command" >> "${TEST_LOG}" 2>&1; then
        ((TESTS_PASSED++))
        log "  ${GREEN}✓ PASSED${NC}"
        return 0
    else
        ((TESTS_FAILED++))
        log "  ${RED}✗ FAILED${NC}"
        return 1
    fi
}

generate_test_tsv() {
    local filename="$1"
    local rows="${2:-10}"
    
    # Generate header
    echo -e "RECORDDATE\tRECORDDATEID\tASSETID\tVALUE" > "${filename}"
    
    # Generate data rows
    for ((i=1; i<=rows; i++)); do
        echo -e "20240701\t20240701\tASSET${i}\t1000" >> "${filename}"
    done
}

#############################################################################
# Test Suite Header
#############################################################################

log "${MAGENTA}================================================${NC}"
log "${MAGENTA}Basic CLI Test Suite (Offline Mode)${NC}"
log "${MAGENTA}================================================${NC}"
log "Started: $(date)"
log "Config: ${CONFIG_FILE}"
log ""

#############################################################################
# Test 1: Python Module Import
#############################################################################

run_test "Python Module Import" "${PYTHON_CMD} -c 'import snowflake_etl; print(snowflake_etl.__version__)'"

#############################################################################
# Test 2: CLI Help
#############################################################################

run_test "CLI Help Text" "${PYTHON_CMD} -m snowflake_etl --help"

#############################################################################
# Test 3: Config Validation (Structure Only)
#############################################################################

run_test "Config File Structure" "${PYTHON_CMD} -c '
import json
with open(\"${CONFIG_FILE}\") as f:
    config = json.load(f)
    assert \"snowflake\" in config
    assert \"files\" in config
    print(\"Config structure valid\")
'"

#############################################################################
# Test 4: Generate Test Data
#############################################################################

TEST_TSV="${TEST_DATA_DIR}/test_basic.tsv"
generate_test_tsv "${TEST_TSV}" 5

run_test "Test Data Generation" "[ -f '${TEST_TSV}' ] && [ \$(wc -l < '${TEST_TSV}') -eq 6 ]"

#############################################################################
# Test 5: File Analysis (without loading)
#############################################################################

run_test "Analyze Test File" "${PYTHON_CMD} -c '
import csv
with open(\"${TEST_TSV}\", \"r\") as f:
    reader = csv.reader(f, delimiter=\"\\t\")
    rows = list(reader)
    assert len(rows) == 6  # 1 header + 5 data
    assert len(rows[0]) == 4  # 4 columns
    print(f\"File has {len(rows)-1} data rows, {len(rows[0])} columns\")
'"

#############################################################################
# Test 6: File Pattern Matching
#############################################################################

run_test "File Pattern Matching" "${PYTHON_CMD} -c '
import re
pattern = r\"factLendingBenchmark_\\d{8}-\\d{8}\\.tsv\"
test_name = \"factLendingBenchmark_20240101-20240131.tsv\"
assert re.match(pattern, test_name)
print(\"Pattern matching works\")
'"

#############################################################################
# Test 7: Date Parsing
#############################################################################

run_test "Date Format Parsing" "${PYTHON_CMD} -c '
from datetime import datetime
date_str = \"2024-07\"
date_obj = datetime.strptime(date_str, \"%Y-%m\")
assert date_obj.year == 2024
assert date_obj.month == 7
print(\"Date parsed: \" + date_obj.strftime(\"%B %Y\"))
'"

#############################################################################
# Test 8: Environment Check
#############################################################################

run_test "Virtual Environment Check" "${PYTHON_CMD} -c '
import sys
import os
venv_path = \"${SCRIPT_DIR}/etl_venv\"
in_venv = hasattr(sys, \"real_prefix\") or (hasattr(sys, \"base_prefix\") and sys.base_prefix != sys.prefix)
print(f\"Python: {sys.executable}\")
print(f\"In venv: {in_venv}\")
'"

#############################################################################
# Test 9: Import All Modules
#############################################################################

run_test "Import Core Modules" "${PYTHON_CMD} -c '
try:
    from snowflake_etl.core.application_context import ApplicationContext
    from snowflake_etl.core.file_analyzer import FileAnalyzer
    from snowflake_etl.core.progress import ProgressTracker
    print(\"Core modules imported successfully\")
except ImportError as e:
    print(f\"Import error: {e}\")
    raise
'"

#############################################################################
# Test 10: CLI Subcommands
#############################################################################

run_test "List CLI Subcommands" "${PYTHON_CMD} -m snowflake_etl --help | grep -E '(load|validate|delete|report|compare)'"

#############################################################################
# Test Summary
#############################################################################

log ""
log "${MAGENTA}================================================${NC}"
log "${MAGENTA}Test Summary${NC}"
log "${MAGENTA}================================================${NC}"
log "Tests Run:    ${TESTS_RUN}"
log "Tests Passed: ${GREEN}${TESTS_PASSED}${NC}"
log "Tests Failed: ${RED}${TESTS_FAILED}${NC}"

if [ ${TESTS_FAILED} -eq 0 ]; then
    log "\n${GREEN}✓ All basic tests passed!${NC}"
    exit 0
else
    log "\n${RED}✗ Some tests failed${NC}"
    exit 1
fi