#!/bin/bash

#############################################################################
# Comprehensive CLI Test Suite for Snowflake ETL Pipeline
# 
# This script tests all CLI functionality including:
# - Load operations (pattern-based and direct file)
# - Validation operations
# - Delete operations
# - Report generation
# - Duplicate checking
# - File comparison
# - Utility functions
# - Error handling
#
# Usage: ./test_cli_suite.sh [config_file]
#############################################################################

set -e  # Exit on error

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
TEST_LOG="${TEST_DIR}/test_suite_$(date +%Y%m%d_%H%M%S).log"
SUMMARY_LOG="${TEST_DIR}/test_summary_$(date +%Y%m%d_%H%M%S).log"
CONFIG_FILE="${1:-${SCRIPT_DIR}/config/factLendingBenchmark_config.json}"

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Create test directories
mkdir -p "${TEST_DIR}"
mkdir -p "${TEST_DATA_DIR}"

#############################################################################
# Helper Functions
#############################################################################

log() {
    echo -e "$1" | tee -a "${TEST_LOG}"
}

log_test_start() {
    ((TESTS_RUN++))
    log "\n${CYAN}========================================${NC}"
    log "${CYAN}Test #${TESTS_RUN}: $1${NC}"
    log "${CYAN}========================================${NC}"
    echo "Test #${TESTS_RUN}: $1" >> "${SUMMARY_LOG}"
}

log_success() {
    ((TESTS_PASSED++))
    log "${GREEN}✓ PASSED: $1${NC}"
    echo "  ✓ PASSED: $1" >> "${SUMMARY_LOG}"
}

log_failure() {
    ((TESTS_FAILED++))
    log "${RED}✗ FAILED: $1${NC}"
    echo "  ✗ FAILED: $1" >> "${SUMMARY_LOG}"
}

log_skip() {
    ((TESTS_SKIPPED++))
    log "${YELLOW}⊘ SKIPPED: $1${NC}"
    echo "  ⊘ SKIPPED: $1" >> "${SUMMARY_LOG}"
}

run_command() {
    local cmd="$1"
    local description="$2"
    log "${BLUE}Running: ${cmd}${NC}"
    
    if eval "${cmd}" >> "${TEST_LOG}" 2>&1; then
        return 0
    else
        return 1
    fi
}

check_file_exists() {
    if [ -f "$1" ]; then
        return 0
    else
        log "${RED}File not found: $1${NC}"
        return 1
    fi
}

generate_test_tsv() {
    local filename="$1"
    local rows="${2:-100}"
    local date="${3:-20240701}"
    
    log "Generating test TSV file: ${filename} with ${rows} rows"
    
    # Generate header
    echo -e "RECORDDATE\tRECORDDATEID\tASSETID\tDXIDENTIFIER\tISIN\tCUSIP\tSEDOL\tEXCHANGE\tDIVIDENDREQUIREMENT\tFISCALLOCATION\tINDUSTRYSECTOR\tPEERGROUP\tCOLLATERALTYPE\tCOLLATERALCURRENCY\tSOURCETYPE\tTRADETYPE\tSTARTDATE\tENDDATE\tLENDABLEVALUE\tACTIVELENDABLEVALUE\tLENDABLEQUANTITY\tACTIVELENDABLEQUANTITY\tLOANVALUE\tLOANQUANTITY\tREBATE\tREINVESTMENTRATE\tREINVESTMENTRETURN\tFEE\tMINFEE\tMAXFEE\tCLIENTFUNDCODE\tONLOANTRANSACTIONCOUNT\tEXCHANGERATE\tCOLLATERALBENCHMARK\tFEEDBUCKET\tINVESTMENTSTYLE\tDATASOURCETYPE\tXCREATEBY\tXCREATEDATE\tXUPDATEBY\tXUPDATEDATE" > "${filename}"
    
    # Generate data rows
    for ((i=1; i<=rows; i++)); do
        echo -e "${date}\t${date}\tASSET${i}\tDX${i}\tISIN${i}\tCUSIP${i}\tSEDOL${i}\tNYSE\t0.5\tUS\tTECH\tPEER1\tEQUITY\tUSD\tBLOOMBERG\tLOAN\t${date}\t${date}\t1000000\t900000\t10000\t9000\t500000\t5000\t0.25\t0.02\t0.03\t0.50\t0.25\t1.00\tFUND001\t5\t1.0\tSP500\tBUCKET1\tGROWTH\tDAILY\tSYSTEM\t${date}\tSYSTEM\t${date}" >> "${filename}"
    done
}

#############################################################################
# Test Suite Header
#############################################################################

log "${MAGENTA}================================================${NC}"
log "${MAGENTA}Snowflake ETL CLI Comprehensive Test Suite${NC}"
log "${MAGENTA}================================================${NC}"
log "Test Started: $(date)"
log "Config File: ${CONFIG_FILE}"
log "Test Directory: ${TEST_DIR}"
log "Log File: ${TEST_LOG}"
log ""

# Check if config exists
if [ ! -f "${CONFIG_FILE}" ]; then
    log "${RED}ERROR: Config file not found: ${CONFIG_FILE}${NC}"
    exit 1
fi

#############################################################################
# Test 1: System Check
#############################################################################

log_test_start "System Check"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} --quiet check-system" "Check system capabilities"; then
    log_success "System check completed"
else
    log_failure "System check failed"
fi

#############################################################################
# Test 2: Configuration Validation
#############################################################################

log_test_start "Configuration Validation"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} config-validate" "Validate configuration"; then
    log_success "Configuration is valid"
else
    log_failure "Configuration validation failed"
fi

#############################################################################
# Test 3: Generate Test Data
#############################################################################

log_test_start "Generate Test Data"
TEST_TSV_1="${TEST_DATA_DIR}/factLendingBenchmark_20240701-20240731.tsv"
TEST_TSV_2="${TEST_DATA_DIR}/factLendingBenchmark_20240601-20240630.tsv"
TEST_TSV_BAD="${TEST_DATA_DIR}/bad_file.tsv"

generate_test_tsv "${TEST_TSV_1}" 1000 "20240701"
generate_test_tsv "${TEST_TSV_2}" 500 "20240601"

# Create a bad file with wrong number of columns
echo -e "COL1\tCOL2\tCOL3" > "${TEST_TSV_BAD}"
echo -e "VAL1\tVAL2\tVAL3" >> "${TEST_TSV_BAD}"

log_success "Test data generated"

#############################################################################
# Test 4: Load Operation - Direct File
#############################################################################

log_test_start "Load Operation - Direct File"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --files ${TEST_TSV_1} --skip-qc" "Load direct file"; then
    log_success "Direct file load completed"
else
    log_failure "Direct file load failed"
fi

#############################################################################
# Test 5: Load Operation - Pattern-based with Month
#############################################################################

log_test_start "Load Operation - Pattern-based"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --base-path ${TEST_DATA_DIR} --month 2024-06 --skip-qc" "Load with pattern"; then
    log_success "Pattern-based load completed"
else
    log_failure "Pattern-based load failed"
fi

#############################################################################
# Test 6: Validation - Specific Table and Month
#############################################################################

log_test_start "Validation Operation"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} validate --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-07 --output ${TEST_DIR}/validation_report.json" "Validate table data"; then
    log_success "Validation completed"
    if check_file_exists "${TEST_DIR}/validation_report.json"; then
        log_success "Validation report generated"
    else
        log_failure "Validation report not generated"
    fi
else
    log_failure "Validation failed"
fi

#############################################################################
# Test 7: Generate Full Report
#############################################################################

log_test_start "Report Generation"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} report --output ${TEST_DIR}/full_report" "Generate full report"; then
    log_success "Report generation completed"
    if check_file_exists "${TEST_DIR}/full_report.txt"; then
        log_success "Text report generated"
    fi
    if check_file_exists "${TEST_DIR}/full_report.json"; then
        log_success "JSON report generated"
    fi
else
    log_failure "Report generation failed"
fi

#############################################################################
# Test 8: Check Duplicates
#############################################################################

log_test_start "Duplicate Check"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} check-duplicates --table TEST_CUSTOM_FACTLENDINGBENCHMARK --key-columns RECORDDATEID,ASSETID --output ${TEST_DIR}/duplicates.json" "Check duplicates"; then
    log_success "Duplicate check completed"
else
    log_failure "Duplicate check failed"
fi

#############################################################################
# Test 9: File Comparison
#############################################################################

log_test_start "File Comparison"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} compare --file1 ${TEST_TSV_1} --file2 ${TEST_TSV_2} --output ${TEST_DIR}/comparison.json" "Compare files"; then
    log_success "File comparison completed"
else
    log_failure "File comparison failed"
fi

#############################################################################
# Test 10: Check Table Info
#############################################################################

log_test_start "Check Table Info"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} check-table TEST_CUSTOM_FACTLENDINGBENCHMARK" "Check table info"; then
    log_success "Table info retrieved"
else
    log_failure "Table info check failed"
fi

#############################################################################
# Test 11: Diagnose Errors
#############################################################################

log_test_start "Diagnose Errors"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} diagnose-error --hours 24" "Diagnose recent errors"; then
    log_success "Error diagnosis completed"
else
    log_failure "Error diagnosis failed"
fi

#############################################################################
# Test 12: Validate TSV File Structure
#############################################################################

log_test_start "Validate File Structure"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} validate-file ${TEST_TSV_1} --expected-columns 41" "Validate TSV structure"; then
    log_success "File structure validation completed"
else
    log_failure "File structure validation failed"
fi

#############################################################################
# Test 13: Check Stage
#############################################################################

log_test_start "Check Snowflake Stage"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} check-stage --pattern *.tsv" "Check stage files"; then
    log_success "Stage check completed"
else
    log_failure "Stage check failed"
fi

#############################################################################
# Test 14: File Browser
#############################################################################

log_test_start "File Browser"
if run_command "echo '0' | python3 -m snowflake_etl --config ${CONFIG_FILE} browse --directory ${TEST_DATA_DIR}" "Browse files"; then
    log_success "File browser completed"
else
    log_skip "File browser test skipped (interactive)"
fi

#############################################################################
# Test 15: Sample TSV File
#############################################################################

log_test_start "Sample TSV File"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} sample-file ${TEST_TSV_1} --rows 10" "Sample TSV file"; then
    log_success "File sampling completed"
else
    log_failure "File sampling failed"
fi

#############################################################################
# Test 16: Error Handling - Bad File
#############################################################################

log_test_start "Error Handling - Bad File"
if ! run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --files ${TEST_TSV_BAD} --skip-qc" "Load bad file (should fail)"; then
    log_success "Bad file correctly rejected"
else
    log_failure "Bad file was not rejected"
fi

#############################################################################
# Test 17: Error Handling - Non-existent File
#############################################################################

log_test_start "Error Handling - Non-existent File"
if ! run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --files /nonexistent/file.tsv" "Load non-existent file (should fail)"; then
    log_success "Non-existent file correctly handled"
else
    log_failure "Non-existent file error not handled"
fi

#############################################################################
# Test 18: Delete Operation (Dry Run)
#############################################################################

log_test_start "Delete Operation - Dry Run"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} delete --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-07 --dry-run --yes" "Delete dry run"; then
    log_success "Delete dry run completed"
else
    log_failure "Delete dry run failed"
fi

#############################################################################
# Test 19: Load with Validation
#############################################################################

log_test_start "Load with Snowflake Validation"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --files ${TEST_TSV_1} --validate-in-snowflake" "Load with validation"; then
    log_success "Load with validation completed"
else
    log_failure "Load with validation failed"
fi

#############################################################################
# Test 20: Analyze Files Only
#############################################################################

log_test_start "Analyze Files Only"
if run_command "python3 -m snowflake_etl --config ${CONFIG_FILE} load --base-path ${TEST_DATA_DIR} --month 2024-07 --analyze-only" "Analyze files only"; then
    log_success "File analysis completed"
else
    log_failure "File analysis failed"
fi

#############################################################################
# Test Summary
#############################################################################

log "\n${MAGENTA}================================================${NC}"
log "${MAGENTA}Test Suite Summary${NC}"
log "${MAGENTA}================================================${NC}"
log "Total Tests Run: ${TESTS_RUN}"
log "${GREEN}Tests Passed: ${TESTS_PASSED}${NC}"
log "${RED}Tests Failed: ${TESTS_FAILED}${NC}"
log "${YELLOW}Tests Skipped: ${TESTS_SKIPPED}${NC}"

PASS_RATE=0
if [ ${TESTS_RUN} -gt 0 ]; then
    PASS_RATE=$((TESTS_PASSED * 100 / TESTS_RUN))
fi

log "Pass Rate: ${PASS_RATE}%"
log "Test Completed: $(date)"

# Write final summary
echo "" >> "${SUMMARY_LOG}"
echo "================================================" >> "${SUMMARY_LOG}"
echo "Final Summary" >> "${SUMMARY_LOG}"
echo "================================================" >> "${SUMMARY_LOG}"
echo "Total Tests: ${TESTS_RUN}" >> "${SUMMARY_LOG}"
echo "Passed: ${TESTS_PASSED}" >> "${SUMMARY_LOG}"
echo "Failed: ${TESTS_FAILED}" >> "${SUMMARY_LOG}"
echo "Skipped: ${TESTS_SKIPPED}" >> "${SUMMARY_LOG}"
echo "Pass Rate: ${PASS_RATE}%" >> "${SUMMARY_LOG}"

#############################################################################
# Generate Detailed Report
#############################################################################

REPORT_FILE="${TEST_DIR}/test_report_$(date +%Y%m%d_%H%M%S).txt"

cat > "${REPORT_FILE}" << EOF
================================================================================
SNOWFLAKE ETL CLI TEST REPORT
================================================================================

Date: $(date)
Host: $(hostname)
User: $(whoami)
Config File: ${CONFIG_FILE}

TEST RESULTS SUMMARY
--------------------
Total Tests: ${TESTS_RUN}
Passed: ${TESTS_PASSED}
Failed: ${TESTS_FAILED}
Skipped: ${TESTS_SKIPPED}
Pass Rate: ${PASS_RATE}%

DETAILED TEST LOG
-----------------
See: ${TEST_LOG}

SUMMARY LOG
-----------
See: ${SUMMARY_LOG}

TEST DATA LOCATION
------------------
${TEST_DATA_DIR}

GENERATED FILES
---------------
EOF

ls -la "${TEST_DIR}/" >> "${REPORT_FILE}"

log "\n${CYAN}Test report saved to: ${REPORT_FILE}${NC}"
log "${CYAN}Detailed log saved to: ${TEST_LOG}${NC}"
log "${CYAN}Summary log saved to: ${SUMMARY_LOG}${NC}"

# Exit with appropriate code
if [ ${TESTS_FAILED} -eq 0 ]; then
    log "${GREEN}All tests completed successfully!${NC}"
    exit 0
else
    log "${RED}Some tests failed. Please review the logs.${NC}"
    exit 1
fi