#!/bin/bash

#############################################################################
# Master Test Runner for Snowflake ETL Pipeline
# 
# Executes all test suites and generates a comprehensive report
#############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_RUN_ID="$(date +%Y%m%d_%H%M%S)"
TEST_DIR="${SCRIPT_DIR}/test_runs/${TEST_RUN_ID}"
MASTER_LOG="${TEST_DIR}/master_test.log"
REPORT_FILE="${TEST_DIR}/TEST_REPORT.txt"
HTML_REPORT="${TEST_DIR}/TEST_REPORT.html"
CONFIG_FILE="${1:-${SCRIPT_DIR}/config/factLendingBenchmark_config.json}"

# Create test directory structure
mkdir -p "${TEST_DIR}"
mkdir -p "${TEST_DIR}/logs"
mkdir -p "${TEST_DIR}/reports"
mkdir -p "${TEST_DIR}/artifacts"

#############################################################################
# Helper Functions
#############################################################################

print_header() {
    echo -e "${BOLD}${MAGENTA}"
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║           SNOWFLAKE ETL COMPREHENSIVE TEST SUITE              ║"
    echo "║                                                                ║"
    echo "║  Version: 3.0.3                                                ║"
    echo "║  Date: $(date +'%Y-%m-%d %H:%M:%S')                              ║"
    echo "║  Test ID: ${TEST_RUN_ID}                                      ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

log() {
    echo -e "$1" | tee -a "${MASTER_LOG}"
}

run_test_suite() {
    local suite_name="$1"
    local script_path="$2"
    local suite_log="${TEST_DIR}/logs/${suite_name// /_}.log"
    
    log "\n${CYAN}▶ Running ${suite_name}...${NC}"
    
    if [ -f "${script_path}" ]; then
        chmod +x "${script_path}"
        # Run the test script
        "${script_path}" "${CONFIG_FILE}" > "${suite_log}" 2>&1
        local exit_code=$?
        if [ $exit_code -eq 0 ]; then
            log "  ${GREEN}✓ ${suite_name} completed successfully${NC}"
            return 0
        else
            log "  ${RED}✗ ${suite_name} failed (exit code: $exit_code)${NC}"
            # Show last few lines of the log for debugging
            if [ -f "${suite_log}" ]; then
                log "  Last lines of log:"
                tail -5 "${suite_log}" | while IFS= read -r line; do
                    log "    $line"
                done
            fi
            return 1
        fi
    else
        log "  ${YELLOW}⊘ ${suite_name} script not found${NC}"
        return 2
    fi
}

check_prerequisites() {
    log "\n${CYAN}Checking Prerequisites...${NC}"
    
    local prereq_ok=true
    
    # Check for virtual environment
    if [ -f "${SCRIPT_DIR}/etl_venv/bin/activate" ]; then
        log "  ${GREEN}✓ Virtual environment found${NC}"
        source "${SCRIPT_DIR}/etl_venv/bin/activate"
        export PYTHON_CMD="${SCRIPT_DIR}/etl_venv/bin/python3"
        export PIP_CMD="${SCRIPT_DIR}/etl_venv/bin/pip"
    else
        export PYTHON_CMD="python3"
        export PIP_CMD="pip"
    fi
    
    # Check Python
    if command -v ${PYTHON_CMD} > /dev/null; then
        local python_version=$(${PYTHON_CMD} --version 2>&1 | cut -d' ' -f2)
        log "  ${GREEN}✓ Python3 installed: ${python_version}${NC}"
    else
        log "  ${RED}✗ Python3 not found${NC}"
        prereq_ok=false
    fi
    
    # Check required Python packages
    local packages=("snowflake.connector:snowflake-connector-python" "pandas:pandas" "numpy:numpy" "tqdm:tqdm")
    for pkg_spec in "${packages[@]}"; do
        IFS=':' read -r import_name display_name <<< "$pkg_spec"
        if ${PYTHON_CMD} -c "import ${import_name}" 2>/dev/null; then
            log "  ${GREEN}✓ Python package '${display_name}' installed${NC}"
        else
            log "  ${YELLOW}⚠ Python package '${display_name}' not installed${NC}"
        fi
    done
    
    # Check config file
    if [ -f "${CONFIG_FILE}" ]; then
        log "  ${GREEN}✓ Configuration file exists${NC}"
    else
        log "  ${RED}✗ Configuration file not found: ${CONFIG_FILE}${NC}"
        prereq_ok=false
    fi
    
    # Check main scripts
    if [ -f "${SCRIPT_DIR}/snowflake_etl.sh" ]; then
        log "  ${GREEN}✓ Main menu script exists${NC}"
    else
        log "  ${YELLOW}⚠ Main menu script not found${NC}"
    fi
    
    if [ "$prereq_ok" = false ]; then
        log "\n${RED}Prerequisites check failed. Exiting.${NC}"
        exit 1
    fi
}

collect_system_info() {
    log "\n${CYAN}Collecting System Information...${NC}"
    
    {
        echo "SYSTEM INFORMATION"
        echo "=================="
        echo "Hostname: $(hostname)"
        echo "OS: $(uname -s) $(uname -r)"
        echo "Python: $(python3 --version 2>&1)"
        echo "User: $(whoami)"
        echo "Date: $(date)"
        echo "Working Dir: ${SCRIPT_DIR}"
        echo ""
    } > "${TEST_DIR}/system_info.txt"
    
    log "  ${GREEN}✓ System information collected${NC}"
}

#############################################################################
# Main Execution
#############################################################################

# Clear screen and show header
clear
print_header | tee "${MASTER_LOG}"

# Phase 1: Prerequisites
log "${BOLD}${BLUE}PHASE 1: Prerequisites Check${NC}"
check_prerequisites

# Phase 2: System Information
log "\n${BOLD}${BLUE}PHASE 2: System Information${NC}"
collect_system_info

# Phase 3: Create Test Scripts if needed
log "\n${BOLD}${BLUE}PHASE 3: Preparing Test Scripts${NC}"

# Check if test scripts exist, if not use the ones we just created
if [ ! -f "${SCRIPT_DIR}/test_cli_suite.sh" ]; then
    log "  ${YELLOW}⚠ CLI test suite not found, skipping${NC}"
else
    log "  ${GREEN}✓ CLI test suite ready${NC}"
fi

if [ ! -f "${SCRIPT_DIR}/test_menu_suite.sh" ]; then
    log "  ${YELLOW}⚠ Menu test suite not found, skipping${NC}"
else
    log "  ${GREEN}✓ Menu test suite ready${NC}"
fi

# Phase 3.5: Test Snowflake Connectivity
log "\n${BOLD}${BLUE}PHASE 3.5: Testing Snowflake Connectivity${NC}"

if ${PYTHON_CMD} "${SCRIPT_DIR}/test_connectivity.py" "${CONFIG_FILE}" > "${TEST_DIR}/connectivity.log" 2>&1; then
    cat "${TEST_DIR}/connectivity.log" | tee -a "${MASTER_LOG}"
    log "  ${GREEN}✓ Snowflake connection successful${NC}"
    SNOWFLAKE_AVAILABLE=true
else
    cat "${TEST_DIR}/connectivity.log" | tee -a "${MASTER_LOG}"
    log "  ${YELLOW}⚠ Snowflake connection failed - running in offline mode${NC}"
    log "  ${YELLOW}  Tests requiring Snowflake will be skipped${NC}"
    SNOWFLAKE_AVAILABLE=false
fi

# Phase 4: Run Test Suites
log "\n${BOLD}${BLUE}PHASE 4: Running Test Suites${NC}"

TEST_RESULTS=()
TOTAL_SUITES=0
PASSED_SUITES=0
FAILED_SUITES=0
SKIPPED_SUITES=0

# Run CLI Test Suite
if [ "$SNOWFLAKE_AVAILABLE" = "true" ] && [ -f "${SCRIPT_DIR}/test_cli_suite.sh" ]; then
    TOTAL_SUITES=$((TOTAL_SUITES + 1))
    if run_test_suite "CLI Test Suite" "${SCRIPT_DIR}/test_cli_suite.sh"; then
        PASSED_SUITES=$((PASSED_SUITES + 1))
        TEST_RESULTS+=("CLI Test Suite:PASSED")
    else
        FAILED_SUITES=$((FAILED_SUITES + 1))
        TEST_RESULTS+=("CLI Test Suite:FAILED")
    fi
elif [ "$SNOWFLAKE_AVAILABLE" = "false" ] && [ -f "${SCRIPT_DIR}/test_cli_basic.sh" ]; then
    TOTAL_SUITES=$((TOTAL_SUITES + 1))
    if run_test_suite "Basic CLI Test Suite (Offline)" "${SCRIPT_DIR}/test_cli_basic.sh"; then
        PASSED_SUITES=$((PASSED_SUITES + 1))
        TEST_RESULTS+=("Basic CLI Test Suite:PASSED")
    else
        FAILED_SUITES=$((FAILED_SUITES + 1))
        TEST_RESULTS+=("Basic CLI Test Suite:FAILED")
    fi
fi

# Run Menu Test Suite (only if Snowflake is available)
if [ "$SNOWFLAKE_AVAILABLE" = "true" ] && [ -f "${SCRIPT_DIR}/test_menu_suite.sh" ]; then
    TOTAL_SUITES=$((TOTAL_SUITES + 1))
    if run_test_suite "Menu Test Suite" "${SCRIPT_DIR}/test_menu_suite.sh"; then
        PASSED_SUITES=$((PASSED_SUITES + 1))
        TEST_RESULTS+=("Menu Test Suite:PASSED")
    else
        FAILED_SUITES=$((FAILED_SUITES + 1))
        TEST_RESULTS+=("Menu Test Suite:FAILED")
    fi
elif [ "$SNOWFLAKE_AVAILABLE" = "false" ]; then
    SKIPPED_SUITES=$((SKIPPED_SUITES + 1))
    TEST_RESULTS+=("Menu Test Suite:SKIPPED")
    log "\n${CYAN}▶ Skipping Menu Test Suite (requires Snowflake)${NC}"
fi

# Phase 5: Generate Reports
log "\n${BOLD}${BLUE}PHASE 5: Generating Reports${NC}"

# Generate text report
{
    echo "════════════════════════════════════════════════════════════════════"
    echo "                    COMPREHENSIVE TEST REPORT"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    echo "Test Run ID: ${TEST_RUN_ID}"
    echo "Date: $(date)"
    echo "Configuration: ${CONFIG_FILE}"
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "                         TEST SUMMARY"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    echo "Total Test Suites: ${TOTAL_SUITES}"
    echo "Passed: ${PASSED_SUITES}"
    echo "Failed: ${FAILED_SUITES}"
    echo "Skipped: ${SKIPPED_SUITES}"
    echo ""
    echo "Success Rate: $(( TOTAL_SUITES > 0 ? PASSED_SUITES * 100 / TOTAL_SUITES : 0 ))%"
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "                      DETAILED RESULTS"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    
    for result in "${TEST_RESULTS[@]}"; do
        IFS=':' read -r suite status <<< "$result"
        if [ "$status" = "PASSED" ]; then
            echo "[✓] ${suite}: PASSED"
        elif [ "$status" = "FAILED" ]; then
            echo "[✗] ${suite}: FAILED"
        else
            echo "[⊘] ${suite}: SKIPPED"
        fi
    done
    
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "                           LOG FILES"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    echo "Master Log: ${MASTER_LOG}"
    echo "Test Directory: ${TEST_DIR}"
    echo ""
    echo "Individual Test Logs:"
    ls -la "${TEST_DIR}/logs/" 2>/dev/null | tail -n +4 || echo "  No individual logs found"
    
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "                      RECOMMENDATIONS"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    
    if [ ${FAILED_SUITES} -eq 0 ]; then
        echo "✅ All tests passed successfully! The system is functioning correctly."
    else
        echo "⚠️  Some tests failed. Please review the individual log files for details."
        echo ""
        echo "Common troubleshooting steps:"
        echo "1. Check the configuration file for correct Snowflake credentials"
        echo "2. Ensure Snowflake warehouse is running and accessible"
        echo "3. Verify network connectivity to Snowflake"
        echo "4. Check file permissions in the test directory"
        echo "5. Review individual test logs for specific error messages"
    fi
    
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "                    END OF REPORT"
    echo "════════════════════════════════════════════════════════════════════"
} > "${REPORT_FILE}"

log "  ${GREEN}✓ Text report generated: ${REPORT_FILE}${NC}"

# Generate HTML report
{
    cat <<EOF
<!DOCTYPE html>
<html>
<head>
    <title>Snowflake ETL Test Report - ${TEST_RUN_ID}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 10px; }
        .summary { background: white; padding: 20px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .passed { color: #27ae60; font-weight: bold; }
        .failed { color: #e74c3c; font-weight: bold; }
        .skipped { color: #f39c12; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; background: white; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #34495e; color: white; }
        .footer { text-align: center; color: #7f8c8d; margin-top: 40px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Snowflake ETL Comprehensive Test Report</h1>
        <p>Test ID: ${TEST_RUN_ID}</p>
        <p>Generated: $(date)</p>
    </div>
    
    <div class="summary">
        <h2>Test Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Total Test Suites</td><td>${TOTAL_SUITES}</td></tr>
            <tr><td>Passed</td><td class="passed">${PASSED_SUITES}</td></tr>
            <tr><td>Failed</td><td class="failed">${FAILED_SUITES}</td></tr>
            <tr><td>Skipped</td><td class="skipped">${SKIPPED_SUITES}</td></tr>
            <tr><td>Success Rate</td><td>$(( TOTAL_SUITES > 0 ? PASSED_SUITES * 100 / TOTAL_SUITES : 0 ))%</td></tr>
        </table>
    </div>
    
    <div class="summary">
        <h2>Test Results</h2>
        <table>
            <tr><th>Test Suite</th><th>Status</th></tr>
EOF
    
    for result in "${TEST_RESULTS[@]}"; do
        IFS=':' read -r suite status <<< "$result"
        if [ "$status" = "PASSED" ]; then
            echo "<tr><td>${suite}</td><td class='passed'>✓ PASSED</td></tr>"
        elif [ "$status" = "FAILED" ]; then
            echo "<tr><td>${suite}</td><td class='failed'>✗ FAILED</td></tr>"
        else
            echo "<tr><td>${suite}</td><td class='skipped'>⊘ SKIPPED</td></tr>"
        fi
    done
    
    cat <<EOF
        </table>
    </div>
    
    <div class="footer">
        <p>Full logs available at: ${TEST_DIR}</p>
    </div>
</body>
</html>
EOF
} > "${HTML_REPORT}"

log "  ${GREEN}✓ HTML report generated: ${HTML_REPORT}${NC}"

# Phase 6: Create Archive
log "\n${BOLD}${BLUE}PHASE 6: Creating Archive${NC}"

ARCHIVE_FILE="${SCRIPT_DIR}/test_results_${TEST_RUN_ID}.tar.gz"
tar -czf "${ARCHIVE_FILE}" -C "${SCRIPT_DIR}/test_runs" "${TEST_RUN_ID}" 2>/dev/null

log "  ${GREEN}✓ Archive created: ${ARCHIVE_FILE}${NC}"

# Final Summary
log "\n${BOLD}${MAGENTA}════════════════════════════════════════════════════════════════════${NC}"
log "${BOLD}${MAGENTA}                         TEST EXECUTION COMPLETE${NC}"
log "${BOLD}${MAGENTA}════════════════════════════════════════════════════════════════════${NC}"

if [ ${FAILED_SUITES} -eq 0 ]; then
    log "\n  ${BOLD}${GREEN}✅ ALL TESTS PASSED!${NC}"
else
    log "\n  ${BOLD}${RED}⚠️  SOME TESTS FAILED${NC}"
fi

log "\n${CYAN}📊 Reports Generated:${NC}"
log "  • Text Report: ${REPORT_FILE}"
log "  • HTML Report: ${HTML_REPORT}"
log "  • Archive: ${ARCHIVE_FILE}"
log ""
log "${CYAN}📁 Test Results Location:${NC}"
log "  ${TEST_DIR}"
log ""

# Display the text report
echo ""
cat "${REPORT_FILE}"

# Exit with appropriate code
if [ ${FAILED_SUITES} -eq 0 ]; then
    exit 0
else
    exit 1
fi