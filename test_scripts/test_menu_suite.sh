#!/bin/bash

#############################################################################
# Menu System Test Suite for Snowflake ETL Pipeline
# 
# This script tests the interactive menu functionality
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
MENU_TEST_LOG="${TEST_DIR}/menu_test_$(date +%Y%m%d_%H%M%S).log"
CONFIG_FILE="${1:-${SCRIPT_DIR}/config/factLendingBenchmark_config.json}"

mkdir -p "${TEST_DIR}"

#############################################################################
# Helper Functions
#############################################################################

log() {
    echo -e "$1" | tee -a "${MENU_TEST_LOG}"
}

test_menu_navigation() {
    local menu_sequence="$1"
    local description="$2"
    
    log "${CYAN}Testing: ${description}${NC}"
    
    # Create expect script for automated menu navigation
    cat > "${TEST_DIR}/menu_test.exp" << EOF
#!/usr/bin/expect -f
set timeout 10
log_file -a "${MENU_TEST_LOG}"

spawn ./snowflake_etl.sh

# Wait for main menu
expect "Main Menu"

# Execute menu sequence
${menu_sequence}

# Exit
send "0\r"
expect eof
EOF
    
    chmod +x "${TEST_DIR}/menu_test.exp"
    
    if command -v expect > /dev/null; then
        "${TEST_DIR}/menu_test.exp"
        log "${GREEN}✓ Test completed${NC}"
    else
        log "${YELLOW}⊘ Skipped (expect not installed)${NC}"
    fi
}

#############################################################################
# Test Suite Header
#############################################################################

log "${MAGENTA}================================================${NC}"
log "${MAGENTA}Menu System Test Suite${NC}"
log "${MAGENTA}================================================${NC}"
log "Test Started: $(date)"
log ""

# Check if expect is installed
if ! command -v expect > /dev/null; then
    log "${YELLOW}Warning: 'expect' is not installed. Install with:${NC}"
    log "${YELLOW}  Ubuntu/Debian: sudo apt-get install expect${NC}"
    log "${YELLOW}  RHEL/CentOS: sudo yum install expect${NC}"
    log "${YELLOW}  macOS: brew install expect${NC}"
    log ""
    log "${YELLOW}Running basic menu structure test only...${NC}"
fi

#############################################################################
# Test 1: Basic Menu Structure
#############################################################################

log "\n${CYAN}Test 1: Checking menu script exists${NC}"
if [ -f "${SCRIPT_DIR}/snowflake_etl.sh" ]; then
    log "${GREEN}✓ Menu script exists${NC}"
    
    # Check if it's executable
    if [ -x "${SCRIPT_DIR}/snowflake_etl.sh" ]; then
        log "${GREEN}✓ Menu script is executable${NC}"
    else
        log "${YELLOW}⚠ Making menu script executable${NC}"
        chmod +x "${SCRIPT_DIR}/snowflake_etl.sh"
    fi
else
    log "${RED}✗ Menu script not found${NC}"
    exit 1
fi

#############################################################################
# Test 2: Test Menu Navigation Sequences
#############################################################################

if command -v expect > /dev/null; then
    
    log "\n${CYAN}Test 2: Navigate to Data Operations${NC}"
    test_menu_navigation "send \"1\r\"; expect \"Data Operations\"; send \"0\r\"" "Data Operations menu"
    
    log "\n${CYAN}Test 3: Navigate to File Tools${NC}"
    test_menu_navigation "send \"2\r\"; expect \"File Tools\"; send \"0\r\"" "File Tools menu"
    
    log "\n${CYAN}Test 4: Navigate to Monitoring${NC}"
    test_menu_navigation "send \"3\r\"; expect \"Monitoring\"; send \"0\r\"" "Monitoring menu"
    
    log "\n${CYAN}Test 5: Navigate to Settings${NC}"
    test_menu_navigation "send \"4\r\"; expect \"Settings\"; send \"0\r\"" "Settings menu"
    
else
    log "${YELLOW}Automated menu tests skipped (expect not installed)${NC}"
fi

#############################################################################
# Test 3: Test run_loader.sh directly
#############################################################################

log "\n${CYAN}Test: Direct run_loader.sh execution${NC}"
if [ -f "${SCRIPT_DIR}/run_loader.sh" ]; then
    # Test help output
    if "${SCRIPT_DIR}/run_loader.sh" --help >> "${MENU_TEST_LOG}" 2>&1; then
        log "${GREEN}✓ run_loader.sh help works${NC}"
    else
        log "${RED}✗ run_loader.sh help failed${NC}"
    fi
else
    log "${RED}✗ run_loader.sh not found${NC}"
fi

#############################################################################
# Test 4: Test drop_month.sh directly
#############################################################################

log "\n${CYAN}Test: Direct drop_month.sh execution${NC}"
if [ -f "${SCRIPT_DIR}/drop_month.sh" ]; then
    # Test help output
    if "${SCRIPT_DIR}/drop_month.sh" --help >> "${MENU_TEST_LOG}" 2>&1; then
        log "${GREEN}✓ drop_month.sh help works${NC}"
    else
        log "${RED}✗ drop_month.sh help failed${NC}"
    fi
else
    log "${RED}✗ drop_month.sh not found${NC}"
fi

#############################################################################
# Summary
#############################################################################

log "\n${MAGENTA}================================================${NC}"
log "${MAGENTA}Menu Test Summary${NC}"
log "${MAGENTA}================================================${NC}"
log "Test log saved to: ${MENU_TEST_LOG}"
log ""

if command -v expect > /dev/null; then
    log "${GREEN}Full menu navigation tests completed${NC}"
else
    log "${YELLOW}Basic tests completed. Install 'expect' for full menu testing.${NC}"
fi

exit 0