#!/bin/bash

#############################################################################
# Simplified Test Runner for Snowflake ETL Pipeline
# 
# This version runs tests without hanging issues
#############################################################################

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_RUN_ID="$(date +%Y%m%d_%H%M%S)"
TEST_DIR="${SCRIPT_DIR}/test_runs/${TEST_RUN_ID}"
CONFIG_FILE="${1:-${SCRIPT_DIR}/config/factLendingBenchmark_config.json}"

# Create directories
mkdir -p "${TEST_DIR}/logs"

# Setup Python environment
if [ -f "${SCRIPT_DIR}/etl_venv/bin/activate" ]; then
    source "${SCRIPT_DIR}/etl_venv/bin/activate"
    PYTHON_CMD="${SCRIPT_DIR}/etl_venv/bin/python3"
else
    PYTHON_CMD="python3"
fi

echo -e "${BOLD}${MAGENTA}"
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           SNOWFLAKE ETL TEST SUITE (SIMPLIFIED)               ║"
echo "║                                                                ║"
echo "║  Date: $(date +'%Y-%m-%d %H:%M:%S')                              ║"
echo "║  Test ID: ${TEST_RUN_ID}                                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "\n${BOLD}${BLUE}PHASE 1: Environment Check${NC}"

# Check virtual environment
if [ -f "${SCRIPT_DIR}/etl_venv/bin/activate" ]; then
    echo -e "  ${GREEN}✓ Virtual environment found${NC}"
else
    echo -e "  ${YELLOW}⚠ Virtual environment not found${NC}"
fi

# Check Python
if command -v ${PYTHON_CMD} > /dev/null; then
    python_version=$(${PYTHON_CMD} --version 2>&1 | cut -d' ' -f2)
    echo -e "  ${GREEN}✓ Python installed: ${python_version}${NC}"
else
    echo -e "  ${RED}✗ Python not found${NC}"
    exit 1
fi

# Check packages
echo -e "\n${BOLD}${BLUE}PHASE 2: Package Check${NC}"

packages=("snowflake.connector" "pandas" "numpy" "tqdm")
for pkg in "${packages[@]}"; do
    if ${PYTHON_CMD} -c "import ${pkg}" 2>/dev/null; then
        echo -e "  ${GREEN}✓ ${pkg} installed${NC}"
    else
        echo -e "  ${YELLOW}⚠ ${pkg} not installed${NC}"
    fi
done

# Check Snowflake connectivity
echo -e "\n${BOLD}${BLUE}PHASE 3: Snowflake Connectivity${NC}"

${PYTHON_CMD} "${SCRIPT_DIR}/test_connectivity.py" "${CONFIG_FILE}" 2>&1
if [ $? -eq 0 ]; then
    echo -e "  ${GREEN}✓ Snowflake connection successful${NC}"
    SNOWFLAKE_AVAILABLE=true
else
    echo -e "  ${YELLOW}⚠ Snowflake connection failed - running in offline mode${NC}"
    SNOWFLAKE_AVAILABLE=false
fi

# Run appropriate test suite
echo -e "\n${BOLD}${BLUE}PHASE 4: Running Tests${NC}"

if [ "$SNOWFLAKE_AVAILABLE" = "true" ]; then
    echo -e "\n${CYAN}Running full test suite...${NC}"
    echo -e "${YELLOW}(Not implemented - would run test_cli_suite.sh)${NC}"
else
    echo -e "\n${CYAN}Running basic offline tests...${NC}"
    
    # Run the basic test suite
    if [ -f "${SCRIPT_DIR}/test_cli_basic.sh" ]; then
        "${SCRIPT_DIR}/test_cli_basic.sh" "${CONFIG_FILE}"
        TEST_RESULT=$?
    else
        echo -e "  ${RED}✗ Basic test suite not found${NC}"
        TEST_RESULT=1
    fi
fi

# Summary
echo -e "\n${BOLD}${MAGENTA}════════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${MAGENTA}                         TEST EXECUTION COMPLETE${NC}"
echo -e "${BOLD}${MAGENTA}════════════════════════════════════════════════════════════════════${NC}"

if [ ${TEST_RESULT:-1} -eq 0 ]; then
    echo -e "\n  ${BOLD}${GREEN}✅ TESTS PASSED!${NC}"
    exit 0
else
    echo -e "\n  ${BOLD}${RED}⚠️  SOME TESTS FAILED${NC}"
    exit 1
fi