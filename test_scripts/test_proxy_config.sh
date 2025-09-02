#!/bin/bash

# Test script for proxy configuration functionality
# Tests the PyPI connectivity check and proxy configuration

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}=== Proxy Configuration Test ===${NC}"
echo

# Source just the proxy functions from snowflake_etl.sh
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="${SCRIPT_DIR}/.etl_state"
PREFS_DIR="${STATE_DIR}"

# Extract and test the connectivity function
test_connectivity() {
    local test_url="https://pypi.org/simple/"
    
    echo -e "${YELLOW}Testing direct connectivity to PyPI...${NC}"
    
    # Test with curl
    if command -v curl >/dev/null 2>&1; then
        if curl -s --connect-timeout 5 "$test_url" >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Direct connection successful (curl)${NC}"
            return 0
        else
            echo -e "${RED}✗ Direct connection failed (curl)${NC}"
        fi
    fi
    
    # Test with wget
    if command -v wget >/dev/null 2>&1; then
        if wget -q --timeout=5 --spider "$test_url" 2>/dev/null; then
            echo -e "${GREEN}✓ Direct connection successful (wget)${NC}"
            return 0
        else
            echo -e "${RED}✗ Direct connection failed (wget)${NC}"
        fi
    fi
    
    # Test with Python
    if python3 -c "
import urllib.request
import sys
try:
    urllib.request.urlopen('$test_url', timeout=5)
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
        echo -e "${GREEN}✓ Direct connection successful (Python)${NC}"
        return 0
    else
        echo -e "${RED}✗ Direct connection failed (Python)${NC}"
    fi
    
    return 1
}

# Test proxy environment detection
test_proxy_env() {
    echo -e "${YELLOW}Checking for proxy environment variables...${NC}"
    
    if [[ -n "${https_proxy:-}" ]]; then
        echo -e "${CYAN}Found https_proxy: $https_proxy${NC}"
    fi
    
    if [[ -n "${HTTPS_PROXY:-}" ]]; then
        echo -e "${CYAN}Found HTTPS_PROXY: $HTTPS_PROXY${NC}"
    fi
    
    if [[ -n "${http_proxy:-}" ]]; then
        echo -e "${CYAN}Found http_proxy: $http_proxy${NC}"
    fi
    
    if [[ -n "${HTTP_PROXY:-}" ]]; then
        echo -e "${CYAN}Found HTTP_PROXY: $HTTP_PROXY${NC}"
    fi
    
    if [[ -z "${https_proxy:-}" ]] && [[ -z "${HTTPS_PROXY:-}" ]] && \
       [[ -z "${http_proxy:-}" ]] && [[ -z "${HTTP_PROXY:-}" ]]; then
        echo -e "${YELLOW}No proxy environment variables found${NC}"
    fi
}

# Test saved proxy configuration
test_saved_proxy() {
    local proxy_file="$PREFS_DIR/.proxy_config"
    
    echo -e "${YELLOW}Checking for saved proxy configuration...${NC}"
    
    if [[ -f "$proxy_file" ]]; then
        local proxy=$(cat "$proxy_file")
        echo -e "${GREEN}Found saved proxy: $proxy${NC}"
    else
        echo -e "${YELLOW}No saved proxy configuration${NC}"
    fi
}

# Test proxy connectivity with a mock proxy
test_proxy_connectivity() {
    local test_proxy="http://invalid.proxy.test:8080"
    local test_url="https://pypi.org/simple/"
    
    echo -e "${YELLOW}Testing with mock invalid proxy: $test_proxy${NC}"
    
    if curl -s --proxy "$test_proxy" --connect-timeout 3 "$test_url" >/dev/null 2>&1; then
        echo -e "${RED}Unexpected: Connection succeeded (proxy might be real?)${NC}"
    else
        echo -e "${GREEN}✓ Correctly detected invalid proxy${NC}"
    fi
}

# Run tests
echo -e "${CYAN}1. Testing Direct Connectivity${NC}"
echo "-----------------------------------"
test_connectivity
echo

echo -e "${CYAN}2. Checking Proxy Environment${NC}"
echo "-----------------------------------"
test_proxy_env
echo

echo -e "${CYAN}3. Checking Saved Configuration${NC}"
echo "-----------------------------------"
test_saved_proxy
echo

echo -e "${CYAN}4. Testing Proxy Detection${NC}"
echo "-----------------------------------"
test_proxy_connectivity
echo

# Summary
echo -e "${CYAN}=== Test Summary ===${NC}"
if test_connectivity; then
    echo -e "${GREEN}✓ Direct PyPI access available - no proxy needed${NC}"
else
    echo -e "${YELLOW}⚠ Direct PyPI access blocked - proxy may be required${NC}"
    echo -e "${CYAN}Run ./snowflake_etl.sh to configure proxy interactively${NC}"
fi

# Show how to test with a real proxy
echo
echo -e "${CYAN}To test with a real proxy:${NC}"
echo "  export https_proxy=http://your.proxy:8080"
echo "  ./snowflake_etl.sh"
echo
echo -e "${CYAN}To clear proxy configuration:${NC}"
echo "  rm $PREFS_DIR/.proxy_config"