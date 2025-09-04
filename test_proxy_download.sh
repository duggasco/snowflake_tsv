#!/bin/bash

# Test script for debugging proxy download issues
echo "=== Proxy Download Test ==="
echo ""

# Check environment variables
echo "Current proxy settings:"
echo "  http_proxy: ${http_proxy:-<not set>}"
echo "  https_proxy: ${https_proxy:-<not set>}"
echo "  HTTP_PROXY: ${HTTP_PROXY:-<not set>}"
echo "  HTTPS_PROXY: ${HTTPS_PROXY:-<not set>}"
echo ""

# Test URL
TEST_URL="https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz"
TEST_FILE="/tmp/test_download.tgz"

# Function to test wget
test_wget() {
    echo "=== Testing wget ==="
    
    # Method 1: Environment variables only
    echo "Method 1: Using environment variables..."
    if wget --version >/dev/null 2>&1; then
        wget --spider --tries=1 --timeout=10 "$TEST_URL" 2>&1 | head -20
        echo "Exit code: $?"
    else
        echo "wget not found"
    fi
    echo ""
    
    # Method 2: With --proxy=on
    echo "Method 2: Using --proxy=on flag..."
    if wget --version >/dev/null 2>&1; then
        wget --spider --proxy=on --tries=1 --timeout=10 "$TEST_URL" 2>&1 | head -20
        echo "Exit code: $?"
    else
        echo "wget not found"
    fi
    echo ""
}

# Function to test curl
test_curl() {
    echo "=== Testing curl ==="
    
    # Method 1: Environment variables
    echo "Method 1: Using environment variables..."
    if curl --version >/dev/null 2>&1; then
        curl -I --max-time 10 "$TEST_URL" 2>&1 | head -20
        echo "Exit code: $?"
    else
        echo "curl not found"
    fi
    echo ""
    
    # Method 2: Explicit proxy
    if [[ -n "${https_proxy:-}" ]]; then
        echo "Method 2: Using --proxy flag with: ${https_proxy}"
        if curl --version >/dev/null 2>&1; then
            curl -I --proxy "${https_proxy}" --max-time 10 "$TEST_URL" 2>&1 | head -20
            echo "Exit code: $?"
        fi
    else
        echo "Method 2: Skipped (no proxy set)"
    fi
    echo ""
}

# Function to test actual download
test_download() {
    echo "=== Testing actual download (small file) ==="
    
    # Use a small test file
    local test_url="https://www.python.org/robots.txt"
    
    echo "Downloading $test_url..."
    
    # Try wget
    echo "With wget:"
    rm -f /tmp/robots.txt
    if wget -O /tmp/robots.txt --tries=1 --timeout=10 "$test_url" 2>&1; then
        echo "SUCCESS: Downloaded $(wc -c < /tmp/robots.txt) bytes"
        rm -f /tmp/robots.txt
    else
        echo "FAILED: wget download failed"
    fi
    echo ""
    
    # Try curl
    echo "With curl:"
    rm -f /tmp/robots.txt
    if curl -o /tmp/robots.txt --max-time 10 "$test_url" 2>&1; then
        echo "SUCCESS: Downloaded $(wc -c < /tmp/robots.txt) bytes"
        rm -f /tmp/robots.txt
    else
        echo "FAILED: curl download failed"
    fi
}

# Run tests
test_wget
test_curl
test_download

echo ""
echo "=== Recommendations ==="
echo ""
echo "If downloads are failing:"
echo "1. Check if proxy URL is correct and includes protocol (http:// or https://)"
echo "2. Check if proxy requires authentication (user:pass@proxy:port)"
echo "3. Try setting both http_proxy and https_proxy"
echo "4. Some proxies may block HTTPS - try HTTP URLs"
echo "5. Corporate proxies may require specific authentication methods"
echo ""
echo "Example proxy formats:"
echo "  export https_proxy=http://proxy.company.com:8080"
echo "  export https_proxy=http://user:pass@proxy.company.com:8080"
echo "  export http_proxy=\$https_proxy"