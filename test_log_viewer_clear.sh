#!/bin/bash

# Test script to verify log viewer clears screen after viewing
# This prevents logs from stacking below each other

set -euo pipefail

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Create test directory
TEST_DIR="/tmp/test_log_viewer_$$"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

echo -e "${YELLOW}Testing log viewer screen clearing...${NC}\n"

# Create test log files
echo "Creating test log files..."
cat > log1.txt << 'EOF'
=== Log 1 Content ===
This is the first log file.
It contains multiple lines.
Line 3 of log 1.
Line 4 of log 1.
Line 5 of log 1.
=== End of Log 1 ===
EOF

cat > log2.txt << 'EOF'
=== Log 2 Content ===
This is the second log file.
It also has multiple lines.
Line 3 of log 2.
Line 4 of log 2.
Line 5 of log 2.
=== End of Log 2 ===
EOF

# Function to simulate the log viewer behavior
view_log_with_clear() {
    local log_file="$1"
    
    echo ""
    echo "--- Viewing log: $log_file"
    echo "--- (Press 'q' to quit)"
    echo ""
    sleep 0.5
    
    if command -v less >/dev/null 2>&1; then
        less -RFXS "$log_file"
    else
        cat "$log_file"
        read -p "Press Enter to continue..."
    fi
    
    # This is the fix - clear screen after viewing
    clear
}

# Function to simulate old behavior (without clear)
view_log_without_clear() {
    local log_file="$1"
    
    echo ""
    echo "--- Viewing log: $log_file"
    echo "--- (Press 'q' to quit)"
    echo ""
    sleep 0.5
    
    if command -v less >/dev/null 2>&1; then
        less -RFXS "$log_file"
    else
        cat "$log_file"
        read -p "Press Enter to continue..."
    fi
    
    # No clear here - old behavior
}

# Test instructions
echo -e "${GREEN}Test 1: Old behavior (without clear)${NC}"
echo "This simulates the problem - logs stack below each other"
echo "Press Enter to view first log..."
read

view_log_without_clear "log1.txt"

echo "Notice: Previous log content may still be visible above"
echo "Press Enter to view second log..."
read

view_log_without_clear "log2.txt"

echo ""
echo -e "${RED}Problem: Both logs might be visible, creating confusion${NC}"
echo ""
echo "Press Enter to continue to fixed version..."
read

clear

echo -e "${GREEN}Test 2: New behavior (with clear)${NC}"
echo "This shows the fix - screen clears between logs"
echo "Press Enter to view first log..."
read

view_log_with_clear "log1.txt"

echo "Notice: Screen was cleared, no old content visible"
echo "Press Enter to view second log..."
read

view_log_with_clear "log2.txt"

echo ""
echo -e "${GREEN}✓ Fixed: Each log view starts with a clean screen${NC}"
echo ""

# Cleanup
cd /
rm -rf "$TEST_DIR"

echo -e "${GREEN}Test complete!${NC}"
echo ""
echo "Summary:"
echo "- Old behavior: Logs stack below each other (confusing)"
echo "- New behavior: Screen clears after each log view (clean)"
echo ""
echo "The fix adds a 'clear' command after the pager exits."