#!/bin/bash
# Test script to see menu output

echo "Testing menu display..."
echo ""

# Use expect or similar to interact
(
    sleep 1
    echo ""  # Press enter to get past any initial prompts
    sleep 1
    echo "0"  # Select exit
) | ./snowflake_etl.sh 2>&1 | tee menu_output.txt

echo ""
echo "Menu output saved to menu_output.txt"
echo "First 50 lines:"
head -50 menu_output.txt