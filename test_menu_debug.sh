#!/bin/bash
# Test what's happening with the menu

# Source the libraries
source /root/snowflake/lib/colors.sh
source /root/snowflake/lib/ui_components.sh
source /root/snowflake/lib/common_functions.sh

# Set text mode
USE_DIALOG=false

# Create a simple test of main_menu logic
test_menu() {
    echo "Starting test_menu" >&2
    local counter=0
    while true; do
        counter=$((counter + 1))
        echo "Loop iteration: $counter" >&2
        
        local choice=$(show_menu "TEST MENU" \
            "Option 1" \
            "Option 2" \
            "Exit")
        
        echo "Got choice: '$choice'" >&2
        
        case "$choice" in
            1) echo "Selected option 1" >&2 ;;
            2) echo "Selected option 2" >&2 ;;
            3) echo "Selected Exit (3)" >&2; break ;;
            0|"") 
                echo "Selected 0 or empty" >&2
                break 
                ;;
            *) 
                echo "Invalid option: '$choice'" >&2
                if [[ $counter -gt 5 ]]; then
                    echo "Too many iterations, breaking" >&2
                    break
                fi
                ;;
        esac
    done
}

echo "Test 1: Direct test"
echo "0" | test_menu

echo ""
echo "Test 2: Check what stdin has"
echo "test input" | ( 
    read first_input
    echo "First read got: '$first_input'"
    read second_input  
    echo "Second read got: '$second_input'"
)