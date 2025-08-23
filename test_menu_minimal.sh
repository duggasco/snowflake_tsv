#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source libraries
source "${SCRIPT_DIR}/lib/colors.sh"
source "${SCRIPT_DIR}/lib/ui_components.sh"

# Force text mode
USE_DIALOG=false

echo "Testing menu directly..."
choice=$(echo "0" | show_menu "TEST MENU" "Option 1" "Option 2" "Option 3")
echo "Menu returned: '$choice'"

if [[ "$choice" == "0" ]]; then
    echo "Success - got 0"
else
    echo "Failed - got something else"
fi