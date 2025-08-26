#!/bin/bash
# lib/ui_components.sh - UI components for shell scripts
# Part of Snowflake ETL Pipeline Manager

# Color codes - define if not already defined by parent script
if [[ -z "$RED" ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    NC='\033[0m' # No Color
fi

# Dialog configuration - use values from main script if set, otherwise defaults
DIALOG_CMD=${DIALOG_CMD:-dialog}
USE_DIALOG=${USE_DIALOG:-false}
DIALOG_MAX_HEIGHT=${DIALOG_MAX_HEIGHT:-40}
DIALOG_MAX_WIDTH=${DIALOG_MAX_WIDTH:-120}

# Only check for dialog if USE_DIALOG not already set by main script
if [[ "$USE_DIALOG" == "false" ]]; then
    if command -v $DIALOG_CMD >/dev/null 2>&1; then
        USE_DIALOG=true
    fi
fi

# Get terminal size
get_terminal_size() {
    local rows cols
    if command -v tput >/dev/null 2>&1; then
        rows=$(tput lines)
        cols=$(tput cols)
    else
        rows=24
        cols=80
    fi
    echo "$rows $cols"
}

# Calculate appropriate dialog dimensions based on content
calculate_dialog_dimensions() {
    local content="$1"
    local min_height="${2:-8}"
    local min_width="${3:-50}"
    
    # Count lines and find max line length
    local line_count=0
    local max_length=0
    
    while IFS= read -r line; do
        ((line_count++))
        local line_length=${#line}
        if [[ $line_length -gt $max_length ]]; then
            max_length=$line_length
        fi
    done <<< "$content"
    
    # Calculate height (add padding for borders and buttons)
    local calc_height=$((line_count + 6))
    if [[ $calc_height -lt $min_height ]]; then
        calc_height=$min_height
    elif [[ $calc_height -gt $DIALOG_MAX_HEIGHT ]]; then
        calc_height=$DIALOG_MAX_HEIGHT
    fi
    
    # Calculate width (add padding for borders)
    local calc_width=$((max_length + 10))
    if [[ $calc_width -lt $min_width ]]; then
        calc_width=$min_width
    elif [[ $calc_width -gt $DIALOG_MAX_WIDTH ]]; then
        calc_width=$DIALOG_MAX_WIDTH
    fi
    
    # Return dimensions
    echo "$calc_height $calc_width"
}

# Show menu with dynamic sizing
show_menu() {
    local title="$1"
    shift
    local options=("$@")
    
    if [[ "$USE_DIALOG" == true ]]; then
        local menu_items=()
        local i=1
        local max_option_length=0
        
        for opt in "${options[@]}"; do
            if [[ "$opt" != "---" ]]; then
                menu_items+=("$i" "$opt")
                # Track longest option for width calculation
                if [[ ${#opt} -gt $max_option_length ]]; then
                    max_option_length=${#opt}
                fi
                ((i++))
            fi
        done
        
        # Calculate dynamic dimensions
        local menu_height=$((${#options[@]} + 8))
        local menu_width=$((max_option_length + 20))
        
        # Apply min/max limits
        if [[ $menu_height -lt 12 ]]; then
            menu_height=12
        elif [[ $menu_height -gt $DIALOG_MAX_HEIGHT ]]; then
            menu_height=$DIALOG_MAX_HEIGHT
        fi
        
        if [[ $menu_width -lt 60 ]]; then
            menu_width=60
        elif [[ $menu_width -gt $DIALOG_MAX_WIDTH ]]; then
            menu_width=$DIALOG_MAX_WIDTH
        fi
        
        # Ensure it fits in terminal
        local term_size=$(get_terminal_size)
        local term_height=$(echo $term_size | cut -d' ' -f1)
        local term_width=$(echo $term_size | cut -d' ' -f2)
        
        if [[ $menu_height -gt $((term_height - 4)) ]]; then
            menu_height=$((term_height - 4))
        fi
        if [[ $menu_width -gt $((term_width - 4)) ]]; then
            menu_width=$((term_width - 4))
        fi
        
        local menu_list_height=$((menu_height - 8))
        if [[ $menu_list_height -lt 1 ]]; then
            menu_list_height=1
        fi
        
        local choice
        choice=$($DIALOG_CMD --clear --title "$title" \
            --menu "Select an option:" $menu_height $menu_width $menu_list_height \
            "${menu_items[@]}" 2>&1 >/dev/tty)
        
        echo "$choice"
    else
        # Fallback to text menu - display to stderr so it's always visible
        echo "" >&2
        echo "╔════════════════════════════════════════════════════════╗" >&2
        printf "║  %-54s║\n" "$title" >&2
        echo "╠════════════════════════════════════════════════════════╣" >&2
        
        local i=1
        for opt in "${options[@]}"; do
            if [[ "$opt" == "---" ]]; then
                echo "║────────────────────────────────────────────────────────║" >&2
            else
                printf "║  ${CYAN}%2d)${NC} %-50s║\n" "$i" "$opt" >&2
                ((i++))
            fi
        done
        
        echo "╠════════════════════════════════════════════════════════╣" >&2
        printf "║  ${CYAN}%2d)${NC} %-50s║\n" "0" "Back/Exit" >&2
        echo "╚════════════════════════════════════════════════════════╝" >&2
        echo "" >&2
        
        read -p "Enter choice: " choice
        echo "$choice"
    fi
}

# Show message with dynamic sizing
show_message() {
    local title="$1"
    local message="$2"
    
    if [[ "$USE_DIALOG" == true ]]; then
        # Calculate dynamic dimensions based on content
        local dimensions=$(calculate_dialog_dimensions "$message" 8 60)
        local height=$(echo $dimensions | cut -d' ' -f1)
        local width=$(echo $dimensions | cut -d' ' -f2)
        
        # Ensure dialog fits in terminal
        local term_size=$(get_terminal_size)
        local term_height=$(echo $term_size | cut -d' ' -f1)
        local term_width=$(echo $term_size | cut -d' ' -f2)
        
        # Leave room for terminal borders
        if [[ $height -gt $((term_height - 4)) ]]; then
            height=$((term_height - 4))
        fi
        if [[ $width -gt $((term_width - 4)) ]]; then
            width=$((term_width - 4))
        fi
        
        # Use scrollable text box for very long content
        if [[ ${#message} -gt 2000 ]]; then
            # For very long content, use --textbox with temp file
            local temp_file="/tmp/dialog_msg_$$"
            echo "$message" > "$temp_file"
            $DIALOG_CMD --title "$title" --textbox "$temp_file" $height $width
            rm -f "$temp_file"
        else
            $DIALOG_CMD --title "$title" --msgbox "$message" $height $width
        fi
    else
        echo ""
        echo -e "${BOLD}=== $title ===${NC}"
        echo -e "$message"  # Use -e to interpret escape sequences
        echo ""
        read -p "Press Enter to continue..."
    fi
}

# Get input from user
get_input() {
    local title="$1"
    local prompt="$2"
    local default="${3:-}"
    
    if [[ "$USE_DIALOG" == true ]]; then
        local result
        result=$($DIALOG_CMD --title "$title" --inputbox "$prompt" 10 60 "$default" 2>&1 >/dev/tty)
        echo "$result"
    else
        echo ""
        echo -e "${BOLD}$title${NC}"
        if [[ -n "$default" ]]; then
            read -p "$prompt [$default]: " result
            echo "${result:-$default}"
        else
            read -p "$prompt: " result
            echo "$result"
        fi
    fi
}

# Yes/No confirmation
confirm_action() {
    local message="$1"
    
    if [[ "$USE_DIALOG" == true ]]; then
        $DIALOG_CMD --title "Confirmation" --yesno "$message" 10 60
        return $?
    else
        echo ""
        echo -en "${YELLOW}$message (y/N): ${NC}"  # Use -e to interpret colors, -n to avoid newline
        read -n 1 -r
        echo ""
        [[ $REPLY =~ ^[Yy]$ ]]
        return $?
    fi
}

# Export functions for use in scripts that source this file
export -f get_terminal_size
export -f calculate_dialog_dimensions
export -f show_menu
export -f show_message
export -f get_input
export -f confirm_action