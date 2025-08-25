#!/bin/bash
# lib/common_functions.sh - Common utility functions for shell scripts
# Part of Snowflake ETL Pipeline Manager

# Default directories
STATE_DIR="${STATE_DIR:-$HOME/.snowflake_etl}"
CONFIG_DIR="${CONFIG_DIR:-$HOME/snowflake/config}"
JOBS_DIR="$STATE_DIR/jobs"
LOCKS_DIR="$STATE_DIR/locks"
LOGS_DIR="${LOGS_DIR:-logs}"
PREFS_FILE="$STATE_DIR/preferences"

# Create necessary directories
init_directories() {
    mkdir -p "$STATE_DIR" "$JOBS_DIR" "$LOCKS_DIR" "$LOGS_DIR"
    touch "$PREFS_FILE"
}

# Save a preference
save_preference() {
    local key="$1"
    local value="$2"
    
    # Remove old value if exists
    grep -v "^$key=" "$PREFS_FILE" > "${PREFS_FILE}.tmp" 2>/dev/null || true
    echo "$key=$value" >> "${PREFS_FILE}.tmp"
    mv "${PREFS_FILE}.tmp" "$PREFS_FILE"
}

# Get a preference
get_preference() {
    local key="$1"
    local default="${2:-}"
    
    if [[ -f "$PREFS_FILE" ]]; then
        local value=$(grep "^$key=" "$PREFS_FILE" 2>/dev/null | cut -d'=' -f2-)
        echo "${value:-$default}"
    else
        echo "$default"
    fi
}

# Get tables from config file
get_tables_from_config() {
    local config_file="${1:-$CONFIG_FILE}"
    
    if [[ ! -f "$config_file" ]]; then
        echo ""
        return 1
    fi
    
    # Extract table names from the JSON config
    python3 -c "
import json
import sys

try:
    with open('$config_file', 'r') as f:
        config = json.load(f)
    
    tables = []
    if 'files' in config:
        for file_config in config['files']:
            if 'table_name' in file_config:
                tables.append(file_config['table_name'])
    
    # Return unique tables
    print(' '.join(sorted(set(tables))))
except Exception as e:
    sys.exit(1)
" 2>/dev/null
}

# Select table from config or prompt
select_table() {
    local prompt_msg="${1:-Select Table}"
    local default_table="${2:-}"
    local allow_all="${3:-false}"  # Allow 'all' as an option
    
    # Get tables from config
    local config_tables=$(get_tables_from_config)
    
    if [[ -z "$config_tables" ]]; then
        # No tables in config, prompt for input
        local table=$(get_input "$prompt_msg" "Enter table name" "$default_table")
        echo "$table"
        return
    fi
    
    # Convert to array
    local tables_array=($config_tables)
    
    # If only one table, use it automatically (unless allow_all is true)
    if [[ ${#tables_array[@]} -eq 1 ]] && [[ "$allow_all" != "true" ]]; then
        echo "${tables_array[0]}"
        return
    fi
    
    # Multiple tables or allow_all, show selection menu
    local menu_options=()
    
    # Add 'all' option if requested
    if [[ "$allow_all" == "true" ]]; then
        menu_options+=("[ALL TABLES]")
    fi
    
    for table in "${tables_array[@]}"; do
        menu_options+=("$table")
    done
    menu_options+=("[Enter custom table name]")
    
    local choice=$(show_menu "$prompt_msg" "${menu_options[@]}")
    
    if [[ "$choice" == "0" ]] || [[ -z "$choice" ]]; then
        echo ""
        return 1
    fi
    
    local selected_index=$((choice - 1))
    
    # Check for special options
    if [[ "$allow_all" == "true" ]] && [[ $selected_index -eq 0 ]]; then
        echo "ALL"
        return
    fi
    
    # Adjust index if all option was present
    if [[ "$allow_all" == "true" ]]; then
        selected_index=$((selected_index - 1))
    fi
    
    # Check if custom entry was selected
    if [[ $selected_index -ge ${#tables_array[@]} ]]; then
        local custom_table=$(get_input "$prompt_msg" "Enter table name" "$default_table")
        echo "$custom_table"
    else
        echo "${tables_array[$selected_index]}"
    fi
}

# Execute with file lock
with_lock() {
    local lock_file="${LOCKS_DIR}/manager.lock"
    local lock_fd=200
    
    # Create lock file if it doesn't exist
    touch "$lock_file"
    
    # Open lock file on file descriptor
    eval "exec $lock_fd>\"$lock_file\""
    
    # Try to acquire lock
    if ! flock -n $lock_fd; then
        echo "ERROR: Another operation is already in progress." >&2
        eval "exec $lock_fd>&-"
        return 1
    fi
    
    # Set trap to release lock on exit
    trap "flock -u $lock_fd; eval \"exec $lock_fd>&-\"" EXIT INT TERM
    
    # Execute the command
    "$@"
    local result=$?
    
    # Release lock
    flock -u $lock_fd
    eval "exec $lock_fd>&-"
    
    # Remove trap
    trap - EXIT INT TERM
    
    return $result
}

# Select configuration file
select_config_file() {
    local require_selection="${1:-false}"  # If true, force selection even if CONFIG_FILE is set
    
    # If we already have a valid config and not forcing selection, use it
    if [[ -f "$CONFIG_FILE" ]] && [[ "$require_selection" != "true" ]]; then
        return 0
    fi
    
    # Find all JSON config files
    local config_files=()
    if [[ -d "$CONFIG_DIR" ]]; then
        while IFS= read -r -d '' file; do
            config_files+=("$(basename "$file")")
        done < <(find "$CONFIG_DIR" -maxdepth 1 -name "*.json" -type f -print0 | sort -z)
    fi
    
    # Check if any configs exist
    if [[ ${#config_files[@]} -eq 0 ]]; then
        show_message "Error" "No configuration files found in $CONFIG_DIR\nPlease create a config file first."
        return 1
    fi
    
    # If only one config exists, use it automatically
    if [[ ${#config_files[@]} -eq 1 ]] && [[ "$require_selection" != "true" ]]; then
        CONFIG_FILE="$CONFIG_DIR/${config_files[0]}"
        save_preference "LAST_CONFIG" "$CONFIG_FILE"
        return 0
    fi
    
    # Show selection menu
    local title="Select Configuration File"
    if [[ -n "$CONFIG_FILE" ]]; then
        title="$title (Current: $(basename "$CONFIG_FILE"))"
    fi
    
    # Build menu options
    local menu_options=()
    for config in "${config_files[@]}"; do
        # Try to extract description from config
        local desc=""
        if [[ -f "$CONFIG_DIR/$config" ]]; then
            # Try to get database/warehouse from config
            local db=$(grep -o '"database"[[:space:]]*:[[:space:]]*"[^"]*"' "$CONFIG_DIR/$config" 2>/dev/null | head -1 | cut -d'"' -f4)
            local wh=$(grep -o '"warehouse"[[:space:]]*:[[:space:]]*"[^"]*"' "$CONFIG_DIR/$config" 2>/dev/null | head -1 | cut -d'"' -f4)
            if [[ -n "$db" ]] || [[ -n "$wh" ]]; then
                desc=" (${db:-?}/${wh:-?})"
            fi
        fi
        menu_options+=("$config$desc")
    done
    
    # Show menu and get selection
    local choice=$(show_menu "$title" "${menu_options[@]}")
    
    if [[ "$choice" == "0" ]] || [[ -z "$choice" ]]; then
        if [[ -z "$CONFIG_FILE" ]]; then
            show_message "Error" "No configuration selected. Operation cancelled."
            return 1
        fi
        return 0  # Keep existing config
    fi
    
    # Extract just the filename from the selection (remove description)
    local selected_index=$((choice - 1))
    local selected_config="${config_files[$selected_index]}"
    CONFIG_FILE="$CONFIG_DIR/$selected_config"
    
    # Save as preference
    save_preference "LAST_CONFIG" "$CONFIG_FILE"
    
    echo -e "${GREEN}Selected configuration: $CONFIG_FILE${NC}"
    return 0
}

# Check dependencies
check_dependencies() {
    local missing=()
    
    # Check for required commands
    for cmd in python3 grep cut wc; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo -e "${RED}Missing required commands: ${missing[*]}${NC}" >&2
        return 1
    fi
    
    return 0
}

# Export functions for use in scripts that source this file
export -f init_directories
export -f save_preference
export -f get_preference
export -f get_tables_from_config
export -f select_table
export -f with_lock
export -f select_config_file
export -f check_dependencies