#!/bin/bash

# Snowflake ETL Pipeline Manager - Unified Wrapper Script
# Version: 2.6.0 - Enhanced job results display
# Description: Interactive menu system for all Snowflake ETL operations

set -euo pipefail

# ============================================================================
# CONFIGURATION & GLOBALS
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"
VERSION="2.6.0"

# State management directories
STATE_DIR="${SCRIPT_DIR}/.etl_state"
JOBS_DIR="${STATE_DIR}/jobs"
LOCKS_DIR="${STATE_DIR}/locks"
PREFS_FILE="${STATE_DIR}/preferences"
LOGS_DIR="${SCRIPT_DIR}/logs"

# Color codes (for fallback mode)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default configuration
CONFIG_FILE=""  # Will be selected by user
BASE_PATH="${SCRIPT_DIR}/data"
DEFAULT_WORKERS="auto"
CONFIG_DIR="${SCRIPT_DIR}/config"

# Dialog/UI configuration
USE_DIALOG=false
DIALOG_CMD=""
DIALOG_HEIGHT=20
DIALOG_WIDTH=70

# ============================================================================
# CRITICAL SECURITY FIXES
# ============================================================================

# Extract tables from config file
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
    
    # Check if 'all' was selected
    if [[ "$allow_all" == "true" ]] && [[ $selected_index -eq 0 ]]; then
        echo "all"
        return
    fi
    
    # Adjust index if 'all' option was included
    if [[ "$allow_all" == "true" ]]; then
        selected_index=$((selected_index - 1))
    fi
    
    if [[ $selected_index -eq ${#tables_array[@]} ]]; then
        # User selected custom entry
        local table=$(get_input "$prompt_msg" "Enter table name" "$default_table")
        echo "$table"
    elif [[ $selected_index -ge 0 ]] && [[ $selected_index -lt ${#tables_array[@]} ]]; then
        echo "${tables_array[$selected_index]}"
    fi
}

# Safe file parsing - no source/eval
parse_job_file() {
    local file="$1"
    local key="$2"
    
    if [[ -f "$file" ]]; then
        grep "^${key}=" "$file" 2>/dev/null | cut -d'=' -f2- || echo ""
    else
        echo ""
    fi
}

# Write job file safely
write_job_file() {
    local file="$1"
    shift
    
    # Clear file first
    > "$file"
    
    # Write each key-value pair
    while [[ $# -gt 0 ]]; do
        local key="$1"
        local value="$2"
        # Escape any special characters in value
        printf "%s=%s\n" "$key" "$(printf '%q' "$value")" >> "$file"
        shift 2
    done
}

# Update job file value safely
update_job_file() {
    local file="$1"
    local key="$2"
    local value="$3"
    
    if [[ -f "$file" ]]; then
        # Create temp file with updated value
        grep -v "^${key}=" "$file" > "${file}.tmp" || true
        printf "%s=%s\n" "$key" "$(printf '%q' "$value")" >> "${file}.tmp"
        mv "${file}.tmp" "$file"
    fi
}

# Robust locking mechanism with cleanup
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

# ============================================================================
# INITIALIZATION
# ============================================================================

# Create necessary directories
init_directories() {
    mkdir -p "$STATE_DIR" "$JOBS_DIR" "$LOCKS_DIR" "$LOGS_DIR"
    touch "$PREFS_FILE"
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
    
    # Check for required Python modules (basic check)
    if ! python3 -c "import sys" 2>/dev/null; then
        missing+=("python3-functional")
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR: Missing required dependencies: ${missing[*]}"
        echo "Please install the missing tools and try again."
        exit 1
    fi
}

# Health check - clean up stale jobs
health_check_jobs() {
    local cleaned=0
    
    for job_file in "$JOBS_DIR"/*.job; do
        if [[ -f "$job_file" ]]; then
            local status=$(parse_job_file "$job_file" "STATUS")
            local pid=$(parse_job_file "$job_file" "PID")
            
            if [[ "$status" == "RUNNING" ]] && [[ -n "$pid" ]]; then
                # Check if process is still running
                if ! kill -0 "$pid" 2>/dev/null; then
                    update_job_file "$job_file" "STATUS" "CRASHED"
                    ((cleaned++))
                fi
            fi
        fi
    done
    
    if [[ $cleaned -gt 0 ]]; then
        echo "Note: Marked $cleaned stale job(s) as crashed."
    fi
}

# Detect dialog/whiptail availability
detect_ui_system() {
    if command -v dialog >/dev/null 2>&1; then
        USE_DIALOG=true
        DIALOG_CMD="dialog"
    elif command -v whiptail >/dev/null 2>&1; then
        USE_DIALOG=true
        DIALOG_CMD="whiptail"
    else
        USE_DIALOG=false
    fi
}

# Load preferences safely
load_preferences() {
    if [[ -f "$PREFS_FILE" ]]; then
        while IFS='=' read -r key value; do
            # Remove quotes if present
            value="${value%\"}"
            value="${value#\"}"
            
            case "$key" in
                LAST_CONFIG) 
                    # Verify config still exists
                    if [[ -f "$value" ]]; then
                        CONFIG_FILE="$value"
                    fi
                    ;;
                LAST_BASE_PATH) BASE_PATH="$value" ;;
                LAST_WORKERS) DEFAULT_WORKERS="$value" ;;
                USE_COLOR) 
                    if [[ "$value" == "false" ]]; then
                        RED="" GREEN="" YELLOW="" BLUE="" CYAN="" MAGENTA="" BOLD="" NC=""
                    fi
                    ;;
            esac
        done < "$PREFS_FILE"
    fi
}

# Select config file from available options
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
    
    show_message "Config Selected" "Using configuration: $selected_config"
    return 0
}

# Save preferences safely
save_preference() {
    local key="$1"
    local value="$2"
    
    with_lock bash -c "
        if [[ -f '$PREFS_FILE' ]]; then
            grep -v '^${key}=' '$PREFS_FILE' > '${PREFS_FILE}.tmp' || true
            mv '${PREFS_FILE}.tmp' '$PREFS_FILE'
        fi
        printf '%s=%q\n' '$key' '$value' >> '$PREFS_FILE'
    "
}

# ============================================================================
# UI FUNCTIONS (unchanged)
# ============================================================================

# Show dialog or fallback to echo/read
show_menu() {
    local title="$1"
    shift
    local options=("$@")
    
    if [[ "$USE_DIALOG" == true ]]; then
        local menu_items=()
        local i=1
        for opt in "${options[@]}"; do
            if [[ "$opt" != "---" ]]; then
                menu_items+=("$i" "$opt")
                ((i++))
            fi
        done
        
        local choice
        choice=$($DIALOG_CMD --clear --title "$title" \
            --menu "Select an option:" $DIALOG_HEIGHT $DIALOG_WIDTH $((DIALOG_HEIGHT-8)) \
            "${menu_items[@]}" 2>&1 >/dev/tty)
        
        echo "$choice"
    else
        # Fallback to text menu
        echo ""
        echo "╔════════════════════════════════════════════════════════╗"
        printf "║  %-54s║\n" "$title"
        echo "╠════════════════════════════════════════════════════════╣"
        
        local i=1
        for opt in "${options[@]}"; do
            if [[ "$opt" == "---" ]]; then
                echo "║────────────────────────────────────────────────────────║"
            else
                printf "║  ${CYAN}%2d)${NC} %-50s║\n" "$i" "$opt"
                ((i++))
            fi
        done
        
        echo "║                                                        ║"
        printf "║  ${RED}0)${NC} %-50s║\n" "Exit/Back"
        echo "╚════════════════════════════════════════════════════════╝"
        echo ""
        
        read -p "Enter choice: " choice
        
        # Validate input is numeric
        if ! [[ "$choice" =~ ^[0-9]+$ ]]; then
            echo "0"  # Default to exit on invalid input
        else
            echo "$choice"
        fi
    fi
}

# Show message
show_message() {
    local title="$1"
    local message="$2"
    
    if [[ "$USE_DIALOG" == true ]]; then
        $DIALOG_CMD --title "$title" --msgbox "$message" 10 60
    else
        echo ""
        echo "${BOLD}=== $title ===${NC}"
        echo "$message"
        echo ""
        read -p "Press Enter to continue..."
    fi
}

# Get input
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
        echo "${BOLD}$title${NC}"
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
        read -p "${YELLOW}$message (y/N): ${NC}" -n 1 -r
        echo ""
        [[ $REPLY =~ ^[Yy]$ ]]
        return $?
    fi
}

# ============================================================================
# JOB MANAGEMENT (IMPROVED)
# ============================================================================

# Start a background job (with optional real-time output)
start_background_job() {
    local job_name="$1"
    shift  # Remaining arguments are the command
    
    local job_id="$(date +%Y%m%d_%H%M%S)_$$"
    local job_file="${JOBS_DIR}/${job_id}.job"
    local log_file="${LOGS_DIR}/${job_name}_${job_id}.log"
    
    # Write job file safely
    write_job_file "$job_file" \
        "JOB_ID" "$job_id" \
        "JOB_NAME" "$job_name" \
        "COMMAND" "$*" \
        "START_TIME" "$(date +"%Y-%m-%d %H:%M:%S")" \
        "STATUS" "RUNNING" \
        "PID" "$$" \
        "LOG_FILE" "$log_file"
    
    # Check if we should show real-time output
    local show_output="${SHOW_REALTIME_OUTPUT:-false}"
    
    if [[ "$show_output" == "true" ]]; then
        # Run in foreground with output visible
        echo "${BOLD}${BLUE}Starting: $job_name${NC}"
        echo "Log: $log_file"
        echo "${YELLOW}Progress will be shown below:${NC}"
        echo "----------------------------------------"
        
        # Use tee to show output and save to log
        if "$@" 2>&1 | tee "$log_file"; then
            update_job_file "$job_file" "STATUS" "COMPLETED"
            echo "${GREEN}SUCCESS Job completed successfully${NC}"
        else
            update_job_file "$job_file" "STATUS" "FAILED"
            echo "${RED}FAILED Job failed${NC}"
        fi
        update_job_file "$job_file" "END_TIME" "$(date +"%Y-%m-%d %H:%M:%S")"
    else
        # Original background behavior
        (
            # Execute command and capture result
            if "$@" > "$log_file" 2>&1; then
                update_job_file "$job_file" "STATUS" "COMPLETED"
            else
                update_job_file "$job_file" "STATUS" "FAILED"
            fi
            update_job_file "$job_file" "END_TIME" "$(date +"%Y-%m-%d %H:%M:%S")"
        ) &
        
        local bg_pid=$!
        update_job_file "$job_file" "PID" "$bg_pid"
        
        show_message "Job Started" "Job: $job_name\nID: $job_id\nPID: $bg_pid\nLog: $log_file\n\nTip: Check 'Job Status' menu to monitor progress"
    fi
    
    return 0
}

# Start a foreground job with real-time output
start_foreground_job() {
    SHOW_REALTIME_OUTPUT=true start_background_job "$@"
}

# Show job status with option to view live logs
show_job_status() {
    while true; do
        local jobs_found=false
        local running_jobs=()
        local all_jobs=()
        
        # Collect job information
        for job_file in "$JOBS_DIR"/*.job; do
            if [[ -f "$job_file" ]]; then
                jobs_found=true
                all_jobs+=("$job_file")
                
                local status=$(parse_job_file "$job_file" "STATUS")
                local pid=$(parse_job_file "$job_file" "PID")
                
                # Check if running process is still alive
                if [[ "$status" == "RUNNING" ]] && [[ -n "$pid" ]]; then
                    if ! kill -0 "$pid" 2>/dev/null; then
                        update_job_file "$job_file" "STATUS" "CRASHED"
                    else
                        running_jobs+=("$job_file")
                    fi
                fi
            fi
        done
        
        if [[ "$jobs_found" == false ]]; then
            show_message "Job Status" "No jobs found."
            return
        fi
        
        # Build menu options
        local menu_options=()
        menu_options+=("View All Jobs Summary (with Results)")
        
        # Add completed jobs for full log viewing
        local completed_jobs=()
        for job_file in "${all_jobs[@]}"; do
            local status=$(parse_job_file "$job_file" "STATUS")
            if [[ "$status" == "COMPLETED" ]] || [[ "$status" == "FAILED" ]]; then
                completed_jobs+=("$job_file")
            fi
        done
        
        if [[ ${#completed_jobs[@]} -gt 0 ]]; then
            menu_options+=("---")
            for job_file in "${completed_jobs[@]}"; do
                local job_name=$(parse_job_file "$job_file" "JOB_NAME")
                local status=$(parse_job_file "$job_file" "STATUS")
                local label="View Log"
                if [[ "$status" == "FAILED" ]]; then
                    label="View Error"
                fi
                menu_options+=("$label: $job_name [$status]")
            done
        fi
        
        if [[ ${#running_jobs[@]} -gt 0 ]]; then
            menu_options+=("---")
            for job_file in "${running_jobs[@]}"; do
                local job_name=$(parse_job_file "$job_file" "JOB_NAME")
                local job_id=$(parse_job_file "$job_file" "JOB_ID")
                menu_options+=("Monitor: $job_name [RUNNING]")
            done
        fi
        
        menu_options+=("---")
        menu_options+=("Clean Completed Jobs")
        menu_options+=("Refresh")
        
        local choice=$(show_menu "Job Status Menu" "${menu_options[@]}")
        
        case "$choice" in
            1) show_all_jobs_summary "${all_jobs[@]}" ;;
            0|"") return ;;
            *)
                local selected_option="${menu_options[$((choice - 1))]}"
                
                # Check what type of selection it is
                if [[ "$selected_option" == "Clean Completed Jobs" ]]; then
                    clean_completed_jobs
                elif [[ "$selected_option" == "Refresh" ]]; then
                    continue
                elif [[ "$selected_option" =~ ^View\ Log:|^View\ Error: ]]; then
                    # Extract job name from the selection
                    local job_to_view="${selected_option#*: }"
                    job_to_view="${job_to_view% \[*\]}"  # Remove status suffix
                    
                    # Find the job file
                    for job_file in "${completed_jobs[@]}"; do
                        local job_name=$(parse_job_file "$job_file" "JOB_NAME")
                        if [[ "$job_name" == "$job_to_view" ]]; then
                            view_job_full_log "$job_file"
                            break
                        fi
                    done
                elif [[ "$selected_option" =~ ^Monitor: ]]; then
                    # Extract job name from the selection
                    local job_to_monitor="${selected_option#Monitor: }"
                    job_to_monitor="${job_to_monitor% \[*\]}"  # Remove status suffix
                    
                    # Find the job file
                    for job_file in "${running_jobs[@]}"; do
                        local job_name=$(parse_job_file "$job_file" "JOB_NAME")
                        if [[ "$job_name" == "$job_to_monitor" ]]; then
                            monitor_job_progress "$job_file"
                            break
                        fi
                    done
                fi
                ;;
        esac
    done
}

# Show summary of all jobs
show_all_jobs_summary() {
    local status_text=""
    
    for job_file in "$@"; do
        if [[ -f "$job_file" ]]; then
            local job_name=$(parse_job_file "$job_file" "JOB_NAME")
            local job_id=$(parse_job_file "$job_file" "JOB_ID")
            local status=$(parse_job_file "$job_file" "STATUS")
            local start_time=$(parse_job_file "$job_file" "START_TIME")
            local end_time=$(parse_job_file "$job_file" "END_TIME")
            local log_file=$(parse_job_file "$job_file" "LOG_FILE")
            
            # Truncate long job names for display
            if [[ ${#job_name} -gt 50 ]]; then
                job_name="${job_name:0:47}..."
            fi
            
            status_text+="\n----------------------------\n"
            status_text+="Job: $job_name\n"
            status_text+="ID: $job_id\n"
            status_text+="Status: $status\n"
            status_text+="Started: $start_time\n"
            
            if [[ -n "$end_time" ]]; then
                status_text+="Ended: $end_time\n"
            fi
            
            # Show log output for all completed/failed jobs
            if [[ "$status" == "COMPLETED" ]]; then
                status_text+="\n=== RESULTS ===\n"
                if [[ -f "$log_file" ]]; then
                    # For completed jobs, show last 20 lines which usually contains the results
                    local results=$(tail -20 "$log_file" 2>/dev/null | head -15)
                    if [[ -n "$results" ]]; then
                        status_text+="$results\n"
                    else
                        status_text+="(No output captured)\n"
                    fi
                else
                    status_text+="(Log file not found)\n"
                fi
            elif [[ "$status" == "FAILED" ]] || [[ "$status" == "CRASHED" ]]; then
                status_text+="\n=== ERROR OUTPUT ===\n"
                if [[ -f "$log_file" ]]; then
                    status_text+="$(tail -10 "$log_file" 2>/dev/null || echo "Log file not accessible")\n"
                else
                    status_text+="(Log file not found)\n"
                fi
            elif [[ "$status" == "RUNNING" ]]; then
                status_text+="\n(Job is still running - select 'Monitor' to view live output)\n"
            fi
        fi
    done
    
    show_message "All Jobs Summary" "$status_text"
}

# View full log for a completed job
view_job_full_log() {
    local job_file="$1"
    local job_name=$(parse_job_file "$job_file" "JOB_NAME")
    local job_id=$(parse_job_file "$job_file" "JOB_ID")
    local status=$(parse_job_file "$job_file" "STATUS")
    local log_file=$(parse_job_file "$job_file" "LOG_FILE")
    
    if [[ ! -f "$log_file" ]]; then
        show_message "Error" "Log file not found for job: $job_name"
        return
    fi
    
    # Get log content
    local log_content=$(cat "$log_file" 2>/dev/null)
    
    if [[ -z "$log_content" ]]; then
        show_message "Job Log: $job_name" "No output captured in log file."
    else
        # Show the full log with proper title
        local title="Job Results: $job_name [$status]"
        show_message "$title" "$log_content"
    fi
}

# Monitor a specific job's progress
monitor_job_progress() {
    local job_file="$1"
    local job_name=$(parse_job_file "$job_file" "JOB_NAME")
    local log_file=$(parse_job_file "$job_file" "LOG_FILE")
    
    if [[ ! -f "$log_file" ]]; then
        show_message "Error" "Log file not found: $log_file"
        return
    fi
    
    clear
    echo "${BOLD}${BLUE}Monitoring Job: $job_name${NC}"
    echo "${YELLOW}Press Ctrl+C to stop monitoring${NC}"
    echo "----------------------------------------"
    
    # Use tail -f to show live output
    tail -f "$log_file" 2>/dev/null || show_message "Error" "Could not monitor log file"
}

# Clean completed jobs
clean_completed_jobs() {
    local cleaned=0
    
    with_lock bash -c "
        for job_file in '$JOBS_DIR'/*.job; do
            if [[ -f \"\$job_file\" ]]; then
                status=\$(grep '^STATUS=' \"\$job_file\" | cut -d'=' -f2)
                if [[ \"\$status\" == \"COMPLETED\" ]]; then
                    rm -f \"\$job_file\"
                    ((cleaned++))
                fi
            fi
        done
        echo \$cleaned
    "
    
    show_message "Cleanup" "Cleaned $cleaned completed job(s)."
}

# ============================================================================
# MODULE FUNCTIONS (using arrays instead of eval for commands)
# ============================================================================

# Current menu path for breadcrumbs
MENU_PATH="Main"

# Update menu path
push_menu() {
    MENU_PATH="$MENU_PATH > $1"
}

pop_menu() {
    MENU_PATH="${MENU_PATH% > *}"
}

# Quick Load Menu
menu_quick_load() {
    push_menu "Quick Load"
    while true; do
        local choice=$(show_menu "$MENU_PATH" \
            "Load Current Month" \
            "Load Last Month" \
            "Load Specific File" \
            "Load with Validation" \
            "Load without QC (Fast)")
        
        case "$choice" in
            1) quick_load_current_month ;;
            2) quick_load_last_month ;;
            3) quick_load_specific_file ;;
            4) quick_load_with_validation ;;
            5) quick_load_without_qc ;;
            0|"") pop_menu; break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Data Operations Menu
menu_data_operations() {
    push_menu "Data Operations"
    while true; do
        local choice=$(show_menu "$MENU_PATH" \
            "Load Data" \
            "Validate Data" \
            "Delete Data" \
            "Check Duplicates" \
            "Check Table Info" \
            "Compare Files")
        
        case "$choice" in
            1) menu_load_data ;;
            2) menu_validate_data ;;
            3) menu_delete_data ;;
            4) check_duplicates ;;
            5) check_table_info ;;
            6) compare_files ;;
            0|"") pop_menu; break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# File Tools Menu
menu_file_tools() {
    push_menu "File Tools"
    while true; do
        local choice=$(show_menu "$MENU_PATH" \
            "Sample TSV File" \
            "Generate Config" \
            "Analyze File Structure" \
            "Check for Issues" \
            "View File Stats")
        
        case "$choice" in
            1) sample_tsv_file ;;
            2) generate_config ;;
            3) analyze_file_structure ;;
            4) check_file_issues ;;
            5) view_file_stats ;;
            0|"") pop_menu; break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Recovery Menu
menu_recovery() {
    push_menu "Recovery & Fix"
    while true; do
        local choice=$(show_menu "$MENU_PATH" \
            "Diagnose Failed Load" \
            "Fix VARCHAR Errors" \
            "Recover from Logs" \
            "Clean Stage Files" \
            "Generate Clean Files")
        
        case "$choice" in
            1) diagnose_failed_load ;;
            2) fix_varchar_errors ;;
            3) recover_from_logs ;;
            4) clean_stage_files ;;
            5) generate_clean_files ;;
            0|"") pop_menu; break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Quick load current month (using arrays)
quick_load_current_month() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local current_month=$(date +%Y-%m)
    
    if confirm_action "Load data for $current_month?\nUsing config: $(basename "$CONFIG_FILE")"; then
        with_lock start_background_job "load_${current_month}" \
            ./run_loader.sh --month "$current_month" --config "$CONFIG_FILE" --base-path "$BASE_PATH"
    fi
}

# Quick load last month (using arrays)
quick_load_last_month() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local last_month=$(date -d "last month" +%Y-%m 2>/dev/null || date -v-1m +%Y-%m)
    
    if confirm_action "Load data for $last_month?\nUsing config: $(basename "$CONFIG_FILE")"; then
        with_lock start_background_job "load_${last_month}" \
            ./run_loader.sh --month "$last_month" --config "$CONFIG_FILE" --base-path "$BASE_PATH"
    fi
}

# Quick load specific file
quick_load_specific_file() {
    local file_path=$(get_input "Load Specific File" "Enter TSV file path")
    
    if [[ -z "$file_path" ]]; then
        show_message "Error" "No file path provided"
        return
    fi
    
    if [[ ! -f "$file_path" ]]; then
        show_message "Error" "File not found: $file_path"
        return
    fi
    
    if confirm_action "Load file: $(basename "$file_path")?"; then
        with_lock start_background_job "load_file_$(basename "$file_path")" \
            ./run_loader.sh --direct-file "$file_path" --config "$CONFIG_FILE"
    fi
}

# Quick load with validation
quick_load_with_validation() {
    local month=$(get_input "Load with Validation" "Enter month (YYYY-MM)" "$(date +%Y-%m)")
    
    if confirm_action "Load $month with Snowflake validation?"; then
        with_lock start_background_job "load_validated_${month}" \
            ./run_loader.sh --month "$month" --config "$CONFIG_FILE" --base-path "$BASE_PATH" --validate-in-snowflake
    fi
}

# Quick load without QC
quick_load_without_qc() {
    local month=$(get_input "Load without QC" "Enter month (YYYY-MM)" "$(date +%Y-%m)")
    
    if confirm_action "Load $month without quality checks (faster but riskier)?"; then
        with_lock start_background_job "load_fast_${month}" \
            ./run_loader.sh --month "$month" --config "$CONFIG_FILE" --base-path "$BASE_PATH" --skip-qc
    fi
}

# Load data menu
menu_load_data() {
    local month=$(get_input "Load Data" "Enter month(s) - comma separated or 'all'" "$(date +%Y-%m)")
    
    if [[ "$month" == "all" ]]; then
        if confirm_action "Load ALL months from $BASE_PATH?"; then
            with_lock start_background_job "load_batch_all" \
                ./run_loader.sh --batch --config "$CONFIG_FILE" --base-path "$BASE_PATH"
        fi
    else
        if confirm_action "Load month(s): $month?"; then
            with_lock start_background_job "load_${month}" \
                ./run_loader.sh --months "$month" --config "$CONFIG_FILE" --base-path "$BASE_PATH"
        fi
    fi
}

# Validate data menu (runs synchronously - quick operation)
menu_validate_data() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local month=$(get_input "Validate Data" "Enter month (YYYY-MM)" "$(date +%Y-%m)")
    
    if confirm_action "Validate data for $month?\nUsing config: $(basename "$CONFIG_FILE")"; then
        # Ask user for execution mode
        local response=$(get_input "Execution Mode" "Show real-time progress? (Y=foreground, N=background)" "Y")
        
        if [[ "${response^^}" == "Y" ]]; then
            # Run in foreground with visible progress
            with_lock start_foreground_job "validate_${month}" \
                python3 tsv_loader.py --config "$CONFIG_FILE" --month "$month" --validate-only
        else
            # Run in background
            with_lock start_background_job "validate_${month}" \
                python3 tsv_loader.py --config "$CONFIG_FILE" --month "$month" --validate-only
        fi
    fi
}

# Delete data menu
menu_delete_data() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local table=$(select_table "Delete Data")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table selection cancelled"
        return
    fi
    
    local month=$(get_input "Delete Data" "Enter month (YYYY-MM) for table: $table")
    
    if [[ -z "$table" ]] || [[ -z "$month" ]]; then
        show_message "Error" "Table and month are required"
        return
    fi
    
    # Extra safety check for deletion
    local confirm_text=$(get_input "CONFIRM DELETION" "Type 'DELETE $month' to confirm")
    
    if [[ "$confirm_text" != "DELETE $month" ]]; then
        show_message "Cancelled" "Deletion cancelled - confirmation text did not match"
        return
    fi
    
    if confirm_action "FINAL WARNING: Delete $month from $table?"; then
        with_lock start_background_job "delete_${table}_${month}" \
            ./drop_month.sh --config "$CONFIG_FILE" --table "$table" --month "$month" --yes
    fi
}

# Check duplicates - now parameterized
check_duplicates() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local table=$(select_table "Check Duplicates")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table name is required"
        return
    fi
    
    local month=$(get_input "Check Duplicates" "Enter month (YYYY-MM) or leave empty for all")
    local key_columns=$(get_input "Key Columns" "Enter key columns (comma-separated)" "recordDate,assetId,fundId")
    
    # Convert month to date range
    local start_date=""
    local end_date=""
    if [[ -n "$month" ]]; then
        start_date="${month}-01"
        # Calculate last day of month
        if date -d "${month}-01 +1 month -1 day" >/dev/null 2>&1; then
            end_date=$(date -d "${month}-01 +1 month -1 day" +%Y-%m-%d)
        else
            # macOS date command
            end_date=$(date -v+1m -v-1d -j -f "%Y-%m-%d" "${month}-01" +%Y-%m-%d 2>/dev/null || echo "${month}-31")
        fi
    fi
    
    if confirm_action "Check for duplicates in $table?\nKey columns: $key_columns\nDate range: ${start_date:-'all'} to ${end_date:-'all'}"; then
        # Check if the interactive script exists
        if [[ -f "check_duplicates_interactive.py" ]]; then
            # Use the interactive script with job management
            local response=$(get_input "Execution Mode" "Show real-time progress? (Y=foreground, N=background)" "Y")
            
            # Set date parameters
            local date_start="${start_date:-none}"
            local date_end="${end_date:-none}"
            
            if [[ "${response^^}" == "Y" ]]; then
                # Run in foreground with visible progress
                with_lock start_foreground_job "check_duplicates_${table}" \
                    python3 check_duplicates_interactive.py "$CONFIG_FILE" "$table" "$key_columns" "$date_start" "$date_end"
            else
                # Run in background
                with_lock start_background_job "check_duplicates_${table}" \
                    python3 check_duplicates_interactive.py "$CONFIG_FILE" "$table" "$key_columns" "$date_start" "$date_end"
            fi
        else
            # Fall back to inline execution
            show_message "Running" "Checking for duplicates..."
            local output=$(python3 -c "
import json
import sys
from datetime import datetime
from tsv_loader import SnowflakeDataValidator

# Load config
with open('$CONFIG_FILE', 'r') as f:
    config = json.load(f)

snowflake_params = config['snowflake']

# Parse key columns
key_columns = '$key_columns'.split(',')
key_columns = [col.strip() for col in key_columns]

print(f'Checking table: $table')
print(f'Key columns: {key_columns}')
if '$start_date':
    print(f'Date range: $start_date to $end_date')
else:
    print('Date range: All data')
print('-' * 60)

# Initialize validator
validator = SnowflakeDataValidator(snowflake_params)

try:
    # Determine date column (usually first key column)
    date_column = key_columns[0] if key_columns else 'recordDate'
    
    # Run duplicate check
    if '$start_date':
        result = validator.check_duplicates(
            table_name='$table',
            key_columns=key_columns,
            date_column=date_column,
            start_date='$start_date',
            end_date='$end_date',
            sample_limit=5
        )
    else:
        # Check all data
        result = validator.check_duplicates(
            table_name='$table',
            key_columns=key_columns,
            date_column=date_column,
            start_date=None,
            end_date=None,
            sample_limit=5
        )
    
    # Display results
    if result.get('error'):
        print(f'Error: {result[\"error\"]}')
    elif result.get('has_duplicates'):
        stats = result['statistics']
        print(f'\\nWARNING: DUPLICATES FOUND!')
        print(f'Total rows: {stats[\"total_rows\"]:,}')
        print(f'Duplicate keys: {stats[\"duplicate_key_combinations\"]:,}')
        print(f'Excess rows: {stats[\"excess_rows\"]:,}')
        print(f'Duplicate %: {stats[\"duplicate_percentage\"]:.2f}%')
        print(f'Severity: {result[\"severity\"]}')
        
        # Show samples
        if result.get('sample_duplicates'):
            print('\\nSample duplicate keys:')
            for sample in result['sample_duplicates'][:5]:
                key_str = ', '.join([f'{k}={v}' for k, v in sample['key_values'].items()])
                print(f'  - {key_str} (x{sample[\"duplicate_count\"]})')
    else:
        print('SUCCESS: No duplicates found!')
        
finally:
    validator.close()
" 2>&1 | head -100)
            
            show_message "Duplicate Check Results" "$output"
        fi
    fi
}

# Compare files
compare_files() {
    local good_file=$(get_input "Compare Files" "Enter path to good/working file")
    local bad_file=$(get_input "Compare Files" "Enter path to problematic file")
    
    if [[ ! -f "$good_file" ]] || [[ ! -f "$bad_file" ]]; then
        show_message "Error" "Both files must exist"
        return
    fi
    
    local use_quick=""
    if confirm_action "Use quick mode (sampling) for large files?"; then
        use_quick="--quick"
    fi
    
    if confirm_action "Compare files?"; then
        with_lock start_background_job "compare_files" \
            python3 compare_tsv_files.py $use_quick "$good_file" "$bad_file"
    fi
}

# Sample TSV file
sample_tsv_file() {
    local file_path=$(get_input "Sample TSV File" "Enter TSV file path")
    
    if [[ -z "$file_path" ]] || [[ ! -f "$file_path" ]]; then
        show_message "Error" "Invalid file path"
        return
    fi
    
    local output=$(./tsv_sampler.sh "$file_path" 10 2>&1)
    show_message "TSV Sample Results" "$output"
}

# Generate config
generate_config() {
    local file_path=$(get_input "Generate Config" "Enter TSV file path")
    local output_path=$(get_input "Output Path" "Enter output config path" "config/generated.json")
    
    if [[ -z "$file_path" ]] || [[ ! -f "$file_path" ]]; then
        show_message "Error" "Invalid file path"
        return
    fi
    
    if confirm_action "Generate config from $(basename "$file_path")?"; then
        local output=$(./generate_config.sh -o "$output_path" "$file_path" 2>&1)
        show_message "Config Generation" "$output"
        save_preference "LAST_CONFIG" "$output_path"
    fi
}

# Analyze file structure
analyze_file_structure() {
    local file=$(get_input "Analyze Structure" "Enter TSV file path")
    if [[ -f "$file" ]]; then
        local rows=$(wc -l < "$file")
        local cols=$(head -1 "$file" | tr '\t' '\n' | wc -l)
        local size=$(du -h "$file" | cut -f1)
        local encoding=$(file -b --mime-encoding "$file")
        show_message "File Structure" "File: $(basename "$file")\nSize: $size\nRows: $rows\nColumns: $cols\nEncoding: $encoding"
    else
        show_message "Error" "File not found: $file"
    fi
}

# Check file issues
check_file_issues() {
    local file=$(get_input "Check File Issues" "Enter TSV file path")
    if [[ -f "$file" ]]; then
        if confirm_action "Check file for issues? This may take time for large files."; then
            with_lock start_background_job "check_issues_$(basename "$file")" \
                python3 validate_tsv_file.py "$file"
        fi
    else
        show_message "Error" "File not found: $file"
    fi
}

# View file stats
view_file_stats() {
    local file=$(get_input "View File Stats" "Enter TSV file path")
    if [[ -f "$file" ]]; then
        local size=$(du -h "$file" | cut -f1)
        local lines=$(wc -l < "$file")
        local cols=$(head -1 "$file" | tr '\t' '\n' | wc -l)
        local nulls=$(grep -c $'\x00' "$file" 2>/dev/null || echo "0")
        show_message "File Statistics" "File: $(basename "$file")\nSize: $size\nRows: $lines\nColumns: $cols\nNull bytes: $nulls"
    else
        show_message "Error" "File not found: $file"
    fi
}

# Check table existence and info
check_table_info() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local table=$(select_table "Check Table")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table name is required"
        return
    fi
    
    if confirm_action "Check table $table in Snowflake?"; then
        # Ask user for execution mode
        local response=$(get_input "Execution Mode" "Show real-time progress? (Y=foreground, N=background)" "Y")
        
        if [[ "${response^^}" == "Y" ]]; then
            # Run in foreground with visible progress
            with_lock start_foreground_job "check_table_${table}" \
                python3 check_snowflake_table.py "$CONFIG_FILE" "$table"
        else
            # Run in background
            with_lock start_background_job "check_table_${table}" \
                python3 check_snowflake_table.py "$CONFIG_FILE" "$table"
        fi
    fi
}

# Diagnose failed load
diagnose_failed_load() {
    local log_file=$(get_input "Diagnose Failed Load" "Enter log file path or job ID")
    
    # Check if it's a job ID
    if [[ -f "$JOBS_DIR/${log_file}.job" ]]; then
        log_file=$(parse_job_file "$JOBS_DIR/${log_file}.job" "LOG_FILE")
    fi
    
    if [[ -f "$log_file" ]]; then
        local output=$(python3 diagnose_copy_error.py "$log_file" 2>&1 | head -100)
        show_message "Diagnosis Results" "$output"
    else
        show_message "Error" "Log file not found: $log_file"
    fi
}

# Recovery and fix functions
fix_varchar_errors() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local table=$(select_table "Fix VARCHAR Errors")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table selection cancelled"
        return
    fi
    
    local month=$(get_input "Fix VARCHAR Errors" "Enter month (YYYY-MM) with VARCHAR errors for table: $table")
    
    if [[ -z "$month" ]] || [[ -z "$table" ]]; then
        show_message "Error" "Month and table are required"
        return
    fi
    
    if confirm_action "Attempt to fix VARCHAR date errors for $table in $month?"; then
        with_lock start_background_job "fix_varchar_${table}_${month}" \
            ./recover_failed_load.sh --config "$CONFIG_FILE" --table "$table" --month "$month" --action cleanup
    fi
}

recover_from_logs() {
    local log_pattern=$(get_input "Recover from Logs" "Enter log file pattern or job ID")
    
    if [[ -z "$log_pattern" ]]; then
        show_message "Error" "Log pattern is required"
        return
    fi
    
    # Check if it's a job ID
    if [[ -f "$JOBS_DIR/${log_pattern}.job" ]]; then
        local log_file=$(parse_job_file "$JOBS_DIR/${log_pattern}.job" "LOG_FILE")
        local table=$(parse_job_file "$JOBS_DIR/${log_pattern}.job" "JOB_NAME" | grep -oE 'load_[A-Z]+' | sed 's/load_//')
        
        if [[ -f "$log_file" ]]; then
            show_message "Running" "Attempting recovery from $log_file..."
            with_lock start_background_job "recover_${log_pattern}" \
                ./recover_failed_load.sh --config "$CONFIG_FILE" --action diagnose
        else
            show_message "Error" "Log file not found: $log_file"
        fi
    else
        # Try to find matching log files
        local logs=$(find "$LOGS_DIR" -name "*${log_pattern}*" -type f | head -5)
        if [[ -n "$logs" ]]; then
            show_message "Found Logs" "$logs\n\nUse diagnose_failed_load with specific file."
        else
            show_message "Error" "No matching logs found for pattern: $log_pattern"
        fi
    fi
}

clean_stage_files() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local table=$(select_table "Clean Stage Files" "" "true")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table name is required"
        return
    fi
    
    if [[ "$table" == "all" ]]; then
        if confirm_action "Clean ALL stage files? This will remove all uploaded TSV files from Snowflake stages."; then
            show_message "Running" "Cleaning all stage files..."
            local output=$(python3 check_stage_and_performance.py "$CONFIG_FILE" 2>&1 | grep -E "(Found|Total|Would)" | head -20)
            show_message "Stage Status" "$output\n\nRun 'python3 check_stage_and_performance.py $CONFIG_FILE' to clean interactively."
        fi
    else
        if confirm_action "Clean stage files for table $table?"; then
            with_lock start_background_job "clean_stage_${table}" \
                python3 -c "
import snowflake.connector
import json

config = json.load(open('$CONFIG_FILE'))
conn = snowflake.connector.connect(**config['snowflake'])
cursor = conn.cursor()

stage_pattern = f'@~/tsv_stage/$table/'
print(f'Cleaning stage: {stage_pattern}')

try:
    cursor.execute(f'REMOVE {stage_pattern}')
    print('Stage cleaned successfully')
except Exception as e:
    print(f'Error cleaning stage: {e}')

cursor.close()
conn.close()
"
        fi
    fi
}

generate_clean_files() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local source_file=$(get_input "Generate Clean Files" "Enter problematic TSV file path")
    
    if [[ -z "$source_file" ]]; then
        show_message "Error" "Source file is required"
        return
    fi
    
    local table=$(select_table "Generate Clean Files")
    
    if [[ -z "$table" ]]; then
        show_message "Error" "Table selection cancelled"
        return
    fi
    
    local month=$(get_input "Generate Clean Files" "Enter month (YYYY-MM) to process for table: $table")
    
    if [[ ! -f "$source_file" ]]; then
        show_message "Error" "Source file not found: $source_file"
        return
    fi
    
    local output_dir=$(get_input "Output Directory" "Enter output directory for clean files" "./cleaned_data")
    
    if confirm_action "Generate clean version of $(basename "$source_file")?"; then
        mkdir -p "$output_dir"
        
        # Use recover_failed_load.sh split action as a base
        with_lock start_background_job "generate_clean_$(basename "$source_file")" \
            bash -c "
                # Create clean version by removing problematic rows
                echo 'Analyzing file for issues...'
                
                # Count total lines
                total_lines=\$(wc -l < '$source_file')
                echo \"Total lines: \$total_lines\"
                
                # Remove lines with common issues:
                # - Lines with wrong number of columns
                # - Lines with null bytes
                # - Lines with encoding issues
                
                first_line=\$(head -1 '$source_file')
                expected_cols=\$(echo \"\$first_line\" | awk -F'\\t' '{print NF}')
                echo \"Expected columns: \$expected_cols\"
                
                output_file='$output_dir/$(basename "$source_file" .tsv)_clean.tsv'
                
                echo 'Creating clean file...'
                awk -F'\\t' -v cols=\"\$expected_cols\" 'NF == cols && !/\\x00/' '$source_file' > \"\$output_file\"
                
                clean_lines=\$(wc -l < \"\$output_file\")
                removed_lines=\$((total_lines - clean_lines))
                
                echo \"Clean file created: \$output_file\"
                echo \"Removed \$removed_lines problematic lines\"
                echo \"Clean lines: \$clean_lines\"
                
                # Optionally split if still too large
                file_size=\$(du -m \"\$output_file\" | cut -f1)
                if [[ \$file_size -gt 5000 ]]; then
                    echo 'File is large (>5GB), splitting into 1GB chunks...'
                    split -b 1G \"\$output_file\" \"\${output_file%.tsv}_part_\"
                    echo 'Created chunks:'
                    ls -lh \"\${output_file%.tsv}_part_\"*
                fi
            "
    fi
}

# Configure workers
configure_workers() {
    local workers=$(get_input "Configure Workers" "Enter number of workers (or 'auto')" "$DEFAULT_WORKERS")
    
    if [[ "$workers" == "auto" ]] || [[ "$workers" =~ ^[0-9]+$ ]]; then
        DEFAULT_WORKERS="$workers"
        save_preference "LAST_WORKERS" "$DEFAULT_WORKERS"
        show_message "Success" "Default workers set to: $DEFAULT_WORKERS"
    else
        show_message "Error" "Invalid worker count: $workers"
    fi
}

# Toggle colors
toggle_colors() {
    if [[ -n "$RED" ]]; then
        RED="" GREEN="" YELLOW="" BLUE="" CYAN="" MAGENTA="" BOLD="" NC=""
        save_preference "USE_COLOR" "false"
        show_message "Colors Disabled" "Color output has been disabled"
    else
        RED='\033[0;31m'
        GREEN='\033[0;32m'
        YELLOW='\033[1;33m'
        BLUE='\033[0;34m'
        CYAN='\033[0;36m'
        MAGENTA='\033[0;35m'
        BOLD='\033[1m'
        NC='\033[0m'
        save_preference "USE_COLOR" "true"
        show_message "Colors Enabled" "Color output has been enabled"
    fi
}

# Set base path
set_base_path() {
    local new_path=$(get_input "Set Base Path" "Enter base path for TSV files" "$BASE_PATH")
    
    if [[ -d "$new_path" ]]; then
        BASE_PATH="$new_path"
        save_preference "LAST_BASE_PATH" "$BASE_PATH"
        show_message "Success" "Base path set to: $BASE_PATH"
    else
        show_message "Error" "Directory not found: $new_path"
    fi
}

# View preferences
view_preferences() {
    local prefs="Current Settings:\n\n"
    prefs+="Config File: ${CONFIG_FILE:-'Not selected'}\n"
    prefs+="Base Path: $BASE_PATH\n"
    prefs+="Default Workers: $DEFAULT_WORKERS\n"
    prefs+="Dialog Available: $USE_DIALOG ($DIALOG_CMD)\n"
    prefs+="State Directory: $STATE_DIR\n"
    prefs+="Logs Directory: $LOGS_DIR\n"
    
    if [[ -f "$PREFS_FILE" ]]; then
        prefs+="\n----------------------------\n"
        prefs+="Saved Preferences:\n"
        prefs+="$(cat "$PREFS_FILE")"
    fi
    
    show_message "Preferences" "$prefs"
}

# Settings Menu
menu_settings() {
    while true; do
        local choice=$(show_menu "Settings" \
            "Select/Change Config" \
            "Set Base Path" \
            "Configure Workers" \
            "Toggle Colors" \
            "View Preferences" \
            "Clear State" \
            "Clean Completed Jobs")
        
        case "$choice" in
            1) set_default_config ;;
            2) set_base_path ;;
            3) configure_workers ;;
            4) toggle_colors ;;
            5) view_preferences ;;
            6) clear_state ;;
            7) clean_completed_jobs ;;
            0|"") break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Set/change config
set_default_config() {
    # Force selection menu even if config is already set
    if select_config_file "true"; then
        show_message "Success" "Config set to: $(basename "$CONFIG_FILE")"
    fi
}

# Clear state (with safety check)
clear_state() {
    if [[ -z "$JOBS_DIR" ]] || [[ "$JOBS_DIR" == "/" ]]; then
        show_message "Error" "Invalid JOBS_DIR path - aborting for safety"
        return
    fi
    
    if confirm_action "Clear all state and job history? This cannot be undone!"; then
        with_lock bash -c "
            rm -rf '$JOBS_DIR'/*
            rm -rf '$LOCKS_DIR'/*
        "
        show_message "Success" "State cleared successfully"
    fi
}

# ============================================================================
# CLI ARGUMENT PARSING
# ============================================================================

# Parse command line arguments
parse_cli_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            load)
                shift
                if [[ "${1:-}" == "--month" ]] && [[ -n "${2:-}" ]]; then
                    shift
                    ./run_loader.sh --month "$1" --config "$CONFIG_FILE" --base-path "$BASE_PATH"
                    exit $?
                elif [[ "${1:-}" == "--file" ]] && [[ -n "${2:-}" ]]; then
                    shift
                    ./run_loader.sh --direct-file "$1" --config "$CONFIG_FILE"
                    exit $?
                else
                    echo "Usage: $SCRIPT_NAME load --month YYYY-MM"
                    echo "       $SCRIPT_NAME load --file path/to/file.tsv"
                    exit 1
                fi
                ;;
            validate)
                shift
                if [[ "${1:-}" == "--month" ]] && [[ -n "${2:-}" ]]; then
                    shift
                    python3 tsv_loader.py --config "$CONFIG_FILE" --month "$1" --validate-only
                    exit $?
                else
                    echo "Usage: $SCRIPT_NAME validate --month YYYY-MM"
                    exit 1
                fi
                ;;
            delete)
                shift
                if [[ "${1:-}" == "--table" ]] && [[ -n "${2:-}" ]] && [[ "${3:-}" == "--month" ]] && [[ -n "${4:-}" ]]; then
                    shift 3
                    ./drop_month.sh --config "$CONFIG_FILE" --table "$1" --month "$2" --yes
                    exit $?
                else
                    echo "Usage: $SCRIPT_NAME delete --table TABLE --month YYYY-MM"
                    exit 1
                fi
                ;;
            status|jobs)
                # Show job status in non-interactive mode
                for job_file in "$JOBS_DIR"/*.job; do
                    if [[ -f "$job_file" ]]; then
                        echo "----------------------------"
                        grep "^JOB_NAME=" "$job_file" | cut -d'=' -f2
                        grep "^STATUS=" "$job_file" | cut -d'=' -f2
                        grep "^START_TIME=" "$job_file" | cut -d'=' -f2
                    fi
                done
                exit 0
                ;;
            clean)
                # Clean completed jobs
                local count=0
                for job_file in "$JOBS_DIR"/*.job; do
                    if [[ -f "$job_file" ]]; then
                        local status=$(parse_job_file "$job_file" "STATUS")
                        if [[ "$status" == "COMPLETED" ]]; then
                            rm -f "$job_file"
                            ((count++))
                        fi
                    fi
                done
                echo "Cleaned $count completed job(s)"
                exit 0
                ;;
            --version|-v)
                echo "Snowflake ETL Pipeline Manager v$VERSION"
                exit 0
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
        shift
    done
}

# Show help
show_help() {
    cat << EOF
Snowflake ETL Pipeline Manager v$VERSION

Usage:
  $SCRIPT_NAME                          # Interactive menu mode
  $SCRIPT_NAME load --month YYYY-MM     # Load data for a month
  $SCRIPT_NAME load --file file.tsv     # Load specific file
  $SCRIPT_NAME validate --month YYYY-MM # Validate data
  $SCRIPT_NAME delete --table TABLE --month YYYY-MM # Delete data
  $SCRIPT_NAME status                   # Show job status
  $SCRIPT_NAME clean                    # Clean completed jobs
  $SCRIPT_NAME --help                   # Show this help
  $SCRIPT_NAME --version                # Show version

Interactive Mode:
  Launch without arguments to enter the interactive menu system.
  
Command Line Mode:
  Use direct commands for automation and scripting.

Examples:
  $SCRIPT_NAME                          # Launch interactive menu
  $SCRIPT_NAME load --month 2024-01     # Load January 2024 data
  $SCRIPT_NAME status                   # Check running jobs
  $SCRIPT_NAME clean                    # Remove completed job files

Configuration:
  Default config: $CONFIG_FILE
  Default base path: $BASE_PATH
  State directory: $STATE_DIR

For more information, see README.md
EOF
}

# ============================================================================
# MAIN MENU
# ============================================================================

main_menu() {
    while true; do
        # Build menu title with current config
        local menu_title="SNOWFLAKE ETL PIPELINE MANAGER v$VERSION"
        if [[ -n "$CONFIG_FILE" ]]; then
            menu_title="$menu_title [Config: $(basename "$CONFIG_FILE")]"
        else
            menu_title="$menu_title [No config selected]"
        fi
        
        local choice=$(show_menu "$menu_title" \
            "Quick Load        - Common loading tasks" \
            "Data Operations   - Load/Validate/Delete" \
            "File Tools        - Analyze/Compare/Generate" \
            "Recovery & Fix    - Error recovery tools" \
            "---" \
            "Job Status        - Monitor operations" \
            "Settings          - Configure defaults")
        
        case "$choice" in
            1) menu_quick_load ;;
            2) menu_data_operations ;;
            3) menu_file_tools ;;
            4) menu_recovery ;;
            5) show_job_status ;;  # Now has interactive menu
            6) menu_settings ;;
            0|"") 
                echo "${GREEN}Thank you for using Snowflake ETL Manager!${NC}"
                exit 0 
                ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    # Initialize
    init_directories
    check_dependencies
    detect_ui_system
    load_preferences
    
    # Run health check to clean up stale jobs
    health_check_jobs
    
    # Parse CLI arguments if provided
    if [[ $# -gt 0 ]]; then
        # Check if it's a help/version command that doesn't need config
        case "$1" in
            --help|-h|--version|-v|status|jobs|clean)
                parse_cli_args "$@"
                ;;
            *)
                # For other CLI commands, try to auto-select config if only one exists
                select_config_file >/dev/null 2>&1 || true
                parse_cli_args "$@"
                ;;
        esac
        # If we get here, no valid CLI command was found
        show_help
        exit 1
    fi
    
    # Interactive mode
    # Clear screen for interactive mode
    if [[ "$USE_DIALOG" == true ]]; then
        clear
    fi
    
    # Show welcome message
    if [[ "$USE_DIALOG" == false ]]; then
        echo ""
        echo "${BOLD}${CYAN}Welcome to Snowflake ETL Pipeline Manager${NC}"
        echo "${YELLOW}Version $VERSION - Security Hardened${NC}"
        echo ""
    fi
    
    # Enter main menu
    main_menu
}

# Run main function
main "$@"