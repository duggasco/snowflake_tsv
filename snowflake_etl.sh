#!/bin/bash

# Snowflake ETL Pipeline Manager - Unified Wrapper Script
# Version: 3.4.12 - Added SSL/TLS handling for proxy environments
# Description: Interactive menu system for all Snowflake ETL operations

set -euo pipefail

# ============================================================================
# CONFIGURATION & GLOBALS
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"
VERSION="3.4.19"  # Fixed venv recreation bug - now properly reuses existing venv

# Skip flags (can be set via environment or command line)
SKIP_VENV="${SKIP_VENV:-false}"
SKIP_INSTALL="${SKIP_INSTALL:-false}"

# Source library files
source "${SCRIPT_DIR}/lib/colors.sh"
source "${SCRIPT_DIR}/lib/ui_components.sh"
source "${SCRIPT_DIR}/lib/common_functions.sh"

# State management directories
STATE_DIR="${SCRIPT_DIR}/.etl_state"
JOBS_DIR="${STATE_DIR}/jobs"
LOCKS_DIR="${STATE_DIR}/locks"
PREFS_FILE="${STATE_DIR}/preferences"
PREFS_DIR="${STATE_DIR}"  # Directory for preference files
LOGS_DIR="${SCRIPT_DIR}/logs"

# Color codes (for fallback mode)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
GRAY='\033[0;90m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default configuration
CONFIG_FILE=""  # Will be selected by user
BASE_PATH="${SCRIPT_DIR}/data"
DEFAULT_WORKERS="auto"
CONFIG_DIR="${SCRIPT_DIR}/config"
LOG_VIEWER="auto"  # auto, less, nano, cat

# Dialog/UI configuration
USE_DIALOG=false
DIALOG_CMD=""
DIALOG_HEIGHT=20
DIALOG_WIDTH=70
DIALOG_MAX_HEIGHT=40  # Maximum height for very long content
DIALOG_MAX_WIDTH=120  # Maximum width for wide content

# ============================================================================
# DIALOG SIZING FUNCTIONS
# ============================================================================

# Calculate dialog dimensions based on content
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

# Get terminal dimensions
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
    
    # Load log viewer preference if it exists
    local viewer_pref_file="${STATE_DIR}/log_viewer.pref"
    if [[ -f "$viewer_pref_file" ]]; then
        LOG_VIEWER=$(cat "$viewer_pref_file")
    fi
}

# Confirm Python installation
confirm_install_python() {
    local version="${1:-3.11}"
    
    echo -e "${CYAN}=== Python $version Installation ===${NC}"
    echo -e "${CYAN}Python $version is recommended for optimal compatibility.${NC}"
    echo -e "${CYAN}Would you like to install it now?${NC}"
    echo ""
    echo -e "${YELLOW}Installation options:${NC}"
    echo "  1. Install from source (compile locally)"
    echo "  2. Use system package manager (if available)"
    echo "  3. Skip installation"
    echo ""
    
    read -p "Choose option [1-3]: " choice
    
    case "$choice" in
        1|2)
            return 0
            ;;
        3)
            return 1
            ;;
        *)
            echo -e "${YELLOW}Invalid choice, skipping installation${NC}"
            return 1
            ;;
    esac
}

# Install Python 3.11 from source or package manager
install_python_311() {
    local install_base=""
    local python_prefix=""
    
    # Check if we need to configure proxy first
    local proxy_file="$PREFS_DIR/.proxy_config"
    if [[ -f "$proxy_file" ]]; then
        local proxy=$(cat "$proxy_file")
        echo -e "${CYAN}Using saved proxy for download: $proxy${NC}"
        export http_proxy="$proxy"
        export https_proxy="$proxy"
        export HTTP_PROXY="$proxy"
        export HTTPS_PROXY="$proxy"
    elif [[ -n "${https_proxy:-}" ]] || [[ -n "${HTTPS_PROXY:-}" ]] || [[ -n "${http_proxy:-}" ]] || [[ -n "${HTTP_PROXY:-}" ]]; then
        # Use existing proxy environment variables
        local proxy="${https_proxy:-${HTTPS_PROXY:-${http_proxy:-$HTTP_PROXY}}}"
        echo -e "${CYAN}Using existing proxy for download: $proxy${NC}"
    else
        # Test if we can connect to python.org
        echo -e "${YELLOW}Testing connection to python.org...${NC}"
        if ! curl -s --connect-timeout 5 https://www.python.org >/dev/null 2>&1; then
            echo -e "${YELLOW}Cannot connect to python.org directly.${NC}"
            echo -e "${YELLOW}You may need to configure a proxy first.${NC}"
            
            # Try to configure proxy if in interactive mode
            if [[ -t 0 ]] && [[ -t 1 ]]; then
                configure_proxy
                # Reload proxy settings if saved
                if [[ -f "$proxy_file" ]]; then
                    local proxy=$(cat "$proxy_file")
                    export http_proxy="$proxy"
                    export https_proxy="$proxy"
                    export HTTP_PROXY="$proxy"
                    export HTTPS_PROXY="$proxy"
                fi
            else
                echo -e "${RED}Cannot download Python without proxy in non-interactive mode${NC}"
                return 1
            fi
        fi
    fi
    
    echo -e "${CYAN}=== Installing Python 3.11 ===${NC}"
    echo ""
    
    # Ask for installation method
    echo -e "${CYAN}Choose installation method:${NC}"
    echo "  1. Install from source (recommended for custom path)"
    echo "  2. Use system package manager (requires sudo)"
    echo ""
    
    read -p "Select method [1-2]: " method
    
    if [[ "$method" == "1" ]]; then
        # Install from source
        echo -e "${CYAN}Installing Python 3.11 from source...${NC}"
        echo ""
        
        # Ask for installation base path
        echo -e "${CYAN}Enter installation base path${NC}"
        echo -e "${CYAN}(press Enter for default: $HOME/.local):${NC}"
        read -p "Installation path: " install_base
        
        if [[ -z "$install_base" ]]; then
            install_base="$HOME/.local"
        fi
        
        # Expand tilde if used
        install_base="${install_base/#\~/$HOME}"
        
        # Create directory if it doesn't exist
        mkdir -p "$install_base"
        
        python_prefix="$install_base"
        
        # Check for required build tools
        local build_deps=()
        for tool in gcc make wget tar; do
            if ! command -v "$tool" >/dev/null 2>&1; then
                build_deps+=("$tool")
            fi
        done
        
        if [[ ${#build_deps[@]} -gt 0 ]]; then
            echo -e "${YELLOW}Missing build dependencies: ${build_deps[*]}${NC}"
            echo -e "${YELLOW}Please install them first:${NC}"
            echo "  Ubuntu/Debian: sudo apt-get install build-essential wget"
            echo "  RHEL/CentOS: sudo yum groupinstall 'Development Tools' && sudo yum install wget"
            echo "  macOS: Install Xcode Command Line Tools"
            return 1
        fi
        
        # Download and compile Python 3.11
        local python_version="3.11.9"  # Latest 3.11 as of now
        local python_url_https="https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz"
        local python_url_http="http://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz"
        local temp_dir="/tmp/python311_build_$$"
        local python_archive="Python-${python_version}.tgz"
        
        mkdir -p "$temp_dir"
        cd "$temp_dir"
        
        # Check if user wants to use a pre-downloaded package
        echo -e "${CYAN}Do you have a pre-downloaded Python ${python_version} package?${NC}"
        echo -e "${CYAN}(Enter path to .tar.gz/.tgz file, or press Enter to download)${NC}"
        read -p "Path to Python package (or Enter to download): " local_package
        
        local download_success=0
        
        if [[ -n "$local_package" ]]; then
            # Expand tilde if used
            local_package="${local_package/#\~/$HOME}"
            
            if [[ -f "$local_package" ]]; then
                echo -e "${YELLOW}Using pre-downloaded package: $local_package${NC}"
                
                # Copy the file to our temp directory
                if cp "$local_package" "$python_archive"; then
                    download_success=1
                    echo -e "${GREEN}✓ Using local Python package${NC}"
                else
                    echo -e "${RED}Failed to copy local package${NC}"
                fi
            else
                echo -e "${RED}File not found: $local_package${NC}"
                echo -e "${YELLOW}Will attempt to download instead...${NC}"
            fi
        fi
        
        # If no local package or copy failed, download it
        if [[ $download_success -eq 0 ]]; then
            echo -e "${YELLOW}Downloading Python ${python_version}...${NC}"
            local python_url="$python_url_https"
        
        if [[ -n "${https_proxy:-}" ]]; then
            echo -e "${CYAN}Using proxy for download: ${https_proxy}${NC}"
            echo -e "${YELLOW}Attempting HTTPS download through proxy...${NC}"
            
            # wget should use environment variables automatically
            # Ensure they are exported
            export http_proxy="${http_proxy:-$https_proxy}"
            export https_proxy="${https_proxy}"
            export HTTP_PROXY="${HTTP_PROXY:-$https_proxy}"
            export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
            
            # Try HTTPS first with wget (uses env vars automatically)
            if wget --no-check-certificate \
                    --tries=2 \
                    --timeout=30 \
                    --progress=bar:force \
                    -O "$python_archive" \
                    "$python_url_https" 2>&1; then
                download_success=1
                echo -e "${GREEN}✓ Downloaded via HTTPS through proxy${NC}"
            else
                echo -e "${YELLOW}HTTPS failed, trying HTTP (proxy may block HTTPS tunneling)...${NC}"
                
                # Try HTTP as fallback for proxies that block HTTPS
                if wget --no-check-certificate \
                        --tries=2 \
                        --timeout=30 \
                        --progress=bar:force \
                        -O "$python_archive" \
                        "$python_url_http" 2>&1; then
                    download_success=1
                    echo -e "${GREEN}✓ Downloaded via HTTP through proxy${NC}"
                else
                    echo -e "${YELLOW}wget failed with both HTTPS and HTTP${NC}"
                    
                    # Try with explicit proxy URL for wget (older syntax)
                    echo -e "${YELLOW}Trying wget with explicit proxy flag...${NC}"
                    if wget --no-check-certificate \
                            --proxy=on \
                            --tries=2 \
                            --timeout=30 \
                            -O "$python_archive" \
                            "$python_url_http" 2>&1; then
                        download_success=1
                        echo -e "${GREEN}✓ Downloaded via wget with explicit proxy flag${NC}"
                    fi
                fi
            fi
        else
            # Try without proxy
            if wget --no-check-certificate \
                    --tries=3 \
                    --timeout=30 \
                    --progress=bar:force \
                    -O "$python_archive" \
                    "$python_url" 2>&1; then
                download_success=1
            else
                echo -e "${YELLOW}wget failed, trying curl...${NC}"
            fi
        fi
        
        # If wget failed, try curl
        if [[ $download_success -eq 0 ]]; then
            echo -e "${YELLOW}Trying alternative download with curl...${NC}"
            
            if [[ -n "${https_proxy:-}" ]]; then
                # Parse proxy URL to handle authentication
                local proxy_url="${https_proxy}"
                echo -e "${CYAN}Using proxy for curl: ${proxy_url}${NC}"
                
                # Try HTTPS with curl through proxy
                echo -e "${YELLOW}Attempting curl with HTTPS...${NC}"
                if curl -L \
                        --insecure \
                        --proxy "${proxy_url}" \
                        --retry 2 \
                        --connect-timeout 30 \
                        --max-time 300 \
                        --progress-bar \
                        -o "$python_archive" \
                        "$python_url_https" 2>&1; then
                    download_success=1
                    echo -e "${GREEN}✓ Downloaded via curl HTTPS through proxy${NC}"
                else
                    echo -e "${YELLOW}HTTPS failed, trying HTTP with curl...${NC}"
                    
                    # Try HTTP as fallback
                    if curl -L \
                            --insecure \
                            --proxy "${proxy_url}" \
                            --retry 2 \
                            --connect-timeout 30 \
                            --max-time 300 \
                            --progress-bar \
                            -o "$python_archive" \
                            "$python_url_http" 2>&1; then
                        download_success=1
                        echo -e "${GREEN}✓ Downloaded via curl HTTP through proxy${NC}"
                    else
                        echo -e "${YELLOW}Standard proxy failed, trying environment variable method...${NC}"
                        
                        # Try using environment variables (some curl versions prefer this)
                        export http_proxy="${http_proxy:-$https_proxy}"
                        export https_proxy="${https_proxy}"
                        if curl -L \
                                --insecure \
                                --retry 2 \
                                --connect-timeout 30 \
                                --max-time 300 \
                                --progress-bar \
                                -o "$python_archive" \
                                "$python_url_http" 2>&1; then
                            download_success=1
                            echo -e "${GREEN}✓ Downloaded via curl with environment proxy${NC}"
                        else
                            echo -e "${RED}All curl proxy methods failed${NC}"
                        fi
                    fi
                fi
            else
                # Curl without proxy
                if curl -L \
                        --insecure \
                        --retry 3 \
                        --connect-timeout 30 \
                        --progress-bar \
                        -o "$python_archive" \
                        "$python_url" 2>&1; then
                    download_success=1
                else
                    echo -e "${RED}curl also failed${NC}"
                fi
            fi
        fi
        fi  # Close the download block
        
        if [[ $download_success -eq 0 ]]; then
            echo -e "${RED}Failed to download Python source with both wget and curl${NC}"
            echo -e "${YELLOW}Please check your network connection and proxy settings${NC}"
            echo -e "${YELLOW}Proxy being used: ${https_proxy:-none}${NC}"
            echo ""
            echo -e "${CYAN}Troubleshooting tips:${NC}"
            echo -e "${CYAN}1. If you see 'proxy tunneling failed: Forbidden'${NC}"
            echo -e "${CYAN}   Your proxy may block HTTPS tunneling. HTTP fallback was attempted.${NC}"
            echo -e "${CYAN}2. Try setting both http_proxy and https_proxy to the same value${NC}"
            echo -e "${CYAN}3. Some proxies require authentication: http://user:pass@proxy:port${NC}"
            echo -e "${CYAN}4. You can manually download Python from:${NC}"
            echo -e "${CYAN}   ${python_url_http}${NC}"
            echo -e "${CYAN}   Then run this script again and provide the path to the downloaded file${NC}"
            cd - >/dev/null
            rm -rf "$temp_dir"
            return 1
        fi
        
        echo -e "${YELLOW}Extracting Python source...${NC}"
        tar -xzf "$python_archive"
        cd "Python-${python_version}"
        
        echo -e "${YELLOW}Configuring Python build...${NC}"
        echo -e "${CYAN}This will install to: $python_prefix${NC}"
        
        # Configure with optimizations and essential modules
        if ! ./configure --prefix="$python_prefix" \
                        --enable-optimizations \
                        --with-ensurepip=install \
                        --enable-shared \
                        LDFLAGS="-Wl,-rpath,$python_prefix/lib" \
                        >/dev/null 2>&1; then
            echo -e "${RED}Configuration failed${NC}"
            echo -e "${YELLOW}Trying without optimizations...${NC}"
            
            # Try without optimizations (faster but less optimal)
            if ! ./configure --prefix="$python_prefix" \
                            --with-ensurepip=install \
                            --enable-shared \
                            LDFLAGS="-Wl,-rpath,$python_prefix/lib" \
                            >/dev/null 2>&1; then
                echo -e "${RED}Configuration failed completely${NC}"
                cd - >/dev/null
                rm -rf "$temp_dir"
                return 1
            fi
        fi
        
        echo -e "${YELLOW}Building Python (this may take 5-10 minutes)...${NC}"
        
        # Get number of CPU cores for parallel build
        local num_cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
        
        if ! make -j"$num_cores" >/dev/null 2>&1; then
            echo -e "${RED}Build failed${NC}"
            cd - >/dev/null
            rm -rf "$temp_dir"
            return 1
        fi
        
        echo -e "${YELLOW}Installing Python to $python_prefix...${NC}"
        
        if ! make install >/dev/null 2>&1; then
            echo -e "${RED}Installation failed${NC}"
            cd - >/dev/null
            rm -rf "$temp_dir"
            return 1
        fi
        
        # Clean up
        cd - >/dev/null
        rm -rf "$temp_dir"
        
        # Update PATH for current session
        export PATH="$python_prefix/bin:$PATH"
        
        # Save installation path for future reference
        echo "$python_prefix/bin" > "$PREFS_DIR/.python311_path"
        
        echo -e "${GREEN}✓ Python 3.11 installed successfully to $python_prefix${NC}"
        echo -e "${CYAN}Added to PATH for current session${NC}"
        echo ""
        echo -e "${YELLOW}To make permanent, add to your shell profile:${NC}"
        echo "  export PATH=\"$python_prefix/bin:\$PATH\""
        echo ""
        
        return 0
        
    elif [[ "$method" == "2" ]]; then
        # Use system package manager
        echo -e "${CYAN}Installing Python 3.11 using system package manager...${NC}"
        
        # Detect OS and use appropriate package manager
        if [[ -f /etc/os-release ]]; then
            . /etc/os-release
            local os_id="${ID,,}"  # Convert to lowercase
            
            case "$os_id" in
                ubuntu|debian)
                    echo -e "${YELLOW}Detected Ubuntu/Debian${NC}"
                    echo -e "${CYAN}Installing Python 3.11...${NC}"
                    
                    # Add deadsnakes PPA for Ubuntu (has Python 3.11)
                    if [[ "$os_id" == "ubuntu" ]]; then
                        echo "sudo add-apt-repository ppa:deadsnakes/ppa -y"
                        echo "sudo apt-get update"
                        echo "sudo apt-get install python3.11 python3.11-venv python3.11-dev -y"
                    else
                        echo "sudo apt-get update"
                        echo "sudo apt-get install python3.11 python3.11-venv python3.11-dev -y"
                    fi
                    
                    echo ""
                    echo -e "${YELLOW}Please run the above commands with sudo privileges${NC}"
                    echo -e "${CYAN}After installation, re-run this script${NC}"
                    return 1
                    ;;
                    
                rhel|centos|fedora|rocky|almalinux)
                    echo -e "${YELLOW}Detected RHEL/CentOS/Fedora${NC}"
                    echo -e "${CYAN}Commands to install Python 3.11:${NC}"
                    
                    echo "sudo dnf install python3.11 python3.11-devel -y"
                    echo ""
                    echo -e "${YELLOW}Please run the above command with sudo privileges${NC}"
                    echo -e "${CYAN}After installation, re-run this script${NC}"
                    return 1
                    ;;
                    
                arch|manjaro)
                    echo -e "${YELLOW}Detected Arch Linux${NC}"
                    echo -e "${CYAN}Commands to install Python 3.11:${NC}"
                    
                    echo "sudo pacman -S python python-pip"
                    echo ""
                    echo -e "${YELLOW}Please run the above command with sudo privileges${NC}"
                    echo -e "${CYAN}After installation, re-run this script${NC}"
                    return 1
                    ;;
                    
                *)
                    echo -e "${YELLOW}Unknown Linux distribution: $os_id${NC}"
                    echo -e "${CYAN}Please install Python 3.11 manually${NC}"
                    return 1
                    ;;
            esac
            
        elif [[ "$(uname)" == "Darwin" ]]; then
            # macOS
            echo -e "${YELLOW}Detected macOS${NC}"
            
            if command -v brew >/dev/null 2>&1; then
                echo -e "${CYAN}Installing Python 3.11 using Homebrew...${NC}"
                brew install python@3.11
                return $?
            else
                echo -e "${YELLOW}Homebrew not found${NC}"
                echo -e "${CYAN}Install Homebrew first: https://brew.sh${NC}"
                echo -e "${CYAN}Then run: brew install python@3.11${NC}"
                return 1
            fi
        else
            echo -e "${RED}Unable to detect operating system${NC}"
            echo -e "${CYAN}Please install Python 3.11 manually${NC}"
            return 1
        fi
    else
        echo -e "${YELLOW}Invalid choice${NC}"
        return 1
    fi
}

# Load custom Python path if it exists
load_python_path() {
    local python_path_file="$PREFS_DIR/.python311_path"
    
    if [[ -f "$python_path_file" ]]; then
        local python_bin_path=$(cat "$python_path_file")
        if [[ -d "$python_bin_path" ]]; then
            export PATH="$python_bin_path:$PATH"
        fi
    fi
    # Always return success - missing file is not an error
    return 0
}

# Check dependencies
check_dependencies() {
    local missing=()
    local first_run_file="$PREFS_DIR/.venv_setup_complete"
    local venv_dir="$SCRIPT_DIR/etl_venv"
    
    # Try to load custom Python installation path first
    load_python_path
    
    # Check for required system commands
    for cmd in grep cut wc; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    
    # Check for Python 3.11 specifically, or any Python 3
    local python_cmd=""
    if command -v python3.11 >/dev/null 2>&1; then
        python_cmd="python3.11"
    elif command -v python3 >/dev/null 2>&1; then
        python_cmd="python3"
        local current_version=$(python3 --version 2>&1 | awk '{print $2}')
        
        # Only offer to install if running interactively and not skipping installation
        if [[ -t 0 ]] && [[ -t 1 ]] && [[ "$SKIP_INSTALL" != "true" ]]; then
            echo -e "${YELLOW}Note: Python 3.11 preferred, found Python $current_version${NC}"
            # Offer to install Python 3.11
            if confirm_install_python "3.11"; then
                if install_python_311; then
                    python_cmd="python3.11"
                fi
            fi
        fi
    else
        echo -e "${RED}Python 3 not found in system${NC}"
        
        # Only offer to install if running interactively and not skipping installation
        if [[ -t 0 ]] && [[ -t 1 ]] && [[ "$SKIP_INSTALL" != "true" ]]; then
            # Offer to install Python 3.11
            if confirm_install_python "3.11"; then
                if install_python_311; then
                    python_cmd="python3.11"
                else
                    echo -e "${RED}Failed to install Python 3.11${NC}"
                    exit 1
                fi
            else
                echo -e "${RED}Python is required to continue${NC}"
                exit 1
            fi
        else
            echo -e "${RED}Python is required to continue. Please install Python 3 and try again.${NC}"
            exit 1
        fi
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo -e "${RED}ERROR: Missing required system dependencies: ${missing[*]}${NC}"
        echo "Please install the missing tools and try again."
        exit 1
    fi
    
    # Skip venv setup if flag is set
    if [[ "$SKIP_VENV" == "true" ]]; then
        echo -e "${YELLOW}Skipping virtual environment setup (--no-venv flag set)${NC}"
    else
        # Check if this is first run or venv doesn't exist
        if [[ ! -f "$first_run_file" ]] || [[ ! -d "$venv_dir" ]]; then
            if [[ "$SKIP_INSTALL" != "true" ]]; then
                echo -e "${CYAN}First run detected. Setting up Python environment...${NC}"
                setup_python_environment "$python_cmd"
                # After setup, the venv is already activated by setup_python_environment
            else
                echo -e "${YELLOW}Skipping package installation (--skip-install flag set)${NC}"
            fi
        fi
        
        # Activate venv if it exists (and wasn't just created/activated by setup)
        if [[ -d "$venv_dir" ]] && [[ -f "$venv_dir/bin/activate" ]]; then
            source "$venv_dir/bin/activate"
            export VIRTUAL_ENV="$venv_dir"
            export PATH="$venv_dir/bin:$PATH"
        fi
        
        # Verify required Python packages are installed AFTER venv is activated
        # This check should happen inside the venv context
        if [[ "$SKIP_INSTALL" != "true" ]]; then
            if ! python3 -c "import snowflake.connector" 2>/dev/null; then
                echo -e "${YELLOW}Required Python packages not found in virtual environment. Installing...${NC}"
                setup_python_environment "$python_cmd"
            fi
        fi
    fi
    
    # Load saved proxy configuration if it exists
    local proxy_file="$PREFS_DIR/.proxy_config"
    if [[ -f "$proxy_file" ]]; then
        local proxy=$(cat "$proxy_file")
        export http_proxy="$proxy"
        export https_proxy="$proxy"
        export HTTP_PROXY="$proxy"
        export HTTPS_PROXY="$proxy"
    fi
    
    # When skipping venv entirely, warn about packages but don't install
    if [[ "$SKIP_VENV" == "true" ]] || [[ "$SKIP_INSTALL" == "true" ]]; then
        if ! python3 -c "import snowflake.connector" 2>/dev/null; then
            echo -e "${YELLOW}Warning: Required Python packages may not be installed.${NC}"
            echo -e "${YELLOW}Proceeding anyway due to skip flags...${NC}"
        fi
    fi
}

# Test connectivity to PyPI
test_pypi_connectivity() {
    local proxy="${1:-}"
    local test_url="https://pypi.org/simple/"
    
    echo -e "${YELLOW}Testing connectivity to PyPI...${NC}"
    
    # Try with curl first (more common in minimal systems)
    if command -v curl >/dev/null 2>&1; then
        if [[ -n "$proxy" ]]; then
            if curl -s --proxy "$proxy" --connect-timeout 10 "$test_url" >/dev/null 2>&1; then
                return 0
            fi
        else
            if curl -s --connect-timeout 10 "$test_url" >/dev/null 2>&1; then
                return 0
            fi
        fi
    fi
    
    # Try with wget as fallback
    if command -v wget >/dev/null 2>&1; then
        if [[ -n "$proxy" ]]; then
            export http_proxy="$proxy"
            export https_proxy="$proxy"
        fi
        
        if wget -q --timeout=10 --spider "$test_url" 2>/dev/null; then
            return 0
        fi
    fi
    
    # Try with Python as last resort
    if python3 -c "
import urllib.request
import sys
import os
timeout = 10
proxy = os.environ.get('https_proxy', '$proxy')
if proxy:
    proxy_handler = urllib.request.ProxyHandler({'https': proxy})
    opener = urllib.request.build_opener(proxy_handler)
    urllib.request.install_opener(opener)
try:
    urllib.request.urlopen('$test_url', timeout=timeout)
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Configure proxy settings
configure_proxy() {
    local proxy_file="$PREFS_DIR/.proxy_config"
    local proxy=""
    
    # Check if proxy is already configured
    if [[ -f "$proxy_file" ]]; then
        proxy=$(cat "$proxy_file")
        echo -e "${CYAN}Using saved proxy configuration: $proxy${NC}"
        export http_proxy="$proxy"
        export https_proxy="$proxy"
        export HTTP_PROXY="$proxy"
        export HTTPS_PROXY="$proxy"
        return 0
    fi
    
    # Test direct connection first
    if test_pypi_connectivity; then
        echo -e "${GREEN}✓ Direct connection to PyPI successful${NC}"
        return 0
    fi
    
    echo -e "${YELLOW}⚠ Cannot connect to PyPI directly${NC}"
    echo -e "${CYAN}This might be due to a corporate firewall or proxy requirement.${NC}"
    echo ""
    
    # Check for existing proxy environment variables
    if [[ -n "${https_proxy:-}" ]] || [[ -n "${HTTPS_PROXY:-}" ]] || [[ -n "${http_proxy:-}" ]] || [[ -n "${HTTP_PROXY:-}" ]]; then
        proxy="${https_proxy:-${HTTPS_PROXY:-${http_proxy:-$HTTP_PROXY}}}"
        echo -e "${CYAN}Found existing proxy setting: $proxy${NC}"
        
        if test_pypi_connectivity "$proxy"; then
            echo -e "${GREEN}✓ Connection successful with existing proxy${NC}"
            echo "$proxy" > "$proxy_file"
            return 0
        else
            echo -e "${YELLOW}Existing proxy setting didn't work${NC}"
        fi
    fi
    
    # Interactive proxy configuration
    echo -e "${CYAN}=== Proxy Configuration Required ===${NC}"
    echo -e "${CYAN}Please enter your proxy server details.${NC}"
    echo -e "${CYAN}Common formats:${NC}"
    echo -e "  - http://proxy.company.com:8080"
    echo -e "  - http://username:password@proxy.company.com:8080"
    echo -e "  - socks5://proxy.company.com:1080"
    echo ""
    
    while true; do
        read -p "Enter proxy URL (or 'skip' to proceed without proxy): " proxy
        
        if [[ "$proxy" == "skip" ]] || [[ -z "$proxy" ]]; then
            echo -e "${YELLOW}Proceeding without proxy configuration${NC}"
            echo -e "${YELLOW}Note: Package installation may fail${NC}"
            return 1
        fi
        
        # Validate proxy format
        if [[ ! "$proxy" =~ ^https?:// ]] && [[ ! "$proxy" =~ ^socks5?:// ]]; then
            proxy="http://$proxy"
            echo -e "${CYAN}Added http:// prefix: $proxy${NC}"
        fi
        
        # Test the proxy
        echo -e "${YELLOW}Testing proxy connection...${NC}"
        if test_pypi_connectivity "$proxy"; then
            echo -e "${GREEN}✓ Proxy connection successful!${NC}"
            
            # Save proxy configuration
            mkdir -p "$PREFS_DIR"
            echo "$proxy" > "$proxy_file"
            
            # Export for current session
            export http_proxy="$proxy"
            export https_proxy="$proxy"
            export HTTP_PROXY="$proxy"
            export HTTPS_PROXY="$proxy"
            
            echo -e "${GREEN}Proxy configuration saved${NC}"
            
            # Check if SSL issues are likely with this proxy
            echo ""
            echo -e "${CYAN}=== SSL Configuration ===${NC}"
            echo -e "${YELLOW}Corporate proxies often intercept SSL connections.${NC}"
            echo -e "${YELLOW}If you encounter SSL handshake errors with Snowflake:${NC}"
            echo ""
            echo "  1. Normal mode (recommended) - Try first"
            echo "  2. Insecure mode - Disable SSL verification (use with caution)"
            echo ""
            read -p "Select SSL mode [1-2] (default: 1): " ssl_choice
            
            if [[ "$ssl_choice" == "2" ]]; then
                echo -e "${YELLOW}⚠ WARNING: Disabling SSL verification${NC}"
                echo -e "${YELLOW}This should only be used in trusted environments${NC}"
                read -p "Are you sure? [y/N]: " confirm
                
                if [[ "$confirm" =~ ^[Yy]$ ]]; then
                    touch "$PREFS_DIR/.insecure_mode"
                    export SNOWFLAKE_INSECURE_MODE=1
                    echo -e "${YELLOW}Insecure mode enabled for Snowflake connections${NC}"
                else
                    echo -e "${GREEN}Keeping SSL verification enabled (recommended)${NC}"
                fi
            else
                # Remove insecure mode flag if it exists
                rm -f "$PREFS_DIR/.insecure_mode"
                unset SNOWFLAKE_INSECURE_MODE
                echo -e "${GREEN}SSL verification enabled (recommended)${NC}"
            fi
            
            return 0
        else
            echo -e "${RED}✗ Could not connect through proxy${NC}"
            echo -e "${YELLOW}Please check the proxy URL and try again${NC}"
            echo ""
        fi
    done
}

# Setup Python virtual environment
setup_python_environment() {
    local python_cmd="${1:-python3}"
    local venv_dir="$SCRIPT_DIR/etl_venv"
    local first_run_file="$PREFS_DIR/.venv_setup_complete"
    
    echo -e "${CYAN}=== Python Environment Setup ===${NC}"
    echo -e "${CYAN}This will create a virtual environment and install required packages.${NC}"
    echo -e "${CYAN}This only needs to be done once.${NC}"
    echo ""
    
    # Check Python version
    local python_version=$($python_cmd -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
    echo -e "${GREEN}Using Python $python_version${NC}"
    
    # Test connectivity and configure proxy if needed
    configure_proxy
    
    # Create virtual environment
    echo -e "${YELLOW}Creating virtual environment in $venv_dir...${NC}"
    if $python_cmd -m venv "$venv_dir"; then
        echo -e "${GREEN}✓ Virtual environment created${NC}"
    else
        echo -e "${RED}Failed to create virtual environment${NC}"
        echo "Try installing python3-venv: sudo apt-get install python3-venv"
        exit 1
    fi
    
    # Activate virtual environment
    source "$venv_dir/bin/activate"
    
    # Set up pip with proxy if configured
    local pip_args=""
    if [[ -n "${https_proxy:-}" ]]; then
        pip_args="--proxy $https_proxy"
        echo -e "${CYAN}Using proxy for pip: $https_proxy${NC}"
    fi
    
    # Upgrade pip
    echo -e "${YELLOW}Upgrading pip...${NC}"
    python3 -m pip install $pip_args --upgrade pip --quiet
    
    # Install requirements
    if [[ -f "$SCRIPT_DIR/requirements.txt" ]]; then
        echo -e "${YELLOW}Installing required packages from requirements.txt...${NC}"
        if python3 -m pip install $pip_args -r "$SCRIPT_DIR/requirements.txt" --quiet; then
            echo -e "${GREEN}✓ All packages installed successfully${NC}"
        else
            echo -e "${RED}Failed to install some packages${NC}"
            echo "You may need to install them manually"
            
            # If proxy is configured, show proxy-specific help
            if [[ -n "${https_proxy:-}" ]]; then
                echo -e "${YELLOW}Proxy is configured. If installation failed, check:${NC}"
                echo "  - Proxy allows HTTPS connections to pypi.org"
                echo "  - Proxy credentials are correct (if required)"
                echo "  - Corporate firewall rules"
            fi
        fi
    else
        echo -e "${YELLOW}requirements.txt not found, installing essential packages...${NC}"
        python3 -m pip install $pip_args snowflake-connector-python pandas numpy tqdm --quiet
    fi
    
    # Install the snowflake_etl package if setup.py exists
    if [[ -f "$SCRIPT_DIR/setup.py" ]]; then
        echo -e "${YELLOW}Installing snowflake_etl package...${NC}"
        if python3 -m pip install $pip_args -e "$SCRIPT_DIR" --quiet; then
            echo -e "${GREEN}✓ snowflake_etl package installed${NC}"
        else
            echo -e "${YELLOW}Note: snowflake_etl package installation skipped${NC}"
        fi
    fi
    
    # Mark setup as complete
    mkdir -p "$PREFS_DIR"
    touch "$first_run_file"
    
    echo ""
    echo -e "${GREEN}=== Setup Complete ===${NC}"
    echo -e "${GREEN}Virtual environment is ready at: $venv_dir${NC}"
    echo -e "${GREEN}The environment will be automatically activated for future runs.${NC}"
    
    if [[ -f "$PREFS_DIR/.proxy_config" ]]; then
        echo -e "${CYAN}Proxy configuration saved for future use${NC}"
        echo -e "${CYAN}To reset proxy: rm $PREFS_DIR/.proxy_config${NC}"
    fi
    
    echo ""
    echo -e "${CYAN}Press Enter to continue...${NC}"
    read -r
}

# Clear proxy configuration (utility function)
clear_proxy_config() {
    local proxy_file="$PREFS_DIR/.proxy_config"
    
    if [[ -f "$proxy_file" ]]; then
        rm -f "$proxy_file"
        echo -e "${GREEN}Proxy configuration cleared${NC}"
        echo -e "${CYAN}The next venv setup will re-test connectivity${NC}"
    else
        echo -e "${YELLOW}No proxy configuration found${NC}"
    fi
    
    # Clear environment variables
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
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
    # Force text mode if not a TTY (e.g., running in pipes or non-interactive)
    if ! [[ -t 0 ]] || ! [[ -t 1 ]]; then
        USE_DIALOG=false
        return
    fi
    
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

# Note: UI functions (show_menu, show_message, show_error, show_success) are now provided by lib/ui_components.sh

# Show message with dynamic sizing
# Note: show_message, get_input, and confirm_action functions are now provided by lib/ui_components.sh

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
        echo -e "${BOLD}${BLUE}Starting: $job_name${NC}"
        echo "Log: $log_file"
        echo -e "${YELLOW}Progress will be shown below:${NC}"
        echo "----------------------------------------"
        
        # Create a temporary file for capturing the exit status
        local exit_status_file="/tmp/job_exit_status_$$"
        
        # Run command with tee, capturing both output and exit status
        # Use unbuffer or stdbuf if available to prevent buffering issues
        if command -v stdbuf >/dev/null 2>&1; then
            # Use stdbuf to disable buffering for better real-time output
            { stdbuf -o0 -e0 "$@" 2>&1; echo $? > "$exit_status_file"; } | tee "$log_file"
        else
            # Fallback to regular execution
            { "$@" 2>&1; echo $? > "$exit_status_file"; } | tee "$log_file"
        fi
        
        # Read the actual exit status
        local exit_code=$(cat "$exit_status_file" 2>/dev/null || echo 1)
        rm -f "$exit_status_file"
        
        if [[ "$exit_code" -eq 0 ]]; then
            update_job_file "$job_file" "STATUS" "COMPLETED"
            echo -e "${GREEN}SUCCESS Job completed successfully${NC}"
        else
            update_job_file "$job_file" "STATUS" "FAILED"
            echo -e "${RED}FAILED Job failed with exit code: $exit_code${NC}"
        fi
        update_job_file "$job_file" "END_TIME" "$(date +"%Y-%m-%d %H:%M:%S")"
        
        # Verify log file was written
        if [[ -s "$log_file" ]]; then
            local log_size=$(wc -c < "$log_file")
            echo -e "${BLUE}Log file saved: $log_file (${log_size} bytes)${NC}"
        else
            echo -e "${YELLOW}Warning: Log file is empty or missing: $log_file${NC}"
        fi
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
        
        # Format message properly for show_message function
        local msg="Job: $job_name
ID: $job_id
PID: $bg_pid
Log: $log_file

Tip: Check 'Job Status' menu to monitor progress"
        show_message "Job Started" "$msg"
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
                # Create clean array without separators for proper indexing
                local clean_options=()
                for opt in "${menu_options[@]}"; do
                    if [[ "$opt" != "---" ]]; then
                        clean_options+=("$opt")
                    fi
                done
                
                # Now index into the clean array
                local selected_option="${clean_options[$((choice - 1))]}"
                
                # Check what type of selection it is
                if [[ "$selected_option" == "Clean Completed Jobs" ]]; then
                    clean_completed_jobs
                    # Automatically refresh the menu after cleaning
                    continue
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
                    # Sanitize to prevent escape sequence attacks
                    # Use portable sed syntax without \x hex escapes
                    if command -v sed >/dev/null 2>&1; then
                        # Remove ANSI escape codes using printf for the escape character
                        local results=$(tail -20 "$log_file" 2>/dev/null | head -15 | sed $'s/\033\\[[0-9;]*[a-zA-Z]//g; s/\033\\].*\007//g' | tr -d '\000-\010\013\014\016-\037\177')
                    else
                        local results=$(tail -20 "$log_file" 2>/dev/null | head -15)
                    fi
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
                    # Sanitize error output to prevent escape sequence attacks
                    # Use portable sed syntax without \x hex escapes
                    if command -v sed >/dev/null 2>&1; then
                        status_text+="$(tail -10 "$log_file" 2>/dev/null | sed $'s/\033\\[[0-9;]*[a-zA-Z]//g; s/\033\\].*\007//g' | tr -d '\000-\010\013\014\016-\037\177' || echo "Log file not accessible")\n"
                    else
                        status_text+="$(tail -10 "$log_file" 2>/dev/null || echo "Log file not accessible")\n"
                    fi
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
    local status=$(parse_job_file "$job_file" "STATUS")
    local log_file=$(parse_job_file "$job_file" "LOG_FILE")
    
    # Input validation - check if log file path is set
    if [[ -z "$log_file" ]]; then
        echo -e "${RED}Error: Log file path is missing for job: $job_name${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file exists
    if [[ ! -f "$log_file" ]]; then
        echo -e "${RED}Error: Log file not found: $log_file${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file is readable
    if [[ ! -r "$log_file" ]]; then
        echo -e "${RED}Error: Cannot read log file (permission denied): $log_file${NC}" >&2
        read -p "Press Enter to continue..."
        return 1
    fi
    
    # Check if file is empty
    if [[ ! -s "$log_file" ]]; then
        echo -e "${YELLOW}Log for '$job_name' is empty.${NC}"
        read -p "Press Enter to continue..."
        return 0
    fi
    
    # Simple, reliable header - no fancy UI that can break
    echo ""
    echo -e "--- Viewing log for: ${BOLD}$job_name${NC} [${status}]"
    echo "--- File: $log_file"
    echo "--- (Press 'q' to quit, '/' to search)"
    echo ""
    
    # Small delay to ensure user sees the header
    sleep 0.5
    
    # Sanitize log content to prevent escape sequence attacks
    # Create a temporary file with sanitized content
    local sanitized_log="/tmp/sanitized_log_$$"
    
    # Strip dangerous control sequences while preserving readability
    # This removes ANSI escape codes, terminal control sequences, etc.
    if command -v sed >/dev/null 2>&1 && command -v tr >/dev/null 2>&1; then
        # Remove ANSI escape sequences and control characters
        # Use portable syntax with $'...' for escape chars
        sed $'s/\033\\[[0-9;]*[a-zA-Z]//g; s/\033\\].*\007//g' "$log_file" | tr -d '\000-\010\013\014\016-\037\177' > "$sanitized_log"
    else
        # Fallback: copy as-is if sed/tr not available (less safe)
        cp "$log_file" "$sanitized_log"
        echo -e "${YELLOW}Warning: Could not sanitize log content${NC}"
    fi
    
    # Use the configured viewer or auto-detect
    local viewer_choice="${LOG_VIEWER:-auto}"
    
    case "$viewer_choice" in
        "nano")
            if command -v nano >/dev/null 2>&1; then
                # Use nano - very stable for problematic logs
                nano -v "$sanitized_log"  # -v for view mode (read-only)
            else
                echo -e "${YELLOW}nano not found, falling back to auto${NC}"
                viewer_choice="auto"
            fi
            ;;
        "less")
            if command -v less >/dev/null 2>&1; then
                less -FXS "$sanitized_log"
            else
                echo -e "${YELLOW}less not found, falling back to auto${NC}"
                viewer_choice="auto"
            fi
            ;;
        "cat")
            cat "$sanitized_log"
            echo ""
            echo "--- End of log ---"
            read -p "Press Enter to continue..."
            ;;
        *)
            viewer_choice="auto"
            ;;
    esac
    
    # Auto mode - detect best available
    if [[ "$viewer_choice" == "auto" ]]; then
        if command -v nano >/dev/null 2>&1; then
            # Prefer nano for stability with problematic logs
            nano -v "$sanitized_log"
        elif command -v less >/dev/null 2>&1; then
            less -FXS "$sanitized_log"
        elif command -v more >/dev/null 2>&1; then
            more "$sanitized_log"
        else
            cat "$sanitized_log"
            echo ""
            echo "--- End of log ---"
            read -p "Press Enter to continue..."
        fi
    fi
    
    # Clean up temporary file
    rm -f "$sanitized_log"
    
    # Clear the screen after viewing to prevent log stacking
    clear
    
    # Return success
    return 0
}

# Monitor a specific job's progress
monitor_job_progress() {
    local job_file="$1"
    local job_name=$(parse_job_file "$job_file" "JOB_NAME")
    local log_file=$(parse_job_file "$job_file" "LOG_FILE")
    
    if [[ ! -f "$log_file" ]]; then
        show_message "Error" "Log file not found: $log_file"
        return 1
    fi
    
    # Save original environment
    local original_trap=$(trap -p SIGINT)
    local original_options="$-"
    local tail_pid=""
    
    # Enable job control if not already enabled
    # This puts background processes in their own process group
    if [[ "$original_options" != *m* ]]; then
        set -m
        # Verify job control was enabled
        if [[ "$-" != *m* ]]; then
            echo -e "${RED}Error: Job control not supported in this shell${NC}"
            echo "Unable to monitor job properly without job control."
            return 2
        fi
    fi
    
    clear
    echo -e "${BOLD}${BLUE}Monitoring Job: $job_name${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop monitoring and return to menu${NC}"
    echo "----------------------------------------"
    
    # Start tail in background (in its own process group due to set -m)
    if command -v sed >/dev/null 2>&1 && command -v tr >/dev/null 2>&1; then
        # Strip escape sequences while tailing - use portable syntax
        tail -f "$log_file" 2>/dev/null | sed -u $'s/\033\\[[0-9;]*[a-zA-Z]//g; s/\033\\].*\007//g' | tr -d '\000-\010\013\014\016-\037\177' &
        tail_pid=$!
    else
        # Fallback without sanitization (with warning)
        echo -e "${YELLOW}Warning: Cannot sanitize live output - be cautious of escape sequences${NC}"
        tail -f "$log_file" 2>/dev/null &
        tail_pid=$!
    fi
    
    # Set up trap for SIGINT (Ctrl+C)
    # This will only be triggered in the foreground shell, not the background tail
    trap '
        echo ""
        echo -e "${GREEN}Stopped monitoring. Returning to menu...${NC}"
        if [[ -n "$tail_pid" ]]; then
            kill "$tail_pid" 2>/dev/null
        fi
        sleep 1
        # Restore original environment before returning
        if [[ "$original_options" != *m* ]]; then
            set +m
        fi
        eval "${original_trap:-trap - SIGINT}"
        return 0
    ' SIGINT
    
    # Wait for the tail process
    # This will be interrupted by Ctrl+C, triggering the trap
    wait "$tail_pid" 2>/dev/null
    local exit_code=$?
    
    # Restore original environment
    if [[ "$original_options" != *m* ]]; then
        set +m
    fi
    eval "${original_trap:-trap - SIGINT}"
    
    # If we got here normally (not via Ctrl+C), tail exited on its own
    if [[ $exit_code -ne 130 && $exit_code -ne 0 ]]; then
        echo -e "${YELLOW}Monitoring ended (log file may have been rotated)${NC}"
        sleep 1
    fi
    
    return 0
}

# Clean completed jobs
clean_completed_jobs() {
    local cleaned=0
    local failed=0
    
    # Check if jobs directory exists and has job files
    if [[ ! -d "$JOBS_DIR" ]]; then
        show_message "No Jobs to Clean" "Jobs directory not found."
        return 0
    fi
    
    # Use nullglob to handle no matches gracefully
    local old_nullglob=$(shopt -p nullglob)
    shopt -s nullglob
    
    # Simple direct approach without complex subshells
    for job_file in "$JOBS_DIR"/*.job; do
        if [[ -f "$job_file" ]]; then
            local status=$(grep '^STATUS=' "$job_file" | cut -d'=' -f2)
            
            if [[ "$status" == "COMPLETED" ]] || [[ "$status" == "FAILED" ]] || [[ "$status" == "CRASHED" ]]; then
                # Remove the job file
                rm -f "$job_file"
                
                if [[ "$status" == "COMPLETED" ]]; then
                    cleaned=$((cleaned + 1))
                else
                    failed=$((failed + 1))
                fi
            fi
        fi
    done
    
    # Restore nullglob setting
    eval "$old_nullglob"
    
    local total=$((cleaned + failed))
    if [[ $total -eq 0 ]]; then
        show_message "No Jobs to Clean" "No completed or failed jobs found to clean."
    else
        local message="Cleaned $total job(s) from job status:\n\n"
        if [[ $cleaned -gt 0 ]]; then
            message+="[OK] $cleaned completed job(s)\n"
        fi
        if [[ $failed -gt 0 ]]; then
            message+="[FAILED] $failed failed/crashed job(s)\n"
        fi
        message+="\n(Log files preserved for debugging)"
        show_message "Jobs Cleaned" "$message"
    fi
    
    return 0
}

# ============================================================================
# CORE LOADING FUNCTIONS (Migrated from run_loader.sh)
# ============================================================================

# Function to check prerequisites
check_prerequisites() {
    local quiet_mode="${1:-}"
    local venv_dir="$SCRIPT_DIR/etl_venv"
    
    # Ensure venv is activated if it exists
    if [[ -d "$venv_dir" ]] && [[ -f "$venv_dir/bin/activate" ]]; then
        source "$venv_dir/bin/activate"
    fi
    
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}ERROR: Python 3 is not installed${NC}"
        return 1
    fi
    
    # Check Python version
    local python_version=$(python3 --version 2>&1 | awk '{print $2}')
    if [[ -z "$quiet_mode" ]]; then
        echo -e "${CYAN}Prerequisites Check:${NC}"
        echo -e "  Python version: ${python_version}"
        if [[ -n "$VIRTUAL_ENV" ]]; then
            echo -e "  Virtual env: ${GREEN}Active${NC} ($venv_dir)"
        fi
    fi
    
    # Check if required Python packages are installed
    local missing_packages=""
    
    for package in snowflake-connector-python pandas numpy; do
        if ! python3 -c "import ${package//-/_}" 2>/dev/null; then
            missing_packages="${missing_packages} ${package}"
        fi
    done
    
    if [[ -n "$missing_packages" ]]; then
        if [[ -z "$quiet_mode" ]]; then
            echo -e "${YELLOW}Warning: Missing Python packages:${missing_packages}${NC}"
            echo -e "${YELLOW}Install with: pip install${missing_packages}${NC}"
        fi
        # Don't fail on missing packages, just warn
    fi
    
    # Check if logs directory exists
    if [[ ! -d "$LOGS_DIR" ]]; then
        if [[ -z "$quiet_mode" ]]; then
            echo -e "${BLUE}Creating logs directory...${NC}"
        fi
        mkdir -p "$LOGS_DIR"
    fi
    
    if [[ -z "$quiet_mode" ]]; then
        echo -e "${GREEN}Prerequisites check complete${NC}\n"
    fi
    
    return 0
}

# Function to convert month format to YYYY-MM
convert_month_format() {
    local month_dir=$1
    # Check if already in YYYY-MM format
    if [[ $month_dir =~ ^([0-9]{4})-([0-9]{2})$ ]]; then
        echo "$month_dir"
    # Extract MM and YYYY from MMYYYY format
    elif [[ $month_dir =~ ^([0-9]{2})([0-9]{4})$ ]]; then
        echo "${BASH_REMATCH[2]}-${BASH_REMATCH[1]}"
    else
        echo ""
    fi
}

# Function to find all month directories
find_month_directories() {
    local base_path=$1
    local months=()
    
    # Look for directories matching MMYYYY pattern
    for dir in $(find "${base_path}" -maxdepth 1 -type d -name "[0-9][0-9][0-9][0-9][0-9][0-9]" 2>/dev/null | sort); do
        local dirname=$(basename "$dir")
        months+=("$dirname")
    done
    
    echo "${months[@]}"
}

# Direct Python CLI execution wrapper
execute_python_cli() {
    local operation="$1"
    shift
    local args=("$@")
    
    # Check prerequisites before running
    if ! check_prerequisites "quiet"; then
        echo -e "${RED}Prerequisites check failed${NC}"
        return 1
    fi
    
    # Build the command
    local cmd="python3 -m snowflake_etl"
    
    # Add config if available
    if [[ -n "$CONFIG_FILE" ]] && [[ -f "$CONFIG_FILE" ]]; then
        cmd="$cmd --config \"$CONFIG_FILE\""
    fi
    
    # Add operation and arguments
    cmd="$cmd $operation ${args[@]}"
    
    # Execute
    echo -e "${CYAN}Executing: $cmd${NC}"
    eval "$cmd"
    return $?
}

# Process a single month
process_month_direct() {
    local month="$1"
    local base_path="$2"
    local extra_args="${3:-}"
    
    echo -e "${BLUE}Processing month: $month${NC}"
    
    # Convert month format if needed
    local formatted_month=$(convert_month_format "$month")
    if [[ -z "$formatted_month" ]]; then
        echo -e "${RED}Invalid month format: $month${NC}"
        return 1
    fi
    
    # Build and execute command
    local args="--month \"$formatted_month\" --base-path \"$base_path\""
    
    # Add extra arguments (like --skip-qc, --validate-in-snowflake)
    if [[ -n "$extra_args" ]]; then
        args="$args $extra_args"
    fi
    
    execute_python_cli "load" "$args"
    return $?
}

# Process direct files
process_direct_files() {
    local files="$1"
    local extra_args="${2:-}"
    
    echo -e "${BLUE}Processing direct files: $files${NC}"
    
    # Build and execute command
    local args="--files \"$files\""
    
    # Add extra arguments
    if [[ -n "$extra_args" ]]; then
        args="$args $extra_args"
    fi
    
    execute_python_cli "load" "$args"
    return $?
}

# ============================================================================
# BATCH AND PARALLEL PROCESSING FUNCTIONS (Phase 2)
# ============================================================================

# Process all months in batch mode
process_batch_months() {
    local base_path="${1:-$BASE_PATH}"
    local extra_args="${2:-}"
    local parallel_jobs="${3:-1}"
    
    echo -e "${CYAN}Discovering months in: $base_path${NC}"
    
    # Find all month directories
    local months_array=($(find_month_directories "$base_path"))
    
    if [[ ${#months_array[@]} -eq 0 ]]; then
        echo -e "${YELLOW}No month directories found in $base_path${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Found ${#months_array[@]} months to process${NC}"
    
    # Process months (parallel or sequential)
    if [[ $parallel_jobs -gt 1 ]]; then
        process_months_parallel "${months_array[@]}" "$base_path" "$extra_args" "$parallel_jobs"
    else
        process_months_sequential "${months_array[@]}" "$base_path" "$extra_args"
    fi
    
    return $?
}

# Process multiple months sequentially
process_months_sequential() {
    # Parse arguments - last two are base_path and extra_args
    local args=("$@")
    local num_args=${#args[@]}
    local extra_args="${args[$((num_args-1))]}"
    local base_path="${args[$((num_args-2))]}"
    
    # Extract months array (all but last two arguments)
    local months=("${args[@]:0:$((num_args-2))}")
    
    local total=${#months[@]}
    local successful=0
    local failed=0
    
    for i in "${!months[@]}"; do
        local month="${months[$i]}"
        local current=$((i + 1))
        
        echo -e "\n${MAGENTA}[$current/$total] Processing month: $month${NC}"
        
        if process_month_direct "$month" "$base_path" "$extra_args"; then
            ((successful++))
            echo -e "${GREEN}[OK] Month $month completed successfully${NC}"
        else
            ((failed++))
            echo -e "${RED}[FAILED] Month $month failed${NC}"
        fi
    done
    
    # Summary
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}BATCH PROCESSING SUMMARY${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Total Months:    $total"
    echo -e "Successful:      $successful"
    echo -e "Failed:          $failed"
    echo -e "${GREEN}========================================${NC}"
    
    [[ $failed -eq 0 ]] && return 0 || return 1
}

# Process multiple months in parallel
process_months_parallel() {
    # Parse arguments - last three are base_path, extra_args, and parallel_jobs
    local args=("$@")
    local num_args=${#args[@]}
    local parallel_jobs="${args[$((num_args-1))]}"
    local extra_args="${args[$((num_args-2))]}"
    local base_path="${args[$((num_args-3))]}"
    
    # Extract months array (all but last three arguments)
    local months=("${args[@]:0:$((num_args-3))}")
    
    local total=${#months[@]}
    local successful=0
    local failed=0
    declare -A job_pids  # Track PIDs and their months
    
    echo -e "${CYAN}Processing $total months with $parallel_jobs parallel jobs${NC}"
    
    # Function to wait for a job slot
    wait_for_job_slot() {
        while [[ $(jobs -r | wc -l) -ge $parallel_jobs ]]; do
            sleep 0.5
        done
    }
    
    # Function to check completed jobs
    check_completed_jobs() {
        local temp_pids=()
        for pid in "${!job_pids[@]}"; do
            if ! kill -0 $pid 2>/dev/null; then
                wait $pid
                local exit_code=$?
                local month="${job_pids[$pid]}"
                
                if [[ $exit_code -eq 0 ]]; then
                    ((successful++))
                    echo -e "${GREEN}[OK] Month $month completed successfully${NC}"
                else
                    ((failed++))
                    echo -e "${RED}[FAILED] Month $month failed${NC}"
                fi
                
                unset job_pids[$pid]
            fi
        done
    }
    
    # Launch parallel jobs
    for i in "${!months[@]}"; do
        local month="${months[$i]}"
        local current=$((i + 1))
        
        wait_for_job_slot
        check_completed_jobs
        
        echo -e "${BLUE}[$current/$total] Starting month: $month${NC}"
        
        # Run in background
        {
            process_month_direct "$month" "$base_path" "$extra_args"
        } &
        
        job_pids[$!]="$month"
    done
    
    # Wait for all remaining jobs
    echo -e "\n${BLUE}Waiting for all parallel jobs to complete...${NC}"
    while [[ ${#job_pids[@]} -gt 0 ]]; do
        check_completed_jobs
        sleep 0.5
    done
    
    # Summary
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}PARALLEL BATCH PROCESSING SUMMARY${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Total Months:    $total"
    echo -e "Successful:      $successful"
    echo -e "Failed:          $failed"
    echo -e "${GREEN}========================================${NC}"
    
    [[ $failed -eq 0 ]] && return 0 || return 1
}

# Process multiple comma-separated months
process_multiple_months() {
    local months_str="$1"
    local base_path="${2:-$BASE_PATH}"
    local extra_args="${3:-}"
    
    # Split comma-separated months into array
    IFS=',' read -ra months_array <<< "$months_str"
    
    echo -e "${CYAN}Processing ${#months_array[@]} specified months${NC}"
    
    # Process sequentially (can be made parallel if needed)
    process_months_sequential "${months_array[@]}" "$base_path" "$extra_args"
    
    return $?
}

# ============================================================================
# DELETE OPERATIONS (Migrated from drop_month.sh)
# ============================================================================

# Delete data for a specific month from a table
delete_month_data() {
    local table="$1"
    local month="$2"
    local extra_args="${3:-}"
    
    echo -e "${YELLOW}Deleting data from $table for month $month${NC}"
    
    # Build and execute command
    local args="--table \"$table\" --month \"$month\""
    
    # Add extra arguments (like --yes for no confirmation)
    if [[ -n "$extra_args" ]]; then
        args="$args $extra_args"
    fi
    
    execute_python_cli "delete" "$args"
    return $?
}

# ============================================================================
# CONFIG GENERATION FUNCTIONS (Phase 3 - Migrated from generate_config.sh)
# ============================================================================

# Detect file pattern (date_range vs month)
detect_file_pattern() {
    local filename="$1"
    local base_name="$(basename "$filename")"
    
    # Remove .tsv extension
    base_name="${base_name%.tsv}"
    
    # Check for date range pattern (YYYYMMDD-YYYYMMDD)
    if [[ "$base_name" =~ ([0-9]{8})-([0-9]{8}) ]]; then
        # Replace the date range with placeholder
        local pattern="${base_name/${BASH_REMATCH[0]}/{date_range}}.tsv"
        echo "$pattern"
        return 0
    fi
    
    # Check for month pattern (YYYY-MM)
    if [[ "$base_name" =~ [0-9]{4}-[0-9]{2} ]]; then
        # Replace the month with placeholder
        local pattern="${base_name/${BASH_REMATCH[0]}/{month}}.tsv"
        echo "$pattern"
        return 0
    fi
    
    # No pattern detected, return original filename
    echo "$(basename "$filename")"
}

# Extract table name from filename
extract_table_name() {
    local filename="$1"
    local base_name="$(basename "$filename" .tsv)"
    
    # Remove date range patterns (YYYYMMDD-YYYYMMDD)
    base_name=$(echo "$base_name" | sed -E 's/_?[0-9]{8}-[0-9]{8}//g')
    
    # Remove month patterns (YYYY-MM)
    base_name=$(echo "$base_name" | sed -E 's/_?[0-9]{4}-[0-9]{2}//g')
    
    # Convert to uppercase and replace non-alphanumeric with underscore
    echo "$base_name" | tr '[:lower:]' '[:upper:]' | sed 's/[^A-Z0-9]/_/g'
}

# Analyze TSV file structure
analyze_tsv_file() {
    local file="$1"
    local column_count=0
    
    if [[ ! -f "$file" ]]; then
        echo -e "${RED}Error: File not found: $file${NC}" >&2
        return 1
    fi
    
    # Get column count from first line
    column_count=$(head -1 "$file" | awk -F'\t' '{print NF}')
    
    echo "$column_count"
    return 0
}

# Query Snowflake for table columns
query_snowflake_columns() {
    local table_name="$1"
    local config_file="${2:-$CONFIG_FILE}"
    
    if [[ ! -f "$config_file" ]]; then
        echo -e "${RED}Error: Config file not found: $config_file${NC}" >&2
        return 1
    fi
    
    echo -e "${CYAN}Querying Snowflake for table: $table_name${NC}" >&2
    
    # Use Python CLI to get table info
    local result=$(python3 -m snowflake_etl --config "$config_file" check-table "$table_name" 2>&1)
    
    if [[ "$result" == *"exists with"* ]]; then
        # Extract column names from output
        echo "$result" | grep -oP '(?<=columns: ).*' | tr ',' '\n' | tr -d ' '
        return 0
    else
        echo -e "${YELLOW}Warning: Could not query table $table_name${NC}" >&2
        return 1
    fi
}

# Interactive mode for credentials
prompt_snowflake_credentials() {
    local output_file="${1:-}"
    
    echo -e "${CYAN}Enter Snowflake credentials:${NC}"
    
    read -p "Account: " sf_account
    read -p "User: " sf_user
    read -s -p "Password: " sf_password
    echo
    read -p "Warehouse: " sf_warehouse
    read -p "Database: " sf_database
    read -p "Schema: " sf_schema
    read -p "Role: " sf_role
    
    # Create credentials JSON
    local creds_json=$(cat <<EOF
{
    "snowflake": {
        "account": "$sf_account",
        "user": "$sf_user",
        "password": "$sf_password",
        "warehouse": "$sf_warehouse",
        "database": "$sf_database",
        "schema": "$sf_schema",
        "role": "$sf_role"
    }
}
EOF
)
    
    if [[ -n "$output_file" ]]; then
        echo "$creds_json" > "$output_file"
        echo -e "${GREEN}Credentials saved to: $output_file${NC}"
    else
        echo "$creds_json"
    fi
}

# Generate config from TSV files
generate_config_from_files() {
    local files="$1"
    local output_file="${2:-}"
    local table_name="${3:-}"
    local column_headers="${4:-}"
    local date_column="${5:-RECORDDATEID}"
    
    echo -e "${CYAN}Generating configuration for TSV files${NC}"
    
    # Start building config
    local config_json='{"files": ['
    local first=true
    
    # Process each file
    for file in $files; do
        if [[ ! -f "$file" ]]; then
            echo -e "${YELLOW}Warning: File not found: $file${NC}"
            continue
        fi
        
        # Get file pattern
        local pattern=$(detect_file_pattern "$file")
        
        # Get table name if not provided
        if [[ -z "$table_name" ]]; then
            table_name=$(extract_table_name "$file")
        fi
        
        # Get column count
        local col_count=$(analyze_tsv_file "$file")
        
        # Get or generate column headers
        local columns=""
        if [[ -n "$column_headers" ]]; then
            columns="$column_headers"
        elif [[ -n "$table_name" ]] && [[ -f "$CONFIG_FILE" ]]; then
            # Try to query Snowflake
            columns=$(query_snowflake_columns "$table_name" "$CONFIG_FILE" | tr '\n' ',' | sed 's/,$//')
        fi
        
        # Generate generic columns if needed
        if [[ -z "$columns" ]]; then
            columns=""
            for i in $(seq 1 $col_count); do
                [[ -n "$columns" ]] && columns="$columns,"
                columns="${columns}COLUMN$i"
            done
        fi
        
        # Add to config
        [[ "$first" == false ]] && config_json="$config_json,"
        config_json="$config_json
        {
            \"file_pattern\": \"$pattern\",
            \"table_name\": \"$table_name\",
            \"date_column\": \"$date_column\",
            \"expected_columns\": [$(echo "$columns" | sed 's/,/","/g' | sed 's/^/"/;s/$/"/')]
        }"
        first=false
    done
    
    config_json="$config_json
    ]}"
    
    # Output or save config
    if [[ -n "$output_file" ]]; then
        # Check if we need to merge with Snowflake credentials
        if [[ -f "$CONFIG_FILE" ]]; then
            # Extract Snowflake section from existing config
            local sf_section=$(python3 -c "import json; c=json.load(open('$CONFIG_FILE')); print(json.dumps(c.get('snowflake',{})))" 2>/dev/null)
            if [[ -n "$sf_section" ]] && [[ "$sf_section" != "{}" ]]; then
                # Merge with Snowflake section
                config_json=$(echo "$config_json" | python3 -c "import json,sys; c=json.load(sys.stdin); c['snowflake']=json.loads('$sf_section'); print(json.dumps(c, indent=2))")
            fi
        fi
        
        echo "$config_json" | python3 -m json.tool > "$output_file"
        echo -e "${GREEN}Configuration saved to: $output_file${NC}"
    else
        echo "$config_json" | python3 -m json.tool
    fi
}

# Main config generation wrapper
generate_config_direct() {
    local files="${1:-}"
    local output_file="${2:-}"
    local options="${3:-}"
    
    # Parse options
    local table_name=""
    local column_headers=""
    local interactive=false
    
    if [[ "$options" == *"--interactive"* ]]; then
        interactive=true
    fi
    
    if [[ "$options" =~ --table[[:space:]]+([^[:space:]]+) ]]; then
        table_name="${BASH_REMATCH[1]}"
    fi
    
    if [[ "$options" =~ --headers[[:space:]]+([^[:space:]]+) ]]; then
        column_headers="${BASH_REMATCH[1]}"
    fi
    
    # Handle interactive credentials if needed
    if [[ "$interactive" == true ]]; then
        if [[ -z "$output_file" ]]; then
            output_file="config/generated_config.json"
        fi
        prompt_snowflake_credentials "$output_file.tmp"
        CONFIG_FILE="$output_file.tmp"
    fi
    
    # Generate config from files
    if [[ -n "$files" ]]; then
        generate_config_from_files "$files" "$output_file" "$table_name" "$column_headers"
    elif [[ "$interactive" == true ]]; then
        # Just save credentials
        [[ -f "$output_file.tmp" ]] && mv "$output_file.tmp" "$output_file"
        echo -e "${GREEN}Credentials configuration created${NC}"
    else
        echo -e "${RED}Error: No files specified and not in interactive mode${NC}"
        return 1
    fi
    
    # Cleanup temp file
    [[ -f "$output_file.tmp" ]] && rm -f "$output_file.tmp"
    
    return 0
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
            "Load Custom Month" \
            "Load Specific File")
        
        case "$choice" in
            1) quick_load_current_month ;;
            2) quick_load_last_month ;;
            3) quick_load_custom_month ;;
            4) quick_load_specific_file ;;
            0|"") pop_menu; break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Snowflake Operations Menu
menu_snowflake_operations() {
    push_menu "Snowflake Operations"
    while true; do
        local choice=$(show_menu "$MENU_PATH" \
            "Load Data" \
            "Validate Data" \
            "Delete Data" \
            "Check Duplicates" \
            "Check Table Info" \
            "Generate Full Table Report")
        
        case "$choice" in
            1) menu_load_data ;;
            2) menu_validate_data ;;
            3) menu_delete_data ;;
            4) check_duplicates ;;
            5) check_table_info ;;
            6) generate_full_table_report ;;
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
            "View File Stats" \
            "Compare Files" \
            "Compress TSV File (No Upload)")
        
        case "$choice" in
            1) sample_tsv_file ;;
            2) generate_config ;;
            3) analyze_file_structure ;;
            4) check_file_issues ;;
            5) view_file_stats ;;
            6) compare_files ;;
            7) compress_tsv_file ;;
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

# Helper function to select quality check method
select_quality_check_method() {
    local qc_choice=$(show_menu "Quality Check Method" \
        "File-based quality checks (thorough but slower)" \
        "Snowflake-based validation (fast, requires connection)" \
        "Skip quality checks (fastest but no validation)")
    
    case "$qc_choice" in
        1) echo "file" ;;
        2) echo "snowflake" ;;
        3) echo "skip" ;;
        0|"") echo "cancelled" ;;
        *) echo "cancelled" ;;
    esac
}

# Quick load current month (using arrays)
quick_load_current_month() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local current_month=$(date +%Y-%m)
    
    # Ask for quality check preference
    local qc_method=$(select_quality_check_method)
    
    if [[ "$qc_method" == "cancelled" ]]; then
        return
    fi
    
    local qc_flags=""
    case "$qc_method" in
        "file") qc_flags="" ;;  # Default behavior
        "snowflake") qc_flags="--validate-in-snowflake" ;;
        "skip") qc_flags="--skip-qc" ;;
    esac
    
    if confirm_action "Load data for $current_month?\nQC Method: $qc_method\nUsing config: $(basename "$CONFIG_FILE")"; then
        # Use direct Python CLI call instead of run_loader.sh
        with_lock start_background_job "load_${current_month}" \
            process_month_direct "$current_month" "$BASE_PATH" "$qc_flags"
    fi
}

# Quick load last month (using arrays)
quick_load_last_month() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local last_month=$(date -d "last month" +%Y-%m 2>/dev/null || date -v-1m +%Y-%m)
    
    # Ask for quality check preference
    local qc_method=$(select_quality_check_method)
    
    if [[ "$qc_method" == "cancelled" ]]; then
        return
    fi
    
    local qc_flags=""
    case "$qc_method" in
        "file") qc_flags="" ;;  # Default behavior
        "snowflake") qc_flags="--validate-in-snowflake" ;;
        "skip") qc_flags="--skip-qc" ;;
    esac
    
    if confirm_action "Load data for $last_month?\nQC Method: $qc_method\nUsing config: $(basename "$CONFIG_FILE")"; then
        # Use direct Python CLI call instead of run_loader.sh
        with_lock start_background_job "load_${last_month}" \
            process_month_direct "$last_month" "$BASE_PATH" "$qc_flags"
    fi
}

# Quick load specific file
quick_load_specific_file() {
    local file_path=$(get_input "Load Specific File" "Enter TSV or TSV.GZ file path")
    
    if [[ -z "$file_path" ]]; then
        show_message "Error" "No file path provided"
        return
    fi
    
    if [[ ! -f "$file_path" ]]; then
        show_message "Error" "File not found: $file_path"
        return
    fi
    
    # Check if it's a supported file type
    if [[ ! "$file_path" =~ \.(tsv|tsv\.gz)$ ]]; then
        show_message "Warning" "File should be .tsv or .tsv.gz format. Proceeding anyway..."
    fi
    
    # If it's a .gz file, note it for the user
    if [[ "$file_path" =~ \.gz$ ]]; then
        echo -e "${YELLOW}Detected pre-compressed file (.gz). Will skip compression step.${NC}"
    fi
    
    # Ask for quality check preference
    local qc_method=$(select_quality_check_method)
    
    if [[ "$qc_method" == "cancelled" ]]; then
        return
    fi
    
    local qc_flags=""
    case "$qc_method" in
        "file") qc_flags="" ;;  # Default behavior
        "snowflake") qc_flags="--validate-in-snowflake" ;;
        "skip") qc_flags="--skip-qc" ;;
    esac
    
    if confirm_action "Load file: $(basename "$file_path")?\nQC Method: $qc_method"; then
        # Use direct Python CLI call instead of run_loader.sh
        with_lock start_background_job "load_file_$(basename "$file_path")" \
            process_direct_files "$file_path" "$qc_flags"
    fi
}

# Quick load with custom month
quick_load_custom_month() {
    local month=$(get_input "Load Custom Month" "Enter month (YYYY-MM)" "$(date +%Y-%m)")
    
    if [[ -z "$month" ]]; then
        show_message "Error" "No month provided"
        return
    fi
    
    # Ask for quality check preference
    local qc_method=$(select_quality_check_method)
    
    if [[ "$qc_method" == "cancelled" ]]; then
        return
    fi
    
    local qc_flags=""
    case "$qc_method" in
        "file") qc_flags="" ;;  # Default behavior
        "snowflake") qc_flags="--validate-in-snowflake" ;;
        "skip") qc_flags="--skip-qc" ;;
    esac
    
    if confirm_action "Load $month?\nQC Method: $qc_method\nUsing config: $(basename "$CONFIG_FILE")"; then
        # Use direct Python CLI call instead of run_loader.sh
        with_lock start_background_job "load_${month}" \
            process_month_direct "$month" "$BASE_PATH" "$qc_flags"
    fi
}

# Load data menu
menu_load_data() {
    # First, ask user to choose method
    local choice=$(show_menu "Load Data Method" \
        "Browse for TSV files interactively" \
        "Specify base path and month" \
        "Load all months from base path")
    
    case "$choice" in
        1)
            # Interactive file browser
            browse_and_load_files
            ;;
        2)
            # Traditional month-based loading - prompt for both base path and month
            local base_path=$(get_input "Load Data" "Enter base path for TSV files" "$BASE_PATH")
            
            if [[ ! -d "$base_path" ]]; then
                show_message "Error" "Base path does not exist: $base_path"
                return
            fi
            
            local month=$(get_input "Load Data" "Enter month(s) - comma separated (YYYY-MM)" "$(date +%Y-%m)")
            
            if [[ -n "$month" ]] && [[ -n "$base_path" ]]; then
                # Ask for quality check preference
                local qc_method=$(select_quality_check_method)
                
                if [[ "$qc_method" == "cancelled" ]]; then
                    return
                fi
                
                local qc_flags=""
                case "$qc_method" in
                    "file") qc_flags="" ;;  # Default behavior
                    "snowflake") qc_flags="--validate-in-snowflake" ;;
                    "skip") qc_flags="--skip-qc" ;;
                esac
                
                if confirm_action "Load month(s): $month from $base_path?\nQC Method: $qc_method"; then
                    # Use direct function for multi-month processing
                    with_lock start_background_job "load_${month}" \
                        process_multiple_months "$month" "$base_path" "$qc_flags"
                fi
            fi
            ;;
        3)
            # Load all months
            # Ask for quality check preference
            local qc_method=$(select_quality_check_method)
            
            if [[ "$qc_method" == "cancelled" ]]; then
                return
            fi
            
            local qc_flags=""
            case "$qc_method" in
                "file") qc_flags="" ;;  # Default behavior
                "snowflake") qc_flags="--validate-in-snowflake" ;;
                "skip") qc_flags="--skip-qc" ;;
            esac
            
            if confirm_action "Load ALL months from $BASE_PATH?\nQC Method: $qc_method"; then
                # Use direct batch processing function
                with_lock start_background_job "load_batch_all" \
                    process_batch_months "$BASE_PATH" "$qc_flags" "1"
            fi
            ;;
        0|"")
            return
            ;;
    esac
}

# Interactive file browser for loading
browse_and_load_files() {
    # Use the Python file browser to select files
    local temp_file="/tmp/tsv_browser_selection_$$.txt"
    
    echo -e "${BLUE}Opening interactive file browser...${NC}"
    echo -e "${YELLOW}Controls: Arrow keys to navigate, Enter to select directory/file${NC}"
    echo -e "${YELLOW}         Space to multi-select, 'p' to preview, '/' to search${NC}"
    echo -e "${YELLOW}         'q' to quit, 'h' for help${NC}"
    sleep 2
    
    # Run the file browser
    # Note: File browser needs to be migrated to new subcommand
    # For now, keeping old script until fully migrated
    if python3 tsv_file_browser.py --start-dir "${BASE_PATH:-$(pwd)}" \
                                   --config-dir "${CONFIG_DIR:-config}" \
                                   --output "$temp_file"; then
        
        # Check if files were selected
        if [[ -f "$temp_file" ]] && [[ -s "$temp_file" ]]; then
            local num_files=$(wc -l < "$temp_file")
            echo -e "${GREEN}Selected $num_files file(s)${NC}"
            
            # Validate against current config
            local files_array=()
            while IFS= read -r file; do
                files_array+=("$file")
            done < "$temp_file"
            
            # Run validation
            echo -e "${BLUE}Validating files against current config...${NC}"
            # Note: Browser integration needs migration
            local validation_result=$(python3 tsv_browser_integration.py \
                "${files_array[@]}" \
                --current-config "$CONFIG_FILE" \
                --config-dir "${CONFIG_DIR:-config}" \
                --json 2>/dev/null)
            
            if [[ -n "$validation_result" ]]; then
                local all_match=$(echo "$validation_result" | jq -r '.all_match_current')
                
                if [[ "$all_match" != "true" ]]; then
                    # Files don't match current config
                    echo -e "${YELLOW}Warning: Some files don't match current config${NC}"
                    
                    # Check for suggestions
                    local num_suggestions=$(echo "$validation_result" | jq '.suggestions | length')
                    
                    if [[ "$num_suggestions" -gt 0 ]]; then
                        echo -e "${BLUE}Found matching configurations:${NC}"
                        
                        # Build menu of suggested configs
                        local config_options=()
                        local config_paths=()
                        
                        while IFS= read -r line; do
                            local cfg_path=$(echo "$line" | jq -r '.config_path')
                            local cfg_name=$(echo "$line" | jq -r '.config_name')
                            local tables=$(echo "$line" | jq -r '.tables | join(", ")')
                            
                            config_options+=("$cfg_name - Tables: $tables")
                            config_paths+=("$cfg_path")
                        done < <(echo "$validation_result" | jq -c '.suggestions[]')
                        
                        config_options+=("Keep current config anyway")
                        config_options+=("Cancel")
                        
                        local choice=$(show_menu "Select Configuration" "${config_options[@]}")
                        
                        if [[ "$choice" -ge 1 ]] && [[ "$choice" -le "${#config_paths[@]}" ]]; then
                            # Switch to suggested config
                            CONFIG_FILE="${config_paths[$((choice-1))]}"
                            echo -e "${GREEN}Switched to config: $(basename "$CONFIG_FILE")${NC}"
                        elif [[ "$choice" == "$((${#config_options[@]}-1))" ]]; then
                            # Keep current config
                            echo -e "${YELLOW}Proceeding with current config${NC}"
                        else
                            # Cancel
                            rm -f "$temp_file"
                            return
                        fi
                    else
                        # No matching configs found
                        if ! confirm_action "No matching configs found. Proceed anyway?"; then
                            rm -f "$temp_file"
                            return
                        fi
                    fi
                fi
            fi
            
            # Process the selected files
            echo -e "${BLUE}Processing selected files...${NC}"
            
            # Build comma-separated list of files
            local files_list=$(tr '\n' ',' < "$temp_file" | sed 's/,$//')
            
            # Ask for quality check preference
            local qc_method=$(select_quality_check_method)
            
            if [[ "$qc_method" == "cancelled" ]]; then
                rm -f "$temp_file"
                return
            fi
            
            local extra_args=""
            case "$qc_method" in
                "file") extra_args="" ;;  # Default behavior
                "snowflake") extra_args="--validate-in-snowflake" ;;
                "skip") extra_args="--skip-qc" ;;
            esac
            
            # Start the job
            if confirm_action "Process ${num_files} file(s) with config $(basename "$CONFIG_FILE")?"; then
                # Use direct file processing function
                with_lock start_background_job "load_selected_files" \
                    process_direct_files "$files_list" "$extra_args"
            fi
        else
            echo -e "${YELLOW}No files selected${NC}"
        fi
    else
        echo -e "${YELLOW}File browser cancelled${NC}"
    fi
    
    # Cleanup
    rm -f "$temp_file"
}

# Validate data menu (runs synchronously - quick operation)
menu_validate_data() {
    # Ensure config is selected
    if ! select_config_file; then
        return 1
    fi
    
    local month=$(get_input "Validate Data" "Enter month (YYYY-MM) or leave empty for ALL data")
    
    # Set display text based on whether month is specified
    local validate_text
    local job_name
    if [[ -z "$month" ]]; then
        validate_text="Validate ALL data in tables"
        job_name="validate_all"
    else
        validate_text="Validate data for $month"
        job_name="validate_${month}"
    fi
    
    if confirm_action "$validate_text?\nUsing config: $(basename "$CONFIG_FILE")"; then
        # Ask user for execution mode
        local response=$(get_input "Execution Mode" "Show real-time progress? (Y=foreground, N=background)" "Y")
        
        # Build command with or without month parameter
        local cmd="python -m snowflake_etl --config \"$CONFIG_FILE\" validate"
        if [[ -n "$month" ]]; then
            cmd="$cmd --month \"$month\""
        fi
        
        if [[ "${response^^}" == "Y" ]]; then
            # Run in foreground with visible progress
            with_lock start_foreground_job "$job_name" bash -c "$cmd"
        else
            # Run in background
            with_lock start_background_job "$job_name" bash -c "$cmd"
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
        # Use direct delete function
        with_lock start_background_job "delete_${table}_${month}" \
            delete_month_data "$table" "$month" "--yes"
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
                    python -m snowflake_etl --config "$CONFIG_FILE" check-duplicates --table "$table" --key-columns "$key_columns" --date-start "$date_start" --date-end "$date_end"
            else
                # Run in background
                with_lock start_background_job "check_duplicates_${table}" \
                    python -m snowflake_etl --config "$CONFIG_FILE" check-duplicates --table "$table" --key-columns "$key_columns" --date-start "$date_start" --date-end "$date_end"
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
            python -m snowflake_etl --config "$CONFIG_FILE" compare $use_quick --file1 "$good_file" --file2 "$bad_file"
    fi
}

# Sample TSV file
sample_tsv_file() {
    local file_path=$(get_input "Sample TSV File" "Enter TSV file path")
    
    if [[ -z "$file_path" ]] || [[ ! -f "$file_path" ]]; then
        show_message "Error" "Invalid file path"
        return
    fi
    
    # Internal TSV sampling (replaces tsv_sampler.sh dependency)
    local sample_rows=10
    local output=""
    
    # Get file info
    local filename=$(basename "$file_path")
    local filesize=$(ls -lh "$file_path" | awk '{print $5}')
    local total_rows=$(wc -l < "$file_path")
    local total_cols=$(head -1 "$file_path" | awk -F'\t' '{print NF}')
    
    output+="TSV File Analysis\n"
    output+="================\n"
    output+="File: $filename\n"
    output+="Size: $filesize\n"
    output+="Rows: $total_rows\n"
    output+="Columns: $total_cols\n\n"
    
    # Detect pattern using our function
    local pattern=$(detect_file_pattern "$filename")
    output+="Pattern: $pattern\n"
    
    # Extract table name using our function
    local table_name=$(extract_table_name "$filename")
    output+="Table: $table_name\n\n"
    
    # Show sample rows
    output+="First $sample_rows rows:\n"
    output+="-------------------\n"
    output+=$(head -n $sample_rows "$file_path" | sed 's/\t/ | /g')
    
    # Check for potential date columns
    output+="\n\nPotential Date Columns:\n"
    local row2=$(sed -n '2p' "$file_path")
    local col_num=1
    for value in $(echo "$row2" | tr '\t' '\n'); do
        if [[ "$value" =~ ^[0-9]{8}$ ]] || [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
            output+="  Column $col_num: Possible date format detected ($value)\n"
        fi
        col_num=$((col_num + 1))
    done
    
    show_message "TSV Sample Results" "$output"
}

# Compress TSV file for cross-environment transfer
compress_tsv_file() {
    echo -e "${BLUE}Compress TSV File (No Snowflake Upload)${NC}"
    echo -e "${YELLOW}This will compress TSV files for manual transfer across environments${NC}"
    echo ""
    
    # Get file path
    local file_path=$(get_input "TSV File" "Enter TSV file path to compress")
    
    if [[ -z "$file_path" ]] || [[ ! -f "$file_path" ]]; then
        show_message "Error" "Invalid file path: $file_path"
        return
    fi
    
    # Get compression level
    local compression_level=$(get_input "Compression Level" "Enter gzip compression level (1=fastest, 9=best, 6=default)" "6")
    
    if ! [[ "$compression_level" =~ ^[1-9]$ ]]; then
        show_message "Error" "Invalid compression level. Must be 1-9."
        return
    fi
    
    # Get output directory
    local output_dir=$(get_input "Output Directory" "Enter output directory for compressed file" "$(dirname "$file_path")")
    
    if [[ ! -d "$output_dir" ]]; then
        mkdir -p "$output_dir" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            show_message "Error" "Cannot create output directory: $output_dir"
            return
        fi
    fi
    
    # Generate output filename
    local basename=$(basename "$file_path")
    local compressed_file="$output_dir/${basename}.gz"
    
    # Check if output file already exists
    if [[ -f "$compressed_file" ]]; then
        if ! confirm_action "File $compressed_file already exists. Overwrite?"; then
            return
        fi
    fi
    
    # Get file size for progress display
    local file_size=$(stat -c%s "$file_path" 2>/dev/null || stat -f%z "$file_path" 2>/dev/null)
    local file_size_mb=$((file_size / 1048576))
    
    echo -e "${BLUE}File Information:${NC}"
    echo -e "  Source: $file_path"
    echo -e "  Size: ${file_size_mb} MB"
    echo -e "  Output: $compressed_file"
    echo -e "  Compression Level: $compression_level"
    echo ""
    
    if confirm_action "Compress this file?"; then
        echo -e "${YELLOW}Compressing file...${NC}"
        
        # Use pv for progress if available, otherwise use plain gzip
        if command -v pv >/dev/null 2>&1; then
            pv -cN "Compressing" "$file_path" | gzip -$compression_level > "$compressed_file"
            local result=$?
        else
            # Use Python for compression with progress
            python3 -c "
import sys
import gzip
import os
from pathlib import Path

def compress_file(input_path, output_path, level=$compression_level):
    file_size = os.path.getsize(input_path)
    bytes_read = 0
    chunk_size = 10 * 1024 * 1024  # 10MB chunks
    
    with open(input_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb', compresslevel=level) as f_out:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
                bytes_read += len(chunk)
                progress = (bytes_read / file_size) * 100
                print(f'\\rProgress: {progress:.1f}%', end='', file=sys.stderr)
    print('\\rProgress: 100.0%', file=sys.stderr)
    print('', file=sys.stderr)

try:
    compress_file('$file_path', '$compressed_file')
    print('Compression completed successfully')
    sys.exit(0)
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
"
            local result=$?
        fi
        
        if [[ $result -eq 0 ]]; then
            # Get compressed file size
            local compressed_size=$(stat -c%s "$compressed_file" 2>/dev/null || stat -f%z "$compressed_file" 2>/dev/null)
            local compressed_size_mb=$((compressed_size / 1048576))
            local compression_ratio=$(echo "scale=1; 100 - ($compressed_size * 100 / $file_size)" | bc)
            
            echo ""
            echo -e "${GREEN}Compression completed successfully!${NC}"
            echo -e "  Original size: ${file_size_mb} MB"
            echo -e "  Compressed size: ${compressed_size_mb} MB"
            echo -e "  Compression ratio: ${compression_ratio}%"
            echo -e "  Output file: $compressed_file"
            echo ""
            echo -e "${YELLOW}This file can now be transferred to another environment and loaded using:${NC}"
            echo -e "  1. Copy the compressed file to the target environment"
            echo -e "  2. Place it in the appropriate data directory"
            echo -e "  3. Use the 'Load Data' menu to process it (it will be automatically decompressed)"
            
            show_message "Success" "File compressed successfully to:\n$compressed_file\n\nSize reduction: ${compression_ratio}%"
        else
            show_message "Error" "Compression failed. Check the file path and permissions."
            # Clean up partial file if it exists
            [[ -f "$compressed_file" ]] && rm -f "$compressed_file"
        fi
    fi
}

# Generate config
generate_config() {
    local file_path=$(get_input "Generate Config" "Enter TSV file path")
    local output_path=$(get_input "Output Path" "Enter output config path" "config/generated.json")
    
    if [[ -z "$file_path" ]] || [[ ! -f "$file_path" ]]; then
        show_message "Error" "Invalid file path"
        return
    fi
    
    # Extract probable table name from filename
    local default_table=$(extract_table_name "$file_path")
    
    # Ask for Snowflake table name to query for column headers
    local table_name=$(get_input "Snowflake Table" "Enter Snowflake table name (or press Enter to skip)" "$default_table")
    
    # Build options for config generation
    local options=""
    local manual_cols=""
    
    # Add table flag if provided
    if [[ -n "$table_name" ]]; then
        options="--table $table_name"
        
        # Use current config for Snowflake credentials if available
        if [[ -f "$CONFIG_FILE" ]]; then
            show_message "Info" "Using credentials from $CONFIG_FILE to query table $table_name"
        else
            show_message "Warning" "No config file selected. Table query will fail without credentials.\nUse 'Select Config File' first or manually specify columns."
            
            # Offer to manually specify columns
            manual_cols=$(get_input "Column Headers" "Enter comma-separated column names (or press Enter to use generic names)")
            if [[ -n "$manual_cols" ]]; then
                options="$options --headers $manual_cols"
                show_message "Info" "Using manually specified column headers"
            fi
        fi
    else
        # No table specified, ask if user wants to provide column headers manually
        manual_cols=$(get_input "Column Headers" "Enter comma-separated column names (or press Enter for generic names)")
        if [[ -n "$manual_cols" ]]; then
            options="--headers $manual_cols"
            show_message "Info" "Using manually specified column headers"
        else
            show_message "Info" "No table or headers specified. Will generate generic column names (COLUMN1, COLUMN2, etc.)"
        fi
    fi
    
    # Use the new direct config generation function
    if confirm_action "Generate config from $(basename "$file_path")?"; then
        # Call our new function
        generate_config_direct "$file_path" "$output_path" "$options"
        
        if [[ -f "$output_path" ]]; then
            show_message "Success" "Configuration generated successfully at:\n$output_path"
            save_preference "LAST_CONFIG" "$output_path"
        else
            show_message "Error" "Failed to generate configuration"
        fi
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
                python3 -m snowflake_etl --config "$CONFIG_FILE" validate-file "$file"
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
                python3 -m snowflake_etl --config "$CONFIG_FILE" check-table "$table"
        else
            # Run in background
            with_lock start_background_job "check_table_${table}" \
                python3 -m snowflake_etl --config "$CONFIG_FILE" check-table "$table"
        fi
    fi
}

# Generate comprehensive report for all tables
generate_full_table_report() {
    echo -e "${BLUE}Generate Full Table Report${NC}"
    echo -e "${YELLOW}This will analyze all tables across all configuration files${NC}"
    echo -e "${YELLOW}and generate a comprehensive report with statistics and validation.${NC}"
    echo ""
    
    # Count configs and tables
    local config_count=0
    local table_count=0
    
    for config in config/*.json; do
        [[ -f "$config" ]] || continue
        # Skip credential files
        if [[ "$config" == *"creds"* ]] || [[ "$config" == *"credentials"* ]]; then
            continue
        fi
        
        config_count=$((config_count + 1))
        local tables=$(jq -r '.files[]?.table_name // empty' "$config" 2>/dev/null | sort -u | wc -l)
        table_count=$((table_count + tables))
    done
    
    echo -e "${BLUE}Found: $config_count config file(s) with approximately $table_count table(s)${NC}"
    echo ""
    
    # Ask for options
    local filter_choice=$(get_input "Filter Options" "Apply filters? (Y/N)" "N")
    local config_filter=""
    local table_filter=""
    
    if [[ "${filter_choice^^}" == "Y" ]]; then
        config_filter=$(get_input "Config Filter" "Config file pattern (e.g., fact*.json) or leave empty")
        table_filter=$(get_input "Table Filter" "Table name pattern (e.g., FACT*) or leave empty")
    fi
    
    # Build command
    local cmd="python -m snowflake_etl --config \"$CONFIG_FILE\" report"
    
    # Add credentials file if available
    if [[ -f "snowflake_creds.json" ]]; then
        cmd="$cmd --creds snowflake_creds.json"
    elif [[ -f "config/snowflake_creds.json" ]]; then
        cmd="$cmd --creds config/snowflake_creds.json"
    fi
    
    # Add filters if specified
    if [[ -n "$config_filter" ]]; then
        cmd="$cmd --config-filter \"$config_filter\""
    fi
    if [[ -n "$table_filter" ]]; then
        cmd="$cmd --table-filter \"$table_filter\""
    fi
    
    # Confirm and run
    if confirm_action "Generate comprehensive table report?"; then
        local job_name="table_report_$(date +%Y%m%d_%H%M%S)"
        
        echo -e "${BLUE}Starting report generation...${NC}"
        echo -e "${YELLOW}This may take several minutes for large numbers of tables${NC}"
        echo -e "${YELLOW}Check Job Status menu to monitor progress and view results${NC}"
        
        # Run as background job so user can continue working
        with_lock start_background_job "$job_name" bash -c "$cmd"
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
        local output=$(python3 -m snowflake_etl --config "$CONFIG_FILE" diagnose-error 2>&1 | head -100)
        show_message "Diagnosis Results" "$output"
    else
        show_message "Error" "Log file not found: $log_file"
    fi
}

# Recovery and fix functions
fix_varchar_errors() {
    # Functionality moved to Python CLI
    show_message "Use Python CLI" "VARCHAR error recovery is now available via:\n\npython -m snowflake_etl diagnose-error --file <error_log>\n\nThis provides better error handling and recovery options.\n\nNote: recover_failed_load.sh is deprecated as of v3.0.0"
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
        
        if [[ -f "$log_file" ]]; then
            show_message "Recovery Options" "Log file found: $log_file\n\nTo diagnose errors, run:\npython -m snowflake_etl diagnose-error --file \"$log_file\"\n\nThis will analyze the log and suggest recovery steps."
        else
            show_message "Error" "Log file not found: $log_file"
        fi
    else
        # Try to find matching log files
        local logs=$(find "$LOGS_DIR" -name "*${log_pattern}*" -type f | head -5)
        if [[ -n "$logs" ]]; then
            show_message "Found Logs" "$logs\n\nRun diagnose-error with a specific file:\npython -m snowflake_etl diagnose-error --file <log_path>"
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
            local output=$(python3 -m snowflake_etl --config "$CONFIG_FILE" check-stage 2>&1 | grep -E "(Found|Total|Would)" | head -20)
            show_message "Stage Status" "$output\n\nRun 'python3 -m snowflake_etl --config $CONFIG_FILE check-stage' to clean interactively."
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
    prefs+="Log Viewer: $LOG_VIEWER\n"
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
            "Log Viewer" \
            "View Preferences" \
            "Clear State" \
            "Clean Completed Jobs")
        
        case "$choice" in
            1) set_default_config ;;
            2) set_base_path ;;
            3) configure_workers ;;
            4) toggle_colors ;;
            5) set_log_viewer ;;
            6) view_preferences ;;
            7) clear_state ;;
            8) clean_completed_jobs ;;
            0|"") break ;;
            *) show_message "Error" "Invalid option" ;;
        esac
    done
}

# Set log viewer preference
set_log_viewer() {
    local current_viewer="${LOG_VIEWER:-auto}"
    
    # Show current setting
    echo ""
    echo -e "${BOLD}Current Log Viewer: ${CYAN}$current_viewer${NC}"
    echo ""
    echo "Available viewers:"
    echo "  1) Auto (detect best available)"
    echo "  2) less (powerful pager with search)"
    echo "  3) nano (text editor - stable for problematic logs)"
    echo "  4) cat (simple output)"
    echo ""
    echo -n "Select viewer [1-4, 0 to cancel]: "
    read -n 1 choice
    echo ""
    
    case "$choice" in
        1) LOG_VIEWER="auto" ;;
        2) LOG_VIEWER="less" ;;
        3) LOG_VIEWER="nano" ;;
        4) LOG_VIEWER="cat" ;;
        0|"") return ;;
        *) 
            show_message "Error" "Invalid option"
            return
            ;;
    esac
    
    # Save preference to state file
    local viewer_pref_file="${STATE_DIR}/log_viewer.pref"
    echo "$LOG_VIEWER" > "$viewer_pref_file"
    
    show_message "Success" "Log viewer set to: $LOG_VIEWER"
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
            --no-venv)
                export SKIP_VENV="true"
                shift
                ;;
            --skip-install)
                export SKIP_INSTALL="true"
                shift
                ;;
            load)
                shift
                if [[ "${1:-}" == "--month" ]] && [[ -n "${2:-}" ]]; then
                    shift
                    # Use direct Python CLI call
                    process_month_direct "$1" "$BASE_PATH" ""
                    exit $?
                elif [[ "${1:-}" == "--file" ]] && [[ -n "${2:-}" ]]; then
                    shift
                    # Use direct Python CLI call
                    process_direct_files "$1" ""
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
                    python -m snowflake_etl --config "$CONFIG_FILE" validate --month "$1"
                    exit $?
                else
                    echo "Usage: $SCRIPT_NAME validate --month YYYY-MM"
                    exit 1
                fi
                ;;
            delete)
                shift
                if [[ "${1:-}" == "--table" ]] && [[ -n "${2:-}" ]] && [[ "${3:-}" == "--month" ]] && [[ -n "${4:-}" ]]; then
                    local table="$2"
                    local month="$4"
                    # Use direct delete function
                    delete_month_data "$table" "$month" "--yes"
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
  $SCRIPT_NAME [OPTIONS]                # Interactive menu mode
  $SCRIPT_NAME [OPTIONS] load --month YYYY-MM     # Load data for a month
  $SCRIPT_NAME [OPTIONS] load --file file.tsv     # Load specific file
  $SCRIPT_NAME [OPTIONS] validate --month YYYY-MM # Validate data
  $SCRIPT_NAME [OPTIONS] delete --table TABLE --month YYYY-MM # Delete data
  $SCRIPT_NAME [OPTIONS] status         # Show job status
  $SCRIPT_NAME [OPTIONS] clean          # Clean completed jobs
  $SCRIPT_NAME --help                   # Show this help
  $SCRIPT_NAME --version                # Show version

Options:
  --no-venv         Skip virtual environment setup (use system Python)
  --skip-install    Skip package installation (assume requirements met)
  --help, -h        Show this help message
  --version, -v     Show version information

Environment Variables:
  SKIP_VENV=true    Same as --no-venv flag
  SKIP_INSTALL=true Same as --skip-install flag

Interactive Mode:
  Launch without arguments to enter the interactive menu system.
  
Command Line Mode:
  Use direct commands for automation and scripting.

Examples:
  $SCRIPT_NAME                          # Launch interactive menu
  $SCRIPT_NAME --no-venv load --month 2024-01  # Load without venv
  $SCRIPT_NAME --skip-install status    # Check status, skip installs
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
            "Quick Load          - Common loading tasks" \
            "Snowflake Operations - Load/Validate/Delete" \
            "File Tools        - Analyze/Compare/Generate" \
            "Recovery & Fix    - Error recovery tools" \
            "---" \
            "Job Status        - Monitor operations" \
            "Settings          - Configure defaults")
        
        case "$choice" in
            1) menu_quick_load ;;
            2) menu_snowflake_operations ;;
            3) menu_file_tools ;;
            4) menu_recovery ;;
            5) show_job_status ;;  # Now has interactive menu
            6) menu_settings ;;
            0|"") 
                echo -e "${GREEN}Thank you for using Snowflake ETL Manager!${NC}"
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
    # Parse skip flags FIRST before any dependency checks
    # This needs to happen before check_dependencies is called
    for arg in "$@"; do
        case "$arg" in
            --no-venv)
                export SKIP_VENV="true"
                ;;
            --skip-install)
                export SKIP_INSTALL="true"
                ;;
        esac
    done
    
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
    fi
    
    # Interactive mode
    # Set up global trap for interactive mode to prevent accidental exits
    # Using a simple inline trap is more reliable
    trap 'echo ""; echo -e "${YELLOW}Use the menu option '"'"'0'"'"' to exit properly${NC}"' SIGINT
    
    # Clear screen for interactive mode
    if [[ "$USE_DIALOG" == true ]]; then
        clear
    fi
    
    # Show welcome message
    if [[ "$USE_DIALOG" == false ]]; then
        echo ""
        echo -e "${BOLD}${CYAN}Welcome to Snowflake ETL Pipeline Manager${NC}"
        echo -e "${YELLOW}Version $VERSION - Fully Consolidated${NC}"
        echo ""
    fi
    
    # Enter main menu
    main_menu
}

# Run main function
main "$@"