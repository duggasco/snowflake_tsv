#!/bin/bash

# Enhanced TSV to Snowflake Loader Script with Clean Progress Display
# Supports parallel processing with progress monitoring
# Shows clean progress bars instead of raw output

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Default values
CONFIG_FILE="config/config.json"
BASE_PATH=""
MONTH=""
MONTHS_LIST=""
BATCH_MODE=""
PARALLEL_JOBS=1
CONTINUE_ON_ERROR=""
DRY_RUN=""
MAX_WORKERS=""
SKIP_QC=""
ANALYZE_ONLY=""
QUIET_MODE="yes"  # Default to quiet mode

# Progress tracking files (in /tmp for safety)
PROGRESS_DIR="/tmp/tsv_loader_progress_$$"
mkdir -p "${PROGRESS_DIR}"

# Cleanup on exit
trap cleanup EXIT

cleanup() {
    rm -rf "${PROGRESS_DIR}"
    # Kill any remaining background monitoring processes
    jobs -p | xargs -r kill 2>/dev/null
}

# Function to convert month format
convert_month_format() {
    local input=$1
    
    # Check if it's already in YYYY-MM format
    if [[ $input =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
        echo "$input"
        return 0
    fi
    
    # Check if it's in MMYYYY format
    if [[ $input =~ ^[0-9]{6}$ ]]; then
        local month="${input:0:2}"
        local year="${input:2:4}"
        echo "${year}-${month}"
        return 0
    fi
    
    # Invalid format
    return 1
}

# Function to monitor a single process
monitor_process() {
    local pid=$1
    local month=$2
    local log_file=$3
    local progress_file="${PROGRESS_DIR}/${month}.progress"
    local status_file="${PROGRESS_DIR}/${month}.status"
    
    # Initialize progress
    echo "0|Initializing..." > "${progress_file}"
    echo "running" > "${status_file}"
    
    # Monitor the log file for progress indicators
    while kill -0 $pid 2>/dev/null; do
        if [ -f "${log_file}" ]; then
            # Extract progress information from log
            local last_line=$(tail -n 100 "${log_file}" 2>/dev/null | grep -E "Processing|Loading|Analyzing|Compressing|Uploading|COPY|complete|failed|ERROR" | tail -1)
            
            if [ -n "${last_line}" ]; then
                # Parse different progress indicators
                if echo "${last_line}" | grep -q "Analyzing.*files"; then
                    echo "10|Analyzing files..." > "${progress_file}"
                elif echo "${last_line}" | grep -q "Quality checks"; then
                    echo "20|Running quality checks..." > "${progress_file}"
                elif echo "${last_line}" | grep -q "Compressing"; then
                    echo "40|Compressing file..." > "${progress_file}"
                elif echo "${last_line}" | grep -q "PUT.*stage"; then
                    echo "60|Uploading to stage..." > "${progress_file}"
                elif echo "${last_line}" | grep -q "COPY INTO"; then
                    echo "80|Loading to Snowflake..." > "${progress_file}"
                elif echo "${last_line}" | grep -q "Successfully loaded"; then
                    echo "100|Complete" > "${progress_file}"
                elif echo "${last_line}" | grep -q -i "error\|failed"; then
                    echo "0|ERROR" > "${progress_file}"
                    echo "failed" > "${status_file}"
                fi
            fi
        fi
        sleep 2
    done
    
    # Check final status
    wait $pid
    local exit_code=$?
    
    if [ ${exit_code} -eq 0 ]; then
        echo "100|Complete" > "${progress_file}"
        echo "success" > "${status_file}"
    else
        echo "0|Failed (exit code: ${exit_code})" > "${progress_file}"
        echo "failed" > "${status_file}"
    fi
}

# Function to display progress bars
display_progress() {
    local months=("$@")
    local all_done=false
    
    # Clear screen and hide cursor
    tput clear
    tput civis  # Hide cursor
    
    while [ "${all_done}" != "true" ]; do
        # Move cursor to top
        tput cup 0 0
        
        # Header
        echo -e "${BOLD}${CYAN}╔════════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${BOLD}${CYAN}║                    TSV Loader - Parallel Processing                       ║${NC}"
        echo -e "${BOLD}${CYAN}╚════════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        all_done=true
        local active_count=0
        local complete_count=0
        local failed_count=0
        
        for month in "${months[@]}"; do
            local progress_file="${PROGRESS_DIR}/${month}.progress"
            local status_file="${PROGRESS_DIR}/${month}.status"
            local log_file="logs/run_${month}_*.log"
            
            if [ -f "${progress_file}" ]; then
                IFS='|' read -r percent message < "${progress_file}"
                local status=$(cat "${status_file}" 2>/dev/null || echo "pending")
                
                # Format month display (fixed width)
                printf "${BOLD}%-10s${NC} " "${month}:"
                
                # Draw progress bar (50 chars wide)
                local bar_width=50
                local filled=$((percent * bar_width / 100))
                
                # Choose color based on status
                local bar_color="${BLUE}"
                local status_icon="⟳"
                if [ "${status}" = "success" ]; then
                    bar_color="${GREEN}"
                    status_icon="✓"
                    ((complete_count++))
                elif [ "${status}" = "failed" ]; then
                    bar_color="${RED}"
                    status_icon="✗"
                    ((failed_count++))
                elif [ "${status}" = "running" ]; then
                    ((active_count++))
                    all_done=false
                fi
                
                # Draw the bar
                echo -ne "${bar_color}["
                for ((i=0; i<bar_width; i++)); do
                    if [ $i -lt $filled ]; then
                        echo -ne "█"
                    else
                        echo -ne "░"
                    fi
                done
                echo -ne "]${NC} "
                
                # Status and message
                printf "%3d%% %s %-20s\n" "${percent}" "${status_icon}" "${message:0:20}"
            else
                # Not started yet
                printf "${BOLD}%-10s${NC} ${YELLOW}[%50s] %3d%% ◯ %-20s\n" "${month}:" "$(printf '%.0s░' {1..50})" "0" "Pending..."
                all_done=false
            fi
        done
        
        # Summary line
        echo ""
        echo -e "${BOLD}Summary:${NC} Active: ${active_count} | Complete: ${complete_count} | Failed: ${failed_count} | Total: ${#months[@]}"
        echo ""
        echo -e "${YELLOW}Press Ctrl+C to abort. Logs are saved in ./logs/${NC}"
        
        # Don't refresh too frequently
        sleep 1
    done
    
    # Show cursor again
    tput cnorm
}

# Function to process a month (silent version)
process_month_silent() {
    local month_dir=$1
    local workers_for_month=$2
    local month_formatted=$(convert_month_format "$month_dir")
    
    if [ -z "$month_formatted" ]; then
        echo "ERROR|Invalid month format: $month_dir" > "${PROGRESS_DIR}/${month_dir}.progress"
        echo "failed" > "${PROGRESS_DIR}/${month_dir}.status"
        return 1
    fi
    
    # Build the Python command
    local month_base_path="${BASE_PATH%/}/${month_dir}"
    local cmd="python3 tsv_loader.py"
    cmd="${cmd} --config ${CONFIG_FILE}"
    cmd="${cmd} --base-path ${month_base_path}"
    cmd="${cmd} --month ${month_formatted}"
    
    # Add optional arguments
    if [ -n "${workers_for_month}" ]; then
        cmd="${cmd} --max-workers ${workers_for_month}"
    elif [ -n "${MAX_WORKERS}" ]; then
        cmd="${cmd} --max-workers ${MAX_WORKERS}"
    fi
    
    if [ -n "${SKIP_QC}" ]; then
        cmd="${cmd} ${SKIP_QC}"
    fi
    
    if [ -n "${ANALYZE_ONLY}" ]; then
        cmd="${cmd} ${ANALYZE_ONLY}"
    fi
    
    # Always add --yes flag for non-interactive mode
    cmd="${cmd} --yes"
    
    # Create a log file name with timestamp and month
    local log_file="logs/run_${month_dir}_$(date +%Y%m%d_%H%M%S).log"
    
    if [ -n "${DRY_RUN}" ]; then
        echo "100|Dry run complete" > "${PROGRESS_DIR}/${month_dir}.progress"
        echo "success" > "${PROGRESS_DIR}/${month_dir}.status"
        return 0
    fi
    
    # Execute the command silently, redirecting all output to log file
    ${cmd} > "${log_file}" 2>&1 &
    local pid=$!
    
    # Start monitoring in background
    monitor_process $pid "${month_dir}" "${log_file}" &
    
    # Return the main process PID
    echo $pid
}

# Function to check prerequisites
check_prerequisites() {
    echo -e "${BLUE}Checking prerequisites...${NC}"
 
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}ERROR: Python 3 is not installed${NC}"
        exit 1
    fi
 
    # Check Python version
    python_version=$(python3 --version 2>&1 | awk '{print $2}')
    echo -e "  Python version: ${python_version}"
 
    # Check if required Python packages are installed
    missing_packages=""
 
    for package in snowflake-connector-python pandas numpy; do
        if ! python3 -c "import ${package//-/_}" 2>/dev/null; then
            missing_packages="${missing_packages} ${package}"
        fi
    done
 
    if [ -n "${missing_packages}" ]; then
        echo -e "${YELLOW}Warning: Missing Python packages:${missing_packages}${NC}"
        echo -e "${YELLOW}Install with: pip install${missing_packages}${NC}"
    fi
 
    # Check if config file exists
    if [ ! -f "${CONFIG_FILE}" ]; then
        echo -e "${YELLOW}Warning: Config file not found: ${CONFIG_FILE}${NC}"
    fi
 
    # Check if logs directory exists
    if [ ! -d "logs" ]; then
        echo -e "${BLUE}Creating logs directory...${NC}"
        mkdir -p logs
    fi
 
    echo -e "${GREEN}Prerequisites check complete${NC}\n"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --base-path)
            BASE_PATH="$2"
            shift 2
            ;;
        --month)
            MONTH="$2"
            shift 2
            ;;
        --months)
            MONTHS_LIST="$2"
            shift 2
            ;;
        --batch)
            BATCH_MODE="yes"
            shift
            ;;
        --parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --continue-on-error)
            CONTINUE_ON_ERROR="yes"
            shift
            ;;
        --dry-run)
            DRY_RUN="yes"
            shift
            ;;
        --max-workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        --skip-qc)
            SKIP_QC="--skip-qc"
            shift
            ;;
        --analyze-only)
            ANALYZE_ONLY="--analyze-only"
            shift
            ;;
        --verbose)
            QUIET_MODE=""
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --config FILE          Config file path (default: config/config.json)"
            echo "  --base-path PATH       Base directory containing month subdirectories"
            echo "  --month MONTH          Single month to process (YYYY-MM or MMYYYY format)"
            echo "  --months LIST          Comma-separated list of months"
            echo "  --batch                Process all months in base-path"
            echo "  --parallel N           Number of parallel jobs (default: 1)"
            echo "  --continue-on-error    Continue processing even if a month fails"
            echo "  --dry-run              Show what would be executed without running"
            echo "  --max-workers N        Maximum parallel workers for Python processing"
            echo "  --skip-qc              Skip quality checks"
            echo "  --analyze-only         Only analyze files without loading"
            echo "  --verbose              Show full output (default: quiet with progress bars)"
            echo "  --help                 Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check prerequisites
check_prerequisites

# Validate required arguments
if [ -z "${BASE_PATH}" ]; then
    echo -e "${RED}ERROR: --base-path is required${NC}"
    exit 1
fi

# Determine which months to process
months_to_process=()

if [ -n "${MONTH}" ]; then
    # Single month specified
    # Extract just the month directory name from the path
    month_dir=$(basename "${MONTH}")
    # If MONTH is in YYYY-MM format, convert to MMYYYY for directory
    if [[ ${month_dir} =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
        year="${month_dir:0:4}"
        month="${month_dir:5:2}"
        month_dir="${month}${year}"
    fi
    months_to_process+=("${month_dir}")
elif [ -n "${MONTHS_LIST}" ]; then
    # Multiple months specified
    IFS=',' read -ra MONTHS_ARRAY <<< "${MONTHS_LIST}"
    for m in "${MONTHS_ARRAY[@]}"; do
        # Trim whitespace
        m=$(echo "$m" | xargs)
        # Extract just the month directory name
        month_dir=$(basename "$m")
        # If in YYYY-MM format, convert to MMYYYY
        if [[ ${month_dir} =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
            year="${month_dir:0:4}"
            month="${month_dir:5:2}"
            month_dir="${month}${year}"
        fi
        months_to_process+=("${month_dir}")
    done
elif [ -n "${BATCH_MODE}" ]; then
    # Process all subdirectories in base path
    for dir in "${BASE_PATH}"/*/; do
        if [ -d "$dir" ]; then
            month_dir=$(basename "$dir")
            # Check if it looks like a month directory (MMYYYY format)
            if [[ ${month_dir} =~ ^[0-9]{6}$ ]]; then
                months_to_process+=("${month_dir}")
            fi
        fi
    done
    
    # Sort months
    IFS=$'\n' months_to_process=($(sort <<<"${months_to_process[*]}"))
    unset IFS
else
    echo -e "${RED}ERROR: Specify --month, --months, or --batch${NC}"
    exit 1
fi

# Validate months exist
for month_dir in "${months_to_process[@]}"; do
    month_path="${BASE_PATH}/${month_dir}"
    if [ ! -d "${month_path}" ]; then
        echo -e "${RED}ERROR: Directory not found: ${month_path}${NC}"
        exit 1
    fi
done

# Display configuration
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}TSV to Snowflake Loader Configuration${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Config File:       ${CONFIG_FILE}"
echo -e "Base Path:         ${BASE_PATH}"
echo -e "Months to Process: ${months_to_process[*]}"
echo -e "Parallel Jobs:     ${PARALLEL_JOBS}"
echo -e "Display Mode:      $([ -n "${QUIET_MODE}" ] && echo "Progress Bars" || echo "Verbose")"
echo -e "${GREEN}========================================${NC}\n"

# Calculate workers per job if using parallel processing
workers_per_job=""
if [ ${PARALLEL_JOBS} -gt 1 ] && [ -n "${MAX_WORKERS}" ]; then
    workers_per_job=$((MAX_WORKERS / PARALLEL_JOBS))
    echo -e "${BLUE}Distributing ${MAX_WORKERS} workers across ${PARALLEL_JOBS} parallel jobs: ${workers_per_job} workers per month${NC}\n"
fi

# Process months
total_months=${#months_to_process[@]}
declare -A job_pids  # Track PIDs

# Start time for total processing
start_time=$(date +%s)

# Start all jobs
echo -e "${BLUE}Starting ${total_months} processing jobs...${NC}\n"

for month in "${months_to_process[@]}"; do
    # Wait for available job slot if needed
    while [ $(jobs -r | wc -l) -ge ${PARALLEL_JOBS} ]; do
        sleep 0.5
    done
    
    # Start the job and get its PID
    pid=$(process_month_silent "${month}" "${workers_per_job}")
    job_pids["${month}"]=$pid
done

# Display progress bars
if [ -n "${QUIET_MODE}" ]; then
    sleep 2  # Give processes time to start
    display_progress "${months_to_process[@]}"
else
    # Verbose mode - tail all log files
    echo -e "${YELLOW}Verbose mode - showing all outputs:${NC}"
    for month in "${months_to_process[@]}"; do
        log_file="logs/run_${month}_*.log"
        tail -f ${log_file} 2>/dev/null &
    done
    wait
fi

# Wait for all jobs to complete
for month in "${!job_pids[@]}"; do
    wait ${job_pids[$month]}
done

# Calculate results
successful_months=0
failed_months=0
failed_list=()

for month in "${months_to_process[@]}"; do
    status_file="${PROGRESS_DIR}/${month}.status"
    if [ -f "${status_file}" ]; then
        status=$(cat "${status_file}")
        if [ "${status}" = "success" ]; then
            ((successful_months++))
        else
            ((failed_months++))
            failed_list+=("${month}")
        fi
    fi
done

# Calculate total time
end_time=$(date +%s)
total_time=$((end_time - start_time))
hours=$((total_time / 3600))
minutes=$(((total_time % 3600) / 60))
seconds=$((total_time % 60))

# Final summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}BATCH PROCESSING SUMMARY${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Total Months:      ${total_months}"
echo -e "Successful:        ${successful_months} $([ ${successful_months} -gt 0 ] && echo "✓" || echo "")"
echo -e "Failed:            ${failed_months} $([ ${failed_months} -gt 0 ] && echo "✗" || echo "")"

if [ ${#failed_list[@]} -gt 0 ]; then
    echo -e "Failed Months:     ${failed_list[*]}"
fi

echo -e "Total Time:        ${hours}h ${minutes}m ${seconds}s"
echo -e "${GREEN}========================================${NC}"

# Show log file locations
echo -e "\n${BLUE}Log files saved in ./logs/${NC}"
for month in "${months_to_process[@]}"; do
    log_files=(logs/run_${month}_*.log)
    if [ -e "${log_files[0]}" ]; then
        echo -e "  ${month}: ${log_files[0]}"
    fi
done

# Set appropriate exit code
if [ ${failed_months} -gt 0 ]; then
    exit 1
else
    exit 0
fi