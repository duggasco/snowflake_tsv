#!/bin/bash

# run_loader.sh - Enhanced TSV to Snowflake loader runner script with multi-month support
# 
# This script now uses the new snowflake_etl package (v3.0.0)
# It serves as a wrapper for the unified snowflake_etl CLI

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Default values
CONFIG_FILE="config/config.json"
BASE_PATH="./data"
MONTH=$(date +%Y-%m)  # Default to current month
MAX_WORKERS=""  # Empty means auto-detect
SKIP_QC=""
ANALYZE_ONLY=""
CHECK_SYSTEM=""
BATCH_MODE=""
MONTHS_LIST=""
CONTINUE_ON_ERROR=""
DRY_RUN=""
QUIET_MODE=""
VALIDATE_IN_SNOWFLAKE=""
VALIDATE_ONLY=""
PARALLEL_JOBS=1  # Default to sequential processing
DIRECT_FILES=""  # Direct TSV file paths
INTERACTIVE_MODE=""  # Allow interactive prompts if explicitly requested

 

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Load TSV files to Snowflake with quality checks and progress tracking"
    echo ""
    echo "Options:"
    echo "  --config FILE       Configuration file (default: config/config.json)"
    echo "  --base-path PATH    Base path for TSV files (default: ./data)"
    echo "  --direct-file FILE  Process specific TSV file(s) directly (comma-separated)"
    echo "  --month YYYY-MM     Single month to process (default: current month)"
    echo "  --months LIST       Comma-separated list of months (e.g., 092022,102022,112022)"
    echo "  --batch              Process all months found in base-path"
    echo "  --parallel N        Process N months in parallel (default: 1)"
    echo "  --continue-on-error Continue processing if a month fails"
    echo "  --dry-run           Show what would be processed without executing"
    echo "  --max-workers N     Maximum parallel workers (default: auto-detect)"
    echo "                      With --parallel, workers are divided among parallel jobs"
    echo "  --skip-qc           Skip quality checks (not recommended)"
    echo "  --validate-in-snowflake  Skip file QC and validate after loading in Snowflake"
    echo "  --validate-only     Only validate existing data in Snowflake (no loading)"
    echo "  --analyze-only      Only analyze files and show time estimates"
    echo "  --quiet             Suppress output, log to files only (useful for parallel)"
    echo "  --interactive       Allow interactive prompts (default: auto-yes)"
    echo "  --check-system      Check system capabilities and exit"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Process single month"
    echo "  $0 --month 2024-09 --base-path ./data"
    echo ""
    echo "  # Process specific TSV files directly"
    echo "  $0 --direct-file /path/to/file1.tsv,/path/to/file2.tsv"
    echo ""
    echo "  # Process specific months with skip-qc"
    echo "  $0 --months 092022,102022,112022 --skip-qc"
    echo ""
    echo "  # Process with Snowflake validation instead of file QC"
    echo "  $0 --month 2024-09 --validate-in-snowflake"
    echo ""
    echo "  # Only validate existing Snowflake data"
    echo "  $0 --month 2024-09 --validate-only"
    echo ""
    echo "  # Process all months in batch mode"
    echo "  $0 --batch --skip-qc --continue-on-error"
    echo ""
    echo "  # Process months in parallel (4 at a time)"
    echo "  $0 --batch --skip-qc --parallel 4 --max-workers 60"
    echo ""
    echo "  # Dry run to see what would be processed"
    echo "  $0 --batch --dry-run"
    echo ""
    exit 0
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

# Function to process direct TSV files
process_direct_files() {
    local files="$1"
    local workers_for_job=$2
    
    echo -e "\n${CYAN}========================================${NC}"
    echo -e "${CYAN}Processing Direct TSV Files${NC}"
    echo -e "${CYAN}========================================${NC}"
    
    # Build the Python command for direct files
    # Note: --config is a global flag and must come BEFORE the subcommand
    local cmd="python3 -m snowflake_etl --config ${CONFIG_FILE} load"
    
    # Convert comma-separated files to array
    IFS=',' read -ra file_array <<< "$files"
    
    # For direct files, we need to set base-path to the directory containing the files
    # and ensure the config matches the file pattern
    if [ ${#file_array[@]} -gt 0 ]; then
        # Get the directory of the first file
        local first_file="${file_array[0]}"
        local file_dir=$(dirname "$first_file")
        local file_name=$(basename "$first_file")
        
        echo -e "${BLUE}Processing file: ${first_file}${NC}"
        echo -e "${BLUE}Directory: ${file_dir}${NC}"
        echo -e "${BLUE}Filename: ${file_name}${NC}"
        
        # Set base-path to the directory containing the file
        cmd="${cmd} --base-path ${file_dir}"
        
        # Note for user about config matching
        echo -e "${YELLOW}Note: Ensure your config.json file_pattern matches the filename pattern${NC}"
        echo -e "${YELLOW}      File: ${file_name}${NC}"
    fi
    
    # Use the month from the first file if not specified
    if [ -z "${MONTH}" ] && [ -z "${VALIDATE_ONLY}" ]; then
        # Try to extract month from filename
        for file in ${file_list}; do
            if [[ $(basename "$file") =~ ([0-9]{4})-([0-9]{2}) ]]; then
                MONTH="${BASH_REMATCH[1]}-${BASH_REMATCH[2]}"
                echo -e "${BLUE}Detected month from filename: ${MONTH}${NC}"
                break
            elif [[ $(basename "$file") =~ ([0-9]{8})-([0-9]{8}) ]]; then
                # Extract month from date range (use start date)
                local start_date="${BASH_REMATCH[1]}"
                MONTH="${start_date:0:4}-${start_date:4:2}"
                echo -e "${BLUE}Detected month from date range: ${MONTH}${NC}"
                break
            fi
        done
    fi
    
    if [ -n "${MONTH}" ]; then
        cmd="${cmd} --month ${MONTH}"
    fi
    
    # Add optional arguments
    if [ -n "${workers_for_job}" ]; then
        cmd="${cmd} --max-workers ${workers_for_job}"
        echo -e "${BLUE}Workers allocated: ${workers_for_job}${NC}"
    elif [ -n "${MAX_WORKERS}" ]; then
        cmd="${cmd} --max-workers ${MAX_WORKERS}"
    fi
    
    if [ -n "${SKIP_QC}" ]; then
        cmd="${cmd} ${SKIP_QC}"
    fi
    
    if [ -n "${VALIDATE_IN_SNOWFLAKE}" ]; then
        cmd="${cmd} ${VALIDATE_IN_SNOWFLAKE}"
        # Save validation results to a JSON file
        local validation_file="logs/validation_direct_$(date +%Y%m%d_%H%M%S).json"
        cmd="${cmd} --validation-output ${validation_file}"
    fi
    
    if [ -n "${VALIDATE_ONLY}" ]; then
        cmd="${cmd} ${VALIDATE_ONLY}"
        # Save validation results to a JSON file
        local validation_file="logs/validation_direct_$(date +%Y%m%d_%H%M%S).json"
        cmd="${cmd} --validation-output ${validation_file}"
    fi
    
    if [ -n "${ANALYZE_ONLY}" ]; then
        cmd="${cmd} ${ANALYZE_ONLY}"
    fi
    
    # Add --yes flag for automatic processing unless interactive mode is requested
    if [ -z "${INTERACTIVE_MODE}" ]; then
        cmd="${cmd} --yes"
    fi
    
    # Create a log file name with timestamp
    local log_file="logs/run_direct_$(date +%Y%m%d_%H%M%S).log"
    
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "${BLUE}Command: ${cmd}${NC}"
        echo -e "${BLUE}Log file: ${log_file}${NC}"
    fi
    
    if [ -n "${DRY_RUN}" ]; then
        echo -e "${YELLOW}[DRY RUN] Would execute above command${NC}"
        return 0
    fi
    
    # Execute the command
    if [ -n "${QUIET_MODE}" ]; then
        # Quiet mode - pass --quiet to Python, redirect stdout only (keep stderr for progress bars)
        cmd="${cmd} --quiet"
        # Redirect stdout to log, stderr (progress bars) stays on terminal
        exec 3>&1
        ${cmd} > "${log_file}" 2> >(tee -a "${log_file}" >&2)
        local exit_code=$?
        exec 3>&-
    else
        # Verbose mode - show output and save to log
        ${cmd} 2>&1 | tee "${log_file}"
        local exit_code=${PIPESTATUS[0]}
    fi
    
    if [ ${exit_code} -eq 0 ]; then
        echo -e "${GREEN}[OK] Direct files processed successfully${NC}"
        return 0
    else
        echo -e "${RED}[FAILED] Direct files failed with exit code: ${exit_code}${NC}"
        return ${exit_code}
    fi
}

# Function to process a single month
process_month() {
    local month_dir=$1
    local workers_for_month=$2  # New parameter for worker allocation
    local job_position=$3  # Position for parallel progress bars
    local month_formatted=$(convert_month_format "$month_dir")
    
    if [ -z "$month_formatted" ]; then
        echo -e "${RED}ERROR: Invalid month format: $month_dir (expected YYYY-MM or MMYYYY)${NC}"
        return 1
    fi
    
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "\n${CYAN}========================================${NC}"
        echo -e "${CYAN}Processing Month: $month_dir (${month_formatted})${NC}"
        echo -e "${CYAN}========================================${NC}"
    fi
    
    # Build the Python command
    # Note: --config is a global flag and must come BEFORE the subcommand
    local cmd="python3 -m snowflake_etl --config ${CONFIG_FILE} load"
    
    # For validate-only mode, we don't need base-path
    if [ -z "${VALIDATE_ONLY}" ]; then
        # Append the month directory to the base path
        local month_base_path="${BASE_PATH%/}/${month_dir}"
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${BLUE}Data Path: ${month_base_path}${NC}"
        fi
        cmd="${cmd} --base-path ${month_base_path}"
    fi
    
    cmd="${cmd} --month ${month_formatted}"
    
    # Add optional arguments
    # Use provided workers_for_month if set, otherwise use MAX_WORKERS
    if [ -n "${workers_for_month}" ]; then
        cmd="${cmd} --max-workers ${workers_for_month}"
        echo -e "${BLUE}Workers allocated: ${workers_for_month}${NC}"
    elif [ -n "${MAX_WORKERS}" ]; then
        cmd="${cmd} --max-workers ${MAX_WORKERS}"
    fi
    
    if [ -n "${SKIP_QC}" ]; then
        cmd="${cmd} ${SKIP_QC}"
    fi
    
    if [ -n "${VALIDATE_IN_SNOWFLAKE}" ]; then
        cmd="${cmd} ${VALIDATE_IN_SNOWFLAKE}"
        # Save validation results to a JSON file for aggregation
        local validation_file="logs/validation_${month_dir}_$(date +%Y%m%d_%H%M%S).json"
        cmd="${cmd} --validation-output ${validation_file}"
    fi
    
    if [ -n "${VALIDATE_ONLY}" ]; then
        cmd="${cmd} ${VALIDATE_ONLY}"
        # Save validation results to a JSON file for aggregation
        local validation_file="logs/validation_${month_dir}_$(date +%Y%m%d_%H%M%S).json"
        cmd="${cmd} --validation-output ${validation_file}"
    fi
    
    if [ -n "${ANALYZE_ONLY}" ]; then
        cmd="${cmd} ${ANALYZE_ONLY}"
    fi
    
    # Add --yes flag for automatic processing unless interactive mode is requested
    # By default, always add --yes when running from script to prevent interactive prompts
    # This is essential for both parallel and single-job modes
    if [ -z "${INTERACTIVE_MODE}" ]; then
        cmd="${cmd} --yes"
    fi
    
    # Create a log file name with timestamp and month
    local log_file="logs/run_${month_dir}_$(date +%Y%m%d_%H%M%S).log"
    
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "${BLUE}Command: ${cmd}${NC}"
        echo -e "${BLUE}Log file: ${log_file}${NC}"
    fi
    
    if [ -n "${DRY_RUN}" ]; then
        echo -e "${YELLOW}[DRY RUN] Would execute above command${NC}"
        return 0
    fi
    
    # Execute the command - redirect to log file, optionally show output
    if [ -n "${QUIET_MODE}" ]; then
        # Quiet mode - pass --quiet to Python, redirect stdout only (keep stderr for progress bars)
        # Append --quiet flag to the command
        cmd="${cmd} --quiet"
        # Redirect stdout to log, stderr (progress bars) stays on terminal
        # Also capture stderr to log file using process substitution
        exec 3>&1
        TSV_JOB_POSITION="${job_position}" ${cmd} > "${log_file}" 2> >(tee -a "${log_file}" >&2)
        local exit_code=$?
        exec 3>&-
    else
        # Verbose mode - show output and save to log
        TSV_JOB_POSITION="${job_position}" ${cmd} 2>&1 | tee "${log_file}"
        # Capture exit code
        local exit_code=${PIPESTATUS[0]}
    fi
    
    if [ ${exit_code} -eq 0 ]; then
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${GREEN}[OK] Month $month_dir processed successfully${NC}"
        fi
        return 0
    else
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${RED}[FAILED] Month $month_dir failed with exit code: ${exit_code}${NC}"
        fi
        return ${exit_code}
    fi
}

# Function to check prerequisites
check_prerequisites() {
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "${BLUE}Checking prerequisites...${NC}"
    fi
 
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}ERROR: Python 3 is not installed${NC}"
        exit 1
    fi
 
    # Check Python version
    python_version=$(python3 --version 2>&1 | awk '{print $2}')
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "  Python version: ${python_version}"
    fi
 
    # Check if required Python packages are installed
    missing_packages=""
 
    for package in snowflake-connector-python pandas numpy; do
        if ! python3 -c "import ${package//-/_}" 2>/dev/null; then
            missing_packages="${missing_packages} ${package}"
        fi
    done
 
    if [ -n "${missing_packages}" ]; then
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${YELLOW}Warning: Missing Python packages:${missing_packages}${NC}"
            echo -e "${YELLOW}Install with: pip install${missing_packages}${NC}"
        fi
    fi
 
    # Check if config file exists
    if [ ! -f "${CONFIG_FILE}" ]; then
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${YELLOW}Warning: Config file not found: ${CONFIG_FILE}${NC}"
        fi
    fi
 
    # Check if logs directory exists
    if [ ! -d "logs" ]; then
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${BLUE}Creating logs directory...${NC}"
        fi
        mkdir -p logs
    fi
 
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "${GREEN}Prerequisites check complete${NC}\n"
    fi
}

 

# Script now properly integrated with v3.0.0 package

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
        --direct-file)
            DIRECT_FILES="$2"
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
        --validate-in-snowflake)
            VALIDATE_IN_SNOWFLAKE="--validate-in-snowflake"
            shift
            ;;
        --validate-only)
            VALIDATE_ONLY="--validate-only"
            shift
            ;;
        --analyze-only)
            ANALYZE_ONLY="--analyze-only"
            shift
            ;;
        --quiet)
            QUIET_MODE="yes"
            shift
            ;;
        --interactive)
            INTERACTIVE_MODE="yes"
            shift
            ;;
        --check-system)
            CHECK_SYSTEM="--check-system"
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

 

# If check-system flag is set, just run that and exit
if [ -n "${CHECK_SYSTEM}" ]; then
    echo -e "${GREEN}Running system capabilities check...${NC}\n"
    # Check system capabilities (note: this functionality may need to be reimplemented)
    echo -e "${YELLOW}System check not yet implemented in new package${NC}"
    return 0
    exit $?
fi

# Check prerequisites
check_prerequisites

# Handle direct file processing
if [ -n "${DIRECT_FILES}" ]; then
    echo -e "${MAGENTA}Direct file mode: Processing specified TSV files${NC}"
    
    # Process the direct files
    process_direct_files "${DIRECT_FILES}" "${MAX_WORKERS}"
    exit_code=$?
    
    if [ ${exit_code} -ne 0 ] && [ -z "${CONTINUE_ON_ERROR}" ]; then
        echo -e "${RED}Processing failed. Exiting.${NC}"
        exit ${exit_code}
    fi
    
    echo -e "${GREEN}Direct file processing complete${NC}"
    exit ${exit_code}
fi

# Determine which months to process
declare -a months_to_process=()

# Track job position for parallel progress bars
job_position_counter=0

if [ -n "${BATCH_MODE}" ]; then
    # Batch mode: find all month directories
    echo -e "${MAGENTA}Batch mode: Finding all month directories...${NC}"
    months_array=($(find_month_directories "${BASE_PATH}"))
    
    if [ ${#months_array[@]} -eq 0 ]; then
        echo -e "${RED}ERROR: No month directories found in ${BASE_PATH}${NC}"
        echo -e "${YELLOW}Expected format: MMYYYY (e.g., 092022, 102022)${NC}"
        exit 1
    fi
    
    months_to_process=("${months_array[@]}")
    echo -e "${GREEN}Found ${#months_to_process[@]} month(s): ${months_to_process[*]}${NC}"
    
elif [ -n "${MONTHS_LIST}" ]; then
    # Specific months provided
    IFS=',' read -ra months_to_process <<< "${MONTHS_LIST}"
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "${MAGENTA}Processing specific months: ${months_to_process[*]}${NC}"
    fi
    
else
    # Single month mode (backward compatibility)
    if [ -n "${MONTH}" ]; then
        # Convert YYYY-MM to MMYYYY for consistency
        if [[ $MONTH =~ ^([0-9]{4})-([0-9]{2})$ ]]; then
            single_month="${BASH_REMATCH[2]}${BASH_REMATCH[1]}"
            months_to_process=("$single_month")
        else
            echo -e "${RED}ERROR: Invalid month format: $MONTH (expected YYYY-MM)${NC}"
            exit 1
        fi
    fi
fi

# Display configuration (unless in quiet mode)
if [ -z "${QUIET_MODE}" ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}TSV to Snowflake Loader Configuration${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Config File:       ${CONFIG_FILE}"
    echo -e "Base Path:         ${BASE_PATH}"

    if [ ${#months_to_process[@]} -gt 1 ]; then
        echo -e "Months to Process: ${#months_to_process[@]} months"
        echo -e "                   ${months_to_process[*]}"
    elif [ ${#months_to_process[@]} -eq 1 ]; then
        echo -e "Month:             ${months_to_process[0]}"
    else
        echo -e "Month:             Current ($(date +%m%Y))"
        months_to_process=("$(date +%m%Y)")
    fi

    if [ -n "${MAX_WORKERS}" ]; then
        echo -e "Max Workers:       ${MAX_WORKERS}"
    else
        echo -e "Max Workers:       Auto-detect"
    fi

    echo -e "Skip QC:           $([ -n "${SKIP_QC}" ] && echo "Yes [WARNING]" || echo "No [OK]")"
    echo -e "Validate in SF:    $([ -n "${VALIDATE_IN_SNOWFLAKE}" ] && echo "Yes [OK]" || echo "No")"
    echo -e "Validate Only:     $([ -n "${VALIDATE_ONLY}" ] && echo "Yes" || echo "No")"
    echo -e "Analyze Only:      $([ -n "${ANALYZE_ONLY}" ] && echo "Yes" || echo "No")"
    echo -e "Continue on Error: $([ -n "${CONTINUE_ON_ERROR}" ] && echo "Yes" || echo "No")"
    echo -e "Dry Run:           $([ -n "${DRY_RUN}" ] && echo "Yes" || echo "No")"
    echo -e "Parallel Jobs:     ${PARALLEL_JOBS}"
    echo -e "${GREEN}========================================${NC}\n"
fi

# Calculate workers per job if using parallel processing
workers_per_job=""
if [ ${PARALLEL_JOBS} -gt 1 ] && [ -n "${MAX_WORKERS}" ]; then
    workers_per_job=$((MAX_WORKERS / PARALLEL_JOBS))
    echo -e "${BLUE}Distributing ${MAX_WORKERS} workers across ${PARALLEL_JOBS} parallel jobs: ${workers_per_job} workers per month${NC}\n"
fi

# Suggest quiet mode for parallel processing
if [ ${PARALLEL_JOBS} -gt 1 ] && [ -z "${QUIET_MODE}" ]; then
    echo -e "${YELLOW}Tip: Use --quiet for cleaner output with parallel jobs${NC}\n"
fi

# Add initial spacing for parallel progress bars
# With 5 progress bars per job (Files, QC, Compression, Upload, COPY)
# or 4 if skipping QC (Files, Compression, Upload, COPY)
if [ ${PARALLEL_JOBS} -gt 1 ] && [ -n "${QUIET_MODE}" ]; then
    lines_per_job=5
    if [ -n "${SKIP_QC}" ] || [ -n "${VALIDATE_IN_SNOWFLAKE}" ]; then
        lines_per_job=4
    fi
    total_lines=$((PARALLEL_JOBS * lines_per_job))
    # Add initial spacing to prevent overlapping with existing output
    for ((i=0; i<total_lines; i++)); do
        echo ""
    done
fi

# Process months
total_months=${#months_to_process[@]}
successful_months=0
failed_months=0
failed_list=()
declare -A job_pids  # Associative array to track PIDs and their months
declare -A job_logs  # Track log files for each job

# Start time for total processing
start_time=$(date +%s)

# Function to wait for a job slot
wait_for_job_slot() {
    while [ $(jobs -r | wc -l) -ge ${PARALLEL_JOBS} ]; do
        sleep 1
    done
}

# Function to check completed jobs
check_completed_jobs() {
    local temp_pids=()
    for pid in "${!job_pids[@]}"; do
        if ! kill -0 $pid 2>/dev/null; then
            # Job completed, check exit status
            wait $pid
            local exit_code=$?
            local month="${job_pids[$pid]}"
            
            if [ ${exit_code} -eq 0 ]; then
                if [ -z "${QUIET_MODE}" ]; then
                    echo -e "${GREEN}[OK] Month $month completed successfully${NC}"
                fi
                ((successful_months++))
            else
                if [ -z "${QUIET_MODE}" ]; then
                    echo -e "${RED}[FAILED] Month $month failed with exit code: ${exit_code}${NC}"
                fi
                ((failed_months++))
                failed_list+=("${month}")
                
                if [ -z "${CONTINUE_ON_ERROR}" ] && [ ${PARALLEL_JOBS} -eq 1 ]; then
                    echo -e "${RED}Stopping due to error. Use --continue-on-error to proceed with remaining months.${NC}"
                    return 1
                fi
            fi
            unset job_pids[$pid]
        fi
    done
    return 0
}

# Process months with parallel support
for i in "${!months_to_process[@]}"; do
    month="${months_to_process[$i]}"
    current_num=$((i + 1))
    
    # Wait for available job slot if running in parallel
    if [ ${PARALLEL_JOBS} -gt 1 ]; then
        wait_for_job_slot
        check_completed_jobs || break
    fi
    
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "\n${MAGENTA}[${current_num}/${total_months}] Starting month: ${month}${NC}"
    fi
    
    if [ ${PARALLEL_JOBS} -gt 1 ]; then
        # Run in background for parallel processing
        {
            process_month "${month}" "${workers_per_job}" "${job_position_counter}"
        } &
        
        # Track the background job
        job_pids[$!]="${month}"
        if [ -z "${QUIET_MODE}" ]; then
            echo -e "${BLUE}Month ${month} running in background (PID: $!, position: ${job_position_counter})${NC}"
        fi
        # Increment position counter for next job
        job_position_counter=$((job_position_counter + 1))
    else
        # Sequential processing (original behavior)
        process_month "${month}" "${workers_per_job}" "0"
        exit_code=$?
        
        if [ ${exit_code} -eq 0 ]; then
            ((successful_months++))
        else
            ((failed_months++))
            failed_list+=("${month}")
            
            if [ -z "${CONTINUE_ON_ERROR}" ]; then
                echo -e "${RED}Stopping due to error. Use --continue-on-error to proceed with remaining months.${NC}"
                break
            else
                echo -e "${YELLOW}Continuing despite error (--continue-on-error enabled)${NC}"
            fi
        fi
    fi
done

# Wait for all remaining parallel jobs to complete
if [ ${PARALLEL_JOBS} -gt 1 ]; then
    if [ -z "${QUIET_MODE}" ]; then
        echo -e "\n${BLUE}Waiting for all parallel jobs to complete...${NC}"
    fi
    while [ ${#job_pids[@]} -gt 0 ]; do
        check_completed_jobs
        sleep 1
    done
fi

# Calculate total time
end_time=$(date +%s)
total_time=$((end_time - start_time))
hours=$((total_time / 3600))
minutes=$(((total_time % 3600) / 60))
seconds=$((total_time % 60))

# Final summary
if [ -z "${QUIET_MODE}" ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}BATCH PROCESSING SUMMARY${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "Total Months:      ${total_months}"
    echo -e "Successful:        ${successful_months} $([ ${successful_months} -gt 0 ] && echo "[OK]" || echo "")"
    echo -e "Failed:            ${failed_months} $([ ${failed_months} -gt 0 ] && echo "[FAILED]" || echo "")"

    if [ ${#failed_list[@]} -gt 0 ]; then
        echo -e "Failed Months:     ${failed_list[*]}"
    fi

    echo -e "Total Time:        ${hours}h ${minutes}m ${seconds}s"
    echo -e "${GREEN}========================================${NC}"
fi

# Display aggregate validation results if in validate-only mode OR validate-in-snowflake mode
if [ -n "${VALIDATE_ONLY}" ] || [ -n "${VALIDATE_IN_SNOWFLAKE}" ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}COMPREHENSIVE VALIDATION RESULTS${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # Find all validation JSON files from this run
    validation_files=($(find logs -name "validation_*.json" -mmin -$((total_time/60 + 1)) 2>/dev/null | sort))
    
    # Track totals across all months
    total_valid=0
    total_invalid=0
    total_anomalies=0
    
    if [ ${#validation_files[@]} -gt 0 ]; then
        # First, show aggregate statistics
        python3 -c "
import json
import sys

total_valid = 0
total_invalid = 0
total_anomalies = 0
all_failures = []

# First pass - collect statistics
validation_files = '$validation_files'.split()
for vfile in validation_files:
    try:
        with open(vfile, 'r') as f:
            data = json.load(f)
            results = data.get('results', [])
            month = data.get('month', 'Unknown')
            
            for r in results:
                if r.get('error'):
                    total_invalid += 1
                    all_failures.append((month, r.get('table_name', 'Unknown'), 'ERROR: ' + r['error']))
                elif r.get('valid'):
                    total_valid += 1
                else:
                    total_invalid += 1
                    failure_reasons = r.get('failure_reasons', [])
                    if failure_reasons:
                        reason = '; '.join(failure_reasons)
                    else:
                        reason = f\"{r.get('statistics', {}).get('missing_dates', 0)} dates missing\"
                    all_failures.append((month, r.get('table_name', 'Unknown'), reason))
                    
                # Count anomalies
                anomalies = r.get('row_count_analysis', {}).get('anomalous_dates_count', 0)
                total_anomalies += anomalies
    except Exception as e:
        pass

# Display aggregate summary
print(f'OVERALL STATISTICS:')
print(f'  Total Tables Validated: {total_valid + total_invalid}')
print(f'  [VALID] Tables:        {total_valid}')
print(f'  [INVALID] Tables:      {total_invalid}')
if total_anomalies > 0:
    print(f'  [WARNING] Total Anomalous Dates: {total_anomalies}')

if all_failures:
    print(f'\nFAILED VALIDATIONS:')
    for month, table, reason in all_failures:
        print(f'  - [{month}] {table}: {reason}')
"
        
        # Then show detailed results per month
        echo -e "\n${GREEN}DETAILED RESULTS BY MONTH:${NC}"
        for vfile in "${validation_files[@]}"; do
            if [ -f "$vfile" ]; then
                # Extract month and results using Python
                python3 -c "
import json
import sys
try:
    with open('$vfile', 'r') as f:
        data = json.load(f)
        month = data.get('month', 'Unknown')
        results = data.get('results', [])
        
        print(f'\n{month}:')
        for r in results:
            table = r.get('table_name', 'Unknown')
            if r.get('error'):
                status = '[ERROR] ' + r['error']
                print(f'  {table}: {status}')
            elif r.get('valid'):
                stats = r.get('statistics', {})
                anomalies = r.get('row_count_analysis', {}).get('anomalous_dates_count', 0)
                status = f'[VALID] ({stats.get(\"total_rows\", 0):,} rows, {stats.get(\"unique_dates\", 0)} dates)'
                if anomalies > 0:
                    status += f' - WARNING: {anomalies} anomalies detected'
                print(f'  {table}: {status}')
            else:
                failure_reasons = r.get('failure_reasons', [])
                if failure_reasons:
                    status = '[INVALID] ' + '; '.join(failure_reasons)
                else:
                    stats = r.get('statistics', {})
                    missing = stats.get('missing_dates', 0)
                    status = f'[INVALID] ({missing} dates missing)'
                print(f'  {table}: {status}')
                
                # Show anomalous dates if available
                anomalous_dates = r.get('anomalous_dates', [])
                if anomalous_dates:
                    print(f'    Anomalous dates: {anomalous_dates[0][\"date\"]} ({anomalous_dates[0][\"count\"]} rows)' + 
                          (f' + {len(anomalous_dates)-1} more' if len(anomalous_dates) > 1 else ''))
except Exception as e:
    print(f'  Error reading {vfile}: {e}', file=sys.stderr)
" 2>/dev/null
            fi
        done
    else
        echo -e "No validation results found"
    fi
    
    echo -e "${GREEN}========================================${NC}"
fi

# Set appropriate exit code
if [ ${failed_months} -gt 0 ] && [ -z "${CONTINUE_ON_ERROR}" ]; then
    exit 1
elif [ ${successful_months} -eq ${total_months} ]; then
    exit 0
else
    exit 2  # Partial success
fi
