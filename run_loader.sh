#!/bin/bash

# run_loader.sh - Enhanced TSV to Snowflake loader runner script with multi-month support

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

 

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Load TSV files to Snowflake with quality checks and progress tracking"
    echo ""
    echo "Options:"
    echo "  --config FILE       Configuration file (default: config/config.json)"
    echo "  --base-path PATH    Base path for TSV files (default: ./data)"
    echo "  --month YYYY-MM     Single month to process (default: current month)"
    echo "  --months LIST       Comma-separated list of months (e.g., 092022,102022,112022)"
    echo "  --batch              Process all months found in base-path"
    echo "  --continue-on-error Continue processing if a month fails"
    echo "  --dry-run           Show what would be processed without executing"
    echo "  --max-workers N     Maximum parallel workers (default: auto-detect)"
    echo "  --skip-qc           Skip quality checks (not recommended)"
    echo "  --analyze-only      Only analyze files and show time estimates"
    echo "  --check-system      Check system capabilities and exit"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Process single month"
    echo "  $0 --month 2024-09 --base-path ./data"
    echo ""
    echo "  # Process specific months with skip-qc"
    echo "  $0 --months 092022,102022,112022 --skip-qc"
    echo ""
    echo "  # Process all months in batch mode"
    echo "  $0 --batch --skip-qc --continue-on-error"
    echo ""
    echo "  # Dry run to see what would be processed"
    echo "  $0 --batch --dry-run"
    echo ""
    exit 0
}

 

# Function to convert month format from MMYYYY to YYYY-MM
convert_month_format() {
    local month_dir=$1
    # Extract MM and YYYY from MMYYYY format
    if [[ $month_dir =~ ^([0-9]{2})([0-9]{4})$ ]]; then
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

# Function to process a single month
process_month() {
    local month_dir=$1
    local month_formatted=$(convert_month_format "$month_dir")
    
    if [ -z "$month_formatted" ]; then
        echo -e "${RED}ERROR: Invalid month format: $month_dir (expected MMYYYY)${NC}"
        return 1
    fi
    
    echo -e "\n${CYAN}========================================${NC}"
    echo -e "${CYAN}Processing Month: $month_dir (${month_formatted})${NC}"
    echo -e "${CYAN}========================================${NC}"
    
    # Build the Python command
    local cmd="python3 tsv_loader.py"
    cmd="${cmd} --config ${CONFIG_FILE}"
    cmd="${cmd} --base-path ${BASE_PATH}"
    cmd="${cmd} --month ${month_formatted}"
    
    # Add optional arguments
    if [ -n "${MAX_WORKERS}" ]; then
        cmd="${cmd} --max-workers ${MAX_WORKERS}"
    fi
    
    if [ -n "${SKIP_QC}" ]; then
        cmd="${cmd} ${SKIP_QC}"
    fi
    
    if [ -n "${ANALYZE_ONLY}" ]; then
        cmd="${cmd} ${ANALYZE_ONLY}"
    fi
    
    # Create a log file name with timestamp and month
    local log_file="logs/run_${month_dir}_$(date +%Y%m%d_%H%M%S).log"
    
    echo -e "${BLUE}Command: ${cmd}${NC}"
    echo -e "${BLUE}Log file: ${log_file}${NC}"
    
    if [ -n "${DRY_RUN}" ]; then
        echo -e "${YELLOW}[DRY RUN] Would execute above command${NC}"
        return 0
    fi
    
    # Execute the command with tee to show output and save to log
    ${cmd} 2>&1 | tee "${log_file}"
    
    # Capture exit code
    local exit_code=${PIPESTATUS[0]}
    
    if [ ${exit_code} -eq 0 ]; then
        echo -e "${GREEN}✓ Month $month_dir processed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Month $month_dir failed with exit code: ${exit_code}${NC}"
        return ${exit_code}
    fi
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
    python3 tsv_loader.py --check-system
    exit $?
fi

# Check prerequisites
check_prerequisites

# Determine which months to process
declare -a months_to_process=()

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
    echo -e "${MAGENTA}Processing specific months: ${months_to_process[*]}${NC}"
    
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

# Display configuration
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

echo -e "Skip QC:           $([ -n "${SKIP_QC}" ] && echo "Yes ⚠️" || echo "No ✓")"
echo -e "Analyze Only:      $([ -n "${ANALYZE_ONLY}" ] && echo "Yes" || echo "No")"
echo -e "Continue on Error: $([ -n "${CONTINUE_ON_ERROR}" ] && echo "Yes" || echo "No")"
echo -e "Dry Run:           $([ -n "${DRY_RUN}" ] && echo "Yes" || echo "No")"
echo -e "${GREEN}========================================${NC}\n"

# Process months
total_months=${#months_to_process[@]}
successful_months=0
failed_months=0
failed_list=()

# Start time for total processing
start_time=$(date +%s)

for i in "${!months_to_process[@]}"; do
    month="${months_to_process[$i]}"
    current_num=$((i + 1))
    
    echo -e "\n${MAGENTA}[${current_num}/${total_months}] Starting month: ${month}${NC}"
    
    process_month "${month}"
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

# Set appropriate exit code
if [ ${failed_months} -gt 0 ] && [ -z "${CONTINUE_ON_ERROR}" ]; then
    exit 1
elif [ ${successful_months} -eq ${total_months} ]; then
    exit 0
else
    exit 2  # Partial success
fi
