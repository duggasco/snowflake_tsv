#!/bin/bash

 

# run_loader.sh - Enhanced TSV to Snowflake loader runner script

 

# Color codes

RED='\033[0;31m'

GREEN='\033[0;32m'

YELLOW='\033[1;33m'

BLUE='\033[0;34m'

NC='\033[0m' # No Color

 

# Default values

CONFIG_FILE="config/config.json"

BASE_PATH="./data"

MONTH=$(date +%Y-%m)  # Default to current month

MAX_WORKERS=""  # Empty means auto-detect

SKIP_QC=""

ANALYZE_ONLY=""

CHECK_SYSTEM=""

 

# Function to display usage

usage() {

    echo "Usage: $0 [OPTIONS]"

    echo ""

    echo "Load TSV files to Snowflake with quality checks and progress tracking"

    echo ""

    echo "Options:"

    echo "  --config FILE       Configuration file (default: config/config.json)"

    echo "  --base-path PATH    Base path for TSV files (default: ./data)"

    echo "  --month YYYY-MM     Month to process (default: current month)"

    echo "  --max-workers N     Maximum parallel workers (default: auto-detect)"

    echo "  --skip-qc           Skip quality checks (not recommended)"

    echo "  --analyze-only      Only analyze files and show time estimates"

    echo "  --check-system      Check system capabilities and exit"

    echo "  --help              Show this help message"

    echo ""

    echo "Examples:"

    echo "  # Check system capabilities"

    echo "  $0 --check-system"

    echo ""

    echo "  # Analyze files without processing"

    echo "  $0 --analyze-only --month 2024-09"

    echo ""

    echo "  # Process with auto-detected workers"

    echo "  $0 --month 2024-09 --base-path ./data"

    echo ""

    echo "  # Process with specific worker count"

    echo "  $0 --month 2024-09 --max-workers 8"

    echo ""

    exit 0

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

 

# Build the Python command

cmd="python3 tsv_loader.py"

cmd="${cmd} --config ${CONFIG_FILE}"

cmd="${cmd} --base-path ${BASE_PATH}"

 

# Add optional arguments

if [ -n "${MONTH}" ]; then

    cmd="${cmd} --month ${MONTH}"

fi

 

if [ -n "${MAX_WORKERS}" ]; then

    cmd="${cmd} --max-workers ${MAX_WORKERS}"

else

    echo -e "${BLUE}Auto-detecting optimal worker count...${NC}"

fi

 

if [ -n "${SKIP_QC}" ]; then

    cmd="${cmd} ${SKIP_QC}"

    echo -e "${YELLOW}Warning: Quality checks will be skipped!${NC}"

fi

 

if [ -n "${ANALYZE_ONLY}" ]; then

    cmd="${cmd} ${ANALYZE_ONLY}"

    echo -e "${BLUE}Running in analysis-only mode${NC}"

fi

 

# Display configuration

echo -e "${GREEN}========================================${NC}"

echo -e "${GREEN}TSV to Snowflake Loader Configuration${NC}"

echo -e "${GREEN}========================================${NC}"

echo -e "Config File:    ${CONFIG_FILE}"

echo -e "Base Path:      ${BASE_PATH}"

echo -e "Month:          ${MONTH}"

 

if [ -n "${MAX_WORKERS}" ]; then

    echo -e "Max Workers:    ${MAX_WORKERS}"

else

    echo -e "Max Workers:    Auto-detect"

fi

 

echo -e "Skip QC:        $([ -n "${SKIP_QC}" ] && echo "Yes ⚠️" || echo "No ✓")"

echo -e "Analyze Only:   $([ -n "${ANALYZE_ONLY}" ] && echo "Yes" || echo "No")"

echo -e "${GREEN}========================================${NC}\n"

 

# Check if files exist

if [ ! -n "${ANALYZE_ONLY}" ] && [ ! -n "${CHECK_SYSTEM}" ]; then

    echo -e "${BLUE}Checking for TSV files...${NC}"

    file_count=$(find "${BASE_PATH}" -name "*.tsv" -type f 2>/dev/null | wc -l)

 

    if [ ${file_count} -eq 0 ]; then

        echo -e "${RED}ERROR: No TSV files found in ${BASE_PATH}${NC}"

        exit 1

    else

        echo -e "${GREEN}Found ${file_count} TSV file(s)${NC}\n"

    fi

fi

 

# Create a log file name with timestamp

log_file="logs/run_$(date +%Y%m%d_%H%M%S).log"

 

# Run the loader with real-time output and logging

echo -e "${GREEN}Starting TSV loader...${NC}"

echo -e "${BLUE}Command: ${cmd}${NC}"

echo -e "${BLUE}Log file: ${log_file}${NC}\n"

 

# Execute the command with tee to show output and save to log

${cmd} 2>&1 | tee "${log_file}"

 

# Capture exit code

exit_code=${PIPESTATUS[0]}

 

# Final status

echo ""

if [ ${exit_code} -eq 0 ]; then

    echo -e "${GREEN}========================================${NC}"

    echo -e "${GREEN}✓ PROCESSING COMPLETE${NC}"

    echo -e "${GREEN}========================================${NC}"

    echo -e "Log saved to: ${log_file}"

else

    echo -e "${RED}========================================${NC}"

    echo -e "${RED}✗ PROCESSING FAILED${NC}"

    echo -e "${RED}========================================${NC}"

    echo -e "Exit code: ${exit_code}"

    echo -e "Check log file: ${log_file}"

 

    # Show last few error lines from log

    echo -e "\n${YELLOW}Recent errors:${NC}"

    grep -i "error\|failed\|missing" "${log_file}" | tail -5

fi

 

exit ${exit_code}
