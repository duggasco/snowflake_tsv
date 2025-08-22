#!/bin/bash
#
# ETL CLI Wrapper - Simplified interface to the new Python CLI
# This script provides a convenient wrapper around the refactored ETL CLI
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
DEFAULT_CONFIG="${SNOWFLAKE_ETL_CONFIG:-config/config.json}"
DEFAULT_LOG_DIR="${SNOWFLAKE_ETL_LOG_DIR:-logs}"
DEFAULT_LOG_LEVEL="${SNOWFLAKE_ETL_LOG_LEVEL:-INFO}"

# Python module path
CLI_MODULE="snowflake_etl.cli.main"

# Function to display usage
show_usage() {
    cat << EOF
Usage: $0 [global-options] <command> [command-options]

Global Options:
    -c, --config FILE    Configuration file (default: $DEFAULT_CONFIG)
    -q, --quiet         Suppress console output
    -v, --verbose       Enable debug logging
    -h, --help          Show this help message

Commands:
    load                Load TSV files to Snowflake
    delete              Delete data from Snowflake tables
    validate            Validate data in Snowflake
    report              Generate table report
    check-duplicates    Check for duplicate records
    compare             Compare TSV files

Examples:
    # Load data for a specific month
    $0 load --base-path ./data --month 2024-01

    # Delete data with dry run
    $0 delete --table MY_TABLE --month 2024-01 --dry-run

    # Validate all tables
    $0 validate --output validation_results.json

    # Load with custom config
    $0 -c custom_config.json load --month 2024-01 --skip-qc

EOF
}

# Parse global options
CONFIG_FILE="$DEFAULT_CONFIG"
QUIET_FLAG=""
LOG_LEVEL="$DEFAULT_LOG_LEVEL"

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -q|--quiet)
            QUIET_FLAG="--quiet"
            shift
            ;;
        -v|--verbose)
            LOG_LEVEL="DEBUG"
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            # Stop at first non-global option (command)
            break
            ;;
    esac
done

# Check if command was provided
if [[ $# -eq 0 ]]; then
    echo -e "${RED}Error: No command specified${NC}"
    show_usage
    exit 1
fi

# Get the command
COMMAND="$1"
shift

# Validate config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo -e "${RED}Error: Configuration file not found: $CONFIG_FILE${NC}"
    exit 1
fi

# Build the CLI command
CLI_CMD="python3 -m $CLI_MODULE"
CLI_CMD="$CLI_CMD --config $CONFIG_FILE"
CLI_CMD="$CLI_CMD --log-dir $DEFAULT_LOG_DIR"
CLI_CMD="$CLI_CMD --log-level $LOG_LEVEL"

if [[ -n "$QUIET_FLAG" ]]; then
    CLI_CMD="$CLI_CMD $QUIET_FLAG"
fi

# Add the command and its options
CLI_CMD="$CLI_CMD $COMMAND"

# Function to run command with nice output
run_command() {
    local cmd="$1"
    
    echo -e "${BLUE}Executing: $cmd${NC}"
    echo "----------------------------------------"
    
    # Execute the command
    if eval "$cmd"; then
        echo "----------------------------------------"
        echo -e "${GREEN}✓ Command completed successfully${NC}"
        return 0
    else
        local exit_code=$?
        echo "----------------------------------------"
        echo -e "${RED}✗ Command failed with exit code: $exit_code${NC}"
        return $exit_code
    fi
}

# Handle specific commands with their options
case $COMMAND in
    load)
        # Parse load-specific options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    delete)
        # Parse delete-specific options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    validate)
        # Parse validate-specific options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    report)
        # Parse report-specific options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    check-duplicates)
        # Parse check-duplicates options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    compare)
        # Parse compare options
        while [[ $# -gt 0 ]]; do
            CLI_CMD="$CLI_CMD $1"
            shift
        done
        run_command "$CLI_CMD"
        ;;
        
    *)
        echo -e "${RED}Error: Unknown command: $COMMAND${NC}"
        show_usage
        exit 1
        ;;
esac