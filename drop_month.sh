#!/bin/bash

# drop_month.sh - Wrapper script for safe data deletion from Snowflake
# Usage: ./drop_month.sh --config config.json --table TABLE_NAME --month 2024-01 [options]

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
CONFIG=""
TABLE=""
TABLES=""
ALL_TABLES=""
MONTH=""
MONTHS=""
DRY_RUN=""
PREVIEW=""
YES=""
QUIET=""
OUTPUT_JSON=""

# Function to print colored output
print_color() {
    local color=$1
    shift
    echo -e "${color}$*${NC}"
}

# Function to print usage
usage() {
    echo "Usage: $0 --config CONFIG_FILE [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --config FILE       Path to configuration JSON file (required)"
    echo "  --table TABLE       Single table name to process"
    echo "  --tables LIST       Comma-separated list of table names"
    echo "  --all-tables        Process all tables in config"
    echo "  --month YYYY-MM     Single month to delete"
    echo "  --months LIST       Comma-separated list of months"
    echo "  --dry-run           Analyze without deleting"
    echo "  --preview           Show sample rows to be deleted"
    echo "  --yes, -y           Skip confirmation prompt"
    echo "  --quiet, -q         Suppress console output"
    echo "  --output-json FILE  Output summary report to JSON file"
    echo "  --help, -h          Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Dry run for single table and month"
    echo "  $0 --config config.json --table MY_TABLE --month 2024-01 --dry-run"
    echo ""
    echo "  # Delete with preview"
    echo "  $0 --config config.json --table MY_TABLE --month 2024-01 --preview"
    echo ""
    echo "  # Delete multiple months"
    echo "  $0 --config config.json --table MY_TABLE --months 2024-01,2024-02,2024-03"
    echo ""
    echo "  # Delete from all tables (use with extreme caution!)"
    echo "  $0 --config config.json --all-tables --month 2024-01"
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG="$2"
            shift 2
            ;;
        --table)
            TABLE="$2"
            shift 2
            ;;
        --tables)
            TABLES="$2"
            shift 2
            ;;
        --all-tables)
            ALL_TABLES="--all-tables"
            shift
            ;;
        --month)
            MONTH="$2"
            shift 2
            ;;
        --months)
            MONTHS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --preview)
            PREVIEW="--preview"
            shift
            ;;
        --yes|-y)
            YES="--yes"
            shift
            ;;
        --quiet|-q)
            QUIET="--quiet"
            shift
            ;;
        --output-json)
            OUTPUT_JSON="$2"
            shift 2
            ;;
        --help|-h)
            usage
            ;;
        *)
            print_color $RED "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$CONFIG" ]; then
    print_color $RED "Error: --config is required"
    usage
fi

if [ ! -f "$CONFIG" ]; then
    print_color $RED "Error: Config file not found: $CONFIG"
    exit 1
fi

# Validate table specification
if [ -z "$TABLE" ] && [ -z "$TABLES" ] && [ -z "$ALL_TABLES" ]; then
    print_color $RED "Error: Must specify --table, --tables, or --all-tables"
    usage
fi

# Validate month specification
if [ -z "$MONTH" ] && [ -z "$MONTHS" ]; then
    print_color $RED "Error: Must specify --month or --months"
    usage
fi

# Build the command
cmd="python3 drop_month.py --config $CONFIG"

# Add table specifications
if [ -n "$TABLE" ]; then
    cmd="$cmd --table $TABLE"
fi
if [ -n "$TABLES" ]; then
    cmd="$cmd --tables $TABLES"
fi
if [ -n "$ALL_TABLES" ]; then
    cmd="$cmd $ALL_TABLES"
fi

# Add month specifications
if [ -n "$MONTH" ]; then
    cmd="$cmd --month $MONTH"
fi
if [ -n "$MONTHS" ]; then
    cmd="$cmd --months $MONTHS"
fi

# Add optional flags
if [ -n "$DRY_RUN" ]; then
    cmd="$cmd $DRY_RUN"
fi
if [ -n "$PREVIEW" ]; then
    cmd="$cmd $PREVIEW"
fi
if [ -n "$YES" ]; then
    cmd="$cmd $YES"
fi
if [ -n "$QUIET" ]; then
    cmd="$cmd $QUIET"
fi
if [ -n "$OUTPUT_JSON" ]; then
    cmd="$cmd --output-json $OUTPUT_JSON"
fi

# Print warning for destructive operations
if [ -z "$DRY_RUN" ]; then
    print_color $YELLOW "[WARNING] This operation will DELETE data from Snowflake"
    print_color $YELLOW "Config: $CONFIG"
    
    if [ -n "$TABLE" ]; then
        print_color $YELLOW "Table: $TABLE"
    fi
    if [ -n "$TABLES" ]; then
        print_color $YELLOW "Tables: $TABLES"
    fi
    if [ -n "$ALL_TABLES" ]; then
        print_color $RED "[WARNING] ALL TABLES IN CONFIG"
    fi
    
    if [ -n "$MONTH" ]; then
        print_color $YELLOW "Month: $MONTH"
    fi
    if [ -n "$MONTHS" ]; then
        print_color $YELLOW "Months: $MONTHS"
    fi
    
    if [ -z "$YES" ]; then
        print_color $CYAN "You will be prompted to confirm before deletion."
    else
        print_color $RED "[WARNING] Auto-confirmation enabled (--yes flag)"
    fi
    echo ""
else
    print_color $GREEN "[DRY RUN] MODE - No data will be deleted"
fi

# Show the command being executed
print_color $BLUE "Executing: $cmd"
echo ""

# Execute the command
eval $cmd
exit_code=$?

# Print result
if [ $exit_code -eq 0 ]; then
    if [ -z "$DRY_RUN" ]; then
        print_color $GREEN "[SUCCESS] Deletion completed successfully"
    else
        print_color $GREEN "[SUCCESS] Dry run completed successfully"
    fi
else
    print_color $RED "[FAILED] Operation failed with exit code: $exit_code"
fi

exit $exit_code