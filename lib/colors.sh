#!/bin/bash
# lib/colors.sh - Color definitions for shell scripts
# Part of Snowflake ETL Pipeline Manager

# ANSI Color Codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
GRAY='\033[0;90m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Export colors for use in scripts that source this file
export RED GREEN YELLOW BLUE CYAN MAGENTA GRAY BOLD NC