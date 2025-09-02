#!/bin/bash

# Test script for Python 3.11 installation functionality
# Tests detection, installation options, and path management

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="${SCRIPT_DIR}/.etl_state"
PREFS_DIR="${STATE_DIR}"

echo -e "${CYAN}=== Python 3.11 Installation Test ===${NC}"
echo

# Test 1: Check current Python version
echo -e "${YELLOW}1. Current Python Status${NC}"
echo "-----------------------------------"

if command -v python3.11 >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Python 3.11 found:${NC}"
    python3.11 --version
    which python3.11
elif command -v python3 >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠ Python 3 found (not 3.11):${NC}"
    python3 --version
    which python3
else
    echo -e "${RED}✗ No Python 3 found${NC}"
fi
echo

# Test 2: Check for custom Python installation
echo -e "${YELLOW}2. Custom Python Installation${NC}"
echo "-----------------------------------"

python_path_file="$PREFS_DIR/.python311_path"
if [[ -f "$python_path_file" ]]; then
    custom_path=$(cat "$python_path_file")
    echo -e "${CYAN}Found custom Python path: $custom_path${NC}"
    
    if [[ -d "$custom_path" ]]; then
        if [[ -f "$custom_path/python3.11" ]]; then
            echo -e "${GREEN}✓ Custom Python 3.11 exists at:${NC}"
            echo "  $custom_path/python3.11"
            $custom_path/python3.11 --version 2>/dev/null || echo "  (Unable to run)"
        else
            echo -e "${RED}✗ Custom path exists but Python 3.11 not found${NC}"
        fi
    else
        echo -e "${RED}✗ Custom path directory doesn't exist${NC}"
    fi
else
    echo -e "${YELLOW}No custom Python installation configured${NC}"
fi
echo

# Test 3: Check build dependencies
echo -e "${YELLOW}3. Build Dependencies Check${NC}"
echo "-----------------------------------"

build_tools=(gcc make wget tar)
missing_tools=()

for tool in "${build_tools[@]}"; do
    if command -v "$tool" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ $tool${NC} - $(which $tool)"
    else
        echo -e "${RED}✗ $tool${NC} - Not found"
        missing_tools+=("$tool")
    fi
done

if [[ ${#missing_tools[@]} -eq 0 ]]; then
    echo -e "${GREEN}All build dependencies available${NC}"
else
    echo -e "${YELLOW}Missing tools: ${missing_tools[*]}${NC}"
    echo -e "${CYAN}To install on Ubuntu/Debian:${NC}"
    echo "  sudo apt-get install build-essential wget"
    echo -e "${CYAN}To install on RHEL/CentOS:${NC}"
    echo "  sudo yum groupinstall 'Development Tools' && sudo yum install wget"
fi
echo

# Test 4: Check available installation paths
echo -e "${YELLOW}4. Installation Path Options${NC}"
echo "-----------------------------------"

# Check common installation locations
paths_to_check=(
    "$HOME/.local"
    "$HOME/opt"
    "/opt/python3.11"
    "/usr/local"
)

echo -e "${CYAN}Checking potential installation paths:${NC}"
for path in "${paths_to_check[@]}"; do
    if [[ -w "$path" ]] || [[ ! -e "$path" && -w "$(dirname "$path")" ]]; then
        echo -e "${GREEN}✓ $path${NC} - Writable"
    else
        echo -e "${YELLOW}⚠ $path${NC} - Requires elevated permissions"
    fi
done
echo

# Test 5: OS Detection for package manager
echo -e "${YELLOW}5. OS Detection for Package Manager${NC}"
echo "-----------------------------------"

if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    echo -e "${CYAN}Operating System: $NAME $VERSION${NC}"
    echo -e "${CYAN}OS ID: $ID${NC}"
    
    case "${ID,,}" in
        ubuntu|debian)
            echo -e "${GREEN}Package manager: apt-get${NC}"
            echo "Python 3.11 available via deadsnakes PPA"
            ;;
        rhel|centos|fedora|rocky|almalinux)
            echo -e "${GREEN}Package manager: dnf/yum${NC}"
            echo "Python 3.11 available in EPEL or AppStream"
            ;;
        arch|manjaro)
            echo -e "${GREEN}Package manager: pacman${NC}"
            echo "Python 3.11 in official repositories"
            ;;
        *)
            echo -e "${YELLOW}Package manager: Unknown${NC}"
            echo "Manual installation may be required"
            ;;
    esac
elif [[ "$(uname)" == "Darwin" ]]; then
    echo -e "${CYAN}Operating System: macOS$(NC)"
    if command -v brew >/dev/null 2>&1; then
        echo -e "${GREEN}Package manager: Homebrew${NC}"
        echo "Python 3.11 available via: brew install python@3.11"
    else
        echo -e "${YELLOW}Homebrew not installed${NC}"
        echo "Install from: https://brew.sh"
    fi
else
    echo -e "${RED}Unable to detect operating system${NC}"
fi
echo

# Test 6: Python download connectivity
echo -e "${YELLOW}6. Python.org Connectivity Test${NC}"
echo "-----------------------------------"

python_url="https://www.python.org/ftp/python/"
echo -e "${CYAN}Testing connection to python.org...${NC}"

if curl -s --connect-timeout 5 --head "$python_url" >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Can reach python.org for source downloads${NC}"
elif wget -q --timeout=5 --spider "$python_url" 2>/dev/null; then
    echo -e "${GREEN}✓ Can reach python.org for source downloads (wget)${NC}"
else
    echo -e "${RED}✗ Cannot reach python.org${NC}"
    echo -e "${YELLOW}Source installation may require proxy configuration${NC}"
fi
echo

# Summary
echo -e "${CYAN}=== Installation Recommendation ===${NC}"

if command -v python3.11 >/dev/null 2>&1; then
    echo -e "${GREEN}Python 3.11 is already installed!${NC}"
    echo "No additional installation needed."
elif [[ ${#missing_tools[@]} -eq 0 ]]; then
    echo -e "${GREEN}Ready for source installation${NC}"
    echo "Recommended installation path: $HOME/.local"
    echo "This doesn't require sudo privileges."
else
    echo -e "${YELLOW}Package manager installation recommended${NC}"
    echo "This is faster but may require sudo privileges."
fi

echo
echo -e "${CYAN}To test the installation flow:${NC}"
echo "  1. Remove existing Python 3.11 (if testing)"
echo "  2. Clear custom path: rm -f $PREFS_DIR/.python311_path"
echo "  3. Run: ./snowflake_etl.sh"
echo "  4. Follow the installation prompts"