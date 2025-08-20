#!/bin/bash

# Test script to diagnose JSON parsing issues

CONFIG_FILE="${1:-config/snowflake.json}"

echo "Testing JSON parsing for: $CONFIG_FILE"
echo "================================="

# Check if file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: File not found: $CONFIG_FILE"
    exit 1
fi

# Show file size
echo "File size: $(ls -lh "$CONFIG_FILE" | awk '{print $5}')"
echo ""

# Check if it's valid JSON
echo "Validating JSON structure..."
if python3 -m json.tool "$CONFIG_FILE" > /dev/null 2>&1; then
    echo "[OK] Valid JSON"
else
    echo "[ERROR] Invalid JSON"
    echo ""
    echo "First 5 lines of file:"
    head -5 "$CONFIG_FILE"
    echo ""
    echo "Python JSON validation error:"
    python3 -c "import json; json.load(open('$CONFIG_FILE'))" 2>&1
    exit 1
fi

# Check for Snowflake section
echo ""
echo "Checking Snowflake configuration..."
python3 << EOF
import json
try:
    with open('$CONFIG_FILE', 'r') as f:
        config = json.load(f)
    
    if 'snowflake' in config:
        print("[OK] Snowflake section found")
        sf = config['snowflake']
        required = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        for field in required:
            if field in sf:
                value = sf[field]
                if field == 'password':
                    value = '***hidden***'
                print(f"  - {field}: {value}")
            else:
                print(f"  - {field}: MISSING")
    else:
        print("[ERROR] No 'snowflake' section in config")
        
except Exception as e:
    print(f"Error: {e}")
EOF

echo ""
echo "Testing snowflake module..."

# Detect Python environment
if [[ -n "$VIRTUAL_ENV" ]]; then
    echo "Using virtual environment: $VIRTUAL_ENV"
    PYTHON_CMD="$VIRTUAL_ENV/bin/python3"
elif [[ -n "$CONDA_DEFAULT_ENV" ]]; then
    echo "Using conda environment: $CONDA_DEFAULT_ENV"
    if [[ -n "$CONDA_PREFIX" ]]; then
        PYTHON_CMD="$CONDA_PREFIX/bin/python"
    else
        PYTHON_CMD="python"  # In conda, 'python' is usually the right one
    fi
    echo "Python path: $PYTHON_CMD"
else
    echo "Using system Python"
    PYTHON_CMD="python3"
fi

$PYTHON_CMD -c "import snowflake.connector; print('[OK] snowflake-connector-python is installed')" 2>&1 || echo "[ERROR] snowflake-connector-python NOT installed"