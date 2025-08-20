#!/bin/bash

echo "=== Testing Python Import Methods ==="
echo ""

echo "1. Direct python3 command (like tsv_loader.py):"
python3 -c "import snowflake.connector; print('  SUCCESS: snowflake module found')" 2>&1 || echo "  FAILED: snowflake module not found"

echo ""
echo "2. Creating temp script and running it (like generate_config.sh):"
TEMP_SCRIPT=$(mktemp /tmp/test_import_XXXXXX.py)
cat > "$TEMP_SCRIPT" << 'EOF'
import snowflake.connector
print('  SUCCESS: snowflake module found')
EOF
python3 "$TEMP_SCRIPT" 2>&1 || echo "  FAILED: snowflake module not found"
rm -f "$TEMP_SCRIPT"

echo ""
echo "3. Python environment info:"
python3 -c "import sys; print('  Python:', sys.executable)"
python3 -c "import sys; print('  Version:', sys.version.split()[0])"
python3 -c "import sys; print('  Path:', sys.path[:3])"

echo ""
echo "4. Where is snowflake installed:"
python3 -c "
try:
    import snowflake.connector
    import os
    module_path = snowflake.connector.__file__
    print(f'  Module location: {os.path.dirname(module_path)}')
except ImportError as e:
    print(f'  Not found: {e}')
"