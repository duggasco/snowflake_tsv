#!/bin/bash

echo "=== Python Environment Debug ==="
echo ""

echo "Environment Variables:"
echo "  CONDA_DEFAULT_ENV: ${CONDA_DEFAULT_ENV:-not set}"
echo "  CONDA_PREFIX: ${CONDA_PREFIX:-not set}"
echo "  VIRTUAL_ENV: ${VIRTUAL_ENV:-not set}"
echo "  PATH: ${PATH}"
echo ""

echo "Python commands available:"
echo -n "  which python: "
which python 2>/dev/null || echo "not found"
echo -n "  which python3: "
which python3 2>/dev/null || echo "not found"
echo ""

echo "Python versions:"
if command -v python &>/dev/null; then
    echo -n "  python --version: "
    python --version 2>&1
fi
if command -v python3 &>/dev/null; then
    echo -n "  python3 --version: "
    python3 --version 2>&1
fi
echo ""

echo "Testing snowflake module:"
echo -n "  python -c 'import snowflake.connector': "
python -c "import snowflake.connector; print('OK')" 2>&1 || echo "FAILED"
echo -n "  python3 -c 'import snowflake.connector': "
python3 -c "import snowflake.connector; print('OK')" 2>&1 || echo "FAILED"

if [[ -n "$CONDA_PREFIX" ]]; then
    echo -n "  $CONDA_PREFIX/bin/python -c 'import snowflake.connector': "
    $CONDA_PREFIX/bin/python -c "import snowflake.connector; print('OK')" 2>&1 || echo "FAILED"
fi

echo ""
echo "Python package locations:"
if command -v python &>/dev/null; then
    echo "  python site-packages:"
    python -c "import site; print('    ' + '\\n    '.join(site.getsitepackages()))" 2>/dev/null || echo "    Could not determine"
fi

echo ""
echo "Recommended Python command for this environment:"
if [[ -n "$CONDA_PREFIX" ]]; then
    echo "  $CONDA_PREFIX/bin/python"
elif [[ -n "$VIRTUAL_ENV" ]]; then
    echo "  $VIRTUAL_ENV/bin/python3"
elif command -v python &>/dev/null && python -c "import snowflake.connector" 2>/dev/null; then
    echo "  python"
elif command -v python3 &>/dev/null && python3 -c "import snowflake.connector" 2>/dev/null; then
    echo "  python3"
else
    echo "  No Python with snowflake-connector-python found!"
    echo "  Install with: pip install snowflake-connector-python"
fi