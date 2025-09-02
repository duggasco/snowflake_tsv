# Virtual Environment Auto-Setup

*Added in v3.4.0*

## Overview
The Snowflake ETL Pipeline now automatically creates and manages a Python virtual environment on first run.

## Features

### Automatic Detection
- Checks for Python 3.11 (preferred) or any Python 3.x
- Detects if this is the first run
- Creates virtual environment if not present
- Activates venv automatically on each run

### First Run Experience
When you run `./snowflake_etl.sh` for the first time:

1. **Python Check**: Verifies Python installation
   - Prefers Python 3.11 if available
   - Falls back to any Python 3.x version
   
2. **Virtual Environment Creation**: 
   - Creates `etl_venv/` in the project directory
   - Completely isolated Python environment
   
3. **Package Installation**:
   - Upgrades pip to latest version
   - Installs from `requirements.txt`:
     - snowflake-connector-python
     - pandas
     - numpy
     - tqdm
     - Other dependencies
   
4. **Package Installation** (if setup.py exists):
   - Installs snowflake_etl package in editable mode
   - Makes CLI commands available

## Directory Structure
```
snowflake/
├── etl_venv/           # Virtual environment (auto-created)
│   ├── bin/           # Python executables
│   ├── lib/           # Installed packages
│   └── ...
├── .etl_state/        # State management
│   └── .venv_setup_complete  # Setup marker file
└── snowflake_etl.sh   # Main script
```

## Manual Setup (Optional)
If you prefer to set up the environment manually:

```bash
# Create virtual environment
python3.11 -m venv etl_venv

# Activate it
source etl_venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Install package (optional)
pip install -e .

# Mark as complete
mkdir -p .etl_state
touch .etl_state/.venv_setup_complete
```

## Resetting the Environment
To force a fresh setup:

```bash
# Remove the virtual environment
rm -rf etl_venv

# Remove the setup marker
rm -f .etl_state/.venv_setup_complete

# Run the script - it will recreate everything
./snowflake_etl.sh
```

## Benefits

1. **Isolation**: No system-wide package conflicts
2. **Consistency**: All users get same package versions
3. **Simplicity**: No manual setup required
4. **Reproducibility**: Uses requirements.txt for exact versions
5. **Performance**: Venv cached after first setup

## Troubleshooting

### Issue: Python 3.11 not found
**Solution**: The script will use any Python 3.x version available

### Issue: venv module not found
**Solution**: Install python3-venv package
```bash
# Ubuntu/Debian
sudo apt-get install python3-venv

# RHEL/CentOS
sudo yum install python3-venv
```

### Issue: Package installation fails
**Solution**: Check internet connection and proxy settings
```bash
# Set proxy if needed
export https_proxy=http://proxy.company.com:8080
export http_proxy=http://proxy.company.com:8080
```

### Issue: Permission denied
**Solution**: Ensure you have write permissions in the project directory

## Technical Details

### Functions Added
- `check_dependencies()` - Enhanced to check for venv and trigger setup
- `setup_python_environment()` - Handles complete venv setup
- `check_prerequisites()` - Updated to activate venv when checking packages

### Files Created
- `etl_venv/` - Virtual environment directory
- `.etl_state/.venv_setup_complete` - Marker file to skip setup on subsequent runs

### Environment Variables
When venv is active:
- `VIRTUAL_ENV` - Path to virtual environment
- `PATH` - Updated to use venv's Python first

## Notes
- The virtual environment is specific to this project
- It doesn't affect system Python or other projects
- All Python commands within the script use the venv automatically
- The setup only happens once (unless reset manually)