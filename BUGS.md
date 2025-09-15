# BUGS.md - Issue Tracking and Resolution
*Last Updated: 2025-09-03*
*Current Version: 3.4.4*

## ✅ Recently Fixed Issues (2025-09-03)

### Silent Script Failure (v3.4.4) ✅

1. **Silent Exit on Non-Interactive Execution** ✅
   - **Issue**: Script silently exited with no output when run non-interactively
   - **Severity**: CRITICAL
   - **Root Causes**: 
     - `load_python_path()` returned 1 when custom path file didn't exist
     - With `set -e`, this caused immediate script termination
     - `confirm_install_python()` used `read` command which failed in non-TTY
   - **Resolution**: 
     - Changed `load_python_path()` to always return 0 (missing file is not an error)
     - Added TTY checks before interactive prompts
     - Skip Python installation offers in non-interactive mode
   - **Files Fixed**: snowflake_etl.sh
   - **Commit**: Fixed in v3.4.4

2. **Unconditional Exit After CLI Parsing** ✅
   - **Issue**: Script always executed `show_help` and `exit 1` after parsing any CLI args
   - **Severity**: HIGH
   - **Root Cause**: Logic error - help/exit code ran unconditionally after `parse_cli_args`
   - **Resolution**: Removed unconditional exit code after case statement
   - **Files Fixed**: snowflake_etl.sh (lines 3722-3724)
   - **Commit**: Fixed in v3.4.4

## ✅ Recently Fixed Issues (2025-08-26 Session 2)

### CLI and Loading Issues (v3.0.3) ✅

1. **Month Format Validation Error** ✅
   - **Issue**: "Invalid month format: 2024-07 (expected MMYYYY)" when using YYYY-MM format
   - **Severity**: HIGH
   - **Root Cause**: convert_month_format() only accepted MMYYYY format
   - **Resolution**: Updated function to accept both YYYY-MM and MMYYYY formats
   - **Files Fixed**: run_loader.sh
   - **Commit**: Fixed in v3.0.3

2. **--yes Flag Unrecognized Error** ✅
   - **Issue**: "unrecognized arguments: --yes" for load operations
   - **Severity**: HIGH
   - **Root Cause**: Load operation doesn't have interactive prompts, --yes not needed
   - **Resolution**: Removed --yes flag addition for load operations
   - **Files Fixed**: run_loader.sh
   - **Commit**: Fixed in v3.0.3

3. **base_path Unexpected Keyword Argument** ✅
   - **Issue**: load_files() got unexpected keyword argument 'base_path'
   - **Severity**: CRITICAL
   - **Root Cause**: Method signature mismatch - load_files() expects FileConfig list
   - **Resolution**: Build FileConfig objects from base_path and month before calling
   - **Files Fixed**: snowflake_etl/__main__.py
   - **Commit**: Fixed in v3.0.3

4. **UnboundLocalError for Path Variable** ✅
   - **Issue**: "local variable 'Path' referenced before assignment"
   - **Severity**: CRITICAL
   - **Root Cause**: Duplicate imports inside functions shadowing global imports
   - **Resolution**: Moved all imports to top of file, removed duplicates
   - **Files Fixed**: snowflake_etl/__main__.py, snowflake_etl/cli/main.py
   - **Commit**: Fixed in v3.0.3

5. **Base Path Not Prompted in Load Menu** ✅
   - **Issue**: "Specify base path and month" option only prompted for month
   - **Severity**: MEDIUM
   - **Root Cause**: Missing base path prompt in menu_load_data() option 2
   - **Resolution**: Added base path prompt with directory validation
   - **Files Fixed**: snowflake_etl.sh
   - **Commit**: Fixed in v3.0.3

6. **Direct File Loading Failure** ✅
   - **Issue**: "No files found to process" when using --direct-file
   - **Severity**: HIGH
   - **Root Cause**: System trying to use month-based pattern matching for direct files
   - **Resolution**: Added --files argument to CLI, process direct files separately
   - **Files Fixed**: snowflake_etl/__main__.py, run_loader.sh
   - **Commit**: Fixed in v3.0.3

7. **Quoted File Paths Issue** ✅
   - **Issue**: File paths wrapped in quotes causing path resolution failures
   - **Severity**: HIGH
   - **Root Cause**: Escaped quotes in --files argument becoming part of path
   - **Resolution**: Removed quotes from --files argument in run_loader.sh
   - **Files Fixed**: run_loader.sh
   - **Commit**: Fixed in v3.0.3

8. **Tuple Format String Error** ✅
   - **Issue**: "unsupported format string passed to tuple.__format__"
   - **Severity**: CRITICAL
   - **Root Cause**: expected_date_range passed as string tuple instead of datetime tuple
   - **Resolution**: Convert all date ranges to datetime objects when creating FileConfig
   - **Files Fixed**: snowflake_etl/__main__.py, snowflake_etl/cli/main.py
   - **Commit**: Fixed in v3.0.3

## ✅ Recently Fixed Issues (2025-08-26 Session 1)

### Report Display Issues (v3.0.2) ✅
1. **Truncated Date Lists** ✅
   - **Issue**: Report showing "... and X more" instead of full lists
   - **Severity**: MEDIUM
   - **Resolution**: Removed artificial limits, now shows all dates/gaps/anomalies
   - **Commit**: Fixed in v3.0.2

2. **Unknown Gap Ranges** ✅
   - **Issue**: Gap ranges displaying as "Unknown to Unknown (0 days)"
   - **Severity**: MEDIUM
   - **Resolution**: Added null checks, fixed field name mapping
   - **Commit**: Fixed in v3.0.2

3. **Zero Percentage Calculations** ✅
   - **Issue**: All anomalies showing "0.0% of average"
   - **Severity**: MEDIUM
   - **Resolution**: Fixed field names, added null handling and division protection
   - **Commit**: Fixed in v3.0.2

## ✅ Previously Fixed Issues (v3.0.1)

### 1. Unicode Encoding Errors ✅
- **Issue**: Latin-1 terminals failed with Unicode characters (✓, ✗, ⚠, etc.)
- **Severity**: HIGH
- **Resolution**: Replaced all Unicode with ASCII equivalents ([VALID], [INVALID], WARNING:)
- **Commit**: Fixed in v3.0.1

### 2. Connection Pool Exhaustion ✅
- **Issue**: "Connection pool exhausted (size: 5)" during report generation
- **Severity**: CRITICAL
- **Root Cause**: Nested connection usage in validation operations
- **Resolution**: 
  - Added `validate_table_with_cursor()` to reuse connections
  - Increased default pool size to 10 (configurable)
  - Added automatic worker limiting based on pool size
- **Commit**: Fixed in v3.0.1

### 3. Blank Logs for Foreground Jobs ✅
- **Issue**: Live job progress logs appeared blank when reviewed later
- **Severity**: MEDIUM
- **Resolution**: Fixed log capture using stdbuf and proper exit status handling
- **Commit**: Fixed in v3.0.1

### 4. CLI Argument Order Error ✅
- **Issue**: "unrecognized arguments: --config" when config came after subcommand
- **Severity**: HIGH
- **Resolution**: Fixed all CLI calls to use correct order: `sfl --config FILE.json SUBCOMMAND`
- **Commit**: Fixed in v3.0.1

### 5. ReportOperation Missing Status ✅
- **Issue**: "__init__() missing 1 required positional argument: 'status'"
- **Severity**: HIGH
- **Resolution**: Added status='PENDING' initialization in TableReport
- **Commit**: Fixed in v3.0.1

### 6. ApplicationContext config_data Attribute Error ✅
- **Issue**: "'ApplicationContext' object has no attribute 'config_data'"
- **Severity**: HIGH
- **Resolution**: Changed reference to `self._config` (correct private attribute)
- **Commit**: Fixed in v3.0.1

### 7. Incomplete Validation Details in Reports ✅
- **Issue**: Reports only showed counts, not actual problematic dates
- **Severity**: MEDIUM
- **Resolution**: 
  - Fixed data preservation to store full lists
  - Enhanced display to show all anomalous dates with details
  - Auto-generates comprehensive output files
- **Commit**: Partially fixed in v3.0.1, fully fixed in v3.0.2

## 🔧 Known Issues (Active)

### 1. Progress Bar Terminal Resize
- **Issue**: Progress bars can overlap if terminal is resized during execution
- **Severity**: LOW
- **Workaround**: Avoid resizing terminal during operations
- **Status**: Monitoring

### 2. High Memory Usage for Large Files
- **Issue**: Memory usage high for file-based QC on 50GB+ files
- **Severity**: MEDIUM
- **Workaround**: Use `--validate-in-snowflake` flag to skip file-based QC
- **Status**: Partially addressed with streaming

### 3. Limited Date Format Support
- **Issue**: Date format detection limited to specific formats
- **Severity**: LOW
- **Workaround**: Ensure dates use supported formats (YYYY-MM-DD, YYYYMMDD, MM/DD/YYYY)
- **Status**: Enhancement planned

### 4. No Automatic Log Cleanup
- **Issue**: Old log files accumulate over time
- **Severity**: LOW
- **Workaround**: Manual cleanup of logs/ directory
- **Status**: Feature request

## 🛡️ Monitoring

### Performance Concerns
- Validation of billion+ row tables performs well (~35ms)
- Parallel processing now safely managed with connection pooling
- Async COPY operations prevent timeouts on large files

### Stability Metrics
- Zero critical bugs in production since v3.0.1
- Connection pool exhaustion completely resolved
- Unicode compatibility achieved across all terminals

## 📝 Bug Reporting Guidelines

When reporting new bugs, please include:
1. **Version**: Output of `sfl --version`
2. **Operation**: Which command/operation failed
3. **Error Message**: Complete error output
4. **Config Sample**: Relevant config section (sanitized)
5. **File Details**: Size and row count if applicable
6. **Environment**: OS, Python version, Snowflake warehouse size

## 🔍 Debug Commands

For troubleshooting:
```bash
# Check system capabilities
python tsv_loader.py --check-system

# Validate configuration
sfl --config FILE.json config-validate

# Diagnose Snowflake errors
sfl --config FILE.json diagnose-error

# Check table structure
sfl --config FILE.json check-table TABLE_NAME

# View detailed logs
tail -f logs/snowflake_etl_debug.log
```

---
*Report bugs to: [GitHub Issues](https://github.com/your-repo/issues)*