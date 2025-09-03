# CHANGELOG.md

## [v3.4.12] - 2025-09-03 - Added SSL/TLS Handling for Proxy Environments

### Enhancement
- **Fixed SSL handshake errors with Snowflake through proxies**
  - Added `insecure_mode` option to disable SSL verification when needed
  - Added `ocsp_fail_open` to continue when OCSP responder is unavailable
  - Added `validate_default_parameters=False` to skip parameter validation
  - Interactive SSL configuration during proxy setup
  - Option to enable insecure mode for problematic corporate proxies

### User Experience
- Proxy configuration now asks about SSL verification mode
- Clear warnings about insecure mode risks
- Settings saved in `.insecure_mode` file for persistence
- Can also set `SNOWFLAKE_INSECURE_MODE=1` environment variable

### Technical Details
- SSL options automatically applied when proxy is detected:
  - `ocsp_fail_open=True` - Handles OCSP responder timeouts
  - `validate_default_parameters=False` - Skips strict validation
  - `insecure_mode=True` - Only when explicitly enabled by user
- Works with corporate proxies that perform SSL interception
- Maintains security by default, insecure mode requires confirmation

### Security Note
- Insecure mode should only be used in trusted corporate environments
- Disables SSL certificate verification for Snowflake connections
- Recommended to try normal mode first

## [v3.4.11] - 2025-09-03 - Added Proxy Support for Snowflake Connections

### Enhancement
- **Snowflake connector now uses proxy settings automatically**
  - Detects saved proxy configuration from `.proxy_config` file
  - Falls back to environment variables (https_proxy, http_proxy)
  - Parses proxy URL to extract host, port, username, and password
  - Configures Snowflake connections to route through corporate proxy
  - Same proxy used for PyPI, Python downloads, and now Snowflake

### Technical Details
- Added proxy fields to `ConnectionConfig` dataclass:
  - `proxy_host`, `proxy_port`, `proxy_user`, `proxy_password`, `use_proxy`
- ApplicationContext automatically detects and configures proxy
- Supports proxy URLs in format: `http://[user:pass@]host:port`
- Snowflake connector v2.7.5+ supports these proxy parameters
- Logging shows when proxy is being used for connections

## [v3.4.10] - 2025-09-03 - Relaxed Snowflake Connector Version Requirements

### Compatibility Update
- **Lowered minimum snowflake-connector-python version to 2.7.5**
  - Previously required >=3.0.0, now accepts >=2.7.5
  - Version 2.7.5 fully supports all features we use:
    - `execute_async()` for asynchronous COPY operations
    - `get_results_from_sfqid()` for keepalive and result retrieval
    - Connection pooling and cursor management
  - Helps with restricted environments where only older versions are available
  - Tested and confirmed working with version 2.7.5

### Technical Details
- Async features were added in snowflake-connector-python 2.2.0
- Version 2.7.5 is stable and widely available in corporate repositories
- No functionality loss with older version
- All ETL features remain fully operational

## [v3.4.9] - 2025-09-03 - Support Pre-Downloaded Python Packages

### Enhancement
- **Added support for using pre-downloaded Python packages**
  - Prompts user to provide path to local Python .tar.gz or .tgz file
  - User can press Enter to skip and download normally
  - Accepts absolute paths or paths with tilde (~) expansion
  - Copies the local package to temp directory for installation
  - Falls back to download if local file not found or copy fails
  - Useful for air-gapped systems or when downloads are problematic

### User Experience
- Clear prompts asking if user has a pre-downloaded package
- Validates file existence before attempting to use it
- Success message confirms when local package is being used
- Automatically proceeds with compilation after extraction

### Technical Details
- Supports any Python 3.11.x .tar.gz or .tgz package
- Package must match expected version (currently 3.11.9)
- File is copied to temp build directory to avoid modifying original
- Same build and installation process after extraction

## [v3.4.8] - 2025-09-03 - Added HTTP Fallback for Proxy Tunneling Failures

### Enhancement
- **Added HTTP fallback for proxies that block HTTPS tunneling**
  - Now tries both HTTPS and HTTP URLs when downloading Python
  - Specifically handles "proxy tunneling failed: Forbidden" errors
  - wget tries HTTPS first, then falls back to HTTP if proxy blocks tunneling
  - curl tries HTTPS, HTTP, and finally HTTP with --proxytunnel mode
  - Added `--proxy-insecure` flag for curl to handle proxy SSL issues
  - Better error messages with troubleshooting tips

### Troubleshooting Support
- Added detailed error messages explaining proxy tunneling failures
- Provides manual download instructions as last resort
- Shows which proxy is being used in error messages
- Suggests authentication format for proxies requiring credentials

### Technical Details
- Python URLs: tries both https:// and http://www.python.org/ftp/python/
- Multiple download strategies to work around restrictive proxies:
  1. HTTPS through proxy (standard)
  2. HTTP through proxy (for HTTPS-blocking proxies)
  3. HTTP with --proxytunnel (for specific proxy configurations)
- Each method gets 2 retry attempts to reduce false failures

## [v3.4.7] - 2025-09-03 - Fixed Python Download with Proper Proxy Handling

### Bug Fix
- **Fixed Python source download failures with proxy**
  - Removed problematic `eval` commands that broke with special characters in proxy URLs
  - Now uses direct command execution instead of eval
  - Added `--no-check-certificate` / `--insecure` flags for corporate proxies
  - Increased timeout and retry settings for reliability
  - Added proper error messages showing which proxy is being used
  - Both wget and curl now properly handle proxy authentication
  - Shows detailed output (2>&1) to help diagnose connection issues

### Technical Details
- wget uses: `-e use_proxy=yes -e https_proxy="${https_proxy}"` with proper quoting
- curl uses: `--proxy "${https_proxy}"` with proper quoting
- Added explicit `-O` flag for wget to specify output filename
- Added `--tries=3` and `--retry 3` for better reliability
- Timeout increased to 30 seconds for slower connections

## [v3.4.6] - 2025-09-03 - Added Proxy Support for Python Downloads

### Enhancement
- **Python installation now uses proxy settings**
  - Detects and uses saved proxy configuration from PyPI setup
  - Respects existing proxy environment variables (http_proxy, https_proxy)
  - Tests connection to python.org before attempting download
  - Offers proxy configuration if needed in interactive mode
  - wget and curl commands now properly use proxy settings
  - Fallback from wget to curl if download fails
  - Sets both http_proxy and https_proxy for compatibility

### Technical Details
- Proxy configuration is loaded at start of `install_python_311()`
- Both wget and curl are configured with appropriate proxy flags
- Same proxy used for Python download as for PyPI packages
- Ensures consistency across all network operations

## [v3.4.5] - 2025-09-03 - Fixed Unbound Variable Error

### Bug Fix
- **Fixed unbound variable error in proxy detection**
  - Line 752 was checking `$https_proxy` without proper variable expansion protection
  - With `set -u`, this caused "unbound variable" error when proxy variables weren't set
  - Fixed by using `${var:-}` syntax for all proxy variable checks
  - Ensures script doesn't fail when proxy environment variables are undefined

## [v3.4.4] - 2025-09-03 - Fixed Silent Failure Issues

### Bug Fixes
- **Fixed silent script failure when run non-interactively**
  - `load_python_path()` was returning 1 when no custom path exists, causing exit with `set -e`
  - Changed to always return 0 as missing path file is not an error condition
- **Fixed interactive prompt failures in non-TTY environments**  
  - Added TTY checks before calling `confirm_install_python()`
  - Script now skips Python installation prompts when not interactive
  - Provides clear error message when Python is missing in non-interactive mode
- **Removed unconditional exit bug in main function**
  - Lines 3722-3724 were executing `show_help` and `exit 1` after ANY CLI arguments
  - This prevented valid commands from executing properly

### Technical Details
- Script now properly handles both interactive and non-interactive execution modes
- TTY detection prevents `read` command failures in automated environments
- Function return values properly handled to work with `set -e` 
- CLI argument parsing logic corrected to only show help for invalid commands

## [v3.4.3] - 2025-09-02 - Automatic Python 3.11 Installation

### New Features
- **Automatic Python 3.11 installation when not found**
  - Offers installation if Python 3.11 missing
  - Two methods: source compilation or package manager
  - User-configurable installation base path
  - No sudo required for user directory installations
  - Automatic PATH management for custom installations

### Installation Options
- **Source Installation**
  - Downloads Python 3.11.9 from python.org
  - Compiles with optimizations (PGO)
  - Custom installation path selection
  - Saves path for automatic loading
- **Package Manager**
  - OS detection (Ubuntu, RHEL, macOS, Arch)
  - Provides appropriate installation commands
  - Guides user through manual installation

### Improvements
- Added `install_python_311()` for complete installation
- Added `confirm_install_python()` for user interaction
- Added `load_python_path()` for custom path management
- Enhanced `check_dependencies()` with installation offers
- Test script for Python detection and paths

### Technical Details
- Custom Python path saved to `.etl_state/.python311_path`
- Supports parallel builds based on CPU cores
- Fallback to non-optimized build if needed
- Comprehensive OS detection for package managers

## [v3.4.2] - 2025-09-02 - Proxy Support for Restricted Environments

### New Features
- **Automatic proxy detection and configuration**
  - Tests connectivity to PyPI before package installation
  - Detects existing proxy environment variables
  - Interactive proxy configuration if connection fails
  - Saves working proxy settings for future use
  - Supports HTTP, HTTPS, and SOCKS5 proxies
  - Handles authenticated proxies (user:pass@proxy)

### Improvements
- Added `test_pypi_connectivity()` for network detection
- Added `configure_proxy()` for interactive setup
- Added `clear_proxy_config()` utility function
- All pip commands now use configured proxy
- Better error messages for restricted environments
- Test script for proxy configuration

### Technical Details
- Proxy settings saved to `.etl_state/.proxy_config`
- Multiple connectivity test methods (curl, wget, Python)
- Automatic proxy environment variable export
- Security warnings for password storage

## [v3.4.1] - 2025-09-02 - Automatic Virtual Environment Setup

### New Features
- **Automatic Python environment setup on first run**
  - Detects Python 3.11 (preferred) or any Python 3.x
  - Creates isolated virtual environment (etl_venv/)
  - Installs all dependencies from requirements.txt
  - Activates venv automatically on each subsequent run
  - No manual Python configuration needed

### Improvements
- Added `check_dependencies()` enhancement for venv detection
- Added `setup_python_environment()` for automated setup
- Updated all Python execution to use venv
- Created VENV_SETUP.md documentation

### Technical Details
- Virtual environment location: `./etl_venv/`
- Setup marker: `.etl_state/.venv_setup_complete`
- Supports pip package caching for faster reinstalls
- Compatible with existing setup.py installation

## [v3.4.0] - 2025-09-02 - 100% Consolidation Complete 🎉

### Major Milestone Achieved
- **Successfully consolidated ALL functionality into single snowflake_etl.sh script**
- **Eliminated ALL external script dependencies**
- **Project is now 100% self-contained**

### Dependencies Eliminated
All wrapper scripts have been deprecated and moved to `deprecated_scripts/`:
- `run_loader.sh` - Loading functionality migrated to internal functions
- `drop_month.sh` - Deletion operations migrated to internal functions  
- `generate_config.sh` - Config generation migrated to internal functions
- `tsv_sampler.sh` - TSV sampling replaced with internal function
- `recover_failed_load.sh` - Recovery operations point to Python CLI

### New Internal Functions
- `sample_tsv_file()` - Replaces tsv_sampler.sh with enhanced analysis
- Recovery functions updated to use Python CLI's diagnose-error command
- All Phase 3 config generation functions tested and working

### Statistics
- **Script size**: ~3150 lines (fully self-contained)
- **Functions added**: 20+ new functions across all phases
- **Dependencies removed**: 5 external scripts
- **Consolidation**: 100% complete

### Migration Guide
Users should now use:
- `./snowflake_etl.sh` for interactive operations
- Python CLI (`python -m snowflake_etl`) for automation
- All deprecated scripts moved to `deprecated_scripts/` for reference

## [v3.3.0] - 2025-09-02 - Unified Script Consolidation Complete (95%)

### Major Achievement
- **Successfully consolidated 95% of functionality into single snowflake_etl.sh**
- Eliminated dependencies on all major wrapper scripts
- Added 18 new functions for complete self-sufficiency

### Phase 3 Completed (Config Generation)
- **Ported from generate_config.sh**:
  - `detect_file_pattern()` - Auto-detect date patterns
  - `extract_table_name()` - Extract table from filename
  - `analyze_tsv_file()` - Analyze TSV structure
  - `query_snowflake_columns()` - Query Snowflake schemas
  - `prompt_snowflake_credentials()` - Interactive credentials
  - `generate_config_from_files()` - Generate full config
  - `generate_config_direct()` - Main wrapper function

### Phase 2 Completed (Batch & Parallel Processing)
- **Added Batch Processing**:
  - `process_batch_months()` - Discover and process all months
  - `process_months_sequential()` - Sequential processing
  - `process_months_parallel()` - Parallel with job management
  - `process_multiple_months()` - Handle comma-separated lists
  
- **Added Direct Operations**:
  - `delete_month_data()` - Direct deletion without wrapper

### Dependency Elimination Summary
| Script | Before | After | Status |
|--------|--------|-------|--------|
| run_loader.sh | 11 calls | 0 | ✅ Eliminated |
| drop_month.sh | 2 calls | 0 | ✅ Eliminated |
| generate_config.sh | 1 call | 0 | ✅ Eliminated |
| recover_failed_load.sh | 2 calls | 2 | Deprecated |
| tsv_sampler.sh | 1 call | 1 | Minor dependency |

### Technical Stats
- Script size: ~3100 lines (from ~2500)
- Functions added: 18 total
- Consolidation: 95% complete
- Remaining work: Final cleanup (Phase 6)

## [v3.1.0] - 2025-09-02 - Unified Script Consolidation Phase 1

### Major Changes
- **Started consolidating all bash scripts into single snowflake_etl.sh**
  - Goal: Eliminate need for separate wrapper scripts
  - Maintain single unified entry point for all operations
  
### Phase 1 Completed
- **Added Core Functions to snowflake_etl.sh**:
  - `check_prerequisites()` - Verifies Python installation and required packages
  - `convert_month_format()` - Handles both YYYY-MM and MMYYYY formats
  - `find_month_directories()` - Discovers available months in data directory
  - `execute_python_cli()` - Direct wrapper for Python CLI execution
  - `process_month_direct()` - Processes single months without wrapper script
  - `process_direct_files()` - Handles direct file loading without wrapper
  
- **Updated Menu Functions**:
  - `quick_load_current_month()` - Now uses direct Python CLI calls
  - `quick_load_last_month()` - Now uses direct Python CLI calls
  - `quick_load_specific_file()` - Now uses direct Python CLI calls
  - `quick_load_custom_month()` - Now uses direct Python CLI calls
  - CLI mode load operations - Now use direct functions
  
### Technical Improvements
- Reduced dependency on `run_loader.sh` from 11 calls to 3
- Better error handling with prerequisite checks before operations
- More maintainable code with functions consolidated in single script
- Foundation laid for complete wrapper script elimination

### Remaining for Phase 2
- Multi-month processing with parallel execution
- Batch mode processing for all discovered months
- File browser multi-file selection handling

### Testing
- All Phase 1 functions tested and working
- Script syntax validated
- Backward compatibility maintained for complex operations

## [v3.0.7] - 2025-08-29 - Fix Config Generation to Include Snowflake Headers

### Bug Fix
- **Fixed generate_config() in CLI menu not querying Snowflake for column headers**
  - Menu now prompts for Snowflake table name
  - Automatically uses selected config file for credentials
  - Queries information_schema.columns for proper column ordering
  - Falls back to manual column entry if no config selected
  - Offers manual column specification option for offline use

### Technical Details
- Enhanced `generate_config()` function in snowflake_etl.sh
- Passes `-t TABLE_NAME` and `-c CONFIG_FILE` flags to generate_config.sh
- Extracts default table name from TSV filename pattern
- Provides three methods for column specification:
  1. Query Snowflake table (requires credentials)
  2. Manual comma-separated column names
  3. Generic column1, column2, etc. (fallback)

### User Experience Improvements
- Clear prompts explaining each option
- Warnings when credentials missing for table query
- Info messages showing which method is being used
- Preserves backward compatibility with existing workflows

## [v3.0.6] - 2025-08-27 - Remove COPY Validation to Fix Timeout Issues

### Major Changes
- **Deprecated COPY validation entirely**: Removed the redundant validation step that was causing 5-minute timeouts
  - The validation was running `COPY INTO ... VALIDATION_MODE = 'RETURN_ERRORS'` synchronously
  - Large files (>100MB compressed) would timeout after 5 minutes before keepalive could help
  - Now relies solely on `ON_ERROR = 'ABORT_STATEMENT'` during the actual COPY operation
  
### Technical Details
- Removed `_validate_data()` method from SnowflakeLoader
- Removed `VALIDATION_MODE = 'RETURN_ERRORS'` from COPY query
- Simplified `_copy_to_table()` to execute COPY directly without pre-validation
- Async COPY with keepalive still functions for files >100MB compressed
- Errors are still caught immediately by `ABORT_STATEMENT` setting

### Benefits
- Eliminates 5-minute timeout errors for large files
- Reduces processing time by avoiding redundant validation pass
- Simplifies codebase and reduces complexity
- Maintains data integrity through COPY error handling

### Migration Notes
- No configuration changes required
- Existing workflows will automatically benefit from this change
- Error detection remains unchanged - just happens during COPY instead of before

## [v3.0.5] - 2025-08-26 Session 3 - Critical Bug Fixes and Menu Enhancements

### Critical Bug Fixes
- **Fixed LoadOperation method name error**: 
  - LoadOperation was calling non-existent `check_data_quality()` method
  - Changed to use `validate_file()` - the actual method in DataQualityChecker
  - Added `_extract_validation_errors()` helper to parse validation results
- **Fixed test suite hanging issue**:
  - Removed problematic timeout command from run_test_suite()
  - Fixed arithmetic operations using $(()) instead of (())
  - Fixed string comparisons for SNOWFLAKE_AVAILABLE variable
- **Resolved remote test execution**:
  - Test suite now properly detects virtual environment
  - Uses correct Python package imports (snowflake.connector not snowflake_connector_python)
  - Tests run in offline mode when Snowflake unavailable

### New Features
- **Quality Check Selection for All Load Operations**:
  - Added `select_quality_check_method()` helper function
  - All load operations now prompt for QC method choice
  - Three options: File-based (thorough), Snowflake-based (fast), Skip (no validation)
- **Simplified Menu System**:
  - Removed redundant "Load with Validation" and "Load without QC" options
  - Replaced with unified "Load Custom Month" with QC selection
  - Consistent QC prompts across Quick Load and Normal Load menus

### Improvements
- **Better User Experience**: Clear descriptions for each QC method help users make informed choices
- **Flexible Validation**: Users can choose validation method based on their needs and constraints
- **Cleaner Code**: Eliminated duplicate functions by integrating QC selection into existing operations

## [v3.0.4] - 2025-08-26 - Test Suite and Final Fixes

### New Features
- **Comprehensive Test Suite**: Created complete testing infrastructure
  - `test_cli_suite.sh`: Tests all 20+ CLI operations
  - `test_menu_suite.sh`: Tests interactive menu navigation
  - `run_all_tests.sh`: Master test orchestrator with reporting
- **Test Data Generation**: Automatic creation of test TSV files
- **Advanced Reporting**: HTML reports, text summaries, and archives
- **Error Scenario Testing**: Includes edge cases and failure scenarios

### Bug Fixes
- **Fixed tuple unpacking error**: count_rows_fast() return value now properly unpacked
  - Was causing "unsupported format string passed to tuple.__format__"
  - Fixed in both _process_file() and analyze_files() methods

### Improvements
- **Test Coverage**: 20+ test scenarios covering all CLI functionality
- **Detailed Logging**: Multi-level logging with color-coded output
- **System Info Collection**: Captures environment details for debugging
- **Archive Generation**: Creates shareable .tar.gz of all test results

## [v3.0.3] - 2025-08-26 - CLI and File Loading Fixes

### Bug Fixes
- **Fixed month format validation**: Now accepts both YYYY-MM and MMYYYY formats in run_loader.sh
- **Fixed --yes flag error**: Removed incorrect --yes flag for load operations (only needed for delete)
- **Fixed base_path argument error**: Properly build FileConfig objects instead of passing raw arguments
- **Fixed UnboundLocalError**: Moved all imports to top of files to prevent shadowing
- **Fixed base path prompting**: Load menu option 2 now properly prompts for base path
- **Fixed direct file loading**: Added --files argument to handle specific files without pattern matching
- **Fixed quoted file paths**: Removed incorrect quotes around --files argument
- **Fixed tuple format error**: Convert date ranges to datetime objects instead of strings in FileConfig

### Improvements
- **Better error messages**: Added clearer format hints (YYYY-MM) in prompts
- **Import cleanup**: Consolidated all imports at file tops for consistency
- **Code organization**: Separated direct file handling from pattern-based file discovery

## [v3.0.2] - 2025-08-26 - Report Display Fixes

### Bug Fixes
- **Fixed truncated report displays**: Report was showing "... and X more" instead of full lists
  - Now displays all missing dates (up to 100 as limited by validator)
  - Shows all anomalous dates with complete details
  - Lists all date gaps without truncation
- **Fixed gap range display**: Was showing "Unknown to Unknown (0 days)"
  - Added null checks to skip invalid gaps
  - Corrected field name mapping (start_date/end_date vs gap_start/gap_end)
- **Fixed percentage calculations**: Was showing "0.0% of average" for all anomalies
  - Corrected field name (percent_of_average vs pct_of_avg)
  - Added null handling and division by zero protection
  - Now correctly shows percentages like "198.6% of average"

### Improvements
- Report now provides complete actionable data for investigating issues
- No information is hidden behind "... and more" truncation
- All field names properly aligned between validator and report display

## [v3.0.1] - 2025-08-26 - Critical Fixes & Enhanced Reporting

### Bug Fixes
- **Fixed Unicode encoding issues**: Removed all Unicode characters (✓, ✗, ⚠, etc.) that caused latin-1 encoding errors in terminals
- **Fixed connection pool exhaustion**: 
  - Added `validate_table_with_cursor()` method to reuse existing connections
  - Increased default pool size from 5 to 10 (configurable)
  - Added automatic worker limiting based on pool size
- **Fixed blank log issue for foreground jobs**: Implemented proper log capture using stdbuf and exit status handling
- **Fixed CLI argument order**: Global options like `--config` now correctly come before subcommands
- **Fixed ReportOperation initialization**: Added missing `status` parameter in TableReport instantiation
- **Fixed ApplicationContext config reference**: Changed from `config_data` to `_config`

### Enhanced Features
- **Comprehensive validation reporting**:
  - Now displays actual problematic dates, not just counts
  - Groups anomalies by severity (SEVERELY_LOW, OUTLIER_LOW, etc.)
  - Shows expected row count ranges for context
  - Lists date gaps with exact ranges and missing day counts
  - Provides actionable tips for common patterns
- **Automatic output file generation**:
  - CLI operations now auto-generate detailed output files
  - Reports saved to `reports/` directory with timestamps
  - Both text and JSON formats for maximum utility
  - Validation creates additional `_issues.txt` file with all problematic dates
- **Full data in reports**:
  - Fixed report to preserve complete validation details
  - Shows actual anomalous dates with row counts and percentages
  - Displays complete gap information
  - Lists all missing dates for investigation

### Performance Improvements
- Connection pool management prevents exhaustion during parallel operations
- Unbuffered output for real-time job progress display
- Dynamic worker limiting based on available connections

## [v3.0.0] - 2025-08-23 - Final Migration & Complete Refactoring

### Major Migration Completed
- **Fixed broken v3.0.0 package**: Created missing files (config_manager_v2.py, snowflake_connection_v3.py)
- **Completed migration from monolithic scripts to modular package**
- **All shell scripts now use new `snowflake_etl` package**
- **Unified CLI with comprehensive subcommands**

### New Features
- **Unified CLI**: All operations through `sfl` command
- **Configuration Management**: 
  - `sfl config-generate` - Generate configs from TSV files
  - `sfl config-validate` - Validate configuration files
  - `sfl config-migrate` - Migrate configs between versions
- **Utility Subcommands**:
  - `sfl check-table` - Check Snowflake tables
  - `sfl diagnose-error` - Diagnose COPY errors
  - `sfl validate-file` - Validate TSV files
  - `sfl check-stage` - Manage stage files
- **Connection Management**: Production-ready SnowflakeConnectionManager with pooling and async support
- **Standardized Logging**: Unified ETLLogger for consistent logging across all components

### Breaking Changes
- `tsv_loader.py` deprecated → use `sfl load`
- `drop_month.py` deprecated → use `sfl delete`
- Standalone utility scripts moved to subcommands
- Configuration now requires `--config` flag only for operations that need it

### Technical Improvements
- Proper package entry points (snowflake-etl, sfl, sfe)
- Dependency injection architecture fully implemented
- Connection pooling with configurable size
- Thread-safe connection management
- Async query support for large operations
- Context-aware logging with operation tracking

### Files Deprecated/Removed
- Moved to deprecated_scripts/: check_snowflake_table.py, diagnose_copy_error.py, validate_tsv_file.py, etc.
- Backed up: tsv_loader.py → deprecated_tsv_loader.py.bak
- Backed up: drop_month.py → deprecated_drop_month.py.bak
- Consolidated duplicate test files

## [v3.0.0-rc2] - 2025-01-23 - Project Cleanup & Documentation Update

### Project Cleanup (Session 8)

#### Files Removed (44 total):
- **Old Documentation & Planning**: 
  - Removed temporary planning documents (CONTEXT_HANDOVER.md, REFACTORING_PLAN.md, etc.)
  - Removed implementation plan files (INTERACTIVE_FILE_BROWSER_PLAN*.md, FULL_TABLE_REPORT_PLAN.md)
  - Removed session notes (NEXT_SESSION_PROMPT.md, gemini_loader_design.md, migration_mapping.md)

- **Test Scripts & Debug Files**:
  - Consolidated all tests into tests/ directory
  - Removed standalone test scripts (test_*.py, test_*.sh)
  - Removed debug files (debug_patch.txt, menu_output.txt, snowflake_etl_debug.sh)
  - Removed patch files (snowflake_etl_v3_updates.patch)

- **Duplicate Module Versions**:
  - Cleaned up versioned modules keeping only final versions:
    - Removed: application_context_documented.py, application_context_final.py
    - Removed: snowflake_loader_optimal.py, load_operation_documented.py
    - Removed: report_operation_final.py, report_operation_v2.py
    - Removed: All old config_manager versions (v1, v2)
    - Removed: All old snowflake_connection versions (v1, v2, v3)

- **Other Cleanup**:
  - Removed test_venv/ directory (no longer needed)
  - Removed Python cache directories (__pycache__, egg-info)
  - Removed duplicate setup files (setup_gemini.py, setup_original.py)
  - Removed duplicate manifest files (MANIFEST_gemini.in)
  - Removed unused requirements file (requirements-ci.txt)
  - Removed old SQL files (duplicate_check_queries.sql)
  - Removed deprecated scripts (etl_cli.sh)

#### Final Project Structure:
- **41 files** in root directory (down from 85+)
- **46 Python files** total (consolidated and organized)
- **9 Shell scripts** total (essential tools only)
- Clean package structure with no duplicate versions

#### Essential Files Preserved:
- All core Python modules in snowflake_etl/
- All production shell scripts (snowflake_etl.sh, run_loader.sh, drop_month.sh, etc.)
- All standalone Python tools (tsv_loader.py, drop_month.py, etc.)
- Configuration files and templates
- Core documentation (README, CHANGELOG, TODO, PLAN, CLAUDE, BUGS)
- Complete test suite in tests/ directory
- Library scripts in lib/
- Setup and requirements files

## [v3.0.0-rc1] - 2025-01-23 - Phase 5: Testing & Documentation COMPLETE

### Session 6-7 (2025-01-23) - Phase 5 Complete - FINAL

#### Package Distribution Setup:
- **Created optimized setup.py** combining best practices from both implementations
- **Consolidated dependencies** - eliminated redundant requirements files
- **Granular extras_require** - separate groups for test, docs, lint, performance
- **Simplified requirements-dev.txt** - now just installs package with [dev] extras
- **Added MANIFEST.in** with clean rules using prune and global-exclude
- **PEP 440 compliant versioning** - changed to 3.0.0a1

#### Testing Infrastructure:
- **Created comprehensive test suite structure**:
  - conftest.py with reusable fixtures
  - test_core_operations.py with tests for all operations
  - test_application_context.py for DI container testing
  - test_core_operations_improved.py with edge cases and sad paths
- **Implemented Gemini's feedback**:
  - Added parameterized tests using pytest.mark.parametrize
  - Added tests for error conditions and edge cases
  - Improved mock isolation
  - Added user interaction tests with patched input
- **Test coverage includes**:
  - Happy path and sad path scenarios
  - File system edge cases (empty, malformed, encoding issues)
  - Snowflake connection failures
  - User confirmation flows
  - Thread safety and concurrency

#### Documentation Improvements:
- **Created comprehensive docstrings** following Google/NumPy style:
  - ApplicationContext with full DI explanation
  - LoadOperation with performance characteristics
  - Detailed parameter and return value documentation
  - Practical examples for all major use cases
- **Optimized documentation** based on Gemini's feedback:
  - More specific error documentation
  - Better context in examples
  - Clearer error handling strategies
  - Balanced verbosity for clarity

#### Integration Testing:
- **Created comprehensive integration test suite**:
  - End-to-end pipeline testing
  - Parallel processing verification
  - Error handling and recovery scenarios
  - Thread safety validation
- **Created CLI test suite**:
  - Argument parsing for all subcommands
  - Command execution testing
  - Help text verification
  - Error handling validation

#### Final Status:
- **Version**: 3.0.0-rc1 (Release Candidate 1)
- **All Phases Complete**: 1-5 fully implemented
- **Test Infrastructure**: Unit, integration, and CLI tests ready
- **Documentation**: Comprehensive reference docs with examples
- **Package Distribution**: pip-installable with proper dependencies
- **README**: Brand new comprehensive documentation created

#### Session Accomplishments Summary:
1. **Completed Phase 5** with all testing and documentation
2. **Created professional README.md** from scratch
3. **Implemented Gemini's feedback** on all components
4. **Ready for production deployment** as v3.0.0-rc1

## [v3.0.0-alpha] - 2025-01-23 - Phase 4: Shell Script Consolidation COMPLETE

### Session 5 (2025-01-23) - Phase 4 Completion

#### Critical Bug Fixes:
- **FIXED duplicate show_menu() function** (lines 476-573 removed)
- **FIXED detect_ui_system()** to check for TTY before using dialog
- **Updated version to 3.0.0** in main script
- **All UI functions now properly sourced from lib/**

#### Python CLI Integration:
- Updated validate operations to use `python -m snowflake_etl validate`
- Updated duplicate checks to use `python -m snowflake_etl check-duplicates`
- Updated compare operations to use `python -m snowflake_etl compare`
- Updated report generation to use `python -m snowflake_etl report`
- Kept legacy script calls for tools not yet migrated

#### Testing Results:
- Interactive menu works perfectly with piped input
- Text mode fallback functioning correctly
- All deprecation warnings displaying properly
- Python CLI responding correctly to all commands

#### Architecture State:
- **Dependency Injection**: Fully implemented across all operations
- **Library Consolidation**: All shell functions extracted to lib/
- **Unified CLI**: Working with all major operations
- **Test Environment**: test_venv/ contains snowflake-connector-python
- **Version**: Official 3.0.0 release (alpha tag removed)

#### Ready for Phase 5:
- Package structure complete and tested
- All critical bugs resolved
- Both CLI and interactive menu functional
- Documentation updated

### Session 4 (2025-01-22) - Phase 4 Started

#### Phase 4 Initial Work:
- **Library Extraction Complete**:
  - Created `lib/colors.sh` with all color definitions
  - Created `lib/ui_components.sh` with menu/dialog functions
  - Created `lib/common_functions.sh` with utility functions
  - All libraries properly export their functions

- **Deprecation Warnings Added**:
  - run_loader.sh shows deprecation notice
  - drop_month.sh shows deprecation notice
  - recover_failed_load.sh shows deprecation notice

## [v3.0.0-alpha-phase3] - 2025-01-22 - Phase 3 Complete: All Operations with Security Fixes

### Session 3 Accomplishments (Today)

#### Phase 3 Completed:
- **DuplicateCheckOperation**: Full implementation with severity assessment
  - Supports batch checking across multiple tables
  - Configurable severity thresholds
  - Sample duplicate reporting
  
- **CompareOperation**: Comprehensive file comparison
  - Line ending detection (LF/CRLF/Mixed)
  - Delimiter detection (tab/comma/pipe)
  - Encoding detection with chardet
  - File structure validation
  
- **__main__.py Entry Point**: Complete CLI implementation
  - All operations integrated (load, delete, validate, report, check-duplicates, compare)
  - Proper argument parsing and validation
  - Month to date range conversion
  
#### Security & Performance Improvements:
- **SQL Injection Prevention**: ALL queries use IDENTIFIER(%s) for table/column names
- **Optimized Connection Pooling**: 
  - Rejected Gemini's suggestion to open/close per table
  - Our approach: ~25x faster (4 connections for 100 tables vs 100)
  - Thread-safe with proper cleanup
  
- **Formatter Strategy Pattern**:
  - TextReportFormatter
  - JsonReportFormatter  
  - CsvReportFormatter
  - Factory pattern with registration
  
- **Configurable Severity**: SeverityConfig dataclass for customizable thresholds

#### Testing Infrastructure:
- Created test_venv with Snowflake connector installed
- Comprehensive integration tests created and passing
- Validated all security fixes and architectural patterns
- Tests run successfully with real Snowflake connector

#### Key Design Decisions:
- **Performance Over Caution**: Challenged Gemini's overcautious connection approach
- **Clean Architecture**: All operations inherit from BaseOperation
- **Proper Error Handling**: Comprehensive error handling throughout
- **No Silent Failures**: All operations log and report errors properly

## [v3.0.0-alpha] - 2025-01-22 - Major Refactoring: Dependency Injection Architecture

### Breaking Changes
- Complete architectural overhaul from singleton pattern to dependency injection
- Moved from multiple independent scripts to unified CLI entry point
- Restructured codebase into proper Python package (`snowflake_etl/`)

### Session 2 Accomplishments (2025-08-22)

#### Completed Operations:
- **ReportOperation**: Comprehensive table report generation
  - Parallel processing with ThreadPoolExecutor
  - Multiple output formats (JSON/text)
  - Detailed validation and anomaly detection
  - Summary statistics and issue categorization

#### Gemini Collaboration Results:
- Identified key improvements needed:
  - SQL injection vulnerability in table name queries
  - Connection pooling inefficiency in parallel processing
  - Need for formatter strategy pattern
  - Hardcoded severity mapping should be configurable
- Documented solutions for next session implementation

#### Testing Success:
- All architecture tests passing without Snowflake connector
- Separated dataclasses to avoid import dependencies
- Full validation of dependency injection pattern

### Phase 2 Completion Summary

#### ✅ Completed Components:
1. **SnowflakeLoader** - Fully refactored with dependency injection
2. **SnowflakeDataValidator** - Extracted with ValidationResult dataclass
3. **LoadOperation** - Orchestrates complete ETL pipeline
4. **DeleteOperation** - Handles safe data deletion
5. **ValidateOperation** - Comprehensive data validation
6. **CLI Implementation** - Complete with all subcommands
7. **Testing Suite** - Architecture tests passing

#### 🎯 Key Achievements:
- **Dependency Injection**: All components use ApplicationContext
- **Separation of Concerns**: Clean boundaries between components
- **Configuration Management**: Externalized via dataclasses
- **Progress Tracking**: Abstract interface with multiple implementations
- **Error Handling**: Comprehensive with proper propagation
- **Testing**: Full test coverage without Snowflake dependencies

### Latest Progress (Session 2 - COMPLETED)
- **Completed SnowflakeLoader Extraction**: 
  - Created three versions: initial, Gemini-inspired improvements, and optimal merged version
  - Implemented LoaderConfig dataclass for externalized configuration
  - Changed to UUID-based stage management with guaranteed cleanup
  - Consistent pathlib usage internally with flexible str/Path input
  - Pure logging approach (no print statements)
  
- **Completed SnowflakeDataValidator Extraction**:
  - Full dependency injection implementation
  - ValidationResult dataclass for structured results
  - Comprehensive anomaly detection and duplicate checking
  - Consistent with ApplicationContext pattern
  
- **Created LoadOperation Orchestrator**:
  - Coordinates complete ETL pipeline
  - Inherits from BaseOperation for context access
  - Supports all loading modes (skip QC, validate in Snowflake, validate only)
  - Returns structured results dictionary

### Added - Core Architecture
- **ApplicationContext**: Central context manager for shared resources
  - Manages connection pool, configuration, logging, and progress tracking
  - Resources created once and injected into operations
  - Proper lifecycle management with cleanup
  
- **Unified CLI** (`cli/main.py`):
  - Single entry point for all operations
  - Subcommands: load, delete, validate, report, check-duplicates, compare
  - Replaces individual script invocations
  
- **Progress Tracking Abstraction**:
  - Abstract `ProgressTracker` interface
  - Multiple implementations: NoOp, Logging, Tqdm, Parallel
  - No more bash parallelism complexity (TSV_JOB_POSITION removed)
  - Clean separation between progress reporting and display

### Added - Package Structure
```
snowflake_etl/
├── core/
│   ├── application_context.py  # Dependency injection container
│   ├── progress.py            # Progress tracking abstractions
│   └── file_analyzer.py       # File analysis utilities
├── utils/
│   ├── snowflake_connection_v3.py  # Non-singleton connection manager
│   ├── config_manager_v2.py        # Config with lru_cache
│   └── logging_config.py           # dictConfig-based logging
├── models/
│   └── file_config.py          # Data models
├── validators/
│   └── data_quality.py         # Quality checkers
├── ui/
│   └── progress_bars.py        # Visual progress implementations
└── cli/
    └── main.py                 # Unified entry point
```

### Refactored Components
- **SnowflakeConnectionManager V3**:
  - Removed singleton pattern
  - Uses native Snowflake connection pooling
  - Injected via ApplicationContext
  
- **ConfigManager V2**:
  - Efficient caching with `functools.lru_cache`
  - Automatic cache invalidation on file changes
  - Environment variable overrides
  
- **Logging Configuration**:
  - Declarative configuration using `dictConfig`
  - Operation-specific log files
  - Performance metrics logging

### Migration from v2.x
- Old: `python3 tsv_loader.py --config config.json --month 2024-01`
- New: `python3 -m snowflake_etl.cli.main --config config.json load --month 2024-01`

### Benefits
- **Performance**: Connection pool reused across operations (not recreated)
- **Testing**: Easy dependency injection for mocks
- **Clarity**: Explicit dependencies, no hidden global state
- **Flexibility**: Can run multiple operations in single process
- **Maintainability**: Clean separation of concerns

## [v2.12.0] - 2025-01-22 - Comprehensive Table Report Generation

### Added
- **Generate Full Table Report**: New feature under Snowflake Operations menu
  - Analyzes all tables across all configuration files
  - Generates comprehensive statistics and validation results
  - Reuses existing SnowflakeDataValidator for consistency
  - Parallel processing with ThreadPoolExecutor for performance
  - Supports filtering by config file or table name patterns
  
### Features
- **Table Statistics**: Row count, column count, date ranges, avg rows/day
- **Validation Checks**: Date completeness, gaps, anomalies, duplicates
- **Smart Credential Loading**: Searches multiple locations for credentials
- **Detailed Reporting**: Groups results by config, shows validation issues
- **Performance**: Processes multiple tables concurrently
- **Error Handling**: Distinguishes between table not found vs SQL errors

### Technical Implementation
- `generate_table_report.py`: Core report generation script
- Reuses `SnowflakeDataValidator` from tsv_loader.py
- Single Snowflake connection reused for all tables
- Comprehensive summary at end of job logs for easy viewing

## [v2.11.0] - 2025-01-22 - Interactive File Browser with Config Validation

### Added
- **Interactive TSV File Browser**: Curses-based TUI for visual file selection
  - Navigate directories with arrow keys
  - Multi-file selection with spacebar
  - Real-time search/filter with '/' key
  - File preview with 'p' key
  - Sort by name, size, date, or type
  - Handles special characters in filenames safely
  - Shows file sizes and modification times
  
- **Automatic Config Validation**: Smart config matching and suggestions
  - Validates selected files against current config
  - Suggests matching configs when mismatches detected
  - Offers to switch configs automatically
  - Generates config skeletons for unmatched files
  
- **Performance Optimizations**:
  - Efficient directory scanning with os.scandir()
  - Config caching - loads once at startup
  - Directory content caching with TTL
  - Handles 90,000+ files/second scanning speed
  
### Enhanced
- Load Data menu now offers three methods:
  1. Interactive file browser (new)
  2. Traditional base path and month
  3. Load all months from base path
  
### Technical Implementation
- `tsv_file_browser.py`: Core Python module with curses UI
- `tsv_browser_integration.py`: Config validation helper
- Integrated with existing `snowflake_etl.sh` wrapper
- Proper handling of paths with spaces and special characters
- No silent error suppression - all errors logged

### Testing
- Comprehensive test suite (`test_file_browser.sh`)
- Tests special characters, large directories, performance
- Validates config matching and batch operations

## [v2.10.4] - 2025-01-22 - Fix Script Exit on Clean Jobs

### Fixed
- Fixed script completely exiting when "Clean Completed Jobs" selected
- Added nullglob handling for when no job files exist
- Fixed arithmetic operations that could fail with set -e
- Added explicit return 0 to prevent unexpected exits

### Improved
- Safer glob expansion with nullglob
- Better handling of empty jobs directory
- More robust arithmetic with $(()) instead of (())
- Explicit return codes for function stability

## [v2.10.3] - 2025-01-22 - Fix Menu Indexing Bug (With Gemini's Help)

### Fixed
- **CRITICAL**: Fixed menu indexing bug that caused wrong functions to be triggered
- Clean Completed Jobs was opening log viewer due to array index mismatch
- Menu separators ("---") were causing off-by-one errors in option selection
- Created clean array without separators for proper menu indexing

### Technical Details
- show_menu() displays options skipping "---" separators
- But array indexing was including separators, causing misalignment
- Solution: Build clean_options array matching what user actually sees
- Thanks to Gemini for identifying the root cause!

## [v2.10.2] - 2025-01-22 - Fix Clean Jobs Triggering Log Viewer

### Fixed
- Fixed critical bug where "Clean Completed Jobs" was opening log viewer instead of cleaning
- Removed complex subshell execution that was causing misinterpretation
- Simplified clean_completed_jobs function to use direct file operations

### Improved
- More reliable job cleaning without complex bash -c commands
- Cleaner implementation without nested with_lock calls
- Direct file operations prevent command misinterpretation

## [v2.10.1] - 2025-01-22 - Auto-refresh Job Status Menu After Cleaning

### Fixed
- Job Status menu now automatically refreshes after cleaning completed jobs
- Cleaned jobs immediately disappear from the menu without manual refresh
- Better user experience with automatic menu update

### Improved
- Cleaning workflow is now seamless - clean and see results instantly
- No need to manually select "Refresh" after cleaning

## [v2.10.0] - 2025-01-22 - Complete Removal of Unicode/Emoji Characters

### Changed
- **CRITICAL**: Removed ALL Unicode/emoji characters from entire codebase
- Replaced checkmarks (✓) with [VALID] or [OK] or [PASS]
- Replaced X marks (✗) with [INVALID] or [ERROR] or [FAIL]
- Replaced warning symbols (⚠️) with WARNING text
- Replaced bullets (•) with hyphens (-)
- Replaced arrows (→) with ->
- Updated all Python files (tsv_loader.py, compare_tsv_files.py, validate_tsv_file.py, check_stage_and_performance.py)
- Updated all shell scripts (snowflake_etl.sh, run_loader.sh, tsv_sampler.sh)

### Fixed
- Fixed Unicode encoding errors when running in latin-1 terminals
- Resolved "ordinal not in range(256)" errors during validation
- All output now uses pure ASCII characters for maximum compatibility

### Why This Matters
- Many enterprise terminals use latin-1 or other limited encodings
- Unicode characters cause crashes and unreadable output
- ASCII-only output ensures compatibility across all systems

## [v2.9.3] - 2025-01-22 - Fixed Clean Completed Jobs Function

### Fixed
- Fixed "Clean Completed Jobs" button not properly removing jobs from status menu
- Resolved issue where cleaned count was not captured from subshell
- Jobs are now properly removed and user gets clear feedback

### Improved
- Added support for cleaning both completed and failed/crashed jobs
- Better feedback showing number of each type cleaned
- Log files are preserved for debugging (can be optionally deleted)
- Handles empty result cases gracefully

## [v2.9.2] - 2025-01-22 - Fixed ANSI Color Code Display Issues

### Fixed
- Fixed raw escape codes (\033[) appearing in job progress headers
- Added -e flag to echo statements to properly interpret ANSI color codes
- Job monitoring, log viewing, and status displays now show colors correctly

### Improved
- Cleaner visual output with properly formatted colored text
- Better readability of job status messages and headers

## [v2.9.1] - 2025-01-22 - Enhanced Validation to Support ALL Data

### Added
- Validate Data menu now accepts empty input to validate ALL data in tables
- No date filtering when month is not specified - comprehensive validation of entire table
- Consistent with other menu options that treat empty input as "ALL"

### Fixed
- Fixed AttributeError when expected_date_range contains None values
- Updated validation functions to handle None start/end dates properly
- Query WHERE clauses now conditionally apply date filters

### Improved
- Better user prompts indicating "leave empty for ALL data" option
- Job names reflect whether validating specific month or all data
- More flexible validation allowing complete table quality checks

## [v2.9.0] - 2025-01-22 - Menu Reorganization and Screen Clearing Fix

### Changed
- Renamed "Data Operations" to "Snowflake Operations" for clarity
- Moved "Compare Files" from Snowflake Operations to File Tools menu where it belongs logically
- Better menu organization with related functions grouped together

### Fixed
- Log viewer now clears terminal screen after viewing to prevent log stacking
- Multiple log views no longer appear below each other causing confusion
- Added `clear` command after pager exits for clean viewing experience

### Improved
- Better user experience when viewing multiple job logs sequentially
- Cleaner interface with no residual log content between views
- More intuitive menu structure with Snowflake-specific operations clearly labeled

## [v2.8.1] - 2025-01-22 - Robust Log Viewer with Comprehensive Error Handling

### Fixed
- **Critical**: Fixed unbound variable error (GRAY not defined) that crashed entire script
- Fixed blank black screen issue when viewing logs
- Removed screen clearing that caused jarring user experience
- Fixed handling of permission denied errors

### Improved
- Complete rewrite of log viewer for maximum reliability
- Added comprehensive error checking (missing path, non-existent file, permissions, empty file)
- Implemented intelligent pager selection (less > more > cat fallback)
- Used less -RFXS flags to prevent screen clearing and handle colors properly
- Added proper return codes and error messages with pauses
- Simplified UI to prevent fragility - removed complex headers
- Added robust test suite covering all edge cases

### Added
- GRAY color variable definition
- Comprehensive test script test_log_viewer_robust.sh
- Permission checks before attempting to read files
- Fallback to 'more' and 'cat' if 'less' not available

## [v2.8.0] - 2025-01-22 - Persistent Log Viewer Implementation

### Added
- **Persistent Log Viewer**
  - Uses 'less' pager for log viewing (stays open until user quits)
  - Clear header with job name and status
  - Navigation hints (q to quit, / to search, g/G for navigation)
  - Color preservation with --RAW-CONTROL-CHARS flag
  - Auto-quit for small logs with --quit-if-one-screen
  - Test script test_log_viewer.sh for verification

### Fixed
- Job log viewing now persists instead of disappearing immediately
- Log viewer properly handles empty and missing log files
- Improved user experience with persistent viewing capability

## [v2.7.0] - 2025-01-22 - Dynamic UI Sizing & Complete UX Overhaul

### Major UI/UX Improvements
- **Dynamic Dialog Sizing**
  - Dialogs automatically size based on content
  - Terminal-aware sizing (fits within boundaries)
  - Scrollable view for very long content (>2000 chars)
  - Maximum limits: 40 height, 120 width
  
### Job Management Enhancements  
- **Result Display (v2.6.0)**
  - Shows actual results for completed jobs
  - Error details for failed jobs
  - Full log viewing via menu
  - Proper output capture and display
  
### Black Screen Fixes (v2.5.0)
- All operations use job management system
- Created check_duplicates_interactive.py
- Foreground/background execution choice
- Real-time progress visibility

### Smart Table Selection (v2.4.0)
- Auto-detect tables from config files
- Intelligent selection based on config content
- Context-aware prompts with table names

### Bug Fixes
- Fixed character encoding in job status display (v2.5.1)
- Resolved black screen during operations
- Fixed truncated results in dialogs
- Corrected job summary display issues

## [v4.3.0] - 2025-01-22 - All Operations Use Job Management

### Job Management Improvements
- **Unified Wrapper v2.5.0**
  - Fixed black screen issue during Snowflake operations
  - Updated check_duplicates() to use job management system
  - Updated menu_validate_data() to use job management system
  - Updated check_table_info() to use job management system
  - All operations now offer foreground/background execution choice
  - Created check_duplicates_interactive.py for progress feedback
  
### User Experience
- No more black screens during long operations
- Real-time progress visible when selecting foreground mode
- Background jobs can be monitored via Job Status menu
- Consistent execution model across all operations

## [v4.2.0] - 2025-01-22 - Smart Table Selection from Config

### UI/UX Improvements
- **Unified Wrapper v2.4.0**
  - Added automatic table detection from config files
  - Implemented smart table selection with menu when multiple tables exist
  - Auto-selects table when config contains only one table
  - Allows custom table entry when needed
  - Special handling for 'all tables' option where appropriate
  
### Enhanced User Experience
- No longer prompts for table name if config specifies a single table
- Shows table selection menu when config contains multiple tables
- Provides context in prompts (e.g., "Enter month for table: TABLE_NAME")
- Reduces manual input and potential errors
- Maintains backward compatibility with configs without table specifications

## [v4.1.0] - 2025-01-22 - Unified Wrapper Complete Implementation

### Major Enhancements
- **Unified Wrapper v2.1.0**
  - Implemented all placeholder recovery functions
  - Added parameterized duplicate checking with custom key columns
  - Integrated stage file management and cleanup
  - Added VARCHAR error recovery automation
  - Implemented clean file generation for problematic TSVs
  
### Recovery Tools Implementation
- **fix_varchar_errors()**: Automated cleanup and retry for VARCHAR date format issues
- **recover_from_logs()**: Extract and diagnose errors from job logs
- **clean_stage_files()**: Interactive and batch stage file cleanup
- **generate_clean_files()**: Create sanitized versions of problematic TSV files

### Duplicate Detection Improvements
- Parameterized check_duplicates function accepts:
  - Custom table names
  - Configurable key columns
  - Optional date ranges or specific months
  - Detailed statistics and sample output

### Integration Enhancements  
- Full integration with recover_failed_load.sh
- Direct usage of check_stage_and_performance.py
- Inline Python execution for complex operations
- Improved error handling and user feedback

### Documentation Updates
- Updated UNIFIED_WRAPPER_SUMMARY.md with v2.1.0 features
- Added detailed function descriptions
- Updated TODO.md with completed tasks
- Enhanced troubleshooting guide

## [v4.0.0] - 2025-01-22 - Unified Interface & Security Hardening

### Major Changes
- **Unified Wrapper Script (`snowflake_etl.sh`)**
  - Single entry point for all ETL operations
  - Interactive menu system with breadcrumb navigation
  - CLI mode for automation
  - Background job management with state tracking

- **Duplicate Detection System**
  - Efficient ROW_NUMBER() based detection
  - Configurable composite keys
  - Severity assessment and sampling

- **Security Improvements**
  - Eliminated eval/source vulnerabilities
  - Robust flock locking
  - Input validation throughout

### Performance Fixes
- Fixed file comparison hanging on 12GB+ files
- Added --quick sampling mode
- Optimized duplicate detection for billion-row tables

## [v3.1.0] - 2025-08-21 - Async COPY and Performance Optimizations

### Major Performance Improvements
- **Fixed "ping pong" issue with long-running COPY operations**
  - Added async execution for files >100MB compressed
  - Implemented keepalive mechanism (every 4 minutes) to prevent query cancellation
  - Added status polling with user feedback every 30 seconds
  - Proper error handling with get_query_status_throw_if_error()

- **Resolved slow COPY performance (was taking 1+ hour for 700MB files)**
  - Changed ON_ERROR from 'CONTINUE' to 'ABORT_STATEMENT'
    - CONTINUE was causing row-by-row processing on errors (extremely slow)
    - ABORT_STATEMENT fails fast on first error
  - Added PURGE=TRUE to auto-cleanup stage after successful loads
  - Proper validation with error abort on failures
  - Performance improvement: 1+ hour → 5-15 minutes for 700MB files

### Added Features
- **Warehouse size detection and warnings**
  - Automatically checks current warehouse size on connection
  - Warns if using X-Small/Small warehouse for large files
  - Provides ALTER WAREHOUSE command suggestions

- **Automatic stage cleanup**
  - Removes old stage files before uploading new ones
  - Prevents accumulation of failed/duplicate files
  - Reduces potential for confusion with multiple file versions

- **Diagnostic tool** (`check_stage_and_performance.py`)
  - Lists all files in Snowflake stages with sizes
  - Analyzes recent COPY query performance history
  - Identifies slow queries and bottlenecks
  - Checks warehouse configuration
  - Provides optimization recommendations
  - Interactive stage cleanup option

### Technical Improvements
- Set ABORT_DETACHED_QUERY=FALSE at session level for async support
- Use execute_async() method for large files
- Implement proper polling with is_still_running() and get_query_status()
- Call get_results_from_sfqid() as keepalive to prevent 5-minute timeout
- Backwards compatible - files <100MB still use synchronous execution

### Documentation Updates
- Updated README with performance troubleshooting section
- Added diagnostic tool documentation
- Updated performance benchmarks with optimization results
- Added async execution details to CLAUDE.md

### Cleanup
- Removed unnecessary test files (test*.py)
- Removed old documentation (DROP_MONTH*.md, CONTEXT_HANDOVER.md)
- Removed test virtual environment
- Removed unused image files

## [v3.0.0] - 2025-08-21 - Data Deletion Capability

### Added - New drop_month.py Tool
- **Production-ready data deletion tool** for safely removing monthly data from Snowflake tables
  - Parameterized queries to prevent SQL injection attacks
  - Multi-layer safety features (dry-run, preview, confirmation prompts)
  - Transaction management with automatic rollback on errors
  - Metadata caching for efficient validation
  - Context managers for proper connection lifecycle
  - Comprehensive audit logging with recovery timestamps
  - Support for multiple tables and months in single operation
  - JSON report generation for audit trails

### Security Improvements
- **SQL Injection Prevention**: All queries use parameterized bindings
- **Metadata Validation**: Table and column names validated against cached schemas
- **Resource Management**: Context managers ensure connections are always closed
- **Specific Exception Handling**: Catches ProgrammingError instead of generic Exception

### Architecture Enhancements
- **Separation of Concerns**: Three focused classes instead of monolithic design
  - SnowflakeManager: Connection lifecycle management
  - SnowflakeMetadata: Cached metadata operations  
  - SnowflakeDeleter: Business logic with transactions
- **Performance Optimizations**:
  - Metadata cached after first query (O(1) instead of O(n))
  - Single analysis pass instead of redundant double execution
  - Efficient batch processing with progress tracking

### Documentation Updates
- Added comprehensive implementation comparison document
- Security best practices for destructive operations
- Recovery procedures using Snowflake Time Travel
- Updated README and CLAUDE.md with deletion tool usage

### Development Process
- Implemented initial version with comprehensive features
- Critical review identified SQL injection vulnerability
- Created improved version with security-first design
- Compared implementations to identify best practices
- Merged best features into production-ready version

## [2025-08-21] - Session Summary: Comprehensive Validation Enhancements

### Major Accomplishments
1. **Fixed Critical Bug**: Resolved IndentationError causing all validation tests to fail
2. **Enhanced Validation System**: Added row count anomaly detection
3. **Improved User Experience**: Clear failure explanations and progress bars
4. **Batch Processing**: Comprehensive validation summary at end of runs
5. **Documentation**: Created handover docs for next session

## [2025-08-21] - Comprehensive Batch Validation Summary

### Feature Enhancement
- Added COMPREHENSIVE VALIDATION RESULTS section after batch processing
- Shows aggregated statistics across all months
- Lists all failed validations with specific reasons
- Works for both --validate-only and --validate-in-snowflake modes
- Validation results saved to JSON for both modes

### Display Format
```
OVERALL STATISTICS:
  Total Tables Validated: 12
  ✓ Valid Tables:        8
  ✗ Invalid Tables:      4
  ⚠ Total Anomalous Dates: 25

FAILED VALIDATIONS:
  • [2024-05] TABLE_2: 3 date(s) with critically low row counts
```

## [2025-08-21] - Adjusted Outlier Thresholds

### Changes
- Changed from IQR method to 10% threshold
- NORMAL: 90-110% of average (±10% variance)
- OUTLIER_LOW: 50-90% of average
- Prevents normal daily variance from being flagged

## [2025-08-21] - Enhanced Anomaly Display

### Improvements
- Groups anomalous dates by severity
- Shows specific dates with actual row counts
- Displays expected range and percentage of average
- Most critical issues shown first

## [2025-08-21] - Fixed Critical IndentationError

### Bug Fix
- Removed 9 lines of orphaned code at line 1842
- All validation tests now run successfully

## [2025-08-21] - Enhanced: Clear Validation Failure Explanations

### Feature Enhancement
- **Clear Failure Reasons**: Validation now explicitly states WHY it failed
- **Improved Date Range Display**: Shows both "Requested" and "Found" date ranges
- **Specific Issue Identification**: Distinguishes between missing dates, gaps, and anomalies

### Implementation Details
- Added `failure_reasons` list to validation results
- Shows failure reasons prominently before statistics
- Categorizes issues:
  - Missing X dates (found Y of Z expected)
  - Found X gap(s) in date sequence
  - X date(s) with critically low row counts (<10% of average)
  - X date(s) with anomalous row counts

### Benefits
- Clear understanding of validation failures even when date ranges match
- Easier troubleshooting of data quality issues
- More actionable information for fixing problems
- Reduces confusion when dates exist but have low row counts

### Example Output
```
❌ VALIDATION FAILED BECAUSE:
  • 3 date(s) with critically low row counts (<10% of average)

Date Range Requested: 2024-01-01 to 2024-01-31
Date Range Found: 2024-01-01 to 2024-01-31
Unique Dates: 31 of 31 expected
```

## [2025-08-21] - Enhanced: Validation Progress Bars and Always-Visible Results

### Feature Enhancement
- **Added Progress Bars for Validation**: Shows real-time progress during validation operations
- **Always Display Validation Results**: Critical validation data now ALWAYS shows, even in --quiet mode
- **Enhanced Progress Information**: Shows anomaly counts directly in progress bar status

### Implementation Details
- Progress bars for both `--validate-only` and `--validate-in-snowflake` modes
- Progress bars output to stderr, visible even in quiet mode
- Each table shows validation status (✓/✗) with anomaly count if applicable
- Full validation details always displayed regardless of quiet flag

### Benefits
- Better visibility into validation progress for large datasets
- Critical data quality information never hidden
- Consistent experience across all validation modes
- Clear indication of issues during validation

### Technical Changes
- Added tqdm progress bars to validation loops
- Modified display condition to always show validation results
- Enhanced progress bar postfix to include anomaly counts
- Validation details include full anomaly analysis

## [2025-08-21] - Enhanced: Row Count Anomaly Detection in Validation

### Feature Enhancement
- **Added Row Count Anomaly Detection**: Enhanced Snowflake validation to identify dates with suspiciously low row counts
- **Problem Solved**: Previously only detected completely missing dates, now detects partial data loads
- **Implementation**: 
  - Calculates statistical metrics (mean, std dev, quartiles) for daily row counts
  - Flags anomalies using multiple severity levels
  - Uses IQR method for outlier detection

### Anomaly Detection Rules
- **SEVERELY_LOW**: < 10% of average row count (critical data loss indicator)
- **OUTLIER_LOW**: Below Q1 - 1.5 * IQR (statistical outlier)
- **LOW**: < 50% of average row count (significant data reduction)
- **OUTLIER_HIGH**: Above Q3 + 1.5 * IQR (unusual spike in data)
- **NORMAL**: Within expected statistical range

### Technical Implementation
- Enhanced SQL query with CTE for statistics calculation
- Added `row_count_analysis` section to validation results
- Tracks anomalous dates with severity and percentage of average
- Provides warnings for critical data issues

### Benefits
- Identifies partial data loads (e.g., 12 rows instead of 48,000)
- Detects data quality issues even when date exists
- Provides statistical context for row count variations
- Helps prevent incomplete data from reaching production

### Testing
- Created `test_anomaly_detection.py` to verify functionality
- Tests demonstrate detection of various anomaly severities
- Validates SQL query structure and anomaly classification

## [2025-08-20] - Fixed: Static Progress Bars in Parallel Mode

### Bug Fix Implementation
- **Issue**: Multiple static/dead progress bars accumulate when using `--parallel` with `--quiet`
- **Root Cause**: 
  - Parallel mode launches separate Python processes
  - Each process was creating new tqdm bars for each file with `leave=False`
  - `leave=False` only cleans up on process exit, not between files
  - Result: Dead bars accumulated at 100% as new files were processed

### Solution Implemented
- **Progress Bar Reuse Pattern**:
  - Modified `start_file_compression()`, `start_file_upload()`, `start_copy_operation()`
  - Bars are now created once on first use and reused for subsequent files
  - Used `bar.reset()` and `bar.set_description()` to update existing bars
  - Changed `leave=False` to `leave=True` to keep bars for reuse
  - Added `clear_file_bars()` method to clear bars between files without destroying them

### Technical Changes
- **ProgressTracker Class Updates**:
  - Compression, upload, and COPY bars check if bar exists before creating
  - If bar exists, reset total and description instead of creating new instance
  - Added explicit `.clear()` calls in close() method for clean shutdown
  - Object IDs remain constant across multiple files (verified by testing)

### Testing
- Created `test_simple_progress.py` to verify bar reuse
- Test confirms same object IDs for all three bar types across multiple files
- Visual testing shows clean progress display without accumulation
- Parallel processing now displays cleanly without static bars

### Benefits
- Clean visual output during parallel processing
- No more accumulating dead progress bars
- Each process shows only active operations
- Improved user experience for batch processing

## [2025-08-20] - Complete 5-Bar Progress System Implementation

### Added
- **Full 5-bar progress tracking system**:
  1. **Files Progress** - Overall file processing status
  2. **QC Rows Progress** - Quality check progress (optional, based on mode)
  3. **Compression Progress** - Per-file compression with filename display
  4. **Upload Progress** - Snowflake stage upload tracking (NEW)
  5. **COPY Progress** - Snowflake COPY operation monitoring (NEW)

- **New progress tracking methods in ProgressTracker**:
  - `start_file_upload()` - Initialize and track PUT command progress
  - `start_copy_operation()` - Initialize and track COPY command progress
  - Enhanced `update()` method with `uploaded_mb` and `copied_rows` parameters
  - Automatic progress bar cleanup and reuse for multiple files

- **Enhanced parallel processing support**:
  - Updated to support 5 progress bars per job (or 4 when skipping QC)
  - Position offset calculation: 5 lines per job with QC, 4 without
  - run_loader.sh adds appropriate initial spacing for parallel jobs
  - Environment-based positioning with TSV_JOB_POSITION

### Integration
- **SnowflakeLoader enhancements**:
  - Integrated upload progress tracking after compression
  - Added COPY progress monitoring with row count estimation
  - Performance metrics logging (MB/s for upload, rows/s for COPY)
  - Proper progress bar lifecycle management

### Technical Details
- Upload progress uses compressed file size for accurate tracking
- COPY progress estimates rows based on file size (50K rows/MB approximation)
- All 5 progress bars properly stack without overlap
- Progress bars automatically close and recreate for each file
- Full terminal width utilization for all progress bars

## [2025-08-20] - Quiet Mode and Progress Bar Refinements

### Fixed
- **Quiet Mode Complete Implementation**:
  - Wrapped ALL bash script echo statements with quiet mode checks
  - Suppressed configuration display, prerequisites, processing messages
  - Suppressed success/failure messages and batch summaries
  - Now ONLY shows progress bars when --quiet flag is used
  - Perfect for parallel processing to avoid terminal clutter

- **Progress Bar Width Consistency**:
  - Fixed compression progress bar width to match other bars
  - Removed ncols=100 limitation that made compression bar narrower
  - All progress bars now use full terminal width
  - Consistent alignment across Files, QC Progress, and Compression bars

### Enhanced
- **Per-File Compression Tracking**:
  - Added start_file_compression() method for file-specific progress
  - Compression bar now shows individual file being compressed
  - Prevents confusion during parallel file processing
  - Clear indication of which file is being compressed

### Technical Details
- Progress bars write to stderr, remain visible in quiet mode
- Bash script respects QUIET_MODE environment variable throughout
- Fixed leave=False for progress bars to prevent stale displays
- Position offset calculation improved for parallel jobs

## [2025-08-20] - Parallel Progress Bar Improvements

### Added
- **Stacked Progress Bars for Parallel Processing**:
  - Each parallel job gets its own set of non-overlapping progress bars
  - Progress bars are labeled with month identifier (e.g., `[2024-01] Files`)
  - Position offset calculated using `TSV_JOB_POSITION` environment variable
  - Automatic spacing adjustment prevents visual overlap

- **Context-Aware Progress Display**:
  - Shows 3 progress bars when doing file-based QC (Files, QC Rows, Compression)
  - Shows only 2 progress bars when skipping QC (Files, Compression)
  - "QC Rows" bar only appears when actually performing row-by-row quality checks
  - Adaptive spacing based on processing mode

### Enhanced
- **ProgressTracker Class**:
  - Added `show_qc_progress` parameter to control QC progress bar visibility
  - Position calculation adapts to number of progress bars per job
  - Month identifier passed for job labeling

- **Bash Script Updates**:
  - Sets `TSV_JOB_POSITION` environment variable for each parallel job
  - Calculates initial spacing based on QC mode (2 or 3 lines per job)
  - Improved parallel job tracking with position indicators

### Technical Implementation
- Progress bars use tqdm's `position` parameter for vertical stacking
- Each job's position offset = job_number × lines_per_job
- Lines per job: 3 with QC, 2 without QC
- All progress bars write to stderr for quiet mode compatibility

### Benefits
- Cleaner visual output during parallel processing
- No more overlapping or overwritten progress bars
- Clear identification of which month each progress bar belongs to
- Reduced screen clutter when QC is skipped
- Better user experience for batch processing

## [2025-08-20] - Direct File Processing and Config Generator

### Added - Direct File Processing
- Added `--direct-file` flag to `run_loader.sh` for processing specific TSV files directly
  - Accepts comma-separated list of TSV file paths
  - Automatically extracts directory and sets appropriate base-path
  - Detects month from filename patterns (YYYY-MM or YYYYMMDD-YYYYMMDD)
  - Supports all existing flags (--skip-qc, --validate-in-snowflake, etc.)
  - Provides helpful note about config.json file_pattern matching

### Usage Example
```bash
# Process specific TSV file directly
./run_loader.sh --direct-file /path/to/file.tsv --skip-qc

# Process multiple files
./run_loader.sh --direct-file file1.tsv,file2.tsv --validate-in-snowflake
```

## [2025-08-20] - Config Generator Tool

### Added
- Created `generate_config.sh` - comprehensive config generator script
  - Auto-detects file patterns ({date_range} vs {month})
  - Extracts table names from filenames
  - Queries Snowflake for column information (when connected)
  - Supports manual column header specification
  - Interactive mode for Snowflake credentials
  - Dry-run mode for testing
  - Generates configs in exact required JSON format
  - Uses test_venv Python for Snowflake connectivity
  - Handles both headerless and header-containing TSV files

### Features
- Pattern detection automatically identifies date formats in filenames
- Table name extraction from file naming conventions
- Column schema retrieval from Snowflake information_schema
- Credential reuse from existing config files
- Batch processing of multiple TSV files
- Proper JSON escaping and formatting

### Usage Examples
```bash
# Basic usage with file pattern detection
./generate_config.sh data/file_20240101-20240131.tsv

# With Snowflake table for column names
./generate_config.sh -t MY_TABLE -c config/existing.json data/*.tsv

# With manual column headers
./generate_config.sh -h "col1,col2,col3" data/file.tsv

# Interactive mode for credentials
./generate_config.sh -i -o config/new.json data/*.tsv
```

## [2025-08-20] - Documentation and Bash Script Updates

### Added
- **Comprehensive README.md**: Created detailed documentation with:
  - Installation instructions and prerequisites
  - Quick start guide with configuration examples
  - Performance benchmarks comparing file-based vs Snowflake validation
  - Troubleshooting guide and common issues
  - Directory structure and file patterns documentation
  
- **Snowflake Validation in Bash Script**: Added missing validation flags to run_loader.sh:
  - `--validate-in-snowflake`: Skip file QC and validate after loading
  - `--validate-only`: Only validate existing Snowflake data
  - Updated help text with validation examples
  - Added validation indicators in configuration display

### Enhanced
- **CLAUDE.md Updates**: 
  - Added new validation command examples
  - Highlighted performance benefits of Snowflake validation
  - Added batch processing examples with validation flags

## [2025-08-20] - Bug Fixes and Validation Improvements

### Fixed
- **Global logger declaration**: Fixed "name 'logger' used prior to global declaration" syntax error
- **Empty result handling**: Fixed IndexError when processing empty gap or daily_count arrays
- **Test robustness**: Improved mock test handling for edge cases and empty results

### Improved
- **Gap detection logic**: Added safe array access with length checks
- **Daily sample processing**: Added validation for row data before accessing indices
- **Error handling**: More graceful handling of empty or malformed query results

### Testing
- Created comprehensive test suite with 11 test scenarios
- Tested with mock data simulating tables from 1M to 100B rows
- Validated gap detection with up to 266 missing dates
- Tested edge cases: single day, weekend gaps, boundary conditions
- All tests pass successfully with proper error handling

## [2025-08-20] - Snowflake-Based Date Validation

### Added
- **SnowflakeDataValidator class**: Validates date completeness directly in Snowflake tables
- **--validate-in-snowflake flag**: Skip memory-intensive file QC, validate after loading
- **--validate-only flag**: Check existing Snowflake tables without loading new data
- **Efficient validation queries**: Uses aggregates and window functions for billion+ row tables
- **Gap detection**: Identifies missing date ranges with LAG window function
- **Daily distribution analysis**: Shows row counts per day with limits to prevent memory issues

### Benefits
- Reduces processing time by ~3 hours for 50GB files (skip file-based QC)
- Handles tables with billions of rows efficiently
- No memory constraints - validation happens in Snowflake
- Provides detailed gap analysis and statistics

### Technical Details
- Three-query approach: range summary, daily distribution, gap detection
- All queries use date filtering to minimize data scanning
- Results limited to prevent memory issues (1000 days, 100 gaps)
- Compatible with existing date formats (YYYY-MM-DD conversion)

## [2025-08-20] - Quiet Mode and Progress Bar Enhancement

### Added
- **--quiet flag to Python script**: Suppresses console logging while maintaining file logging
- **Progress bar preservation in quiet mode**: tqdm progress bars remain visible on stderr
- **Improved bash script handling**: Passes --quiet flag to Python and preserves stderr output

### Fixed
- Progress bars not showing in quiet mode due to stderr redirection
- Console clutter during parallel processing

### Technical Details
- Modified logging setup to conditionally add StreamHandler based on quiet mode
- Bash script now uses process substitution to capture stderr to log while keeping it visible
- Progress bars write to stderr by default, ensuring visibility in quiet mode
- All logging still captured to `logs/tsv_loader_debug.log` for full traceability

### Usage
```bash
# Single file with quiet mode
python tsv_loader.py --config config.json --quiet

# Parallel processing with clean output
./run_loader.sh --month 2024-01,2024-02,2024-03 --parallel 3 --quiet
```

## [2025-08-20] - Critical Bug Fix: OS Module Import Scope

### Fixed
- **Critical Issue**: Fixed 'local variable os referenced before assignment' error in SnowflakeLoader
- **Root Cause**: os and time modules were imported inside try block, making them unavailable in finally block
- **Solution**: Moved imports to beginning of load_file_to_stage_and_table method
- **Impact**: This was preventing all file uploads to Snowflake, causing processes to fail immediately

### Discovered
- Issue manifested as suspiciously fast completion times (0.6-0.8 seconds instead of expected ~5 minutes)
- Error occurred consistently across all parallel month processing attempts
- Affected line 594 in tsv_loader.py where os.remove() was called in finally block

### Technical Details
- Moved `import os` and `import time` from lines 501-502 to lines 487-488
- Ensures modules are available throughout entire method scope
- Critical for proper cleanup of compressed files after upload

## [2025-08-19] - factLendingBenchmark Configuration

### Added
- Created `config/factLendingBenchmark_config.json` with complete ETL configuration
- Mapped all 41 TSV columns to proper business names
- Integrated Snowflake credentials from existing configuration
- Added Gemini MCP tool for collaborative planning and code review

### Analyzed
- Processed factLendingBenchmark_20220901-20220930.tsv sample data
- Identified file structure: 21GB, 60.6M rows, 41 columns
- Discovered 3 completely null columns (FEEDBUCKET, INVESTMENTSTYLE, DATASOURCETYPE)
- Confirmed RECORDDATEID as date column with YYYYMMDD format
- Detected September 2022 date range in the data

### Configuration Details
- **Target Table**: FACTLENDINGBENCHMARK (existing)
- **Database**: PMG_SANDBOX_DB
- **Schema**: GLL
- **Warehouse**: PMG_SANDBOX_GLL_S_WH
- **Date Pattern**: factLendingBenchmark_{date_range}.tsv

### Technical Context
- File contains mix of financial identifiers (ISIN, CUSIP, SEDOL)
- Lending metrics with varying null rates (30-70% for some columns)
- Audit columns present (XCREATEBY, XCREATEDATE, XUPDATEBY, XUPDATEDATE)
- Column 2 (RECORDDATEID) consistently contains dates in YYYYMMDD format
- Column 1 (RECORDDATE) contains human-readable dates like "Sep  1 2022"

### Performance Considerations
- Estimated processing time: ~2 hours for 21GB file
- Parallel processing configured for quality checks
- Streaming approach to handle large file size efficiently