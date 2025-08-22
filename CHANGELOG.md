# CHANGELOG.md

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