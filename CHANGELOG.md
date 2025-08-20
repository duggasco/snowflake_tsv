# CHANGELOG.md

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