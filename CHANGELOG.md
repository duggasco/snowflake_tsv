# CHANGELOG.md

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