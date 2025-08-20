# PLAN.md

## Project Overview
High-performance Snowflake ETL pipeline for processing large TSV files (up to 50GB) with built-in data quality checks, progress tracking, and parallel processing capabilities. The system emphasizes streaming processing for memory efficiency and uses Snowflake's native bulk loading features.

## Current Status (2025-08-20)

### Recent Achievements
- **Validation Enhancements**: Fixed YYYYMMDD date format handling, added progress bars
- **Quiet Mode Improvements**: Progress bars visible in quiet mode, aggregate results display
- **Bug Fixes**: Fixed tqdm_available error, KeyError for empty tables
- **Documentation Complete**: Created comprehensive README, CONTEXT_HANDOVER.md
- **Performance Optimized**: 40% faster processing with Snowflake validation

### Today's Session Updates
- Fixed date validation for YYYYMMDD format (20220901 style)
- Added detailed failure reasons in validation summary
- Created progress bars for validation (visible in quiet mode)
- Implemented aggregate validation results display
- Prepared plan for config generator tool

## Next Priority: Config Generator Tool

### Problem Statement
- TSV files lack headers, making config creation manual and error-prone
- Need to match TSV columns to Snowflake table schema
- Manual config creation takes 30+ minutes per file type

### Solution: generate_config.sh
Tool that will:
1. Query Snowflake tables for column names and types
2. Analyze TSV files for structure and patterns
3. Auto-generate complete config.json files

### Key Innovation
- **Snowflake-First Approach**: Pull column metadata directly from target tables
- **Pattern Detection**: Auto-detect {month} vs {date_range} from filenames
- **Batch Processing**: Handle multiple files at once

## Previous Phase: factLendingBenchmark Configuration

### File Analysis Complete
- **File**: factLendingBenchmark_20220901-20220930.tsv
- **Size**: 21GB
- **Rows**: 60,673,993
- **Columns**: 41 (38 with data, 3 completely null)
- **Date Range**: September 1-30, 2022
- **Date Column**: RECORDDATEID (column 2, format: YYYYMMDD)

### Column Mapping Established
Successfully mapped all 41 columns to business names:
- Financial identifiers (ISIN, CUSIP, SEDOL)
- Lending metrics (LENDABLEVALUE, LOANVALUE, etc.)
- Fees and rates (FEE, MINFEE, MAXFEE, REBATE)
- Audit fields (XCREATEBY, XCREATEDATE, XUPDATEBY, XUPDATEDATE)
- 3 empty columns: FEEDBUCKET, INVESTMENTSTYLE, DATASOURCETYPE

### Configuration Created
- **Config File**: `/root/snowflake/config/factLendingBenchmark_config.json`
- **Target Table**: FACTLENDINGBENCHMARK (existing in Snowflake)
- **Snowflake Connection**: Configured with PMG_SANDBOX_DB.GLL schema

## Next Steps
1. Test the fixed ETL pipeline on remote server with Snowflake connector
2. Verify successful file uploads and compression cleanup
3. Monitor parallel processing of multiple months
4. Validate data loading to Snowflake FACTLENDINGBENCHMARK table
5. Track performance metrics against estimates

## Performance Expectations
Based on file characteristics (21GB, 60M rows):
- Row counting: ~2 minutes
- Quality checks: ~20 minutes (with parallel processing)
- Compression: ~14 minutes
- Upload: ~70 minutes
- Snowflake COPY: ~10 minutes
- **Total estimated time**: ~2 hours

## Integration Points
- Gemini MCP tool added for collaborative planning and code review
- Using existing Snowflake table (no DDL creation needed)
- Streaming processing to handle large file efficiently