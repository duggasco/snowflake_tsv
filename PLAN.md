# PLAN.md

## Project Overview
High-performance Snowflake ETL pipeline for processing large TSV files (up to 50GB) with built-in data quality checks, progress tracking, and parallel processing capabilities.

## Current Phase: Bug Fix and Testing

### Critical Bug Fixed (2025-08-20)
- **Issue**: OS module import scope error in SnowflakeLoader
- **Impact**: All uploads were failing immediately 
- **Resolution**: Moved os and time imports to method scope
- **Status**: Fix committed and pushed to main branch

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