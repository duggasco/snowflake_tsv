# Context Handover Document
*Last Updated: 2025-08-20*

## Project Overview
High-performance Snowflake ETL pipeline for loading large TSV files (up to 50GB) with data quality validation, progress tracking, and parallel processing capabilities.

## Recent Session Accomplishments (2025-08-20)

### 1. Config Generator Tool - COMPLETED ✓
- Created `generate_config.sh` with full functionality:
  - Auto-detects file patterns ({date_range} vs {month})
  - Extracts table names from filenames
  - Queries Snowflake for column information
  - Supports manual column header specification
  - Interactive mode for credentials
  - Dry-run mode for testing
  - Uses test_venv for Snowflake connectivity

### 2. Direct File Processing - COMPLETED ✓
- Added `--direct-file` flag to `run_loader.sh`:
  - Process specific TSV files directly
  - Accepts comma-separated list of file paths
  - Auto-extracts directory for base-path
  - Detects month from filename patterns
  - Full compatibility with all existing flags

## Recent Session Accomplishments (Previous)

### 1. Validation System Improvements
- **Fixed YYYYMMDD Date Format**: Updated SQL queries to handle dates stored as YYYYMMDD integers (e.g., 20220901)
- **Progress Bars for Validation**: Added tqdm progress bars that display in stderr (visible in quiet mode)
- **Aggregate Validation Results**: Batch jobs now show consolidated validation summary at the end
- **Detailed Failure Reasons**: Invalid validations now show specific missing date ranges

### 2. Quiet Mode Enhancement
- **--quiet flag**: Suppresses console output while preserving progress bars
- **Validation Results**: Saved to JSON files for aggregation
- **Batch Summary**: Shows validation details after all months complete

### 3. Bug Fixes
- Fixed `tqdm_available` NameError (changed to `TQDM_AVAILABLE`)
- Fixed KeyError 'statistics' for empty Snowflake tables
- Fixed --validate-only mode to work without file paths

## Next Session Priority

### Production Testing & Monitoring
1. **Test full pipeline** with real production data
2. **Monitor performance** metrics with 50GB+ files
3. **Validate Snowflake data** completeness after loads
4. **Document any issues** or optimization opportunities

### Potential Enhancements
1. **Enhanced error recovery**: Resume failed loads from checkpoint
2. **Data profiling**: Generate statistics about loaded data
3. **Notification system**: Email/Slack alerts on completion/failure
4. **Archive management**: Auto-archive processed TSV files

## Current Tools & Scripts

### Core Scripts
1. **tsv_loader.py** - Main ETL script with validation
2. **run_loader.sh** - Enhanced bash wrapper with parallel processing and direct file support
3. **generate_config.sh** - Comprehensive config generator with Snowflake integration
4. **tsv_sampler.sh** - Basic TSV analyzer (generates config snippets)

### Key Features
- Parallel processing (month and file level)
- Snowflake-based validation (faster than file-based)
- Progress tracking with tqdm
- Batch processing with aggregated results

## Configuration Structure
```json
{
  "snowflake": {
    "account": "...",
    "user": "...",
    "password": "...",
    "warehouse": "...",
    "database": "...",
    "schema": "...",
    "role": "..."
  },
  "files": [
    {
      "file_pattern": "filename_{date_range}.tsv",
      "table_name": "TARGET_TABLE",
      "expected_columns": ["col1", "col2", "..."],
      "date_column": "RECORDDATEID"
    }
  ]
}
```

## Known Issues & Limitations
1. TSV files lack headers - need Snowflake table schema
2. Manual config creation is time-consuming
3. Date columns stored as YYYYMMDD integers in Snowflake

## Testing Commands

### Validation
```bash
# Single month validation
./run_loader.sh --validate-only --month 2022-09

# Batch validation with quiet mode
./run_loader.sh --validate-only --batch --parallel 4 --quiet
```

### Processing
```bash
# Process with Snowflake validation (faster)
./run_loader.sh --month 2022-09 --validate-in-snowflake

# Batch processing
./run_loader.sh --batch --parallel 4 --continue-on-error
```

## Environment Notes
- Python 3.7+ required
- Snowflake connector installed
- tqdm for progress bars (optional but recommended)
- Working directory: /root/snowflake

## Next Steps for Config Generator

### Phase 1: Core Script
```bash
#!/bin/bash
# generate_config.sh
# Usage: ./generate_config.sh --table SNOWFLAKE_TABLE --files "*.tsv"
```

### Phase 2: Snowflake Integration
```python
# Query to get column info
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_name = 'TARGET_TABLE'
ORDER BY ordinal_position;
```

### Phase 3: Pattern Detection
- Extract dates from filenames
- Identify {month} vs {date_range} patterns
- Map columns to TSV positions

## Important Context
- Files use RECORDDATEID column for dates (YYYYMMDD format)
- Table TEST_CUSTOM_FACTLENDINGBENCHMARK is the current target
- TSV files have 41 columns, no headers
- Processing 50GB files takes ~4.5 hours with Snowflake validation

## Session Handover Notes
The config generator is the top priority for the next session. The foundation is ready:
- PLAN_CONFIG_GENERATOR.md has full specifications
- tsv_sampler.sh can be extended for basic analysis
- Snowflake connection code exists in tsv_loader.py (can be reused)

Start by creating a simple version that:
1. Takes a Snowflake table name
2. Queries column information
3. Analyzes TSV file structure
4. Generates a valid config.json

Then enhance with pattern detection and multi-file support.