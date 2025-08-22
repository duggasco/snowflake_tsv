# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance Snowflake ETL pipeline for processing large TSV files (up to 50GB) with built-in data quality checks, progress tracking, and parallel processing capabilities. The system emphasizes streaming processing for memory efficiency and uses Snowflake's native bulk loading features.

## Key Components

### Main Scripts
- **tsv_loader.py**: The primary ETL script that orchestrates file analysis, quality checks, compression, and Snowflake loading
- **drop_month.py**: Safe deletion tool for removing monthly data from Snowflake tables with comprehensive safety features
- **drop_month.sh**: Bash wrapper for drop_month.py with colored output and safety warnings
- **run_loader.sh**: Bash wrapper script for convenient execution with color-coded output and prerequisite checking
- **generate_config.sh**: Automatic configuration generator from TSV files and Snowflake schemas
- **tsv_sampler.sh**: TSV file analyzer that samples data to help understand structure and generate configurations
- **check_snowflake_table.py**: Diagnostic tool for verifying table existence and debugging Snowflake connections
- **check_stage_and_performance.py**: Performance diagnostic tool for analyzing slow COPY operations and managing stages

### Core Classes and Their Responsibilities

#### FileConfig (dataclass)
- Defines configuration for each TSV file including path, table name, expected columns, date column, and expected date range

#### FileAnalyzer
- Fast row counting using sampling for large files
- Realistic time estimation based on empirically-tested benchmarks:
  - Row counting: 500K rows/second
  - Quality checks: 50K rows/second (includes date parsing)
  - Compression: 25MB/second (gzip level 6)
  - Upload: 5MB/second (typical network)
  - Snowflake COPY: 100K rows/second

#### DataQualityChecker
- Streaming date completeness validation
- Schema validation without loading full file into memory
- Automatic date format detection (YYYY-MM-DD, YYYYMMDD, MM/DD/YYYY)
- Type inference from sample data

#### SnowflakeDataValidator
- Validates date completeness directly in Snowflake tables
- **Duplicate detection**: Identifies duplicate records based on composite keys
- Efficient aggregate queries for billion+ row tables
- Gap detection using window functions
- Daily distribution analysis with limits to prevent memory issues
- No need to load large files into memory for validation
- Duplicate severity assessment (LOW/MEDIUM/HIGH/CRITICAL)

#### SnowflakeLoader
- Manages Snowflake connection and executes PUT/COPY commands
- **Async COPY support**: Automatically uses execute_async() for files >100MB compressed
- **Keepalive mechanism**: Prevents 5-minute timeout with get_results_from_sfqid() every 4 minutes
- **Warehouse detection**: Warns if using X-Small/Small warehouse for large files
- **Stage cleanup**: Removes old stage files before uploading to prevent conflicts
- **Optimized error handling**: Uses ABORT_STATEMENT instead of CONTINUE for fast failure
- **Auto-purge**: PURGE=TRUE automatically removes files after successful load
- Handles file compression (gzip) before upload
- Uses internal staging (@~/) for efficient bulk loading
- Implements validation mode before actual data loading

#### Drop Month Components (drop_month.py)

##### DeletionTarget (dataclass)
- Represents a single deletion operation with table, date column, month, and date range

##### DeletionResult (dataclass)
- Captures result of deletion including rows affected, status, and execution time

##### SnowflakeManager
- Manages Snowflake connection lifecycle using context managers
- Provides automatic connection cleanup even on exceptions
- Logs connection details for audit purposes

##### SnowflakeMetadata
- Caches table metadata to reduce redundant queries
- Validates table and column existence before operations
- Provides column name listing for validation

##### SnowflakeDeleter
- Executes deletion operations within managed transactions
- Implements dry-run mode for impact analysis
- Provides preview functionality to show sample rows
- Ensures atomic operations with automatic rollback on errors
- Validates row count consistency before committing

#### ProgressTracker
- Real-time progress bars using tqdm (when available)
- Tracks files processed, rows processed (during QC), and compression progress
- Progress bars write to stderr and are visible even in quiet mode
- **Parallel Processing Support**:
  - Stacked progress bars for each parallel job
  - Each job's bars are labeled with month identifier (e.g., `[2024-01] Files`)
  - Position offset calculated from `TSV_JOB_POSITION` environment variable
  - Bars don't overwrite each other - each job has dedicated display lines
- **Context-Aware Display**:
  - Shows 3 progress bars when doing file-based QC (Files, QC Rows, Compression)
  - Shows 2 progress bars when skipping QC (Files, Compression only)
  - Automatically adjusts spacing based on processing mode

## Development Commands

### Install Dependencies
```bash
# Core requirements
pip install snowflake-connector-python pandas numpy

# Optional but recommended
pip install tqdm psutil
```

### Config Generation

```bash
# Generate config from TSV files automatically
./generate_config.sh data/file_20240101-20240131.tsv

# Query Snowflake table for column names
./generate_config.sh -t FACTLENDINGBENCHMARK -c config/existing.json data/*.tsv

# Provide column headers manually for headerless TSVs
./generate_config.sh -h "RECORDDATE,RECORDDATEID,ASSETID,..." data/file.tsv

# Interactive mode for Snowflake credentials
./generate_config.sh -i -o config/my_config.json data/*.tsv

# Dry run to preview generated config
./generate_config.sh --dry-run data/file.tsv
```

### Running the Pipeline

```bash
# Check system capabilities (no config needed)
python tsv_loader.py --check-system

# Analyze files and get time estimates without processing
python tsv_loader.py --config config/config.json --base-path ./data --analyze-only

# Process with auto-detected optimal workers
python tsv_loader.py --config config/config.json --base-path ./data --month 2024-01

# Process with specific worker count
python tsv_loader.py --config config/config.json --base-path ./data --max-workers 8

# Skip quality checks (not recommended)
python tsv_loader.py --config config/config.json --base-path ./data --skip-qc

# Quiet mode - suppress console logging but keep progress bars
python tsv_loader.py --config config/config.json --base-path ./data --quiet

# Skip file-based QC and validate in Snowflake after loading (FASTER for large files)
python tsv_loader.py --config config/config.json --base-path ./data --validate-in-snowflake

# Only validate existing data in Snowflake (no loading)
python tsv_loader.py --config config/config.json --base-path ./data --month 2024-01 --validate-only

# Using the bash wrapper (recommended)
./run_loader.sh --month 2024-01 --base-path ./data

# Process specific TSV files directly
./run_loader.sh --direct-file /path/to/file.tsv --skip-qc
./run_loader.sh --direct-file file1.tsv,file2.tsv --validate-in-snowflake

# Process with Snowflake validation instead of file QC
./run_loader.sh --month 2024-01 --validate-in-snowflake

# Only validate existing Snowflake data
./run_loader.sh --month 2024-01 --validate-only

# Parallel processing with quiet mode for cleaner output
./run_loader.sh --month 2024-01,2024-02,2024-03 --parallel 3 --quiet

# Batch processing with Snowflake validation
./run_loader.sh --batch --validate-in-snowflake --parallel 4
```

### Data Deletion
```bash
# Dry run to preview impact
python drop_month.py --config config/config.json --table MY_TABLE --month 2024-01 --dry-run

# Delete with preview and confirmation
python drop_month.py --config config/config.json --table MY_TABLE --month 2024-01 --preview

# Delete multiple months (skip confirmation with --yes)
python drop_month.py --config config/config.json --table MY_TABLE --months 2024-01,2024-02 --yes

# Delete from all tables in config
python drop_month.py --config config/config.json --all-tables --month 2024-01

# Output deletion report
python drop_month.py --config config/config.json --table MY_TABLE --month 2024-01 --output-json report.json
```

### Debugging
```bash
# Logs are automatically created in logs/ directory
tail -f logs/tsv_loader_debug.log

# Check deletion logs
tail -f logs/drop_month_*.log

# Check recent errors in a run
grep -i "error\|failed" logs/run_*.log | tail -20
```

## Configuration Structure

The pipeline expects a JSON config file with:
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
      "file_pattern": "filename_{date_range}.tsv",  // or "filename_{month}.tsv"
      "table_name": "TARGET_TABLE",
      "date_column": "recordDate",
      "duplicate_key_columns": ["recordDate", "assetId", "fundId"],  // For duplicate detection
      "expected_columns": ["col1", "col2", ...]
    }
  ]
}
```

## File Pattern Matching

The pipeline supports two date pattern types:
1. **Date range pattern**: `{date_range}` matches YYYYMMDD-YYYYMMDD format
2. **Month pattern**: `{month}` matches YYYY-MM format

## Parallel Processing Architecture

The pipeline uses:
- **Multiprocessing** for CPU-bound quality checks (date parsing, validation)
- **Threading** for I/O-bound Snowflake uploads
- Automatic worker detection based on CPU count with diminishing returns model:
  - 1-4 cores: Use all cores
  - 5-8 cores: Use cores - 1
  - 9-16 cores: Use 75% of cores
  - 17-32 cores: Use 60% of cores
  - 33+ cores: Use 50% (max 32)

## Performance Characteristics

For a 50GB TSV file with ~500M rows on a 16-core system:
- Row counting: ~16 seconds
- Quality checks: ~2.5 hours (with 12 workers) 
  - Can be skipped with `--validate-in-snowflake` for faster processing
- Compression: ~35 minutes
- Upload to Snowflake: ~3 hours
- Snowflake COPY: 
  - With ON_ERROR='CONTINUE': 1+ hours (row-by-row on errors - AVOID!)
  - With ON_ERROR='ABORT_STATEMENT': ~15-30 minutes (fast failure)
  - Async execution for files >100MB compressed
- Snowflake validation: ~5-10 seconds (aggregate queries only)
- Total time: ~4 hours with optimizations (was ~7-8 hours)

### Key Performance Optimizations
- **Async COPY**: Files >100MB use execute_async() with keepalive
- **Fast Failure**: ABORT_STATEMENT stops on first error (not row-by-row)
- **Warehouse Sizing**: Medium/Large recommended for files >100MB
- **Stage Cleanup**: Prevents confusion from duplicate files
- **PURGE=TRUE**: Automatic cleanup after successful load

## Error Handling

- Validation mode runs before actual data loading
- Failed quality checks prevent loading to Snowflake
- Compressed files are automatically cleaned up
- Detailed debug logging to `logs/tsv_loader_debug.log`
- Process can be interrupted cleanly with Ctrl+C

## Logging and Output Modes

- **Normal mode**: Full console output plus file logging
- **Quiet mode** (`--quiet`): 
  - Suppresses console logging (stdout)
  - Keeps progress bars visible (stderr)
  - All logs still written to `logs/tsv_loader_debug.log`
  - Ideal for parallel processing to reduce terminal clutter
- **Debug logging**: Always enabled to `logs/tsv_loader_debug.log`

## Memory Management

- Streaming processing with configurable chunk size (default 100,000 rows)
- Files are never fully loaded into memory
- Compression done in 10MB chunks
- Row counting uses 8MB read buffers

## Snowflake-Specific Details

- Uses internal stages (@~/) for temporary file storage
- Implements AUTO_COMPRESS=FALSE (files pre-compressed)
- ON_ERROR='CONTINUE' for resilient loading
- VALIDATION_MODE for pre-load validation
- Supports various NULL representations: '', 'NULL', 'null', '\\N'

## Snowflake Validation Features

### Why Use Snowflake Validation?

For large files (50GB+), file-based quality checks can take 2-3 hours. Snowflake validation:
- Completes in seconds using aggregate queries
- Handles billion+ row tables efficiently
- Reduces total processing time by ~40%
- No memory constraints

### Validation Modes

1. **Skip file QC, validate after loading**:
```bash
python tsv_loader.py --config config.json --validate-in-snowflake
```

2. **Validate existing data only (no loading)**:
```bash
python tsv_loader.py --config config.json --month 2024-01 --validate-only
```

3. **Traditional file-based QC** (default):
```bash
python tsv_loader.py --config config.json
```

### What Gets Validated

The Snowflake validator checks:
- Date range completeness (all expected dates present)
- Gap detection (identifies missing date ranges)
- Row distribution (average rows per day)
- Data boundaries (actual vs requested date ranges)
- **Duplicate records** (based on composite keys like recordDate + assetId + fundId)

### Duplicate Detection

The validator includes efficient duplicate detection using ROW_NUMBER() window functions:

#### How It Works
1. **Composite Key**: Default is `(recordDate, assetId, fundId)` - configurable per table
2. **Fast Aggregation**: Uses GROUP BY with HAVING COUNT(*) > 1 for quick counts
3. **Sample Records**: Returns sample duplicate records for investigation
4. **Severity Assessment**:
   - **CRITICAL**: >10% duplicates or >100 duplicates per key
   - **HIGH**: >5% duplicates or >50 duplicates per key
   - **MEDIUM**: >1% duplicates or >10 duplicates per key
   - **LOW**: Any duplicates below medium threshold

#### Configuration
Add `duplicate_key_columns` to your file configuration:
```json
{
  "file_pattern": "factLendingBenchmark_{date_range}.tsv",
  "table_name": "FACTLENDINGBENCHMARK",
  "date_column": "recordDate",
  "duplicate_key_columns": ["recordDate", "assetId", "fundId"],
  "expected_columns": [...]
}
```

#### Output Example
```
⚠ DUPLICATE RECORDS DETECTED:
  • FACTLENDINGBENCHMARK: 152 duplicate keys, 304 excess rows (0.05% duplicates) - Severity: LOW
    Sample duplicate keys (first 3):
      - recordDate: 20240115, assetId: ABC123, fundId: F001 (appears 2 times)
      - recordDate: 20240120, assetId: DEF456, fundId: F002 (appears 3 times)
```

### Performance Benchmarks

Based on testing with mock data:
- 1M rows: ~4ms
- 100M rows: ~7ms
- 1B rows: ~35ms
- 10B rows: ~300ms
- 100B rows: ~1.5s

Duplicate detection adds minimal overhead (~10-20% to validation time) due to efficient window functions.

## Implementation Details

### Parallel Progress Bar Architecture

The parallel progress bar system uses environment variables and position offsets to prevent overlapping:

1. **Environment Variable**: `TSV_JOB_POSITION` is set by `run_loader.sh` for each parallel job (0, 1, 2, ...)
2. **Position Calculation**: 
   - With QC: position_offset = job_position * 3 (3 progress bars per job)
   - Without QC: position_offset = job_position * 2 (2 progress bars per job)
3. **tqdm Parameters**:
   - `position`: Sets the line position for each progress bar
   - `leave=True`: Keeps progress bars visible after completion
   - `file=sys.stderr`: Ensures progress bars work in quiet mode

### Context-Aware Progress Display

The `ProgressTracker` class adapts based on the `show_qc_progress` parameter:
- Determined by: `show_qc_progress = not skip_qc and not validate_in_snowflake`
- When False, the QC Rows progress bar is set to None and not displayed
- Compression bar position adjusts accordingly (position + 1 instead of position + 2)

## Testing Approach

### Automated Tests
The codebase includes comprehensive test suites:

1. **Main validator tests** (`test_snowflake_validator.py`):
   - Complete date ranges
   - Date gaps detection
   - Empty tables
   - Very large tables (10B rows)
   - Query efficiency
   - Date format handling

2. **Edge case tests** (`test_edge_cases.py`):
   - Single day validation
   - Weekend gaps (business days)
   - Many gaps performance
   - Billion row simulations
   - Date boundary conditions

### Manual Testing
1. Use `--analyze-only` flag to verify file detection and time estimates
2. Use `--check-system` to verify environment capabilities
3. Use `--validate-only` to check existing Snowflake data
4. Test with small sample files before processing large datasets
5. Check logs/tsv_loader_debug.log for detailed execution trace