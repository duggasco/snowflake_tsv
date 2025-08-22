# Snowflake TSV Loader

A high-performance ETL pipeline for loading large TSV files (up to 50GB) into Snowflake with built-in data quality checks, progress tracking, and parallel processing capabilities.

## Features

- **Streaming Processing**: Memory-efficient processing of large files without loading them into memory
- **Async COPY Support**: Automatic async execution for files >100MB with keepalive mechanism
- **Parallel Processing**: Automatic CPU core detection and optimal worker allocation
- **Data Quality Checks**: 
  - File-based validation with date completeness checks
  - Snowflake-based validation for faster processing of large files
  - Schema validation and type inference
  - Row count anomaly detection with statistical analysis
- **Progress Tracking**: 
  - Real-time progress bars with ETA calculations
  - Stacked progress bars for parallel processing
  - Context-aware display (shows QC progress only when performing file-based QC)
- **Performance Optimizations**:
  - Automatic warehouse size detection with warnings
  - Stage cleanup before uploads
  - ABORT_STATEMENT instead of CONTINUE for fast failure
  - PURGE=TRUE for automatic stage cleanup
- **Flexible Date Patterns**: Supports both date range (YYYYMMDD-YYYYMMDD) and month (YYYY-MM) formats
- **Batch Processing**: Process multiple months in parallel with comprehensive error handling
- **Config Generator**: Automatically generate configuration files from TSV files and Snowflake schemas
- **Direct File Processing**: Process specific TSV files directly without directory structure requirements
- **Diagnostic Tools**: Stage inspection and performance analysis utilities

## Installation

### Prerequisites

- Python 3.7+
- Snowflake account with appropriate permissions

### Install Dependencies

```bash
# Core requirements
pip install snowflake-connector-python pandas numpy

# Optional but recommended for progress bars
pip install tqdm psutil
```

## Quick Start

1. **Configure your Snowflake connection** in `config/config.json`:

```json
{
  "snowflake": {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema",
    "role": "your_role"
  },
  "files": [
    {
      "file_pattern": "filename_{date_range}.tsv",
      "table_name": "TARGET_TABLE",
      "date_column": "recordDate",
      "expected_columns": ["col1", "col2", "..."]
    }
  ]
}
```

2. **Run the loader**:

```bash
# Using the bash wrapper (recommended)
./run_loader.sh --month 2024-01 --base-path ./data

# Or directly with Python
python tsv_loader.py --config config/config.json --base-path ./data --month 2024-01
```

## Usage

### Basic Commands

```bash
# Check system capabilities
python tsv_loader.py --check-system

# Analyze files without processing
python tsv_loader.py --config config/config.json --base-path ./data --analyze-only

# Process a single month
./run_loader.sh --month 2024-01 --base-path ./data

# Process specific TSV files directly
./run_loader.sh --direct-file /path/to/file.tsv --skip-qc

# Process multiple months in parallel
./run_loader.sh --months 2024-01,2024-02,2024-03 --parallel 3
```

### Config Generation

```bash
# Generate config from TSV files
./generate_config.sh data/file_20240101-20240131.tsv

# Use Snowflake table for column names
./generate_config.sh -t TABLE_NAME -c config/existing.json data/*.tsv

# With manual column headers
./generate_config.sh -h "col1,col2,col3" -o config/new.json data/file.tsv

# Interactive mode for credentials
./generate_config.sh -i -o config/my_config.json data/*.tsv
```

### Validation Options

The pipeline offers three validation modes with progress tracking:

1. **Traditional File-based QC** (default):
```bash
./run_loader.sh --month 2024-01
```

2. **Skip File QC, Validate in Snowflake** (faster for large files):
```bash
./run_loader.sh --month 2024-01 --validate-in-snowflake
```

3. **Validate Existing Data Only** (no loading):
```bash
./run_loader.sh --month 2024-01 --validate-only
```

### What Gets Validated

The Snowflake validator performs comprehensive data quality checks with progress tracking:

**Important**: Validation results are ALWAYS displayed, even in `--quiet` mode, because this data is critical for data quality assurance.

**Progress Tracking**: All validation operations show progress bars (via stderr) that remain visible even in quiet mode:

#### Date Completeness
- Verifies all expected dates are present
- Identifies gaps in date sequences
- Compares actual vs requested date ranges

#### Row Count Anomaly Detection (NEW)
- **Statistical Analysis**: Calculates mean, median, quartiles, and standard deviation
- **Anomaly Classification**:
  - **SEVERELY_LOW**: < 10% of average row count (critical data loss)
  - **OUTLIER_LOW**: Statistical outlier below Q1 - 1.5 * IQR
  - **LOW**: < 50% of average row count (partial data)
  - **OUTLIER_HIGH**: Statistical outlier above Q3 + 1.5 * IQR
  - **NORMAL**: Within expected statistical range
- **Benefits**:
  - Identifies partial data loads (e.g., 12 rows vs expected 48,000)
  - Detects data quality issues even when date exists
  - Prevents incomplete data from reaching production

#### Example Validation Progress
```
Validating tables: 100%|██████████| 3/3 [00:02<00:00, 1.50table/s, TEST_TABLE_2: ✗ (3 anomalies)]
```

#### Example Validation Output
```
TEST_TABLE_2:
  Status: ✗ INVALID
  Date Range: 2024-01-01 to 2024-01-31
  Total Rows: 1,440,012
  Avg Rows/Day: 46,452
  
  Row Count Analysis:
    Mean: 46,452 rows/day
    Range: 12 - 52,000 rows
    Anomalies Detected: 3 dates
  
  ⚠️ Anomalous Dates (low row counts):
    1) 2024-01-05 - 12 rows (0.03% of avg) - SEVERELY_LOW
    2) 2024-01-15 - 2,400 rows (5.2% of avg) - SEVERELY_LOW
  
  ⚠️ Warnings:
    • CRITICAL: 2 dates have less than 10% of average row count - possible data loss
```

### Advanced Options

```bash
# Process with specific worker count
./run_loader.sh --month 2024-01 --max-workers 8

# Batch process all months found
./run_loader.sh --batch --continue-on-error

# Quiet mode for cleaner output (recommended for parallel processing)
./run_loader.sh --batch --parallel 4 --quiet
# Progress bars remain visible in quiet mode, only console output is suppressed

# Dry run to preview actions
./run_loader.sh --batch --dry-run
```

## Data Deletion Tool (drop_month.py)

### Overview
Safely delete monthly data from Snowflake tables with multiple safety layers and comprehensive audit logging.

### Safety Features
- **Parameterized Queries**: Prevents SQL injection attacks
- **Dry Run Mode**: Analyze impact without deleting data
- **Interactive Confirmation**: Requires explicit user confirmation
- **Transaction Management**: Automatic rollback on errors
- **Metadata Caching**: Efficient validation with cached table schemas
- **Audit Logging**: Complete record of all operations
- **Snowflake Time Travel**: Recovery path documented in logs

### Using the Bash Wrapper (Recommended)
```bash
# Using the convenient bash wrapper with colored output
./drop_month.sh --config config/config.json --table MY_TABLE --month 2024-01 --dry-run

# Delete with preview
./drop_month.sh --config config/config.json --table MY_TABLE --month 2024-01 --preview

# Delete multiple months
./drop_month.sh --config config/config.json --table MY_TABLE --months 2024-01,2024-02,2024-03
```

### Direct Python Usage
```bash
# Dry run - analyze impact without deleting
python drop_month.py --config config/factLendingBenchmark_config.json \
  --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 --dry-run

# Preview sample rows before deletion
python drop_month.py --config config/factLendingBenchmark_config.json \
  --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 --preview

# Delete with confirmation prompt
python drop_month.py --config config/factLendingBenchmark_config.json \
  --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01

# Skip confirmation (use with caution!)
python drop_month.py --config config/factLendingBenchmark_config.json \
  --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 --yes

# Delete multiple months from multiple tables
python drop_month.py --config config/factLendingBenchmark_config.json \
  --tables TABLE1,TABLE2 --months 2024-01,2024-02,2024-03

# Delete from all configured tables
python drop_month.py --config config/factLendingBenchmark_config.json \
  --all-tables --month 2024-01

# Output summary report to JSON
python drop_month.py --config config/factLendingBenchmark_config.json \
  --table TEST_CUSTOM_FACTLENDINGBENCHMARK --month 2024-01 \
  --output-json deletion_report.json
```

### Architecture
The deletion tool uses a secure, separated architecture:
- **SnowflakeManager**: Manages connection lifecycle with context managers
- **SnowflakeMetadata**: Caches table metadata to reduce queries
- **SnowflakeDeleter**: Handles analysis and deletion with transactions

### Security Best Practices
- All user inputs are parameterized (no SQL injection risk)
- Table and column names validated against metadata
- Connection automatically closed even on exceptions
- Transactions ensure atomic operations

### Recovery
If data is accidentally deleted, use Snowflake Time Travel:
```sql
-- Check the recovery timestamp in the logs
-- Restore table to before deletion
CREATE OR REPLACE TABLE my_table AS 
SELECT * FROM my_table AT(TIMESTAMP => '2025-08-21T10:30:00'::timestamp);
```

## Performance

### Benchmarks (50GB file, ~500M rows, 16-core system)

| Operation | File-based QC | Snowflake Validation | With Optimizations |
|-----------|--------------|---------------------|-------------------|
| Row Counting | ~16 seconds | N/A | N/A |
| Quality Checks | ~2.5 hours | ~5-10 seconds | ~5-10 seconds |
| Compression | ~35 minutes | ~35 minutes | ~35 minutes |
| Upload | ~3 hours | ~3 hours | ~3 hours |
| Snowflake COPY | ~1.5 hours | ~1.5 hours | ~15-30 minutes* |
| **Total Time** | **~7-8 hours** | **~4.5 hours** | **~4 hours** |

*With proper warehouse sizing (MEDIUM/LARGE) and ABORT_STATEMENT instead of CONTINUE

### Why Use Snowflake Validation?

- **Speed**: Validation completes in seconds vs hours for large files
- **Scalability**: Handles billion+ row tables efficiently
- **Memory**: No memory constraints as validation runs in Snowflake
- **Reliability**: Uses optimized SQL aggregate queries

### Parallel Processing

The system automatically detects CPU cores and allocates workers:

- 1-4 cores: Use all cores
- 5-8 cores: Use cores - 1
- 9-16 cores: Use 75% of cores
- 17-32 cores: Use 60% of cores
- 33+ cores: Use 50% (max 32)

## Utility Scripts

### TSV Sampler (tsv_sampler.sh)
Analyzes TSV files to help understand structure and create configurations.

```bash
# Sample first 1000 rows (default)
./tsv_sampler.sh data/file_2024-01.tsv

# Sample specific number of rows
./tsv_sampler.sh data/file_2024-01.tsv 5000
```

Features:
- Shows file size and total line count
- Detects column count and headers
- Performs data type inference
- Identifies date columns automatically
- Generates sample config JSON structure

### Snowflake Table Checker (check_snowflake_table.py)
Diagnostic tool for verifying table existence and structure in Snowflake.

```bash
# Check if table exists and get column info
python3 check_snowflake_table.py config/config.json TABLE_NAME

# Useful for debugging connection issues
python3 check_snowflake_table.py config/config.json TEST_CUSTOM_FACTLENDINGBENCHMARK
```

Features:
- Verifies Snowflake connection
- Checks table existence in multiple schemas
- Lists all columns with data types
- Shows row count and table size
- Helps debug permission issues

### Stage and Performance Analyzer (check_stage_and_performance.py)
Diagnostic tool for troubleshooting slow COPY operations and stage management.

```bash
# Check stage contents and query performance
python check_stage_and_performance.py config/config.json

# Check specific table's stage
python check_stage_and_performance.py config/config.json TABLE_NAME

# Interactive cleanup of old stage files
python check_stage_and_performance.py config/config.json TABLE_NAME
```

Features:
- Lists all files in Snowflake stages with sizes
- Analyzes recent COPY query performance
- Identifies slow queries and bottlenecks
- Checks warehouse configuration
- Provides optimization recommendations
- Interactive stage cleanup option

## File Patterns

The loader supports two date pattern types:

1. **Date Range Pattern**: `filename_{date_range}.tsv`
   - Matches: `filename_20240101-20240131.tsv`

2. **Month Pattern**: `filename_{month}.tsv`  
   - Matches: `filename_2024-01.tsv`

## Directory Structure

```
snowflake/
├── tsv_loader.py          # Main ETL script
├── run_loader.sh          # Bash wrapper with colored output
├── config/
│   └── config.json        # Snowflake configuration
├── data/                  # TSV files location
│   ├── 012024/           # Month directories (MMYYYY format)
│   ├── 022024/
│   └── ...
├── logs/                  # Execution logs
│   └── tsv_loader_debug.log
└── tests/                 # Test suites
    ├── test_snowflake_validator.py
    └── test_edge_cases.py
```

## Progress Tracking

### Progress Bar Types

The pipeline displays different progress bars based on the processing mode:

**With File-based QC** (default):
- **Files**: Number of TSV files being processed
- **QC Rows**: Row-by-row quality check progress (date validation, schema checking)
- **Compression**: Megabytes compressed during gzip compression

**Without QC** (`--skip-qc` or `--validate-in-snowflake`):
- **Files**: Number of TSV files being processed
- **Compression**: Megabytes compressed during gzip compression
- *(QC Rows bar is hidden as no row-by-row processing occurs)*

### Parallel Processing Display

When processing multiple months in parallel:
- Each job gets its own set of stacked progress bars
- Progress bars are labeled with the month being processed (e.g., `[2024-01] Files`)
- Bars don't overwrite each other - each job has its own display area
- Automatic spacing adjustment based on number of parallel jobs

Example with 3 parallel jobs:
```
[2024-01] Files: 100%|██████████| 10/10 [00:02<00:00, 4.99file/s]
[2024-01] QC Rows: 100%|██████████| 1000/1000 [00:02<00:00, 498.58rows/s]
[2024-01] Compression: 100%|██████████| 100/100 [00:02<00:00, 49.86MB/s]
[2024-02] Files: 100%|██████████| 10/10 [00:02<00:00, 4.99file/s]
[2024-02] QC Rows: 100%|██████████| 1000/1000 [00:02<00:00, 498.55rows/s]
[2024-02] Compression: 100%|██████████| 100/100 [00:02<00:00, 49.86MB/s]
[2024-03] Files: 100%|██████████| 10/10 [00:02<00:00, 4.99file/s]
[2024-03] QC Rows: 100%|██████████| 1000/1000 [00:02<00:00, 498.55rows/s]
[2024-03] Compression: 100%|██████████| 100/100 [00:02<00:00, 49.86MB/s]
```

## Logging

- **Console Output**: Normal operational messages
- **Debug Log**: Detailed execution trace in `logs/tsv_loader_debug.log`
- **Quiet Mode**: Suppress console output while keeping progress bars visible
- **Run Logs**: Individual logs for each run in `logs/run_*.log`

## Error Handling

- Validation mode runs before actual data loading
- Failed quality checks prevent loading to Snowflake
- Compressed files are automatically cleaned up
- Process can be interrupted cleanly with Ctrl+C
- `--continue-on-error` flag for batch processing resilience

## Testing

### Run Tests

```bash
# Run main validator tests
python -m pytest tests/test_snowflake_validator.py -v

# Run edge case tests
python -m pytest tests/test_edge_cases.py -v

# Run all tests
python -m pytest tests/ -v
```

### Manual Testing

1. Use `--analyze-only` to verify file detection
2. Use `--check-system` to verify environment
3. Use `--validate-only` to check existing data
4. Test with small sample files first
5. Check `logs/tsv_loader_debug.log` for details

## Troubleshooting

### Common Issues

1. **Memory errors with large files**: Use `--validate-in-snowflake` to skip file-based QC
2. **Slow processing**: Increase `--max-workers` or use parallel processing
3. **Connection timeouts**: Check Snowflake warehouse size and network connectivity
4. **Date validation failures**: Verify date format in config matches your files
5. **COPY taking hours for large files**: 
   - Check warehouse size (use MEDIUM or LARGE for files >100MB)
   - Ensure ON_ERROR is set to ABORT_STATEMENT (not CONTINUE)
   - Run `check_stage_and_performance.py` to diagnose issues
   - Files >100MB will automatically use async execution

### Debug Mode

All runs automatically create detailed debug logs in `logs/tsv_loader_debug.log`:

```bash
tail -f logs/tsv_loader_debug.log
```

## Contributing

See CONTRIBUTING.md for development guidelines.

## License

[Your License Here]