# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance Snowflake ETL pipeline for processing large TSV files (up to 50GB) with built-in data quality checks, progress tracking, and parallel processing capabilities. The system emphasizes streaming processing for memory efficiency and uses Snowflake's native bulk loading features.

## Key Components

### Main Script
- **tsv_loader.py**: The primary ETL script that orchestrates file analysis, quality checks, compression, and Snowflake loading
- **run_loader.sh**: Bash wrapper script for convenient execution with color-coded output and prerequisite checking

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

#### SnowflakeLoader
- Manages Snowflake connection and executes PUT/COPY commands
- Handles file compression (gzip) before upload
- Uses internal staging (@~/) for efficient bulk loading
- Implements validation mode before actual data loading

#### ProgressTracker
- Real-time progress bars using tqdm (when available)
- Tracks files processed, rows processed, and provides ETA
- Progress bars write to stderr and are visible even in quiet mode
- Multiple simultaneous progress bars for parallel processing

## Development Commands

### Install Dependencies
```bash
# Core requirements
pip install snowflake-connector-python pandas numpy

# Optional but recommended
pip install tqdm psutil
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

# Using the bash wrapper (recommended)
./run_loader.sh --month 2024-01 --base-path ./data

# Parallel processing with quiet mode for cleaner output
./run_loader.sh --month 2024-01,2024-02,2024-03 --parallel 3 --quiet
```

### Debugging
```bash
# Logs are automatically created in logs/ directory
tail -f logs/tsv_loader_debug.log

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
- Compression: ~35 minutes
- Upload to Snowflake: ~3 hours
- Snowflake COPY: ~1.5 hours
- Total time: ~7-8 hours

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

## Testing Approach

Currently no automated tests. To test:
1. Use `--analyze-only` flag to verify file detection and time estimates
2. Use `--check-system` to verify environment capabilities
3. Test with small sample files before processing large datasets
4. Check logs/tsv_loader_debug.log for detailed execution trace