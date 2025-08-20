# Snowflake TSV Loader

A high-performance ETL pipeline for loading large TSV files (up to 50GB) into Snowflake with built-in data quality checks, progress tracking, and parallel processing capabilities.

## Features

- **Streaming Processing**: Memory-efficient processing of large files without loading them into memory
- **Parallel Processing**: Automatic CPU core detection and optimal worker allocation
- **Data Quality Checks**: 
  - File-based validation with date completeness checks
  - Snowflake-based validation for faster processing of large files
  - Schema validation and type inference
- **Progress Tracking**: Real-time progress bars with ETA calculations
- **Flexible Date Patterns**: Supports both date range (YYYYMMDD-YYYYMMDD) and month (YYYY-MM) formats
- **Batch Processing**: Process multiple months in parallel with comprehensive error handling
- **Performance Optimized**: Utilizes Snowflake's bulk loading features with compression
- **Config Generator**: Automatically generate configuration files from TSV files and Snowflake schemas
- **Direct File Processing**: Process specific TSV files directly without directory structure requirements

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

The pipeline offers three validation modes:

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

### Advanced Options

```bash
# Process with specific worker count
./run_loader.sh --month 2024-01 --max-workers 8

# Batch process all months found
./run_loader.sh --batch --continue-on-error

# Quiet mode for cleaner output
./run_loader.sh --batch --parallel 4 --quiet

# Dry run to preview actions
./run_loader.sh --batch --dry-run
```

## Performance

### Benchmarks (50GB file, ~500M rows, 16-core system)

| Operation | File-based QC | Snowflake Validation |
|-----------|--------------|---------------------|
| Row Counting | ~16 seconds | N/A |
| Quality Checks | ~2.5 hours | ~5-10 seconds |
| Compression | ~35 minutes | ~35 minutes |
| Upload | ~3 hours | ~3 hours |
| Snowflake COPY | ~1.5 hours | ~1.5 hours |
| **Total Time** | **~7-8 hours** | **~4.5 hours** |

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

## Logging

- **Console Output**: Normal operational messages
- **Debug Log**: Detailed execution trace in `logs/tsv_loader_debug.log`
- **Quiet Mode**: Suppress console output while keeping progress bars
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

### Debug Mode

All runs automatically create detailed debug logs in `logs/tsv_loader_debug.log`:

```bash
tail -f logs/tsv_loader_debug.log
```

## Contributing

See CONTRIBUTING.md for development guidelines.

## License

[Your License Here]