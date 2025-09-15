# Snowflake ETL Pipeline

Enterprise-grade ETL pipeline for processing large TSV and CSV files into Snowflake with comprehensive data quality validation, parallel processing, and monitoring capabilities.

**🚀 Version 3.5.0**: Full CSV support with automatic format detection - process both TSV and CSV files seamlessly!

## Features

- **Multi-Format Support**: Process TSV, CSV (comma-separated values), and custom-delimited files with automatic format detection
- **High Performance**: Process 50GB+ data files with streaming and parallel processing
- **Data Quality**: Comprehensive validation including date completeness, duplicates detection, and schema validation
- **Reliability**: Automatic retry, error recovery, and transaction management
- **Monitoring**: Real-time progress tracking with format indicators, detailed logging, and job management
- **Flexibility**: Multiple validation modes, configurable processing options, custom delimiters
- **Cross-Environment Support**: Compress files for manual transfer across restricted environments
- **Pre-Compressed File Support**: Load .tsv.gz and .csv.gz files directly without recompression
- **Architecture**: Clean dependency injection design for testability and maintainability

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Command Line Interface](#command-line-interface)
  - [Interactive Menu](#interactive-menu)
  - [Python API](#python-api)
- [Operations](#operations)
- [Performance](#performance)
- [Architecture](#architecture)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## Installation

### Prerequisites

- Python 3.8 or higher
- Access to a Snowflake account
- Sufficient disk space for file compression (2x largest file size)
- Linux/macOS (Windows via WSL)

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourorg/snowflake-etl-pipeline.git
cd snowflake-etl-pipeline

# Install in production mode
pip install .

# Or install in development mode with all dependencies
pip install -e .[dev]
```

### Install via pip (when published)

```bash
pip install snowflake-etl-pipeline
```

### Running Without Virtual Environment

If your system already has Python and required packages installed, you can skip the automatic virtual environment setup:

```bash
# Skip virtual environment setup
./snowflake_etl.sh --no-venv

# Skip package installation entirely
./snowflake_etl.sh --skip-install

# Both flags can be combined
./snowflake_etl.sh --no-venv --skip-install

# Or set via environment variables
export SKIP_VENV=true
export SKIP_INSTALL=true
./snowflake_etl.sh
```

This is useful for:
- Docker containers with pre-installed dependencies
- CI/CD pipelines with managed environments
- Systems where package installation is restricted
- Environments with custom Python configurations

## Quick Start

### 1. Create Configuration File

Create a `config.json` file with your Snowflake credentials and file mappings:

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
      "file_pattern": "sales_{month}.csv",
      "table_name": "SALES_DATA",
      "file_format": "CSV",
      "delimiter": ",",
      "date_column": "sale_date",
      "expected_columns": ["sale_date", "product_id", "customer_id", "amount"],
      "duplicate_key_columns": ["sale_date", "product_id"]
    },
    {
      "file_pattern": "inventory_{date_range}.tsv",
      "table_name": "INVENTORY_DATA",
      "file_format": "TSV",
      "delimiter": "\t",
      "date_column": "inventory_date",
      "expected_columns": ["inventory_date", "warehouse_id", "product_id", "quantity"]
    }
  ]
}
```

### 2. Load Data

```bash
# Load data files for a specific month (auto-detects CSV/TSV)
snowflake-etl --config config.json load \
  --base-path /path/to/data/files \
  --month 2024-01

# Load specific CSV file
snowflake-etl --config config.json load \
  --file /data/sales_2024-01.csv

# Load compressed files directly
snowflake-etl --config config.json load \
  --file /data/inventory.tsv.gz
```

### 3. Validate Data

```bash
# Validate loaded data
snowflake-etl --config config.json validate \
  --table TARGET_TABLE \
  --month 2024-01
```

## Configuration

### Configuration Structure

The pipeline uses JSON configuration files with the following structure:

```json
{
  "snowflake": {
    "account": "account_identifier",
    "user": "username",
    "password": "password",
    "warehouse": "warehouse_name",
    "database": "database_name",
    "schema": "schema_name",
    "role": "role_name"
  },
  "files": [
    {
      "file_pattern": "pattern_{date_range}.csv",
      "table_name": "SNOWFLAKE_TABLE_NAME",
      "file_format": "CSV",         // Optional: AUTO (default), CSV, TSV
      "delimiter": ",",              // Optional: auto-detected if not specified
      "quote_char": "\"",            // Optional: quote character for fields
      "date_column": "date_column_name",
      "expected_columns": ["col1", "col2", "..."],
      "duplicate_key_columns": ["key1", "key2"]
    }
  ]
}
```

### File Pattern Formats

- `{date_range}`: Matches YYYYMMDD-YYYYMMDD format
- `{month}`: Matches YYYY-MM format

### Supported File Formats

The pipeline automatically detects and processes multiple file formats:

- **CSV Files** (`.csv`, `.csv.gz`): Comma-separated values with optional quoted fields
- **TSV Files** (`.tsv`, `.tsv.gz`): Tab-separated values  
- **Custom Delimited** (`.txt`): Pipe, semicolon, or other delimiters
- **Compressed Files**: Native support for gzip compressed files

Format detection is automatic based on:
1. File extension (`.csv` → CSV, `.tsv` → TSV)
2. Content analysis for unknown extensions
3. Explicit configuration via `file_format` field

### Environment Variables

You can override configuration using environment variables:

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
```

## Usage

### Command Line Interface

The pipeline provides a unified CLI with subcommands for different operations:

#### Load Operation

Load data files into Snowflake (supports CSV, TSV, and compressed files):

```bash
# Auto-detect format and load files for a month
snowflake-etl --config config.json load \
  --base-path /data/files \
  --month 2024-01 \
  --max-workers 8 \
  --validate-in-snowflake

# Load specific CSV file
snowflake-etl --config config.json load \
  --file /data/sales_2024.csv

# Load pre-compressed files directly
snowflake-etl --config config.json load \
  --file /data/compressed/inventory.tsv.gz

# Options:
#   --base-path PATH         Root directory containing data files (required)
#   --file PATH             Load specific file (.csv, .tsv, .gz)
#   --month YYYY-MM         Process files for specific month
#   --skip-qc               Skip file-based quality checks
#   --validate-in-snowflake Validate in Snowflake after loading
#   --validate-only         Only validate existing data (no loading)
#   --max-workers N         Number of parallel workers (default: auto)
```

#### Compress Operation (Standalone)

Compress data files for cross-environment transfer without Snowflake upload:

```bash
# Compress single file (CSV or TSV)
python compress_tsv.py data/sales.csv --level 9

# Compress multiple files to directory
python compress_tsv.py data/*.csv data/*.tsv --output-dir compressed/ --level 7

# Compress with specific output path
python compress_tsv.py data/inventory.csv -o /tmp/inventory.csv.gz

# Options:
#   -o, --output PATH       Output file path (single file only)
#   -d, --output-dir DIR    Output directory for compressed files
#   -l, --level N           Compression level 1-9 (default: 6)
#   --no-progress           Disable progress display
#   -f, --force             Overwrite without prompting
```

#### Validate Operation

Validate data completeness and quality:

```bash
snowflake-etl --config config.json validate \
  --table TARGET_TABLE \
  --date-column recordDate \
  --month 2024-01 \
  --output validation_report.json

# Options:
#   --table TABLE           Table to validate (required)
#   --date-column COLUMN    Date column name
#   --month YYYY-MM        Month to validate
#   --start-date YYYY-MM-DD Start date for validation
#   --end-date YYYY-MM-DD   End date for validation
#   --output FILE           Save results to file
```

#### Delete Operation

Delete data for specific time periods:

```bash
snowflake-etl --config config.json delete \
  --table TARGET_TABLE \
  --month 2024-01 \
  --dry-run

# Options:
#   --table TABLE           Table to delete from (required)
#   --month YYYY-MM        Month to delete (required)
#   --dry-run              Show what would be deleted without executing
#   --yes                  Skip confirmation prompt
```

#### Duplicate Check

Check for duplicate records:

```bash
snowflake-etl --config config.json check-duplicates \
  --table TARGET_TABLE \
  --key-columns recordDate,assetId,fundId \
  --date-start 2024-01-01 \
  --date-end 2024-01-31

# Options:
#   --table TABLE           Table to check (required)
#   --key-columns COLS      Comma-separated key columns
#   --date-start DATE       Start date
#   --date-end DATE         End date
```

#### Generate Report

Generate comprehensive table reports:

```bash
snowflake-etl --config config.json report \
  --output-format json \
  --output-file report.json \
  --tables TABLE1,TABLE2

# Options:
#   --output-format FORMAT  Output format: json, csv, text
#   --output-file FILE      Save report to file
#   --tables TABLES         Comma-separated table names
```

#### Compare Files

Compare two TSV files:

```bash
snowflake-etl --config config.json compare \
  file1.tsv file2.tsv \
  --quick

# Options:
#   --quick                 Quick comparison (size and headers only)
```

### Interactive Menu

Launch the interactive menu system:

```bash
./snowflake_etl.sh
```

The menu provides:
- Quick Load options for common tasks
- Snowflake Operations (Load/Validate/Delete)
- File Tools (Analyze/Compare/Generate)
- Recovery & Fix tools
- Job Status monitoring
- Settings configuration

### Python API

Use the pipeline programmatically:

```python
from snowflake_etl.core.application_context import ApplicationContext
from snowflake_etl.operations.load_operation import LoadOperation

# Create context
with ApplicationContext(config_path="config.json") as context:
    # Create and execute load operation
    load_op = LoadOperation(context)
    result = load_op.execute(
        base_path="/data/tsv",
        month="2024-01",
        validate_in_snowflake=True
    )
    
    print(f"Loaded {result['total_rows']} rows")
    print(f"Status: {result['status']}")
```

## Operations

### Load Operation

The load operation processes TSV files through the following pipeline:

1. **File Discovery**: Find files matching patterns and date ranges
2. **Analysis**: Fast row counting and time estimation
3. **Quality Checks**: Validate data completeness and formats
4. **Compression**: Gzip compression for efficient transfer
5. **Upload**: Transfer to Snowflake internal stage
6. **COPY**: Execute COPY command with error handling
7. **Validation**: Optional post-load validation

### Validation Operation

Validation checks include:

- **Date Completeness**: Verify all expected dates are present
- **Gap Detection**: Identify missing date ranges
- **Row Distribution**: Analyze daily row counts for anomalies
- **Duplicate Detection**: Find duplicate records based on key columns
- **Schema Validation**: Verify column structure matches expectations

### Delete Operation

Safe deletion with:

- **Dry Run Mode**: Preview impact before execution
- **Transaction Management**: Atomic operations with rollback
- **Audit Logging**: Complete deletion history
- **Recovery Options**: Snowflake Time Travel for restoration

## CSV/TSV Processing Examples

### Processing CSV Files

```bash
# Load a month of CSV sales data
snowflake-etl --config config.json load \
  --base-path /data/sales \
  --month 2024-01

# Generate config from CSV files
snowflake-etl --config config.json config-generate \
  --files /data/*.csv \
  --output new_config.json

# Sample a CSV file to inspect structure
snowflake-etl --config config.json sample \
  --file /data/sales_2024.csv \
  --rows 100
```

### Mixed Format Processing

```bash
# Process directory with both CSV and TSV files
snowflake-etl --config mixed_config.json load \
  --base-path /data/mixed \
  --validate-in-snowflake

# Config for mixed formats:
{
  "files": [
    {
      "file_pattern": "sales_{month}.csv",
      "file_format": "CSV",
      "table_name": "SALES"
    },
    {
      "file_pattern": "inventory_{month}.tsv", 
      "file_format": "TSV",
      "table_name": "INVENTORY"
    }
  ]
}
```

### Custom Delimiter Files

```bash
# Process pipe-delimited files
snowflake-etl --config config.json load \
  --file /data/custom.txt

# Config for pipe delimiter:
{
  "files": [{
    "file_pattern": "data_{date_range}.txt",
    "delimiter": "|",
    "file_format": "CSV",
    "table_name": "CUSTOM_DATA"
  }]
}
```

## Performance

### Performance Characteristics

| Operation | Speed | Notes |
|-----------|-------|-------|
| Row Counting | ~500K rows/sec | Sampling-based estimation |
| Quality Checks | ~50K rows/sec | With date parsing |
| Compression | ~25MB/sec | Gzip level 6 |
| Upload | ~5MB/sec | Typical network |
| Snowflake COPY | ~100K rows/sec | Medium warehouse |

### Performance Optimization

#### For Large Files (>10GB)

```bash
# Skip file-based QC and validate in Snowflake
snowflake-etl --config config.json load \
  --base-path /data \
  --month 2024-01 \
  --skip-qc \
  --validate-in-snowflake
```

This reduces processing time from hours to minutes.

#### Parallel Processing

```bash
# Use multiple workers for quality checks
snowflake-etl --config config.json load \
  --base-path /data \
  --month 2024-01 \
  --max-workers 16
```

Worker allocation:
- 1-4 cores: Use all cores
- 5-8 cores: Use cores - 1
- 9-16 cores: Use 75% of cores
- 17+ cores: Use 50% (max 32)

#### Warehouse Sizing

- Small files (<100MB): X-Small warehouse
- Medium files (100MB-1GB): Small warehouse
- Large files (1-10GB): Medium warehouse
- Very large files (>10GB): Large warehouse

## Architecture

### Dependency Injection Architecture

The v3.0.0 architecture uses dependency injection instead of singletons:

```
ApplicationContext (DI Container)
├── ConfigManager (Configuration)
├── ConnectionManager (Database connections)
├── LogManager (Logging)
└── ProgressTracker (Progress bars)

Operations (Use ApplicationContext)
├── LoadOperation
├── ValidateOperation
├── DeleteOperation
├── DuplicateCheckOperation
├── CompareOperation
└── ReportOperation
```

### Key Components

- **ApplicationContext**: Central dependency injection container
- **Operations**: Self-contained operation classes with single responsibilities
- **Validators**: Data quality and schema validation
- **Core**: File analysis, progress tracking, base classes
- **Utils**: Configuration, logging, connection management

### Thread Safety

- Connections are thread-local (one per thread)
- Operations are thread-safe for different file sets
- Progress tracking handles parallel updates

## Development

### Setting Up Development Environment

```bash
# Clone repository
git clone https://github.com/yourorg/snowflake-etl-pipeline.git
cd snowflake-etl-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install in development mode
pip install -e .[dev]

# Run tests
pytest tests/

# Run with coverage
pytest --cov=snowflake_etl tests/

# Run linting
flake8 snowflake_etl/
mypy snowflake_etl/
black snowflake_etl/
```

### Running Tests

```bash
# Unit tests only
pytest tests/test_core_operations.py

# Integration tests
pytest tests/test_integration.py

# CLI tests
pytest tests/test_cli.py

# Run specific test
pytest tests/test_core_operations.py::TestLoadOperation::test_execute
```

### Project Structure

```
snowflake-etl-pipeline/
├── snowflake_etl/          # Main package
│   ├── __main__.py        # CLI entry point
│   ├── core/              # Core components
│   ├── operations/        # Operation implementations
│   ├── validators/        # Data validators
│   └── utils/            # Utilities
├── tests/                 # Test suite
├── lib/                   # Shell script libraries
├── config/               # Configuration examples
├── logs/                 # Log files
└── docs/                 # Documentation
```

## Troubleshooting

### File Format Issues

#### CSV/TSV Format Detection

**Problem**: File format not detected correctly

**Solution**: 
1. Ensure file has correct extension (`.csv` for CSV, `.tsv` for TSV)
2. Explicitly set format in config: `"file_format": "CSV"`
3. For custom delimiters, specify: `"delimiter": "|"`

#### Delimiter Conflicts

**Problem**: Data contains delimiter character

**Solution**: 
- For CSV files with commas in data, ensure proper quoting: `"field,with,comma"`
- Use TSV format if data contains many commas
- Consider pipe delimiter for complex data: `"delimiter": "|"`

#### Quote Character Issues

**Problem**: CSV fields not properly quoted

**Solution**: 
- Specify quote character in config: `"quote_char": "\""`
- Ensure fields with special characters are quoted
- Use `FIELD_OPTIONALLY_ENCLOSED_BY` in Snowflake

### Common Issues

#### Connection Timeout

**Problem**: COPY operations timeout after 5 minutes

**Solution**: The pipeline automatically uses async execution with keepalive for large files

#### Memory Issues with Large Files

**Problem**: Out of memory during quality checks

**Solution**: Use `--skip-qc --validate-in-snowflake` for files >10GB

#### Slow COPY Performance

**Problem**: COPY taking hours for large files

**Solution**: 
1. Ensure using `ON_ERROR='ABORT_STATEMENT'` (default)
2. Use appropriate warehouse size
3. Check for network throttling

#### Duplicate Files in Stage

**Problem**: "File already exists" errors

**Solution**: The pipeline automatically cleans old stage files

### Debug Mode

Enable debug logging:

```bash
snowflake-etl --config config.json --log-level DEBUG load \
  --base-path /data \
  --month 2024-01
```

Check logs in `logs/` directory:
- `snowflake_etl_debug.log`: Detailed debug information
- `tsv_loader_YYYYMMDD_HHMMSS.log`: Operation-specific logs

### Getting Help

```bash
# Main help
snowflake-etl --help

# Subcommand help
snowflake-etl load --help
snowflake-etl validate --help

# Version information
snowflake-etl --version
```

## License

MIT License - See LICENSE file for details

## Contributing

See CONTRIBUTING.md for guidelines on contributing to this project.

## Support

For issues and questions:
- Create an issue on GitHub
- Check existing issues for solutions
- Review logs in the `logs/` directory

## Version History

- **v3.0.0-rc2** - Production-ready release with complete refactoring, dependency injection, and streamlined codebase
- **v3.0.0-rc1** - Release candidate with all 5 phases complete
- **v2.x** - Legacy version with singleton pattern (deprecated)

See CHANGELOG.md for detailed version history.